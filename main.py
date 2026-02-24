import argparse
import csv
import json
import logging
import math
import os
import re
import secrets
import signal
import sqlite3
import sys
import tempfile
import threading
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from flask import Flask, abort, redirect, render_template_string, request, send_file, session, url_for
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from werkzeug.security import check_password_hash, generate_password_hash


# ----------------------------
# Logging
# ----------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("sensor_network_collector")


# ----------------------------
# Signal K mapping helpers
# ----------------------------
SIGNALK_STANDARD_PATHS = {
    "TempOut": "environment.outside.temperature",
    "HumOut": "environment.outside.humidity",
    "Barometer": "environment.outside.pressure",
    "WindSpeed": "environment.wind.speedApparent",
    "WindDir": "environment.wind.angleApparent",
    "TempIn": "environment.inside.temperature",
    "HumIn": "environment.inside.humidity",
}


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


# ----------------------------
# Config helpers
# ----------------------------
def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def parse_boolish(value, default: bool = False) -> bool:
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off", ""):
        return False
    return bool(default)


def parse_cli_bool(value: str) -> bool:
    s = str(value).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    raise argparse.ArgumentTypeError("Expected true|false")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Threaded sensor-network collector (InfluxDB + Signal K + CSV + web gui)"
    )
    parser.add_argument("--config", default="config.json", help="Config file path (default: config.json)")

    # Homogeneous with vantage-publisher style:
    # --signalk true|false, --storage true|false|<path>, --influxdb true|false
    parser.add_argument("--signalk", type=parse_cli_bool, default=None, help="Enable/disable Signal K output")
    parser.add_argument(
        "--influxdb",
        type=parse_cli_bool,
        default=None,
        help="Enable/disable InfluxDB output",
    )
    parser.add_argument(
        "--storage",
        default=None,
        help="Enable/disable storage (true|false) or set storage root path",
    )
    parser.add_argument("--http", type=parse_cli_bool, default=None, help="Enable/disable web gui")
    parser.add_argument("--dry", action="store_true", help="Dry mode: no sink writes, log output only")
    return parser.parse_args()


def cfg_value(raw: dict, keys, env_name=None, default=None):
    for key in keys:
        if key in raw and raw.get(key) is not None:
            return raw.get(key)
    if env_name:
        env = os.getenv(env_name)
        if env is not None:
            return env
    return default


def cfg_required(raw: dict, keys, env_name):
    value = cfg_value(raw, keys, env_name=env_name, default=None)
    if value is None or value == "":
        raise RuntimeError(f"Missing required config key(s) {keys} (or env {env_name})")
    return value


def parse_storage_override(arg_value):
    if arg_value is None:
        return None, None
    s = str(arg_value).strip()
    l = s.lower()
    if l in ("1", "true", "yes", "y", "on"):
        return True, None
    if l in ("0", "false", "no", "n", "off"):
        return False, None
    return True, s


def load_config(path: str, args) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise RuntimeError("Config file must contain a JSON object")

    log_level = str(cfg_value(raw, ["logLevel", "log_level"], env_name="LOG_LEVEL", default="INFO")).upper()

    mqtt_broker = str(cfg_required(raw, ["mqttBroker", "mqtt_address"], "MQTT_ADDRESS"))
    mqtt_port = int(cfg_value(raw, ["mqttPort", "mqtt_port"], env_name="MQTT_PORT", default=1883))
    mqtt_user = cfg_value(raw, ["mqttUser", "mqtt_user"], env_name="MQTT_USER", default=None)
    mqtt_pass = cfg_value(raw, ["mqttPass", "mqtt_password"], env_name="MQTT_PASSWORD", default=None)
    mqtt_topic = str(cfg_value(raw, ["mqttTopic", "mqtt_topic"], env_name="MQTT_TOPIC", default="#"))

    skip_empty = parse_boolish(
        cfg_value(raw, ["skipEmptyFields", "skip_empty_fields"], env_name="SKIP_EMPTY_FIELDS", default=1),
        True,
    )

    storage_cfg_enabled = parse_boolish(cfg_value(raw, ["storage"], env_name="STORAGE", default=False), False)
    storage_root = cfg_value(raw, ["pathStorage", "storage_root"], env_name="STORAGE_ROOT", default=None)

    signalk_cfg_enabled = parse_boolish(
        cfg_value(raw, ["signalk", "signalk_enabled"], env_name="SIGNALK_ENABLED", default=False),
        False,
    )

    influx_cfg_enabled = parse_boolish(
        cfg_value(raw, ["influxdb", "influxdb_enabled"], env_name="INFLUXDB_ENABLED", default=True),
        True,
    )

    http_cfg_enabled = parse_boolish(cfg_value(raw, ["httpEnabled"], env_name="HTTP_ENABLED", default=False), False)

    storage_override_enabled, storage_override_root = parse_storage_override(args.storage)

    enable_signalk = signalk_cfg_enabled if args.signalk is None else bool(args.signalk)
    enable_influx = influx_cfg_enabled if args.influxdb is None else bool(args.influxdb)
    enable_http = http_cfg_enabled if args.http is None else bool(args.http)
    enable_storage = storage_cfg_enabled if storage_override_enabled is None else storage_override_enabled

    if storage_override_root is not None:
        storage_root = storage_override_root
        enable_storage = True

    if enable_storage and not storage_root:
        raise RuntimeError("Storage is enabled but pathStorage/storage_root is not configured")

    cfg = {
        "log_level": log_level,
        "mqtt_broker": mqtt_broker,
        "mqtt_port": mqtt_port,
        "mqtt_user": mqtt_user,
        "mqtt_pass": mqtt_pass,
        "mqtt_topic": mqtt_topic,
        "skip_empty_fields": skip_empty,
        "dry": bool(args.dry),
        "enable_signalk": enable_signalk,
        "enable_influx": enable_influx,
        "enable_storage": enable_storage,
        "enable_http": enable_http,
        "storage_root": str(storage_root) if storage_root else None,
        "influx_measurement": str(
            cfg_value(raw, ["influxMeasurement", "influx_measurement"], env_name="INFLUX_MEASUREMENT", default="mqtt_data")
        ),
        "signalk_server_url": str(
            cfg_value(raw, ["signalkServerUrl", "signalk_server_url"], env_name="SIGNALK_SERVER_URL", default="")
        ).strip(),
        "signalk_token": str(cfg_value(raw, ["signalkToken", "signalk_token"], env_name="SIGNALK_TOKEN", default="") or ""),
        "signalk_context_prefix": str(
            cfg_value(
                raw,
                ["signalkContextPrefix", "signalk_context_prefix", "signalkContext"],
                env_name="SIGNALK_CONTEXT_PREFIX",
                default="meteo",
            )
        ).strip("."),
        "signalk_source_label": str(
            cfg_value(raw, ["signalkSourceLabel", "signalk_source_label"], env_name="SIGNALK_SOURCE_LABEL", default="sensor-network-collector")
        ),
        "http_host": str(cfg_value(raw, ["httpHost"], env_name="HTTP_HOST", default="0.0.0.0")),
        "http_port": int(cfg_value(raw, ["httpPort"], env_name="HTTP_PORT", default=8080)),
        "auth_db_path": str(
            cfg_value(raw, ["authDbPath"], env_name="AUTH_DB_PATH", default="")
            or (str(Path(storage_root) / "collector_auth.sqlite") if storage_root else "collector_auth.sqlite")
        ),
        "web_session_secret": str(cfg_value(raw, ["webSessionSecret"], env_name="WEB_SESSION_SECRET", default="") or ""),
        "admin_user": str(cfg_value(raw, ["adminUser"], env_name="ADMIN_USER", default="admin") or "admin"),
        "admin_password": str(
            cfg_value(raw, ["adminPassword"], env_name="ADMIN_PASSWORD", default="admin") or "admin"
        ),
    }

    raw_map = cfg_value(raw, ["signalkPathMap", "signalk_path_map"], env_name="SIGNALK_PATH_MAP", default={})
    if isinstance(raw_map, str):
        try:
            raw_map = json.loads(raw_map)
        except json.JSONDecodeError:
            logger.warning("Invalid SIGNALK_PATH_MAP JSON, using empty map")
            raw_map = {}
    cfg["signalk_path_map"] = {
        str(k): str(v) for k, v in (raw_map.items() if isinstance(raw_map, dict) else []) if v
    }

    if cfg["enable_signalk"] and not cfg["signalk_server_url"]:
        raise RuntimeError("Signal K enabled but signalkServerUrl/signalk_server_url is empty")

    if cfg["enable_influx"]:
        cfg["influxdb_url"] = str(cfg_required(raw, ["influxdbUrl", "influxdb_url"], "INFLUXDB_URL"))
        cfg["influxdb_token"] = str(cfg_required(raw, ["influxdbToken", "influxdb_token"], "INFLUXDB_TOKEN"))
        cfg["influxdb_org"] = str(cfg_required(raw, ["influxdbOrg", "influxdb_org"], "INFLUXDB_ORG"))
        cfg["influxdb_bucket"] = str(cfg_required(raw, ["influxdbBucket", "influxdb_bucket"], "INFLUXDB_BUCKET"))

    if not cfg["enable_influx"] and not cfg["enable_signalk"] and not cfg["enable_storage"]:
        raise RuntimeError("No outputs enabled: enable at least one of influxdb, signalk, storage")

    return cfg


# ----------------------------
# Data parsing helpers
# ----------------------------
def parse_iso8601_or_none(value: str):
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def utc_now():
    return datetime.now(timezone.utc)


def sanitize_signalk_key(key: str) -> str:
    out = []
    for ch in str(key):
        if ch.isalnum() or ch in ("_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out) or "unknown"


def topic_to_context(topic: str, prefix: str) -> str:
    parts = [sanitize_signalk_key(p) for p in str(topic).split("/") if p]
    suffix = ".".join(parts) if parts else "unknown"
    head = str(prefix or "meteo").strip(".")
    return f"{head}.{suffix}" if head else suffix


def convert_signalk_value(key: str, value):
    if value is None:
        return None
    if key in ("TempOut", "TempIn"):
        return float(value) + 273.15
    if key == "Barometer":
        return float(value) * 100.0
    if key in ("HumOut", "HumIn"):
        return float(value) / 100.0
    if key == "WindDir":
        return math.radians(float(value))
    return value


def extract_position(data: dict, tags: dict):
    position = data.get("position")
    if isinstance(position, dict):
        lat = position.get("latitude")
        lon = position.get("longitude")
        if isinstance(lat, (int, float)) and isinstance(lon, (int, float)):
            return float(lat), float(lon)

    lat_tag = tags.get("latitude")
    lon_tag = tags.get("longitude")
    try:
        if lat_tag is not None and lon_tag is not None:
            return float(lat_tag), float(lon_tag)
    except Exception:
        pass
    return None


def flatten_for_csv(data: dict) -> dict:
    flat = {}
    for k, v in data.items():
        if v is None:
            continue
        if isinstance(v, (bool, int, float, str)):
            flat[k] = v
        else:
            flat[k] = json.dumps(v, separators=(",", ":"), ensure_ascii=False)
    return flat


def extract_payload_fields(payload):
    tags = {}
    event_time_value = None
    data = payload

    if payload.get("type") == "Feature" and isinstance(payload.get("properties"), dict):
        data = payload["properties"]
        geometry = payload.get("geometry")
        if isinstance(geometry, dict):
            coordinates = geometry.get("coordinates")
            if (
                isinstance(coordinates, (list, tuple))
                and len(coordinates) >= 2
                and isinstance(coordinates[0], (int, float))
                and isinstance(coordinates[1], (int, float))
            ):
                tags["longitude"] = str(coordinates[0])
                tags["latitude"] = str(coordinates[1])

        for key in ("uuid", "name"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                tags[key] = value.strip()

    if not isinstance(data, dict):
        return None, tags, event_time_value

    event_time_value = data.get("Datetime") or data.get("DatetimeWS")
    return data, tags, event_time_value


def resolve_instrument_uuid(topic: str, data: dict, tags: dict) -> str:
    for key in ("uuid", "UUID", "station_uuid"):
        value = data.get(key) if isinstance(data, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()

    tag_uuid = tags.get("uuid")
    if isinstance(tag_uuid, str) and tag_uuid.strip():
        return tag_uuid.strip()

    return topic.replace("/", "_") if topic else "unknown"


# ----------------------------
# Signal K / CSV sinks
# ----------------------------
class SignalKWebsocketPublisher:
    """Best-effort Signal K stream publisher over websocket."""

    def __init__(self, server_url: str, token: str = "", timeout: float = 10.0):
        self.server_url = str(server_url).strip()
        self.token = str(token or "").strip()
        self.timeout = float(timeout)
        self._ws = None

    def _build_url(self) -> str:
        if not self.token:
            return self.server_url
        sep = "&" if "?" in self.server_url else "?"
        return f"{self.server_url}{sep}token={self.token}"

    def _connect(self):
        if self._ws is not None:
            return
        try:
            from websocket import create_connection  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "Missing dependency websocket-client. Install with: python3 -m pip install websocket-client"
            ) from e
        self._ws = create_connection(self._build_url(), timeout=self.timeout)

    def publish(self, packet_json: str):
        try:
            self._connect()
            self._ws.send(packet_json)
        except Exception:
            self.close()
            raise

    def close(self):
        if self._ws is None:
            return
        try:
            self._ws.close()
        except Exception:
            pass
        self._ws = None


class CSVHourlyStorage:
    def __init__(self, root_path: str):
        self.root = Path(root_path)

    def _file_path(self, instrument_uuid: str, dt: datetime) -> Path:
        day_dir = self.root / instrument_uuid / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
        name = f"{instrument_uuid}_{dt.strftime('%Y%m%d')}Z{dt.strftime('%H')}00.csv"
        return day_dir / name

    def _read_header(self, csv_path: Path):
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return []
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            return next(reader, [])

    def _ensure_schema(self, csv_path: Path, row_keys):
        existing = self._read_header(csv_path)
        if not existing:
            return list(row_keys)

        new_cols = [c for c in row_keys if c not in existing]
        if not new_cols:
            return existing

        merged = existing + new_cols
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged)
            writer.writeheader()
            for old in rows:
                writer.writerow({k: old.get(k, "") for k in merged})

        return merged

    def write(self, instrument_uuid: str, dt: datetime, row: dict):
        csv_path = self._file_path(instrument_uuid, dt)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        ordered_row = dict(sorted(row.items(), key=lambda kv: kv[0]))
        fieldnames = self._ensure_schema(csv_path, ordered_row.keys())

        has_data = csv_path.exists() and csv_path.stat().st_size > 0
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not has_data:
                writer.writeheader()
            writer.writerow({k: ordered_row.get(k, "") for k in fieldnames})
        return str(csv_path)


def build_signalk_delta(topic: str, data: dict, tags: dict, dt: datetime, cfg: dict):
    context = topic_to_context(topic, cfg.get("signalk_context_prefix", "meteo"))
    path_map = cfg.get("signalk_path_map", {})

    values = []

    pos = extract_position(data, tags)
    if pos is not None:
        lat, lon = pos
        values.append(
            {
                "path": "navigation.position",
                "value": {"latitude": float(lat), "longitude": float(lon)},
            }
        )

    for key, raw in data.items():
        if key in (
            "position",
            "name",
            "uuid",
            "UUID",
            "Datetime",
            "DatetimeWS",
            "type",
            "geometry",
            "properties",
        ):
            continue

        mapped = path_map.get(key)
        path = str(mapped).strip() if mapped else SIGNALK_STANDARD_PATHS.get(key, f"environment.{sanitize_signalk_key(key)}")

        try:
            value = convert_signalk_value(key, raw)
        except Exception:
            value = raw

        if value is None or isinstance(value, (dict, list)):
            continue

        values.append({"path": path, "value": value})

    if not values:
        return None

    return {
        "context": context,
        "updates": [
            {
                "timestamp": dt.isoformat().replace("+00:00", "Z"),
                "$source": cfg.get("signalk_source_label", "sensor-network-collector"),
                "values": values,
            }
        ],
    }


# ----------------------------
# Web GUI auth/policy store
# ----------------------------
class AccessStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        con = sqlite3.connect(self.db_path)
        con.row_factory = sqlite3.Row
        return con

    def _init_schema(self):
        with self._lock:
            with self._connect() as con:
                con.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS users (
                        username TEXT PRIMARY KEY,
                        password_hash TEXT NOT NULL,
                        email TEXT,
                        role TEXT NOT NULL DEFAULT 'user',
                        active INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS account_requests (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        email TEXT,
                        password_hash TEXT NOT NULL,
                        message TEXT,
                        status TEXT NOT NULL DEFAULT 'pending',
                        created_at TEXT NOT NULL,
                        reviewed_by TEXT
                    );

                    CREATE TABLE IF NOT EXISTS instrument_policies (
                        instrument_uuid TEXT PRIMARY KEY,
                        policy TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        updated_by TEXT
                    );

                    CREATE TABLE IF NOT EXISTS user_instruments (
                        username TEXT NOT NULL,
                        instrument_uuid TEXT NOT NULL,
                        PRIMARY KEY (username, instrument_uuid)
                    );
                    """
                )

    def ensure_admin(self, username: str, password: str):
        with self._lock:
            with self._connect() as con:
                row = con.execute("SELECT username FROM users WHERE username = ?", (username,)).fetchone()
                if row is None:
                    con.execute(
                        "INSERT INTO users(username,password_hash,email,role,active,created_at) VALUES(?,?,?,?,?,?)",
                        (username, generate_password_hash(password), "", "admin", 1, now_utc_iso()),
                    )
                    logger.info("Created default admin user '%s'", username)

    def get_user(self, username: str):
        with self._connect() as con:
            row = con.execute(
                "SELECT username,email,role,active,created_at FROM users WHERE username = ?",
                (username,),
            ).fetchone()
            return dict(row) if row else None

    def list_users(self):
        with self._connect() as con:
            rows = con.execute(
                "SELECT username,email,role,active,created_at FROM users ORDER BY username"
            ).fetchall()
            return [dict(r) for r in rows]

    def authenticate(self, username: str, password: str):
        with self._connect() as con:
            row = con.execute(
                "SELECT username,password_hash,email,role,active,created_at FROM users WHERE username = ?",
                (username,),
            ).fetchone()
        if row is None:
            return None
        if int(row["active"]) != 1:
            return None
        if not check_password_hash(row["password_hash"], password):
            return None
        return {
            "username": row["username"],
            "email": row["email"],
            "role": row["role"],
            "active": int(row["active"]),
            "created_at": row["created_at"],
        }

    def create_user(self, username: str, password: str, email: str, role: str = "user", active: int = 1):
        username = username.strip()
        if not username:
            return False, "Username is required"
        role = "admin" if role == "admin" else "user"
        with self._lock:
            try:
                with self._connect() as con:
                    con.execute(
                        "INSERT INTO users(username,password_hash,email,role,active,created_at) VALUES(?,?,?,?,?,?)",
                        (username, generate_password_hash(password), email.strip(), role, int(active), now_utc_iso()),
                    )
                return True, "User created"
            except sqlite3.IntegrityError:
                return False, "User already exists"

    def create_account_request(self, username: str, password: str, email: str, message: str):
        username = username.strip()
        if not username:
            return False, "Username is required"
        if not password:
            return False, "Password is required"

        with self._lock:
            with self._connect() as con:
                exists_user = con.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone()
                if exists_user:
                    return False, "Username already exists"
                con.execute(
                    "INSERT INTO account_requests(username,email,password_hash,message,status,created_at,reviewed_by) VALUES(?,?,?,?,?,?,?)",
                    (
                        username,
                        email.strip(),
                        generate_password_hash(password),
                        message.strip(),
                        "pending",
                        now_utc_iso(),
                        None,
                    ),
                )
        return True, "Request submitted"

    def list_account_requests(self, status=None):
        query = "SELECT id,username,email,message,status,created_at,reviewed_by FROM account_requests"
        params = ()
        if status:
            query += " WHERE status = ?"
            params = (status,)
        query += " ORDER BY created_at DESC"
        with self._connect() as con:
            rows = con.execute(query, params).fetchall()
            return [dict(r) for r in rows]

    def approve_request(self, request_id: int, admin_username: str):
        with self._lock:
            with self._connect() as con:
                req = con.execute(
                    "SELECT id,username,email,password_hash,status FROM account_requests WHERE id = ?",
                    (request_id,),
                ).fetchone()
                if req is None:
                    return False, "Request not found"
                if req["status"] != "pending":
                    return False, f"Request already {req['status']}"

                exists_user = con.execute("SELECT 1 FROM users WHERE username = ?", (req["username"],)).fetchone()
                if exists_user:
                    con.execute(
                        "UPDATE account_requests SET status = 'approved', reviewed_by = ? WHERE id = ?",
                        (admin_username, request_id),
                    )
                    return True, "Request marked approved (user already existed)"

                con.execute(
                    "INSERT INTO users(username,password_hash,email,role,active,created_at) VALUES(?,?,?,?,?,?)",
                    (req["username"], req["password_hash"], req["email"], "user", 1, now_utc_iso()),
                )
                con.execute(
                    "UPDATE account_requests SET status = 'approved', reviewed_by = ? WHERE id = ?",
                    (admin_username, request_id),
                )
                return True, "Request approved and user created"

    def reject_request(self, request_id: int, admin_username: str):
        with self._lock:
            with self._connect() as con:
                req = con.execute("SELECT status FROM account_requests WHERE id = ?", (request_id,)).fetchone()
                if req is None:
                    return False, "Request not found"
                if req["status"] != "pending":
                    return False, f"Request already {req['status']}"
                con.execute(
                    "UPDATE account_requests SET status = 'rejected', reviewed_by = ? WHERE id = ?",
                    (admin_username, request_id),
                )
                return True, "Request rejected"

    def set_policy(self, instrument_uuid: str, policy: str, updated_by: str):
        if policy not in ("open", "account", "restricted"):
            return False, "Invalid policy"
        instrument_uuid = instrument_uuid.strip()
        if not instrument_uuid:
            return False, "instrument UUID is required"

        with self._lock:
            with self._connect() as con:
                con.execute(
                    """
                    INSERT INTO instrument_policies(instrument_uuid, policy, updated_at, updated_by)
                    VALUES(?,?,?,?)
                    ON CONFLICT(instrument_uuid)
                    DO UPDATE SET policy=excluded.policy, updated_at=excluded.updated_at, updated_by=excluded.updated_by
                    """,
                    (instrument_uuid, policy, now_utc_iso(), updated_by),
                )
        return True, "Policy updated"

    def get_policy(self, instrument_uuid: str):
        with self._connect() as con:
            row = con.execute(
                "SELECT policy FROM instrument_policies WHERE instrument_uuid = ?",
                (instrument_uuid,),
            ).fetchone()
        return row["policy"] if row else "account"

    def list_policies(self, instrument_uuids):
        result = {}
        with self._connect() as con:
            rows = con.execute("SELECT instrument_uuid, policy FROM instrument_policies").fetchall()
            for row in rows:
                result[row["instrument_uuid"]] = row["policy"]
        for uid in instrument_uuids:
            result.setdefault(uid, "account")
        return result

    def set_user_instrument_access(self, username: str, instrument_uuid: str, allow: bool):
        username = username.strip()
        instrument_uuid = instrument_uuid.strip()
        if not username or not instrument_uuid:
            return False, "Username and instrument UUID are required"

        with self._lock:
            with self._connect() as con:
                user_exists = con.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone()
                if not user_exists:
                    return False, "User not found"

                if allow:
                    con.execute(
                        "INSERT OR IGNORE INTO user_instruments(username,instrument_uuid) VALUES(?,?)",
                        (username, instrument_uuid),
                    )
                else:
                    con.execute(
                        "DELETE FROM user_instruments WHERE username = ? AND instrument_uuid = ?",
                        (username, instrument_uuid),
                    )
        return True, "Access updated"

    def get_user_instruments(self, username: str):
        with self._connect() as con:
            rows = con.execute(
                "SELECT instrument_uuid FROM user_instruments WHERE username = ? ORDER BY instrument_uuid",
                (username,),
            ).fetchall()
            return [r["instrument_uuid"] for r in rows]

    def can_download(self, user, instrument_uuid: str):
        policy = self.get_policy(instrument_uuid)
        if policy == "open":
            return True
        if user is None:
            return False
        if user.get("role") == "admin":
            return True
        if policy == "account":
            return True
        if policy == "restricted":
            allowed = set(self.get_user_instruments(user["username"]))
            return instrument_uuid in allowed
        return False


def collect_instruments(storage_root: str):
    root = Path(storage_root)
    if not root.exists() or not root.is_dir():
        return []
    return sorted([p.name for p in root.iterdir() if p.is_dir()])


def parse_date_ymd(value: str):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def extract_date_from_name(name: str):
    # UUID_YYYYMMDDZHH00.csv
    m = re.search(r"_(\d{8})Z\d{4}\.csv$", name)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y%m%d").date()
    except ValueError:
        return None


def list_csv_files_for_instrument(storage_root: str, instrument_uuid: str, from_date=None, to_date=None):
    root = Path(storage_root) / instrument_uuid
    if not root.exists():
        return []

    files = []
    for path in root.rglob("*.csv"):
        date = extract_date_from_name(path.name)
        if from_date and (date is None or date < from_date):
            continue
        if to_date and (date is None or date > to_date):
            continue
        files.append(path)
    files.sort()
    return files


def make_zip_for_download(storage_root: str, instrument_uuids, from_date=None, to_date=None):
    tmp = tempfile.NamedTemporaryFile(prefix="collector_download_", suffix=".zip", delete=False)
    tmp_path = Path(tmp.name)
    tmp.close()

    count = 0
    with zipfile.ZipFile(tmp_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for uuid in instrument_uuids:
            files = list_csv_files_for_instrument(storage_root, uuid, from_date=from_date, to_date=to_date)
            for f in files:
                rel = f.relative_to(Path(storage_root))
                zf.write(f, arcname=str(rel))
                count += 1

    return tmp_path, count


def create_web_app(cfg: dict, access_store: AccessStore):
    app = Flask(__name__)
    app.secret_key = cfg["web_session_secret"] or secrets.token_hex(32)

    if not cfg["web_session_secret"]:
        logger.warning("webSessionSecret not set; using ephemeral secret (sessions reset on restart)")

    def current_user():
        username = session.get("username")
        if not username:
            return None
        return access_store.get_user(username)

    def require_login():
        user = current_user()
        if not user:
            return redirect(url_for("login", next=request.path))
        return user

    def require_admin():
        user = current_user()
        if not user:
            return redirect(url_for("login", next=request.path))
        if user.get("role") != "admin":
            abort(403)
        return user

    @app.route("/")
    def index():
        user = current_user()
        storage_root = cfg.get("storage_root")
        instruments = collect_instruments(storage_root) if storage_root else []
        policies = access_store.list_policies(instruments)

        visible = []
        for inst in instruments:
            if access_store.can_download(user, inst):
                visible.append(inst)
            elif policies.get(inst) == "open":
                visible.append(inst)

        return render_template_string(
            """
            <html><body>
            <h1>Sensor Network Collector - Data Portal</h1>
            {% if user %}
              <p>Logged in as <b>{{ user.username }}</b> ({{ user.role }}) - <a href="{{ url_for('logout') }}">Logout</a></p>
              {% if user.role == 'admin' %}<p><a href="{{ url_for('admin') }}">Admin panel</a></p>{% endif %}
            {% else %}
              <p><a href="{{ url_for('login') }}">Login</a> | <a href="{{ url_for('request_account') }}">Request account</a></p>
            {% endif %}

            <h2>Download data</h2>
            {% if not storage_root %}
              <p>Storage is not enabled/configured.</p>
            {% elif not instruments %}
              <p>No instrument folders found under {{ storage_root }}.</p>
            {% elif not visible %}
              <p>No instruments available with your current permissions.</p>
            {% else %}
              <form method="post" action="{{ url_for('download') }}">
                <p>Select one or more instruments:</p>
                {% for inst in visible %}
                  <label><input type="checkbox" name="instrument" value="{{ inst }}"> {{ inst }} (policy={{ policies[inst] }})</label><br/>
                {% endfor %}
                <p>Date filter (optional):</p>
                <label>From <input type="date" name="from_date"></label>
                <label>To <input type="date" name="to_date"></label>
                <p><button type="submit">Download ZIP</button></p>
              </form>
            {% endif %}
            </body></html>
            """,
            user=user,
            instruments=instruments,
            visible=visible,
            policies=policies,
            storage_root=storage_root,
        )

    @app.route("/login", methods=["GET", "POST"])
    def login():
        err = ""
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            password = request.form.get("password", "")
            user = access_store.authenticate(username, password)
            if user:
                session["username"] = user["username"]
                nxt = request.args.get("next") or url_for("index")
                return redirect(nxt)
            err = "Invalid credentials or inactive user"

        return render_template_string(
            """
            <html><body>
            <h1>Login</h1>
            {% if err %}<p style="color:red">{{ err }}</p>{% endif %}
            <form method="post">
              <label>Username <input name="username"></label><br/>
              <label>Password <input name="password" type="password"></label><br/>
              <button type="submit">Login</button>
            </form>
            <p><a href="{{ url_for('request_account') }}">Request account</a></p>
            <p><a href="{{ url_for('index') }}">Home</a></p>
            </body></html>
            """,
            err=err,
        )

    @app.route("/logout")
    def logout():
        session.pop("username", None)
        return redirect(url_for("index"))

    @app.route("/request-account", methods=["GET", "POST"])
    def request_account():
        msg = ""
        err = ""
        if request.method == "POST":
            username = request.form.get("username", "").strip()
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "")
            reason = request.form.get("reason", "")
            ok, text = access_store.create_account_request(username, password, email, reason)
            if ok:
                msg = text
            else:
                err = text

        return render_template_string(
            """
            <html><body>
            <h1>Request account</h1>
            {% if msg %}<p style="color:green">{{ msg }}</p>{% endif %}
            {% if err %}<p style="color:red">{{ err }}</p>{% endif %}
            <form method="post">
              <label>Username <input name="username"></label><br/>
              <label>Email <input name="email" type="email"></label><br/>
              <label>Password <input name="password" type="password"></label><br/>
              <label>Reason <textarea name="reason"></textarea></label><br/>
              <button type="submit">Submit request</button>
            </form>
            <p><a href="{{ url_for('index') }}">Home</a></p>
            </body></html>
            """,
            msg=msg,
            err=err,
        )

    @app.route("/download", methods=["POST"])
    def download():
        user = current_user()
        storage_root = cfg.get("storage_root")
        if not storage_root:
            abort(404)

        instruments = request.form.getlist("instrument")
        if not instruments:
            abort(400, "Select at least one instrument")

        allowed = [inst for inst in instruments if access_store.can_download(user, inst)]
        if not allowed:
            abort(403)

        from_date = parse_date_ymd(request.form.get("from_date", ""))
        to_date = parse_date_ymd(request.form.get("to_date", ""))
        if from_date and to_date and to_date < from_date:
            abort(400, "to_date must be >= from_date")

        zip_path, count = make_zip_for_download(storage_root, allowed, from_date=from_date, to_date=to_date)
        if count == 0:
            zip_path.unlink(missing_ok=True)
            abort(404, "No data files found for selected filters")

        return send_file(
            zip_path,
            as_attachment=True,
            download_name=f"collector_data_{datetime.now().strftime('%Y%m%dT%H%M%S')}.zip",
            mimetype="application/zip",
        )

    @app.route("/admin")
    def admin():
        admin_user = require_admin()
        if not isinstance(admin_user, dict):
            return admin_user

        storage_root = cfg.get("storage_root")
        instruments = collect_instruments(storage_root) if storage_root else []
        policies = access_store.list_policies(instruments)
        users = access_store.list_users()
        pending_requests = access_store.list_account_requests(status="pending")

        user_access = {u["username"]: access_store.get_user_instruments(u["username"]) for u in users}

        return render_template_string(
            """
            <html><body>
            <h1>Admin panel</h1>
            <p>Logged in as {{ admin_user.username }} - <a href="{{ url_for('index') }}">Home</a></p>

            <h2>Create user</h2>
            <form method="post" action="{{ url_for('admin_create_user') }}">
              <label>Username <input name="username"></label>
              <label>Email <input name="email"></label>
              <label>Password <input name="password" type="password"></label>
              <label>Role
                <select name="role">
                  <option value="user">user</option>
                  <option value="admin">admin</option>
                </select>
              </label>
              <button type="submit">Create</button>
            </form>

            <h2>Pending account requests</h2>
            {% if pending_requests %}
              {% for r in pending_requests %}
                <div style="border:1px solid #ccc; margin:6px; padding:6px;">
                  <b>#{{ r.id }} {{ r.username }}</b> ({{ r.email }})<br/>
                  {{ r.message }}<br/>
                  <form method="post" action="{{ url_for('admin_approve_request', request_id=r.id) }}" style="display:inline;">
                    <button type="submit">Approve</button>
                  </form>
                  <form method="post" action="{{ url_for('admin_reject_request', request_id=r.id) }}" style="display:inline;">
                    <button type="submit">Reject</button>
                  </form>
                </div>
              {% endfor %}
            {% else %}
              <p>No pending requests.</p>
            {% endif %}

            <h2>Instrument policies</h2>
            {% if instruments %}
              {% for inst in instruments %}
                <form method="post" action="{{ url_for('admin_set_policy') }}">
                  <input type="hidden" name="instrument_uuid" value="{{ inst }}">
                  <b>{{ inst }}</b>
                  <select name="policy">
                    <option value="open" {% if policies[inst]=='open' %}selected{% endif %}>open (free download)</option>
                    <option value="account" {% if policies[inst]=='account' %}selected{% endif %}>account (authenticated users)</option>
                    <option value="restricted" {% if policies[inst]=='restricted' %}selected{% endif %}>restricted (assigned users only)</option>
                  </select>
                  <button type="submit">Save</button>
                </form>
              {% endfor %}
            {% else %}
              <p>No instruments found in storage.</p>
            {% endif %}

            <h2>User access (for restricted policy)</h2>
            {% for u in users %}
              <div style="border:1px solid #ccc; margin:6px; padding:6px;">
                <b>{{ u.username }}</b> role={{ u.role }} active={{ u.active }}<br/>
                currently allowed: {{ user_access[u.username] }}
                <form method="post" action="{{ url_for('admin_set_user_access') }}">
                  <input type="hidden" name="username" value="{{ u.username }}">
                  <label>Instrument UUID <input name="instrument_uuid"></label>
                  <select name="allow">
                    <option value="1">allow</option>
                    <option value="0">revoke</option>
                  </select>
                  <button type="submit">Apply</button>
                </form>
              </div>
            {% endfor %}
            </body></html>
            """,
            admin_user=admin_user,
            instruments=instruments,
            policies=policies,
            users=users,
            pending_requests=pending_requests,
            user_access=user_access,
        )

    @app.route("/admin/create-user", methods=["POST"])
    def admin_create_user():
        admin_user = require_admin()
        if not isinstance(admin_user, dict):
            return admin_user

        username = request.form.get("username", "")
        email = request.form.get("email", "")
        password = request.form.get("password", "")
        role = request.form.get("role", "user")

        if not password:
            abort(400, "Password required")

        ok, msg = access_store.create_user(username, password, email, role=role)
        if not ok:
            abort(400, msg)
        return redirect(url_for("admin"))

    @app.route("/admin/requests/<int:request_id>/approve", methods=["POST"])
    def admin_approve_request(request_id: int):
        admin_user = require_admin()
        if not isinstance(admin_user, dict):
            return admin_user
        ok, msg = access_store.approve_request(request_id, admin_user["username"])
        if not ok:
            abort(400, msg)
        return redirect(url_for("admin"))

    @app.route("/admin/requests/<int:request_id>/reject", methods=["POST"])
    def admin_reject_request(request_id: int):
        admin_user = require_admin()
        if not isinstance(admin_user, dict):
            return admin_user
        ok, msg = access_store.reject_request(request_id, admin_user["username"])
        if not ok:
            abort(400, msg)
        return redirect(url_for("admin"))

    @app.route("/admin/policy", methods=["POST"])
    def admin_set_policy():
        admin_user = require_admin()
        if not isinstance(admin_user, dict):
            return admin_user

        instrument_uuid = request.form.get("instrument_uuid", "")
        policy = request.form.get("policy", "")
        ok, msg = access_store.set_policy(instrument_uuid, policy, admin_user["username"])
        if not ok:
            abort(400, msg)
        return redirect(url_for("admin"))

    @app.route("/admin/user-access", methods=["POST"])
    def admin_set_user_access():
        admin_user = require_admin()
        if not isinstance(admin_user, dict):
            return admin_user

        username = request.form.get("username", "")
        instrument_uuid = request.form.get("instrument_uuid", "")
        allow = parse_boolish(request.form.get("allow", "1"), True)

        ok, msg = access_store.set_user_instrument_access(username, instrument_uuid, allow)
        if not ok:
            abort(400, msg)
        return redirect(url_for("admin"))

    return app


# ----------------------------
# Runtime state
# ----------------------------
runtime = {
    "config": {},
    "influx_client": None,
    "write_api": None,
    "signalk_client": None,
    "csv_storage": None,
    "http_thread": None,
    "access_store": None,
}


# ----------------------------
# MQTT callbacks
# ----------------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    cfg = runtime["config"]
    if reason_code == 0:
        logger.info("Connected to MQTT broker")
        client.subscribe(cfg["mqtt_topic"])
        logger.info("Subscribed to topic: %s", cfg["mqtt_topic"])
    else:
        logger.error("MQTT connection failed. reason_code=%s", reason_code)


def on_message(client, userdata, message):
    cfg = runtime["config"]
    topic = message.topic
    raw = message.payload

    try:
        payload = json.loads(raw.decode("utf-8", errors="strict"))
    except Exception as e:
        logger.warning("Invalid JSON on topic=%s: %s | raw=%r", topic, e, raw[:500])
        return

    if not isinstance(payload, dict):
        logger.warning("JSON payload is not an object on topic=%s: %r", topic, payload)
        return

    source_data, extracted_tags, event_time_value = extract_payload_fields(payload)
    if not isinstance(source_data, dict):
        logger.warning("Unsupported payload shape on topic=%s: %r", topic, payload)
        return

    dt = None
    if event_time_value:
        dt = parse_iso8601_or_none(event_time_value)
        if dt is None:
            logger.debug("Invalid Datetime=%r on topic=%s; using utc now", event_time_value, topic)
    dt = dt or utc_now()

    flat_fields = flatten_for_csv(source_data)

    if cfg["skip_empty_fields"] and not flat_fields:
        logger.debug("No writable fields on topic=%s, skipping", topic)
        return

    instrument_uuid = resolve_instrument_uuid(topic, source_data, extracted_tags)

    # InfluxDB sink
    if cfg["enable_influx"]:
        influx_record = {
            "measurement": cfg["influx_measurement"],
            "tags": {"topic": topic, "uuid": instrument_uuid, **extracted_tags},
            "fields": flat_fields,
            "time": dt,
        }

        if cfg["dry"]:
            logger.info("DRY INFLUX topic=%s record=%s", topic, influx_record)
        else:
            write_api = runtime["write_api"]
            try:
                write_api.write(bucket=cfg["influxdb_bucket"], org=cfg["influxdb_org"], record=influx_record)
                logger.info("Influx write topic=%s fields=%d time=%s", topic, len(flat_fields), dt.isoformat())
            except Exception as e:
                logger.exception("Influx write failed for topic=%s: %s", topic, e)

    # CSV storage sink
    if cfg["enable_storage"]:
        storage_row = {
            "timestamp": dt.isoformat().replace("+00:00", "Z"),
            "topic": topic,
            "uuid": instrument_uuid,
            **flat_fields,
        }

        if cfg["dry"]:
            csv_path = runtime["csv_storage"]._file_path(instrument_uuid, dt)
            logger.info("DRY STORAGE file=%s row=%s", csv_path, storage_row)
        else:
            try:
                csv_path = runtime["csv_storage"].write(instrument_uuid, dt, storage_row)
                logger.info("Storage write topic=%s file=%s", topic, csv_path)
            except Exception as e:
                logger.exception("Storage write failed topic=%s: %s", topic, e)

    # Signal K sink
    if cfg["enable_signalk"]:
        delta = build_signalk_delta(topic, source_data, extracted_tags, dt, cfg)
        if delta is None:
            logger.debug("No Signal K values built for topic=%s", topic)
            return

        if cfg["dry"]:
            logger.info("DRY SIGNALK topic=%s delta=%s", topic, json.dumps(delta, separators=(",", ":")))
        else:
            signalk_client = runtime["signalk_client"]
            try:
                signalk_client.publish(json.dumps(delta, separators=(",", ":")))
                logger.info(
                    "Signal K write topic=%s context=%s values=%d",
                    topic,
                    delta["context"],
                    len(delta["updates"][0]["values"]),
                )
            except Exception as e:
                logger.exception("Signal K publish failed for topic=%s: %s", topic, e)


# ----------------------------
# Graceful shutdown
# ----------------------------
def shutdown(signum, frame):
    logger.info("Shutting down (signal=%s)...", signum)

    influx_client = runtime.get("influx_client")
    if influx_client is not None:
        try:
            influx_client.close()
        except Exception:
            pass

    signalk_client = runtime.get("signalk_client")
    if signalk_client is not None:
        try:
            signalk_client.close()
        except Exception:
            pass

    sys.exit(0)


def start_web_gui(cfg: dict):
    access_store = AccessStore(cfg["auth_db_path"])
    access_store.ensure_admin(cfg["admin_user"], cfg["admin_password"])

    app = create_web_app(cfg, access_store)

    def run_http():
        app.run(host=cfg["http_host"], port=cfg["http_port"], debug=False, use_reloader=False, threaded=True)

    thread = threading.Thread(target=run_http, name="collector-web-gui", daemon=True)
    thread.start()

    runtime["access_store"] = access_store
    runtime["http_thread"] = thread

    logger.info("Web GUI enabled on http://%s:%s", cfg["http_host"], cfg["http_port"])


def main():
    args = parse_args()
    cfg = load_config(args.config, args)

    logging.getLogger().setLevel(getattr(logging, cfg["log_level"], logging.INFO))
    runtime["config"] = cfg

    if cfg["enable_influx"] and not cfg["dry"]:
        influx_client = InfluxDBClient(
            url=cfg["influxdb_url"],
            token=cfg["influxdb_token"],
            org=cfg["influxdb_org"],
        )
        runtime["influx_client"] = influx_client
        runtime["write_api"] = influx_client.write_api(write_options=SYNCHRONOUS)
        logger.info("InfluxDB output enabled")
    elif cfg["enable_influx"]:
        logger.info("InfluxDB output enabled in dry mode")

    if cfg["enable_storage"]:
        runtime["csv_storage"] = CSVHourlyStorage(cfg["storage_root"])
        if cfg["dry"]:
            logger.info("CSV storage enabled in dry mode (root=%s)", cfg["storage_root"])
        else:
            logger.info("CSV storage enabled (root=%s)", cfg["storage_root"])

    if cfg["enable_signalk"]:
        if cfg["dry"]:
            logger.info("Signal K output enabled in dry mode")
        else:
            runtime["signalk_client"] = SignalKWebsocketPublisher(
                server_url=cfg["signalk_server_url"],
                token=cfg["signalk_token"],
            )
            logger.info("Signal K output enabled (%s)", cfg["signalk_server_url"])

    if cfg["enable_http"]:
        start_web_gui(cfg)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    if cfg["mqtt_user"]:
        mqtt_client.username_pw_set(username=cfg["mqtt_user"], password=cfg["mqtt_pass"])

    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

    logger.info("Connecting to MQTT %s:%d", cfg["mqtt_broker"], cfg["mqtt_port"])
    mqtt_client.connect(cfg["mqtt_broker"], cfg["mqtt_port"])
    mqtt_client.loop_forever()


if __name__ == "__main__":
    main()
