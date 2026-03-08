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
import uuid
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from flask import Flask, abort, redirect, render_template_string, request, send_file, session, url_for
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from werkzeug.security import check_password_hash, generate_password_hash

from signalk_access import SignalKAccessManager


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

INFLUX_FIELD_CONFLICT_RE = re.compile(
    r'input field\s+\\?"(?P<field>[^"\\]+)\\?"\s+.*?\s+is type\s+(?P<input_type>[a-zA-Z]+),\s+already exists as type\s+(?P<existing_type>[a-zA-Z]+)',
    re.IGNORECASE,
)


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
        "signalk_access_client_id": str(
            cfg_value(raw, ["signalkClientId", "signalk_client_id"], env_name="SIGNALK_CLIENT_ID", default="")
            or str(uuid.uuid4())
        ),
        "signalk_access_description": str(
            cfg_value(
                raw,
                ["signalkAccessDescription", "signalk_access_description"],
                env_name="SIGNALK_ACCESS_DESCRIPTION",
                default="sensor-network-collector",
            )
        ),
        "signalk_access_poll_sec": int(
            cfg_value(raw, ["signalkAccessPollSec", "signalk_access_poll_sec"], env_name="SIGNALK_ACCESS_POLL_SEC", default=10)
        ),
        "signalk_access_timeout_sec": int(
            cfg_value(raw, ["signalkAccessTimeoutSec", "signalk_access_timeout_sec"], env_name="SIGNALK_ACCESS_TIMEOUT_SEC", default=10)
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

    parsed_map = {}
    if isinstance(raw_map, dict):
        for k, v in raw_map.items():
            key = str(k)
            if isinstance(v, str):
                path = v.strip()
                if path:
                    parsed_map[key] = {"path": path}
                continue

            if isinstance(v, dict):
                path = str(v.get("path", "")).strip()
                if not path:
                    logger.warning("Ignoring signalkPathMap entry '%s' without a valid 'path'", key)
                    continue
                meta = v.get("meta")
                if meta is None and "meta:" in v:
                    meta = v.get("meta:")
                entry = {"path": path}
                if meta is not None:
                    entry["meta"] = meta
                parsed_map[key] = entry
                continue

            logger.warning("Ignoring signalkPathMap entry '%s' with unsupported type %s", key, type(v).__name__)

    cfg["signalk_path_map"] = parsed_map

    if cfg["enable_signalk"] and not cfg["signalk_server_url"]:
        raise RuntimeError("Signal K enabled but signalkServerUrl/signalk_server_url is empty")

    if cfg["enable_influx"]:
        cfg["influxdb_url"] = str(cfg_required(raw, ["influxdbUrl", "influxdb_url"], "INFLUXDB_URL"))
        cfg["influxdb_token"] = str(cfg_required(raw, ["influxdbToken", "influxdb_token"], "INFLUXDB_TOKEN"))
        cfg["influxdb_org"] = str(cfg_required(raw, ["influxdbOrg", "influxdb_org"], "INFLUXDB_ORG"))
        cfg["influxdb_bucket"] = str(cfg_required(raw, ["influxdbBucket", "influxdb_bucket"], "INFLUXDB_BUCKET"))

    if not cfg["enable_influx"] and not cfg["enable_signalk"] and not cfg["enable_storage"]:
        raise RuntimeError("No outputs enabled: enable at least one of influxdb, signalk, storage")

    cfg["config_path"] = str(Path(path).resolve())
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


def convert_signalk_value(key: str, path: str, value):
    if value is None:
        return None

    # Keep non-numeric values untouched.
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return value

    v = float(value)
    key_l = str(key).lower()
    path_l = str(path or "").lower()

    # Temperatures in Signal K are Kelvin.
    if "temperature" in path_l or key_l.startswith("temp"):
        # Heuristic: values below 200 are assumed Celsius.
        return v + 273.15 if v < 200 else v

    # Pressure in Signal K is Pascal.
    if "pressure" in path_l or "barometer" in key_l:
        # Heuristic: values in hPa are typically around 900-1100.
        return v * 100.0 if abs(v) < 2000 else v

    # Relative humidity ratio [0..1].
    if "humidity" in path_l or key_l in ("humout", "humin"):
        return v / 100.0 if v > 1.0 else v

    # Angular quantities in Signal K are radians.
    angle_tokens = ("angle", "heading", "bearing", "course", "track", "yaw", "pitch", "roll", "leeway")
    if any(token in path_l for token in angle_tokens) or any(token in key_l for token in ("dir", "deg", "angle")):
        return math.radians(v)

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

    def check_connection(self):
        self._connect()

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
    metas = []
    seen_meta_paths = set()

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
        mapped_meta = None

        if isinstance(mapped, dict):
            path = str(mapped.get("path", "")).strip()
            mapped_meta = mapped.get("meta")
            if mapped_meta is None and "meta:" in mapped:
                mapped_meta = mapped.get("meta:")
        elif isinstance(mapped, str):
            path = mapped.strip()
        else:
            path = ""

        if not path:
            path = SIGNALK_STANDARD_PATHS.get(key, f"environment.{sanitize_signalk_key(key)}")

        try:
            value = convert_signalk_value(key, path, raw)
        except Exception:
            value = raw

        if value is None or isinstance(value, (dict, list)):
            continue

        values.append({"path": path, "value": value})

        if mapped_meta is not None and path not in seen_meta_paths:
            metas.append({"path": path, "value": mapped_meta})
            seen_meta_paths.add(path)

    if not values and not metas:
        return None

    update = {
        "timestamp": dt.isoformat().replace("+00:00", "Z"),
        "$source": cfg.get("signalk_source_label", "sensor-network-collector"),
    }
    if values:
        update["values"] = values
    if metas:
        update["meta"] = metas

    return {
        "context": context,
        "updates": [update],
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




def _to_float(value):
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _extract_lat_lon_from_row(row: dict):
    # Direct latitude/longitude columns
    key_pairs = [
        ("latitude", "longitude"),
        ("lat", "lon"),
        ("lat", "lng"),
    ]
    for k_lat, k_lon in key_pairs:
        lat = _to_float(row.get(k_lat))
        lon = _to_float(row.get(k_lon))
        if lat is not None and lon is not None:
            return lat, lon

    # Nested JSON in 'position' column
    pos_raw = row.get("position")
    if isinstance(pos_raw, str) and pos_raw.strip().startswith("{"):
        try:
            pos = json.loads(pos_raw)
            lat = _to_float(pos.get("latitude"))
            lon = _to_float(pos.get("longitude"))
            if lat is not None and lon is not None:
                return lat, lon
        except Exception:
            pass

    return None, None


DEFAULT_FIELD_UNITS = {
    "TempIn": "K",
    "TempOut": "K",
    "HumIn": "ratio",
    "HumOut": "ratio",
    "Barometer": "Pa",
    "BarTrend": "Pa/s",
    "WindSpeed": "m/s",
    "WindSpeed10Min": "m/s",
    "WindDir": "rad",
    "RainRate": "m/s",
    "RainStorm": "m",
    "RainDay": "m",
    "RainMonth": "m",
    "RainYear": "m",
    "ETDay": "m",
    "ETMonth": "m",
    "ETYear": "m",
    "SolarRad": "W/m2",
    "BatteryVolts": "V",
}


def get_field_units(cfg: dict):
    out = dict(DEFAULT_FIELD_UNITS)
    path_map = cfg.get("signalk_path_map", {})
    for key, entry in (path_map.items() if isinstance(path_map, dict) else []):
        if isinstance(entry, dict):
            meta = entry.get("meta")
            if isinstance(meta, dict):
                units = meta.get("units")
                if isinstance(units, str) and units.strip():
                    out[str(key)] = units.strip()
    return out


def parse_iso_ts(value: str):
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


def shift_months(dt: datetime, months: int):
    y = dt.year + ((dt.month - 1 + months) // 12)
    m = ((dt.month - 1 + months) % 12) + 1
    d = min(dt.day, [31, 29 if (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1])
    return dt.replace(year=y, month=m, day=d)


def interval_start(anchor: datetime, interval: str):
    if interval == "hour":
        return anchor - timedelta(hours=1)
    if interval == "day":
        return anchor - timedelta(days=1)
    if interval == "week":
        return anchor - timedelta(weeks=1)
    if interval == "month":
        return shift_months(anchor, -1)
    if interval == "year":
        return shift_months(anchor, -12)
    return anchor - timedelta(days=1)


def shift_anchor(anchor: datetime, interval: str, steps: int):
    if interval == "hour":
        return anchor + timedelta(hours=steps)
    if interval == "day":
        return anchor + timedelta(days=steps)
    if interval == "week":
        return anchor + timedelta(weeks=steps)
    if interval == "month":
        return shift_months(anchor, steps)
    if interval == "year":
        return shift_months(anchor, 12 * steps)
    return anchor + timedelta(days=steps)


def get_station_preview(storage_root: str, instrument_uuid: str):
    files = list_csv_files_for_instrument(storage_root, instrument_uuid)
    if not files:
        return {"latitude": None, "longitude": None, "last_timestamp": None, "rows": 0, "name": instrument_uuid}

    total_rows = 0
    last_timestamp = None
    latitude = None
    longitude = None
    station_name = instrument_uuid

    # Scan newest files first so map can show latest known position quickly.
    for csv_path in reversed(files):
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception:
            continue

        if not rows:
            continue

        total_rows += len(rows)

        if last_timestamp is None:
            last_timestamp = rows[-1].get("timestamp")
        if station_name == instrument_uuid:
            maybe_name = rows[-1].get("name")
            if isinstance(maybe_name, str) and maybe_name.strip():
                station_name = maybe_name.strip()

        if latitude is None or longitude is None:
            lat, lon = _extract_lat_lon_from_row(rows[-1])
            if lat is not None and lon is not None:
                latitude, longitude = lat, lon

    return {
        "latitude": latitude,
        "longitude": longitude,
        "last_timestamp": last_timestamp,
        "rows": total_rows,
        "name": station_name,
    }


def load_station_rows(storage_root: str, instrument_uuid: str, from_date=None, to_date=None, limit=400):
    files = list_csv_files_for_instrument(storage_root, instrument_uuid, from_date=from_date, to_date=to_date)
    out = []
    for csv_path in files:
        try:
            with csv_path.open("r", newline="", encoding="utf-8") as f:
                out.extend(list(csv.DictReader(f)))
        except Exception:
            continue

    if limit is not None and limit > 0 and len(out) > limit:
        out = out[-limit:]
    return out


def extract_numeric_series(rows, excluded=None):
    excluded = set(excluded or [])
    numeric_keys = set()
    for row in rows:
        for k, v in row.items():
            if k in excluded:
                continue
            if _to_float(v) is not None:
                numeric_keys.add(k)
    return sorted(numeric_keys)


def coerce_for_influx_type(value, target_type: str):
    t = str(target_type or "").strip().lower()
    if t in ("integer", "int"):
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float, str)):
            return int(float(value))
    if t in ("float", "double"):
        if isinstance(value, bool):
            return float(int(value))
        if isinstance(value, (int, float, str)):
            return float(value)
    if t in ("string", "str"):
        return str(value)
    if t in ("boolean", "bool"):
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return value != 0
        if isinstance(value, str):
            s = value.strip().lower()
            if s in ("1", "true", "yes", "on"):
                return True
            if s in ("0", "false", "no", "off"):
                return False
    return value


def maybe_fix_influx_record_type_conflict(record: dict, error: Exception):
    msg = str(error)

    body_msg = None
    raw_body = getattr(error, "body", None)
    if isinstance(raw_body, bytes):
        raw_body = raw_body.decode("utf-8", errors="replace")
    if isinstance(raw_body, str) and raw_body.strip():
        try:
            body = json.loads(raw_body)
            if isinstance(body, dict):
                body_msg = body.get("message") or body.get("error")
        except Exception:
            body_msg = raw_body

    candidates = [msg]
    if body_msg:
        candidates.insert(0, str(body_msg))

    m = None
    for candidate in candidates:
        m = INFLUX_FIELD_CONFLICT_RE.search(candidate)
        if m:
            break
    if not m:
        return None, None, None

    field = m.group("field")
    existing_type = m.group("existing_type")
    fields = dict(record.get("fields", {}))
    if field not in fields:
        return None, field, existing_type

    try:
        fields[field] = coerce_for_influx_type(fields[field], existing_type)
    except Exception:
        # If conversion is not possible (for example NaN -> integer), drop only
        # the conflicting field and keep writing the rest of the point.
        fields.pop(field, None)
        fixed = dict(record)
        fixed["fields"] = fields
        return fixed, field, f"drop({existing_type})"

    fixed = dict(record)
    fixed["fields"] = fields
    return fixed, field, existing_type


def write_influx_with_type_conflict_retries(write_api, cfg: dict, record: dict, topic: str, max_retries: int = 3):
    attempt = 0
    current = dict(record)
    while True:
        try:
            write_api.write(bucket=cfg["influxdb_bucket"], org=cfg["influxdb_org"], record=current)
            return True
        except Exception as e:
            fixed_record, field_name, existing_type = maybe_fix_influx_record_type_conflict(current, e)
            if fixed_record is None or attempt >= max_retries:
                logger.exception("Influx write failed for topic=%s: %s", topic, e)
                return False

            attempt += 1
            if not fixed_record.get("fields"):
                logger.error(
                    "Influx retry aborted topic=%s after removing conflicting field=%s; no fields left",
                    topic,
                    field_name,
                )
                return False
            logger.warning(
                "Influx conflict retry topic=%s attempt=%d field=%s target_type=%s",
                topic,
                attempt,
                field_name,
                existing_type,
            )
            current = fixed_record

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

    def require_admin():
        user = current_user()
        if not user:
            return redirect(url_for("login", next=request.path))
        if user.get("role") != "admin":
            abort(403)
        return user

    def storage_root_or_404():
        storage_root = cfg.get("storage_root")
        if not storage_root:
            abort(404, "Storage is not enabled/configured")
        return storage_root

    def available_instruments(storage_root: str):
        return collect_instruments(storage_root)

    def station_is_accessible(user, instrument_uuid: str):
        return access_store.can_download(user, instrument_uuid)

    @app.route("/")
    def index():
        user = current_user()
        storage_root = cfg.get("storage_root")
        instruments = available_instruments(storage_root) if storage_root else []
        policies = access_store.list_policies(instruments)

        stations = []
        for inst in instruments:
            preview = get_station_preview(storage_root, inst)
            can_access = station_is_accessible(user, inst)
            stations.append(
                {
                    "uuid": inst,
                    "name": preview["name"],
                    "policy": policies.get(inst, "account"),
                    "can_access": can_access,
                    "latitude": preview["latitude"],
                    "longitude": preview["longitude"],
                    "last_timestamp": preview["last_timestamp"],
                    "rows": preview["rows"],
                }
            )

        # Map center based on first station with known coordinates.
        center = {"lat": 40.0, "lon": 14.0, "zoom": 6}
        for st in stations:
            if st["latitude"] is not None and st["longitude"] is not None:
                center = {"lat": st["latitude"], "lon": st["longitude"], "zoom": 9}
                break

        clickable = [s for s in stations if s["can_access"]]

        return render_template_string(
            """
            <html>
            <head>
              <title>Sensor Network Collector - Data Portal</title>
              <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
              <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin=""/>
              <style>
                #map { height: 420px; border: 1px solid #ccc; margin-bottom: 16px; }
                .panel { border: 1px solid #ddd; padding: 12px; margin-bottom: 12px; border-radius: 8px; }
              </style>
            </head>
            <body class="container py-4">
              <h1>Sensor Network Collector - Data Portal</h1>
              {% if user %}
                <p>Logged in as <b>{{ user.username }}</b> ({{ user.role }}) - <a href="{{ url_for('logout') }}">Logout</a></p>
                {% if user.role == 'admin' %}<p><a href="{{ url_for('admin') }}">Admin panel</a></p>{% endif %}
              {% else %}
                <p><a href="{{ url_for('login') }}">Login</a> | <a href="{{ url_for('request_account') }}">Request account</a></p>
              {% endif %}

              <div class="panel">
                <h2>Stations map</h2>
                {% if not storage_root %}
                  <p>Storage is not enabled/configured.</p>
                {% elif not stations %}
                  <p>No station folders found under {{ storage_root }}.</p>
                {% else %}
                  <div id="map"></div>
                  <p>Green markers are clickable (browse/download allowed). Gray markers are visible but not accessible with current permissions.</p>
                {% endif %}
              </div>

              <div class="panel">
                <h2>Browse & download</h2>
                {% if not clickable %}
                  <p>No stations available with your current permissions.</p>
                {% else %}
                  <form method="post" action="{{ url_for('download') }}">
                    {% for st in clickable %}
                      <label>
                        <input type="checkbox" name="instrument" value="{{ st.uuid }}"> {{ st.name }} ({{ st.uuid }})
                        (policy={{ st.policy }}, rows={{ st.rows }})
                        <a href="{{ url_for('browse_station', instrument_uuid=st.uuid) }}">browse</a>
                      </label><br/>
                    {% endfor %}
                    <p>Date filter (optional):</p>
                    <label>From <input class="form-control d-inline-block w-auto" type="date" name="from_date"></label>
                    <label>To <input class="form-control d-inline-block w-auto" type="date" name="to_date"></label>
                    <p><button class="btn btn-primary mt-2" type="submit">Download ZIP</button></p>
                  </form>
                {% endif %}
              </div>

              <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
              <script>
                const stations = {{ stations | tojson }};
                if (stations.length > 0 && document.getElementById('map')) {
                  const map = L.map('map').setView([{{ center.lat }}, {{ center.lon }}], {{ center.zoom }});
                  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    maxZoom: 19,
                    attribution: '&copy; OpenStreetMap contributors'
                  }).addTo(map);

                  stations.forEach((s) => {
                    if (s.latitude == null || s.longitude == null) {
                      return;
                    }
                    const color = s.can_access ? '#1f9d55' : '#666';
                    const marker = L.circleMarker([s.latitude, s.longitude], {
                      radius: 8,
                      color: color,
                      fillColor: color,
                      fillOpacity: 0.85
                    }).addTo(map);

                    let html = `<b>${s.name || s.uuid}</b><br/>uuid=${s.uuid}<br/>policy=${s.policy}<br/>rows=${s.rows}`;
                    if (s.last_timestamp) {
                      html += `<br/>last=${s.last_timestamp}`;
                    }
                    if (s.can_access) {
                      html += `<br/><a href="/station/${encodeURIComponent(s.uuid)}">browse station</a>`;
                    } else {
                      html += '<br/>no access with current user';
                    }
                    marker.bindPopup(html);
                  });
                }
              </script>
            </body>
            </html>
            """,
            user=user,
            storage_root=storage_root,
            stations=stations,
            clickable=clickable,
            center=center,
        )

    @app.route("/station/<path:instrument_uuid>")
    def browse_station(instrument_uuid: str):
        user = current_user()
        storage_root = storage_root_or_404()

        instruments = set(available_instruments(storage_root))
        if instrument_uuid not in instruments:
            abort(404, "Station not found")

        if not station_is_accessible(user, instrument_uuid):
            abort(403)

        preview = get_station_preview(storage_root, instrument_uuid)
        station_name = preview.get("name") or instrument_uuid

        interval = request.args.get("interval", "day")
        if interval not in ("hour", "day", "week", "month", "year", "custom"):
            interval = "day"

        from_date = parse_date_ymd(request.args.get("from_date", ""))
        to_date = parse_date_ymd(request.args.get("to_date", ""))
        if from_date and to_date and to_date < from_date:
            abort(400, "to_date must be >= from_date")

        rows = load_station_rows(storage_root, instrument_uuid, from_date=from_date, to_date=to_date, limit=None)
        if not rows:
            return render_template_string(
                """
                <html><body>
                  <p><a href="{{ url_for('index') }}">Home</a></p>
                  <h1>Station {{ station_name }} ({{ instrument_uuid }})</h1>
                  <p>No data rows found for the selected filters.</p>
                </body></html>
                """,
                instrument_uuid=instrument_uuid,
                station_name=station_name,
            )

        rows_ts = []
        for row in rows:
            ts = parse_iso_ts(row.get("timestamp", ""))
            if ts is not None:
                rows_ts.append((ts, row))

        if rows_ts:
            rows_ts.sort(key=lambda x: x[0])
            max_ts = rows_ts[-1][0]
        else:
            max_ts = datetime.now(timezone.utc)

        anchor_param = request.args.get("anchor", "")
        anchor = parse_iso_ts(anchor_param) or max_ts

        if interval == "custom":
            if from_date:
                win_start = datetime.combine(from_date, datetime.min.time(), tzinfo=timezone.utc)
            else:
                win_start = max_ts - timedelta(days=1)
            if to_date:
                win_end = datetime.combine(to_date, datetime.max.time(), tzinfo=timezone.utc)
            else:
                win_end = max_ts
        else:
            win_end = anchor
            win_start = interval_start(anchor, interval)

        filtered_rows = []
        for ts, row in rows_ts:
            if win_start <= ts <= win_end:
                filtered_rows.append(row)

        if not filtered_rows and rows_ts:
            fallback_end = rows_ts[-1][0]
            fallback_start = interval_start(fallback_end, interval if interval != "custom" else "day")
            filtered_rows = [row for ts, row in rows_ts if fallback_start <= ts <= fallback_end]
            win_start, win_end = fallback_start, fallback_end

        rows = filtered_rows

        excluded = {"timestamp", "topic", "uuid", "position", "latitude", "longitude", "lat", "lon", "lng"}
        numeric_cols = extract_numeric_series(rows, excluded=excluded)

        selected_fields = [f for f in request.args.getlist("field") if f in numeric_cols]
        if not selected_fields and numeric_cols:
            selected_fields = [numeric_cols[0]]

        units_map = get_field_units(cfg)

        chart_labels = []
        chart_datasets = []
        for field in selected_fields:
            d_labels = []
            d_values = []
            for row in rows:
                y = _to_float(row.get(field))
                if y is None:
                    continue
                d_labels.append(row.get("timestamp") or "")
                d_values.append(y)
            if len(d_labels) > len(chart_labels):
                chart_labels = d_labels
            chart_datasets.append(
                {
                    "field": field,
                    "label": f"{field} [{units_map.get(field, '-')}]",
                    "data": d_values,
                    "unit": units_map.get(field, ""),
                }
            )

        page = max(1, int(request.args.get("page", "1") or "1"))
        page_size = int(request.args.get("page_size", "50") or "50")
        page_size = min(200, max(10, page_size))
        total_rows = len(rows)
        page_count = max(1, (total_rows + page_size - 1) // page_size)
        if page > page_count:
            page = page_count
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        table_rows = rows[start_idx:end_idx]
        table_cols = list(table_rows[0].keys()) if table_rows else []
        table_headers = {
            c: f"{c} [{units_map.get(c, '-')}]"
            if c not in ("timestamp", "topic", "uuid", "name", "position", "latitude", "longitude", "lat", "lon", "lng")
            else c
            for c in table_cols
        }

        prev_anchor = shift_anchor(anchor, interval, -1).isoformat().replace("+00:00", "Z")
        next_anchor = shift_anchor(anchor, interval, 1).isoformat().replace("+00:00", "Z")

        return render_template_string(
            """
            <html>
            <head>
              <title>Station {{ station_name }} ({{ instrument_uuid }})</title>
              <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
              <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
              <style>
                table { border-collapse: collapse; width: 100%; font-size: 12px; }
                th, td { border: 1px solid #ddd; padding: 4px; }
                th { position: sticky; top: 0; background: #f8f8f8; }
                .table-wrap { max-height: 420px; overflow: auto; border: 1px solid #ddd; }
                .panel { border: 1px solid #ddd; padding: 12px; margin-bottom: 12px; border-radius: 8px; }
              </style>
            </head>
            <body class="container-fluid py-3">
              <p><a href="{{ url_for('index') }}">Home</a></p>
              <h1>Station {{ station_name }} ({{ instrument_uuid }})</h1>

              <div class="panel">
                <h2>Time interval</h2>
                <form method="get" id="intervalForm">
                  <label>Window
                    <select name="interval" id="intervalSelect">
                      {% for it in ['hour','day','week','month','year','custom'] %}
                        <option value="{{ it }}" {% if it==interval %}selected{% endif %}>{{ it }}</option>
                      {% endfor %}
                    </select>
                  </label>
                  <label>From <input type="date" name="from_date" value="{{ request.args.get('from_date','') }}"/></label>
                  <label>To <input type="date" name="to_date" value="{{ request.args.get('to_date','') }}"/></label>
                  <input type="hidden" name="anchor" value="{{ request.args.get('anchor','') }}"/>
                  {% for f in selected_fields %}<input type="hidden" name="field" value="{{ f }}"/>{% endfor %}
                  <input type="hidden" name="page_size" value="{{ page_size }}"/>
                  <button class="btn btn-primary btn-sm" type="submit">Apply</button>
                </form>
                {% if interval != 'custom' %}
                  <p>
                    <a href="{{ url_for('browse_station', instrument_uuid=instrument_uuid, interval=interval, anchor=prev_anchor, page_size=page_size, field=selected_fields) }}">&#8592; previous {{ interval }}</a>
                    |
                    <a href="{{ url_for('browse_station', instrument_uuid=instrument_uuid, interval=interval, anchor=next_anchor, page_size=page_size, field=selected_fields) }}">next {{ interval }} &#8594;</a>
                  </p>
                {% endif %}
                <p>Showing data in window: {{ win_start }} to {{ win_end }} UTC</p>
              </div>

              <div class="panel">
                <h2>Download station data</h2>
                <form method="post" action="{{ url_for('download') }}">
                  <input type="hidden" name="instrument" value="{{ instrument_uuid }}"/>
                  <label>From <input type="date" name="from_date" value="{{ request.args.get('from_date','') }}"></label>
                  <label>To <input type="date" name="to_date" value="{{ request.args.get('to_date','') }}"></label>
                  <button class="btn btn-primary btn-sm" type="submit">Download ZIP</button>
                </form>
              </div>

              <div class="panel">
                <h2>Chart</h2>
                {% if not numeric_cols %}
                  <p>No numeric columns available for charting.</p>
                {% else %}
                  <form method="get" id="chartForm">
                    <input type="hidden" name="interval" value="{{ interval }}"/>
                    <input type="hidden" name="anchor" value="{{ request.args.get('anchor','') }}"/>
                    <input type="hidden" name="from_date" value="{{ request.args.get('from_date','') }}"/>
                    <input type="hidden" name="to_date" value="{{ request.args.get('to_date','') }}"/>
                    <input type="hidden" name="page_size" value="{{ page_size }}"/>
                    <fieldset>
                      <legend>Variables</legend>
                      {% for c in numeric_cols %}
                        <label style="display:inline-block; margin-right:10px;">
                          <input type="checkbox" name="field" value="{{ c }}" {% if c in selected_fields %}checked{% endif %}>
                          {{ c }} [{{ units_map.get(c,'-') }}]
                        </label>
                      {% endfor %}
                    </fieldset>
                    <noscript><button class="btn btn-secondary btn-sm" type="submit">Update</button></noscript>
                  </form>
                  <canvas id="chart" height="110"></canvas>
                {% endif %}
              </div>

              <div class="panel">
                <h2>Data table</h2>
                <p>Rows {{ start_idx + 1 if total_rows else 0 }}-{{ end_idx if end_idx < total_rows else total_rows }} of {{ total_rows }}</p>
                <p>
                  {% if page > 1 %}
                    <a href="{{ url_for('browse_station', instrument_uuid=instrument_uuid, interval=interval, anchor=request.args.get('anchor',''), from_date=request.args.get('from_date',''), to_date=request.args.get('to_date',''), page=page-1, page_size=page_size, field=selected_fields) }}">&#8592; prev page</a>
                  {% endif %}
                  {% if page < page_count %}
                    {% if page > 1 %}|{% endif %}
                    <a href="{{ url_for('browse_station', instrument_uuid=instrument_uuid, interval=interval, anchor=request.args.get('anchor',''), from_date=request.args.get('from_date',''), to_date=request.args.get('to_date',''), page=page+1, page_size=page_size, field=selected_fields) }}">next page &#8594;</a>
                  {% endif %}
                </p>
                <div class="table-wrap">
                  <table class="table table-sm table-striped table-bordered">
                    <thead>
                      <tr>{% for c in table_cols %}<th>{{ table_headers[c] }}</th>{% endfor %}</tr>
                    </thead>
                    <tbody>
                      {% for r in table_rows %}
                        <tr>{% for c in table_cols %}<td>{{ r.get(c, '') }}</td>{% endfor %}</tr>
                      {% endfor %}
                    </tbody>
                  </table>
                </div>
              </div>

              <script>
                const labels = {{ chart_labels | tojson }};
                const datasets = {{ chart_datasets | tojson }};
                if (labels.length > 0 && datasets.length > 0 && document.getElementById('chart')) {
                  new Chart(document.getElementById('chart'), {
                    type: 'line',
                    data: {
                      labels,
                      datasets: datasets.map((d, idx) => ({
                        label: d.label,
                        data: d.data,
                        borderColor: ['#0b57d0','#1f9d55','#d95f02','#7b1fa2','#c2185b'][idx % 5],
                        pointRadius: 0,
                        tension: 0.2
                      }))
                    },
                    options: {
                      responsive: true,
                      scales: {
                        x: { display: true, title: { display: true, text: 'timestamp' } },
                        y: {
                          display: true,
                          title: {
                            display: true,
                            text: (new Set(datasets.map(d => d.unit || '-')).size === 1)
                              ? `value [${datasets[0].unit || '-'}]`
                              : 'value [mixed units]'
                          }
                        }
                      }
                    }
                  });
                }

                const chartForm = document.getElementById('chartForm');
                if (chartForm) {
                  chartForm.querySelectorAll('input[type="checkbox"]').forEach((el) => {
                    el.addEventListener('change', () => chartForm.submit());
                  });
                }
                const intervalSel = document.getElementById('intervalSelect');
                const intervalForm = document.getElementById('intervalForm');
                if (intervalSel && intervalForm) {
                  intervalSel.addEventListener('change', () => intervalForm.submit());
                }
              </script>
            </body>
            </html>
            """,
            instrument_uuid=instrument_uuid,
            station_name=station_name,
            interval=interval,
            win_start=win_start.isoformat().replace("+00:00", "Z"),
            win_end=win_end.isoformat().replace("+00:00", "Z"),
            prev_anchor=prev_anchor,
            next_anchor=next_anchor,
            rows=rows,
            numeric_cols=numeric_cols,
            selected_fields=selected_fields,
            chart_labels=chart_labels,
            chart_datasets=chart_datasets,
            table_rows=table_rows,
            table_cols=table_cols,
            table_headers=table_headers,
            units_map=units_map,
            page=page,
            page_count=page_count,
            page_size=page_size,
            total_rows=total_rows,
            start_idx=start_idx,
            end_idx=end_idx,
            request=request,
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
            <html>
            <head>
              <title>Login</title>
              <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
            </head>
            <body class="container py-5">
              <div class="row justify-content-center">
                <div class="col-12 col-md-6 col-lg-4">
                  <h1 class="h3 mb-3">Login</h1>
                  {% if err %}<div class="alert alert-danger">{{ err }}</div>{% endif %}
                  <form method="post">
                    <div class="mb-3">
                      <label class="form-label">Username</label>
                      <input class="form-control" name="username">
                    </div>
                    <div class="mb-3">
                      <label class="form-label">Password</label>
                      <input class="form-control" name="password" type="password">
                    </div>
                    <button class="btn btn-primary w-100" type="submit">Login</button>
                  </form>
                  <p class="mt-3 mb-1"><a href="{{ url_for('request_account') }}">Request account</a></p>
                  <p><a href="{{ url_for('index') }}">Home</a></p>
                </div>
              </div>
            </body>
            </html>
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
            <html>
            <head>
              <title>Request account</title>
              <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
            </head>
            <body class="container py-5">
              <div class="row justify-content-center">
                <div class="col-12 col-md-8 col-lg-6">
                  <h1 class="h3 mb-3">Request account</h1>
                  {% if msg %}<div class="alert alert-success">{{ msg }}</div>{% endif %}
                  {% if err %}<div class="alert alert-danger">{{ err }}</div>{% endif %}
                  <form method="post">
                    <div class="mb-3">
                      <label class="form-label">Username</label>
                      <input class="form-control" name="username">
                    </div>
                    <div class="mb-3">
                      <label class="form-label">Email</label>
                      <input class="form-control" name="email" type="email">
                    </div>
                    <div class="mb-3">
                      <label class="form-label">Password</label>
                      <input class="form-control" name="password" type="password">
                    </div>
                    <div class="mb-3">
                      <label class="form-label">Reason</label>
                      <textarea class="form-control" name="reason" rows="4"></textarea>
                    </div>
                    <button class="btn btn-primary" type="submit">Submit request</button>
                  </form>
                  <p class="mt-3"><a href="{{ url_for('index') }}">Home</a></p>
                </div>
              </div>
            </body>
            </html>
            """,
            msg=msg,
            err=err,
        )

    @app.route("/download", methods=["POST"])
    def download():
        user = current_user()
        storage_root = storage_root_or_404()
        all_instruments = set(available_instruments(storage_root))

        requested = []
        for inst in request.form.getlist("instrument"):
            inst = str(inst).strip()
            if inst and inst not in requested:
                requested.append(inst)

        if not requested:
            abort(400, "Select at least one instrument")

        unknown = [inst for inst in requested if inst not in all_instruments]
        if unknown:
            abort(404, f"Unknown station(s): {', '.join(unknown)}")

        unauthorized = [inst for inst in requested if not station_is_accessible(user, inst)]
        if unauthorized:
            abort(403, f"Access denied for station(s): {', '.join(unauthorized)}")

        from_date = parse_date_ymd(request.form.get("from_date", ""))
        to_date = parse_date_ymd(request.form.get("to_date", ""))
        if from_date and to_date and to_date < from_date:
            abort(400, "to_date must be >= from_date")

        zip_path, count = make_zip_for_download(storage_root, requested, from_date=from_date, to_date=to_date)
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
            <html>
            <head>
              <title>Admin panel</title>
              <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
              <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
            </head>
            <body class="container py-4">
            <h1 class="h3">Admin panel</h1>
            <p>Logged in as {{ admin_user.username }} - <a href="{{ url_for('index') }}">Home</a></p>

            <div class="card mb-3"><div class="card-body">
            <h2 class="h5">Create user</h2>
            <form method="post" action="{{ url_for('admin_create_user') }}" class="row g-2">
              <div class="col-md-3"><input class="form-control" name="username" placeholder="Username"></div>
              <div class="col-md-3"><input class="form-control" name="email" placeholder="Email"></div>
              <div class="col-md-3"><input class="form-control" name="password" type="password" placeholder="Password"></div>
              <div class="col-md-2">
                <select class="form-select" name="role">
                  <option value="user">user</option>
                  <option value="admin">admin</option>
                </select>
              </div>
              <div class="col-md-1"><button class="btn btn-primary w-100" type="submit">Create</button></div>
            </form>
            </div></div>

            <h2 class="h5">Pending account requests</h2>
            {% if pending_requests %}
              {% for r in pending_requests %}
                <div class="card mb-2"><div class="card-body">
                  <b>#{{ r.id }} {{ r.username }}</b> ({{ r.email }})<br/>
                  {{ r.message }}<br/>
                  <form method="post" action="{{ url_for('admin_approve_request', request_id=r.id) }}" style="display:inline;">
                    <button class="btn btn-success btn-sm" type="submit">Approve</button>
                  </form>
                  <form method="post" action="{{ url_for('admin_reject_request', request_id=r.id) }}" style="display:inline;">
                    <button class="btn btn-danger btn-sm" type="submit">Reject</button>
                  </form>
                </div></div>
              {% endfor %}
            {% else %}
              <p>No pending requests.</p>
            {% endif %}

            <h2 class="h5 mt-4">Instrument policies</h2>
            {% if instruments %}
              {% for inst in instruments %}
                <form method="post" action="{{ url_for('admin_set_policy') }}" class="row g-2 align-items-center mb-2">
                  <input type="hidden" name="instrument_uuid" value="{{ inst }}">
                  <div class="col-md-4"><b>{{ inst }}</b></div>
                  <div class="col-md-5"><select class="form-select" name="policy">
                    <option value="open" {% if policies[inst]=='open' %}selected{% endif %}>open (free download)</option>
                    <option value="account" {% if policies[inst]=='account' %}selected{% endif %}>account (authenticated users)</option>
                    <option value="restricted" {% if policies[inst]=='restricted' %}selected{% endif %}>restricted (assigned users only)</option>
                  </select></div>
                  <div class="col-md-2"><button class="btn btn-primary btn-sm" type="submit">Save</button></div>
                </form>
              {% endfor %}
            {% else %}
              <p>No instruments found in storage.</p>
            {% endif %}

            <h2 class="h5 mt-4">User access (for restricted policy)</h2>
            {% for u in users %}
              <div class="card mb-2"><div class="card-body">
                <b>{{ u.username }}</b> role={{ u.role }} active={{ u.active }}<br/>
                currently allowed: {{ user_access[u.username] }}
                <form method="post" action="{{ url_for('admin_set_user_access') }}" class="row g-2 mt-1">
                  <input type="hidden" name="username" value="{{ u.username }}">
                  <div class="col-md-5"><input class="form-control" name="instrument_uuid" placeholder="Instrument UUID"></div>
                  <div class="col-md-3"><select class="form-select" name="allow">
                    <option value="1">allow</option>
                    <option value="0">revoke</option>
                  </select></div>
                  <div class="col-md-2"><button class="btn btn-secondary btn-sm" type="submit">Apply</button></div>
                </form>
              </div></div>
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
    "signalk_access_manager": None,
    "signalk_lock": threading.Lock(),
    "csv_storage": None,
    "http_thread": None,
    "access_store": None,
}




def get_signalk_client():
    with runtime["signalk_lock"]:
        return runtime.get("signalk_client")


def set_signalk_client(client):
    with runtime["signalk_lock"]:
        old = runtime.get("signalk_client")
        runtime["signalk_client"] = client
    if old is not None and old is not client:
        try:
            old.close()
        except Exception:
            pass


def build_signalk_access_manager(cfg: dict):
    def client_factory(token: str):
        return SignalKWebsocketPublisher(
            server_url=cfg["signalk_server_url"],
            token=token,
            timeout=float(cfg["signalk_access_timeout_sec"]),
        )

    def on_token_approved(token: str):
        cfg["signalk_token"] = token

    return SignalKAccessManager(
        server_url=cfg["signalk_server_url"],
        initial_token=cfg.get("signalk_token", ""),
        config_path=cfg["config_path"],
        config_token_key="signalkToken",
        client_id=cfg["signalk_access_client_id"],
        description=cfg["signalk_access_description"],
        poll_interval_sec=max(2, int(cfg["signalk_access_poll_sec"])),
        request_timeout_sec=max(2, int(cfg["signalk_access_timeout_sec"])),
        client_factory=client_factory,
        on_client_ready=set_signalk_client,
        on_client_unavailable=lambda: set_signalk_client(None),
        on_token_approved=on_token_approved,
        logger=logger,
    )

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
            ok = write_influx_with_type_conflict_retries(write_api, cfg, influx_record, topic)
            if ok:
                logger.info("Influx write topic=%s fields=%d time=%s", topic, len(flat_fields), dt.isoformat())

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
            signalk_client = get_signalk_client()
            if signalk_client is None:
                logger.debug("Signal K client unavailable (access pending or disconnected), skipping topic=%s", topic)
                return
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
                msg = str(e).lower()
                if any(x in msg for x in ("401", "403", "unauthoriz", "forbidden", "token")):
                    manager = runtime.get("signalk_access_manager")
                    if manager is not None:
                        manager.notify_token_invalid()


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

    manager = runtime.get("signalk_access_manager")
    if manager is not None:
        try:
            manager.stop()
        except Exception:
            pass

    signalk_client = get_signalk_client()
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
            manager = build_signalk_access_manager(cfg)
            runtime["signalk_access_manager"] = manager
            manager.start()
            logger.info("Signal K output enabled (%s) with access-request workflow", cfg["signalk_server_url"])

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
