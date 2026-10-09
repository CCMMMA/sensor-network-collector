import argparse
import csv
import json
import logging
import math
import os
import re
import signal
import sys
import tempfile
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote, urlsplit

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

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
    "HumOut": "environment.outside.relativeHumidity",
    "Barometer": "environment.outside.pressure",
    "WindSpeed": "environment.wind.speedApparent",
    "WindDir": "environment.wind.angleApparent",
    "TempIn": "environment.inside.temperature",
    "HumIn": "environment.inside.relativeHumidity",
}

INFLUX_FIELD_CONFLICT_RE = re.compile(
    r'input field\s+\\?"(?P<field>[^"\\]+)\\?"\s+.*?\s+is type\s+(?P<input_type>[a-zA-Z]+),\s+already exists as type\s+(?P<existing_type>[a-zA-Z]+)',
    re.IGNORECASE,
)


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
        description="Threaded sensor-network collector (InfluxDB + Signal K + CSV)"
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
    parser.add_argument(
        "--http",
        default=None,
        help="Deprecated and ignored: the web application moved to the sensor-network-pwa project",
    )
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

    storage_override_enabled, storage_override_root = parse_storage_override(args.storage)

    enable_signalk = signalk_cfg_enabled if args.signalk is None else bool(args.signalk)
    enable_influx = influx_cfg_enabled if args.influxdb is None else bool(args.influxdb)
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
                meta = sanitize_signalk_meta(meta)
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

    if args.http is not None or parse_boolish(cfg_value(raw, ["httpEnabled"], env_name="HTTP_ENABLED"), False):
        logger.warning(
            "The web GUI is no longer part of the collector: --http/httpEnabled is ignored "
            "(run the sensor-network-pwa project against the same storage instead)"
        )

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
    parts = [sanitize_signalk_key(p.replace(".", "_")) for p in str(topic).split("/") if p]
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

    # Rain/ET quantities in Signal K are meters or meters/second.
    if any(token in key_l for token in ("rainday", "rainmonth", "rainyear", "rainstorm", "etday", "etmonth", "etyear")):
        return v / 1000.0 if abs(v) >= 1.0 else v
    if "rainrate" in key_l:
        return (v / 1000.0) / 3600.0 if abs(v) > 0.01 else v
    if "precipitation" in path_l and "rate" in path_l:
        return (v / 1000.0) / 3600.0 if abs(v) > 0.01 else v
    if "precipitation" in path_l:
        return v / 1000.0 if abs(v) > 1.0 else v

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
class SignalKPublishRetryLater(RuntimeError):
    """Raised when Signal K publishing is temporarily suspended after a connection failure."""


class SignalKWebsocketPublisher:
    """Best-effort Signal K stream publisher over websocket."""

    def __init__(self, server_url: str, token: str = "", timeout: float = 10.0):
        self.server_url = str(server_url).strip()
        self.token = str(token or "").strip()
        self.timeout = float(timeout)
        self._ws = None
        self._retry_base_sec = 5.0
        self._retry_max_sec = 300.0
        self._retry_delay_sec = self._retry_base_sec
        self._next_retry_at = 0.0
        self._last_error_summary = ""

    def _build_url(self) -> str:
        url = self.server_url
        # The collector only writes: without this the server streams deltas
        # that are never read and eventually stall the connection.
        if "subscribe=" not in urlsplit(url).query:
            url += ("&" if "?" in url else "?") + "subscribe=none"
        if self.token:
            url += ("&" if "?" in url else "?") + "token=" + quote(self.token, safe="")
        return url

    def _format_error(self, exc: Exception) -> str:
        return " ".join(str(exc).split()) or exc.__class__.__name__

    def _raise_if_retry_deferred(self):
        now = time.monotonic()
        if now >= self._next_retry_at:
            return
        wait_sec = max(1, int(math.ceil(self._next_retry_at - now)))
        detail = self._last_error_summary or "previous websocket failure"
        raise SignalKPublishRetryLater(f"{detail}; retrying in {wait_sec}s")

    def _register_failure(self, exc: Exception) -> str:
        summary = self._format_error(exc)
        delay = max(self._retry_base_sec, self._retry_delay_sec)
        self._last_error_summary = summary
        self._next_retry_at = time.monotonic() + delay
        self._retry_delay_sec = min(self._retry_max_sec, delay * 2.0)
        return f"{summary}; retrying in {int(math.ceil(delay))}s"

    def _reset_retry_state(self):
        self._retry_delay_sec = self._retry_base_sec
        self._next_retry_at = 0.0
        self._last_error_summary = ""

    def _connect(self):
        if self._ws is not None:
            return
        self._raise_if_retry_deferred()
        try:
            from websocket import create_connection  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "Missing dependency websocket-client. Install with: python3 -m pip install websocket-client"
            ) from e
        self._ws = create_connection(self._build_url(), timeout=self.timeout)
        self._reset_retry_state()

    def publish(self, packet_json: str):
        try:
            self._connect()
            self._ws.send(packet_json)
        except SignalKPublishRetryLater:
            raise
        except Exception as exc:
            self.close()
            raise SignalKPublishRetryLater(self._register_failure(exc)) from None

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
        if (
            not isinstance(instrument_uuid, str)
            or not instrument_uuid.strip()
            or instrument_uuid in (".", "..")
            or any(ch in instrument_uuid for ch in ("/", "\\", "\x00"))
        ):
            raise ValueError("Instrument identifier must be a single non-empty directory name")
        day_dir = self.root / instrument_uuid / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d")
        name = f"{instrument_uuid}_{dt.strftime('%Y%m%d')}Z{dt.strftime('%H')}00.csv"
        csv_path = day_dir / name
        if not csv_path.resolve().is_relative_to(self.root.resolve()):
            raise ValueError("CSV path escapes the configured storage root")
        return csv_path

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

        # Keep the current file intact until the expanded schema is fully written.
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", newline="", encoding="utf-8", dir=csv_path.parent,
                prefix=f".{csv_path.name}.", suffix=".tmp", delete=False,
            ) as f:
                tmp_path = Path(f.name)
                writer = csv.DictWriter(f, fieldnames=merged)
                writer.writeheader()
                for old in rows:
                    writer.writerow({k: old.get(k, "") for k in merged})
                f.flush()
                os.fsync(f.fileno())
            tmp_path.chmod(csv_path.stat().st_mode & 0o777)
            os.replace(tmp_path, csv_path)
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)

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

    station_name = data.get("name")
    if isinstance(station_name, str) and station_name.strip():
        values.append({"path": "name", "value": station_name.strip()})

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

        mapped_meta = sanitize_signalk_meta(mapped_meta)
        if mapped_meta is not None and path not in seen_meta_paths:
            metas.append({"path": path, "value": mapped_meta})
            seen_meta_paths.add(path)

    if not values and not metas:
        return None

    update = {
        "timestamp": dt.isoformat().replace("+00:00", "Z"),
        "source": {"label": cfg.get("signalk_source_label", "sensor-network-collector")},
    }
    if values:
        update["values"] = values
    if metas:
        update["meta"] = metas

    return {
        "context": context,
        "updates": [update],
    }


def sanitize_signalk_meta(meta):
    if not isinstance(meta, dict):
        return None
    cleaned = {}
    for key, value in meta.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        cleaned[str(key)] = value
    return cleaned or None


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


def init_influx_runtime(cfg: dict):
    runtime["config"] = cfg
    if not cfg.get("enable_influx") or cfg.get("dry"):
        return None

    influx_client = runtime.get("influx_client")
    if influx_client is not None:
        return influx_client

    influx_client = InfluxDBClient(
        url=cfg["influxdb_url"],
        token=cfg["influxdb_token"],
        org=cfg["influxdb_org"],
    )
    runtime["influx_client"] = influx_client
    runtime["write_api"] = influx_client.write_api(write_options=SYNCHRONOUS)
    return influx_client

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
    # paho re-raises callback errors out of loop_forever(), stopping the collector.
    try:
        _process_message(message)
    except Exception:
        logger.exception("Unhandled error while processing topic=%s", getattr(message, "topic", "?"))


def _process_message(message):
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
            **flat_fields,
            "timestamp": dt.isoformat().replace("+00:00", "Z"),
            "topic": topic,
            "uuid": instrument_uuid,
        }

        try:
            if cfg["dry"]:
                csv_path = runtime["csv_storage"]._file_path(instrument_uuid, dt)
                logger.info("DRY STORAGE file=%s row=%s", csv_path, storage_row)
            else:
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
                logger.warning("Signal K publish failed for topic=%s: %s", topic, e)
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


def main():
    args = parse_args()
    cfg = load_config(args.config, args)

    logging.getLogger().setLevel(getattr(logging, cfg["log_level"], logging.INFO))
    runtime["config"] = cfg

    if cfg["enable_influx"] and not cfg["dry"]:
        init_influx_runtime(cfg)
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
