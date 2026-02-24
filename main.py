import argparse
import csv
import json
import logging
import math
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


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


# ----------------------------
# Config helpers
# ----------------------------
def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def get_required_value(config: dict, key: str, env_name: str) -> str:
    value = config.get(key)
    if value is None or value == "":
        return require_env(env_name)
    return str(value)


def get_optional_value(config: dict, key: str, env_name: str, default=None):
    value = config.get(key)
    if value is not None:
        return value
    return os.getenv(env_name, default)


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


def parse_args():
    parser = argparse.ArgumentParser(description="MQTT collector with pluggable outputs")
    parser.add_argument("--config", required=True, help="Path to JSON configuration file")
    parser.add_argument(
        "--influxdb",
        action="store_true",
        help="Enable InfluxDB output (if no output flags are passed, this is enabled by default)",
    )
    parser.add_argument(
        "--signalk",
        action="store_true",
        help="Enable Signal K output to websocket server",
    )
    parser.add_argument(
        "--storage",
        metavar="ROOT_PATH",
        help="Enable CSV storage and write files under ROOT_PATH",
    )
    parser.add_argument(
        "--dry",
        action="store_true",
        help="Dry mode: do not write to InfluxDB/Signal K/CSV, log generated outputs instead",
    )
    return parser.parse_args()


def load_raw_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise RuntimeError("Config file must contain a JSON object")
    return raw


def determine_outputs(args, raw: dict):
    # Default behavior is backwards-compatible: if no output option is specified,
    # keep writing to InfluxDB.
    explicit = args.influxdb or args.signalk or bool(args.storage)

    enable_influx = bool(args.influxdb)
    enable_signalk = bool(args.signalk)
    storage_root = args.storage

    if not explicit:
        enable_influx = parse_boolish(get_optional_value(raw, "influxdb_enabled", "INFLUXDB_ENABLED", "1"), True)
        enable_signalk = parse_boolish(get_optional_value(raw, "signalk_enabled", "SIGNALK_ENABLED", "0"), False)
        storage_root = get_optional_value(raw, "storage_root", "STORAGE_ROOT", None)

    if not enable_influx and not enable_signalk and not storage_root:
        raise RuntimeError("No outputs enabled. Use --influxdb, --signalk, and/or --storage <ROOT_PATH>")

    return {
        "enable_influx": enable_influx,
        "enable_signalk": enable_signalk,
        "storage_root": storage_root,
    }


def load_config(path: str, args) -> dict:
    raw = load_raw_config(path)
    outputs = determine_outputs(args, raw)

    cfg = {
        "log_level": str(get_optional_value(raw, "log_level", "LOG_LEVEL", "INFO")).upper(),
        "mqtt_address": get_required_value(raw, "mqtt_address", "MQTT_ADDRESS"),
        "mqtt_port": int(get_optional_value(raw, "mqtt_port", "MQTT_PORT", "1883")),
        "mqtt_user": get_optional_value(raw, "mqtt_user", "MQTT_USER", None),
        "mqtt_password": get_optional_value(raw, "mqtt_password", "MQTT_PASSWORD", None),
        "mqtt_topic": str(get_optional_value(raw, "mqtt_topic", "MQTT_TOPIC", "#")),
        "skip_empty_fields": str(get_optional_value(raw, "skip_empty_fields", "SKIP_EMPTY_FIELDS", "1")) == "1",
        "influx_measurement": str(
            get_optional_value(raw, "influx_measurement", "INFLUX_MEASUREMENT", "mqtt_data")
        ),
        "dry": bool(args.dry),
        **outputs,
    }

    if cfg["enable_influx"]:
        cfg.update(
            {
                "influxdb_url": get_required_value(raw, "influxdb_url", "INFLUXDB_URL"),
                "influxdb_token": get_required_value(raw, "influxdb_token", "INFLUXDB_TOKEN"),
                "influxdb_org": get_required_value(raw, "influxdb_org", "INFLUXDB_ORG"),
                "influxdb_bucket": get_required_value(raw, "influxdb_bucket", "INFLUXDB_BUCKET"),
            }
        )

    if cfg["enable_signalk"]:
        cfg["signalk_server_url"] = get_required_value(raw, "signalk_server_url", "SIGNALK_SERVER_URL")
        cfg["signalk_token"] = str(get_optional_value(raw, "signalk_token", "SIGNALK_TOKEN", "") or "")
        cfg["signalk_context_prefix"] = str(
            get_optional_value(raw, "signalk_context_prefix", "SIGNALK_CONTEXT_PREFIX", "meteo")
        )
        raw_map = get_optional_value(raw, "signalk_path_map", "SIGNALK_PATH_MAP", {})
        if isinstance(raw_map, str):
            try:
                raw_map = json.loads(raw_map)
            except json.JSONDecodeError:
                logger.warning("Invalid JSON in SIGNALK_PATH_MAP; ignoring custom path map")
                raw_map = {}
        cfg["signalk_path_map"] = {
            str(k): str(v) for k, v in (raw_map.items() if isinstance(raw_map, dict) else []) if v
        }
        cfg["signalk_source_label"] = str(
            get_optional_value(raw, "signalk_source_label", "SIGNALK_SOURCE_LABEL", "sensor-network-collector")
        )

    return cfg


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


def utc_now_iso():
    return utc_now().isoformat().replace("+00:00", "Z")


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
    # Reuse same conversions as vantage-publisher for standard weather fields.
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
    for k in ("uuid", "UUID", "station_uuid"):
        v = data.get(k) if isinstance(data, dict) else None
        if isinstance(v, str) and v.strip():
            return v.strip()

    tag_uuid = tags.get("uuid")
    if isinstance(tag_uuid, str) and tag_uuid.strip():
        return tag_uuid.strip()

    return topic.replace("/", "_") if topic else "unknown"


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
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        hour = dt.strftime("%H")

        day_dir = self.root / instrument_uuid / year / month / day
        filename = f"{instrument_uuid}_{dt.strftime('%Y%m%d')}Z{hour}00.csv"
        return day_dir / filename

    def _read_header(self, csv_path: Path):
        if not csv_path.exists() or csv_path.stat().st_size == 0:
            return []
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            return next(reader, [])

    def _ensure_schema(self, csv_path: Path, row_keys):
        existing = self._read_header(csv_path)
        if not existing:
            return list(row_keys), False

        new_cols = [c for c in row_keys if c not in existing]
        if not new_cols:
            return existing, False

        merged = existing + new_cols

        with csv_path.open("r", newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))

        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=merged)
            writer.writeheader()
            for old in rows:
                out = {k: old.get(k, "") for k in merged}
                writer.writerow(out)

        return merged, True

    def write(self, instrument_uuid: str, dt: datetime, row: dict):
        csv_path = self._file_path(instrument_uuid, dt)
        csv_path.parent.mkdir(parents=True, exist_ok=True)

        ordered_row = dict(sorted(row.items(), key=lambda kv: kv[0]))
        fieldnames, _ = self._ensure_schema(csv_path, ordered_row.keys())

        file_exists = csv_path.exists() and csv_path.stat().st_size > 0
        with csv_path.open("a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if not file_exists:
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

        if value is None:
            continue
        if isinstance(value, (dict, list)):
            continue

        values.append({"path": path, "value": value})

    if not values:
        return None

    update = {
        "timestamp": dt.isoformat().replace("+00:00", "Z"),
        "$source": cfg.get("signalk_source_label", "sensor-network-collector"),
        "values": values,
    }

    return {"context": context, "updates": [update]}


# ----------------------------
# Runtime state
# ----------------------------
runtime = {
    "config": {},
    "influx_client": None,
    "write_api": None,
    "signalk_client": None,
    "csv_storage": None,
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
    if cfg["storage_root"]:
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
                logger.info("Signal K write topic=%s context=%s values=%d", topic, delta["context"], len(delta["updates"][0]["values"]))
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

    if cfg["storage_root"]:
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

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    if cfg["mqtt_user"] is not None:
        mqtt_client.username_pw_set(username=cfg["mqtt_user"], password=cfg["mqtt_password"])

    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

    logger.info("Connecting to MQTT %s:%d", cfg["mqtt_address"], cfg["mqtt_port"])
    mqtt_client.connect(cfg["mqtt_address"], cfg["mqtt_port"])
    mqtt_client.loop_forever()


if __name__ == "__main__":
    main()
