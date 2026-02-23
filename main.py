import argparse
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone

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
logger = logging.getLogger("mqtt_influx_bridge")


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


def parse_args():
    parser = argparse.ArgumentParser(description="MQTT to InfluxDB collector")
    parser.add_argument("--config", required=True, help="Path to JSON configuration file")
    return parser.parse_args()


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, dict):
        raise RuntimeError("Config file must contain a JSON object")

    cfg = {
        "log_level": str(get_optional_value(raw, "log_level", "LOG_LEVEL", "INFO")).upper(),
        "influxdb_url": get_required_value(raw, "influxdb_url", "INFLUXDB_URL"),
        "influxdb_token": get_required_value(raw, "influxdb_token", "INFLUXDB_TOKEN"),
        "influxdb_org": get_required_value(raw, "influxdb_org", "INFLUXDB_ORG"),
        "influxdb_bucket": get_required_value(raw, "influxdb_bucket", "INFLUXDB_BUCKET"),
        "mqtt_address": get_required_value(raw, "mqtt_address", "MQTT_ADDRESS"),
        "mqtt_port": int(get_optional_value(raw, "mqtt_port", "MQTT_PORT", "1883")),
        "mqtt_user": get_optional_value(raw, "mqtt_user", "MQTT_USER", None),
        "mqtt_password": get_optional_value(raw, "mqtt_password", "MQTT_PASSWORD", None),
        "mqtt_topic": str(get_optional_value(raw, "mqtt_topic", "MQTT_TOPIC", "#")),
        "influx_measurement": str(
            get_optional_value(raw, "influx_measurement", "INFLUX_MEASUREMENT", "mqtt_data")
        ),
        "skip_empty_fields": str(get_optional_value(raw, "skip_empty_fields", "SKIP_EMPTY_FIELDS", "1")) == "1",
    }
    return cfg


def parse_iso8601_or_none(value: str):
    """
    Accepts:
      - 2026-02-18T12:34:56Z
      - 2026-02-18T12:34:56+01:00
      - 2026-02-18T12:34:56
    Returns:
      datetime (aware, UTC) or None
    """
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip()
    try:
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        # make aware if naive
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def utc_now():
    return datetime.now(timezone.utc)


def extract_payload_fields(payload):
    """
    Normalize supported packet shapes into:
      - data object containing sensor values
      - optional tags
      - optional event timestamp candidate

    Supported shapes:
      1) Flat JSON object
      2) GeoJSON Feature object produced by vantage-publisher-threading.py
    """
    tags = {}
    event_time_value = None
    data = payload

    # GeoJSON Feature format:
    # {"type":"Feature","geometry":{"type":"Point","coordinates":[lon,lat]},"properties":{...}}
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

        # Publisher includes station identity in properties
        for key in ("uuid", "name"):
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                tags[key] = value.strip()

    if not isinstance(data, dict):
        return None, tags, event_time_value

    # Prefer collector timestamp set by publisher; fall back to DatetimeWS if present
    event_time_value = data.get("Datetime") or data.get("DatetimeWS")
    return data, tags, event_time_value


# ----------------------------
# Runtime state
# ----------------------------
runtime = {
    "config": {},
    "influx_client": None,
    "write_api": None,
}


# ----------------------------
# MQTT callbacks
# ----------------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    cfg = runtime["config"]
    if reason_code == 0:
        logger.info("Connected to MQTT broker.")
        client.subscribe(cfg["mqtt_topic"])
        logger.info("Subscribed to topic: %s", cfg["mqtt_topic"])
    else:
        logger.error("MQTT connection failed. reason_code=%s", reason_code)


def on_message(client, userdata, message):
    cfg = runtime["config"]
    write_api = runtime["write_api"]
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

    # Extract fields (Influx fields must be scalar; tags are strings)
    fields = {}
    for k, v in source_data.items():
        # ignore nested objects/arrays/null
        if v is None:
            continue
        if isinstance(v, (bool, int, float, str)):
            fields[k] = v
        elif isinstance(v, dict):
            fields[k] = json.dumps(v)

    if cfg["skip_empty_fields"] and not fields:
        logger.debug("No writable fields on topic=%s, skipping.", topic)
        return

    # Time handling
    dt = None
    if event_time_value:
        dt = parse_iso8601_or_none(event_time_value)
        if dt is None:
            logger.debug("Invalid Datetime=%r on topic=%s; using utc now.", event_time_value, topic)

    dt = dt or utc_now()

    record = {
        "measurement": cfg["influx_measurement"],
        "tags": {"topic": topic, **extracted_tags},
        "fields": fields,
        "time": dt,  # influxdb-client accepts datetime
    }

    try:
        write_api.write(bucket=cfg["influxdb_bucket"], org=cfg["influxdb_org"], record=record)
        logger.info("Wrote point topic=%s fields=%d time=%s: %s", topic, len(fields), dt.isoformat(), fields)
    except Exception as e:
        logger.exception("Influx write failed for topic=%s: %s", topic, e)


# ----------------------------
# Graceful shutdown
# ----------------------------
def shutdown(signum, frame):
    logger.info("Shutting down (signal=%s)...", signum)
    try:
        influx_client = runtime.get("influx_client")
        if influx_client is not None:
            influx_client.close()
    except Exception:
        pass
    sys.exit(0)

def main():
    args = parse_args()
    cfg = load_config(args.config)

    logging.getLogger().setLevel(getattr(logging, cfg["log_level"], logging.INFO))
    runtime["config"] = cfg

    influx_client = InfluxDBClient(
        url=cfg["influxdb_url"],
        token=cfg["influxdb_token"],
        org=cfg["influxdb_org"],
    )
    runtime["influx_client"] = influx_client
    runtime["write_api"] = influx_client.write_api(write_options=SYNCHRONOUS)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    if cfg["mqtt_user"] is not None:
        mqtt_client.username_pw_set(username=cfg["mqtt_user"], password=cfg["mqtt_password"])

    # Auto-reconnect backoff (seconds)
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

    logger.info("Connecting to MQTT %s:%d ...", cfg["mqtt_address"], cfg["mqtt_port"])
    mqtt_client.connect(cfg["mqtt_address"], cfg["mqtt_port"])
    mqtt_client.loop_forever()


if __name__ == "__main__":
    main()
