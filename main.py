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
# Env helpers
# ----------------------------
def require_env(name: str) -> str:
    v = os.environ.get(name)
    if not v:
        raise RuntimeError(f"Missing required env var: {name}")
    return v


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


# ----------------------------
# Configuration
# ----------------------------
INFLUXDB_URL = require_env("INFLUXDB_URL")
INFLUXDB_TOKEN = require_env("INFLUXDB_TOKEN")
INFLUXDB_ORG = require_env("INFLUXDB_ORG")
INFLUXDB_BUCKET = require_env("INFLUXDB_BUCKET")

MQTT_ADDRESS = require_env("MQTT_ADDRESS")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

MQTT_TOPIC = os.getenv("MQTT_TOPIC", "#")
MEASUREMENT = os.getenv("INFLUX_MEASUREMENT", "mqtt_data")

# How strict: if no numeric/string/bool fields remain, skip write
SKIP_EMPTY_FIELDS = os.getenv("SKIP_EMPTY_FIELDS", "1") == "1"


# ----------------------------
# InfluxDB client
# ----------------------------
influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
write_api = influx_client.write_api(write_options=SYNCHRONOUS)


# ----------------------------
# MQTT callbacks
# ----------------------------
def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        logger.info("Connected to MQTT broker.")
        client.subscribe(MQTT_TOPIC)
        logger.info("Subscribed to topic: %s", MQTT_TOPIC)
    else:
        logger.error("MQTT connection failed. reason_code=%s", reason_code)


def on_message(client, userdata, message):
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

    # Extract fields (Influx fields must be scalar; tags are strings)
    fields = {}
    for k, v in payload.items():
        # ignore nested objects/arrays/null
        if v is None:
            continue
        if isinstance(v, (bool, int, float, str)):
            fields[k] = v
        elif isinstance(v, dict):
            fields[k] = json.dumps(v)

    if SKIP_EMPTY_FIELDS and not fields:
        logger.debug("No writable fields on topic=%s, skipping.", topic)
        return

    # Time handling
    dt = None
    if "Datetime" in payload:
        dt = parse_iso8601_or_none(payload.get("Datetime"))
        if dt is None:
            logger.debug("Invalid Datetime=%r on topic=%s; using utc now.", payload.get("Datetime"), topic)

    dt = dt or utc_now()

    record = {
        "measurement": MEASUREMENT,
        "tags": {"topic": topic},
        "fields": fields,
        "time": dt,  # influxdb-client accepts datetime
    }

    try:
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=record)
        logger.info("Wrote point topic=%s fields=%d time=%s: %s", topic, len(fields), dt.isoformat(), fields)
    except Exception as e:
        logger.exception("Influx write failed for topic=%s: %s", topic, e)


# ----------------------------
# Graceful shutdown
# ----------------------------
def shutdown(signum, frame):
    logger.info("Shutting down (signal=%s)...", signum)
    try:
        influx_client.close()
    except Exception:
        pass
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ----------------------------
# MQTT client setup
# ----------------------------
mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

if MQTT_USER is not None:
    mqtt_client.username_pw_set(username=MQTT_USER, password=MQTT_PASSWORD)

# Auto-reconnect backoff (seconds)
mqtt_client.reconnect_delay_set(min_delay=1, max_delay=60)

logger.info("Connecting to MQTT %s:%d ...", MQTT_ADDRESS, MQTT_PORT)
mqtt_client.connect(MQTT_ADDRESS, MQTT_PORT)

mqtt_client.loop_forever()
