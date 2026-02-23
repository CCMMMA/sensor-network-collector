# sensor-network-collector

MQTT to InfluxDB collector for the `meteo@uniparthenope` sensor network.

It subscribes to MQTT topics, parses JSON payloads (including GeoJSON packets from `vantage-publisher-threading.py`), and writes points to InfluxDB.

## Features

- MQTT subscriber with auto-reconnect
- InfluxDB synchronous writes
- Supports:
  - flat JSON payloads
  - GeoJSON `Feature` payloads with sensor data in `properties`
- Timestamp handling from payload (`Datetime` / `DatetimeWS`) with UTC fallback

## Requirements

- Python 3.9+ (recommended)
- Access to:
  - an MQTT broker
  - an InfluxDB instance (v2 API)

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration file (`--config`)

`main.py` is configured via a JSON file passed with `--config`.

An example file is included:

- [`config.json`](/Users/raffaelemontella/Documents/New project/sensor-network-collector/config.json)

Keys:

- `log_level`: e.g. `INFO`, `DEBUG` (default: `INFO`)
- `influxdb_url` (required): e.g. `http://localhost:8086`
- `influxdb_token` (required): InfluxDB API token
- `influxdb_org` (required): InfluxDB organization
- `influxdb_bucket` (required): destination bucket
- `mqtt_address` (required): MQTT broker hostname/IP
- `mqtt_port` (default: `1883`)
- `mqtt_user` (optional)
- `mqtt_password` (optional)
- `mqtt_topic` (default: `#`)
- `influx_measurement` (default: `mqtt_data`)
- `skip_empty_fields` (default: `1`)
  - `1`: skip write if payload has no valid scalar fields
  - `0`: allow writing even with empty field set (not recommended)

Note: if a key is omitted in JSON, matching environment variables are still accepted as fallback.

## Quick start

```bash
cp config.json config.local.json
# edit config.local.json with your real values

python3 main.py --config config.local.json
```

## Packet formats

### 1) Flat JSON payload

Example:

```json
{
  "Datetime": "2026-02-23T12:34:56Z",
  "temperature": 21.4,
  "humidity": 62
}
```

### 2) GeoJSON `Feature` payload (from vantage-publisher-threading.py)

Example:

```json
{
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [14.2681, 40.8518]
  },
  "properties": {
    "uuid": "station-001",
    "name": "Napoli Station",
    "Datetime": "2026-02-23T12:34:56Z",
    "temp_out": 18.7
  }
}
```

For GeoJSON packets:
- Influx fields come from `properties`
- `uuid`, `name`, `latitude`, `longitude` are written as tags (when present)

## How timestamps are chosen

1. `Datetime` (if present and valid ISO-8601)
2. `DatetimeWS` (if present and valid)
3. Current UTC time

Accepted examples:
- `2026-02-18T12:34:56Z`
- `2026-02-18T12:34:56+01:00`
- `2026-02-18T12:34:56`

## Run as a service (example with systemd)

Create a service unit that exports the same environment variables and runs:

```bash
python3 /path/to/sensor-network-collector/main.py --config /path/to/config.json
```

## Troubleshooting

- `Missing required env var: ...`
  - A required config key is missing and no env fallback is available.
- `Invalid JSON on topic=...`
  - Publisher payload is not valid JSON.
- `JSON payload is not an object`
  - Root JSON value must be an object.
- `Influx write failed`
  - Check URL/token/org/bucket and network reachability.

## Development quick check

```bash
python3 -m py_compile main.py
```
