# sensor-network-collector

MQTT collector for weather/sensor networks with pluggable outputs:

- InfluxDB
- Signal K (websocket deltas)
- Hourly CSV storage

It supports flat JSON payloads and GeoJSON `Feature` payloads (as emitted by `vantage-publisher`).

## Features

- MQTT subscriber with auto-reconnect
- Multiple outputs enabled at runtime via CLI
- Signal K delta generation from MQTT topics + payload keys
- Hourly rotated CSV files by instrument UUID
- Dry mode (`--dry`) for safe validation and logging-only runs

## Requirements

- Python 3.9+
- MQTT broker reachable from this host
- Optional:
  - InfluxDB v2 API
  - Signal K server (for websocket forwarding)

Install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

`main.py` always reads MQTT from `--config`. Outputs are selected by CLI.

```bash
python3 main.py --config config.json
```

### Output selection

- `--influxdb`: enable InfluxDB output
- `--signalk`: enable Signal K output
- `--storage <ROOT_PATH>`: enable CSV storage under `ROOT_PATH`
- `--dry`: no writes/connections for selected outputs; generated data is logged

You can combine outputs:

```bash
# InfluxDB + Signal K + CSV
python3 main.py --config config.json --influxdb --signalk --storage /data/collector

# Signal K only
python3 main.py --config config.json --signalk

# Signal K + storage in dry mode (no real writes)
python3 main.py --config config.json --signalk --storage /data/collector --dry
```

Notes:

- If no output flags are passed, runtime falls back to config booleans (`influxdb_enabled`, `signalk_enabled`, `storage_root`) for backward compatibility.
- In dry mode, MQTT intake is active, but sink writes are replaced by logs.

## Configuration (`config.json`)

Core keys:

- `mqtt_address` (required)
- `mqtt_port` (default `1883`)
- `mqtt_user` / `mqtt_password` (optional)
- `mqtt_topic` (default `#`)
- `skip_empty_fields` (`1` default)

Influx keys (required only if Influx output is enabled):

- `influxdb_url`
- `influxdb_token`
- `influxdb_org`
- `influxdb_bucket`
- `influx_measurement` (default `mqtt_data`)

Signal K keys (required only if Signal K output is enabled):

- `signalk_server_url` (example: `ws://signalk.local:3000/signalk/v1/stream`)
- `signalk_token` (optional)
- `signalk_context_prefix` (default `meteo`)
- `signalk_source_label` (default `sensor-network-collector`)
- `signalk_path_map` (optional key -> Signal K path overrides)

Optional default output controls:

- `influxdb_enabled` (`1`/`0`)
- `signalk_enabled` (`1`/`0`)
- `storage_root` (path or `null`)

## Payload support

### Flat JSON

```json
{
  "Datetime": "2026-02-24T10:15:40Z",
  "uuid": "it.uniparthenope.meteo.ws1",
  "TempOut": 12.7,
  "HumOut": 62,
  "WindSpeed": 3.4,
  "position": { "latitude": 40.8569, "longitude": 14.2845 }
}
```

### GeoJSON Feature

```json
{
  "type": "Feature",
  "geometry": {
    "type": "Point",
    "coordinates": [14.2845, 40.8569]
  },
  "properties": {
    "uuid": "it.uniparthenope.meteo.ws1",
    "Datetime": "2026-02-24T10:15:40Z",
    "TempOut": 12.7
  }
}
```

## Signal K mapping

The collector builds Signal K deltas (`context` + `updates[].values[]`) and forwards them to websocket `/signalk/v1/stream`.

Mapping rules:

- MQTT topic -> Signal K `context`:
  - `context = <signalk_context_prefix>.<topic with '/' mapped to '.'>`
- Payload key -> Signal K `path`:
  - `signalk_path_map[key]` if configured
  - standard weather mapping (same convention used in `vantage-publisher`), including:
    - `TempOut -> environment.outside.temperature`
    - `HumOut -> environment.outside.humidity`
    - `Barometer -> environment.outside.pressure`
    - `WindSpeed -> environment.wind.speedApparent`
    - `WindDir -> environment.wind.angleApparent`
  - fallback: `environment.<sanitized_key>`

Also:

- If position is available (`position` object or GeoJSON coordinates), `navigation.position` is emitted.
- Standard conversions are applied for common weather keys:
  - `TempOut`, `TempIn`: C -> K
  - `Barometer`: hPa -> Pa
  - `HumOut`, `HumIn`: `%` -> ratio
  - `WindDir`: degrees -> radians

## CSV storage layout

When `--storage <ROOT_PATH>` is enabled, each message is stored in:

`/UUID/YYYY/MM/DD/UUID_YYYYMMDDZHH00.csv`

Where:

- `UUID` is instrument UUID from payload (`uuid`) if available, otherwise topic-derived
- file rotates every hour (`HH`)
- if collector restarts within the same hour, it appends to the same hourly file

## Architecture

Runtime components:

1. MQTT intake
2. Payload normalizer (flat JSON / GeoJSON)
3. Timestamp resolver (`Datetime` -> `DatetimeWS` -> now UTC)
4. Output sinks (InfluxDB, Signal K, CSV)

### Data flow

```text
MQTT broker
  -> sensor-network-collector (subscribe topic filter)
    -> parse/normalize payload
      -> Influx sink (optional)
      -> Signal K sink (optional)
      -> CSV hourly sink (optional)
```

## Example: many `vantage-publisher` stations -> flat MQTT -> Signal K server

Scenario:

- many `vantage-publisher` instances publish flat JSON on topics such as:
  - `it.uniparthenope.meteo.ws1`
  - `it.uniparthenope.meteo.ws2`
  - `it.uniparthenope.meteo.ws3`
- this collector subscribes to `it.uniparthenope.meteo/#`
- it forwards all stations to one Signal K server

Example `config.json`:

```json
{
  "log_level": "INFO",
  "mqtt_address": "mqtt-broker.local",
  "mqtt_port": 1883,
  "mqtt_topic": "it.uniparthenope.meteo/#",

  "signalk_server_url": "ws://signalk.local:3000/signalk/v1/stream",
  "signalk_token": "",
  "signalk_context_prefix": "meteo",
  "signalk_source_label": "sensor-network-collector",

  "signalk_path_map": {
    "TempOut": "environment.outside.temperature",
    "HumOut": "environment.outside.humidity",
    "Barometer": "environment.outside.pressure",
    "WindSpeed": "environment.wind.speedApparent",
    "WindDir": "environment.wind.angleApparent"
  },

  "influxdb_enabled": 0,
  "signalk_enabled": 1,
  "storage_root": null
}
```

Run:

```bash
python3 main.py --config config.json --signalk
```

Examples of context mapping:

- topic `it.uniparthenope.meteo.ws1` -> context `meteo.it.uniparthenope.meteo.ws1`
- topic `it/uniparthenope/meteo/ws2` -> context `meteo.it.uniparthenope.meteo.ws2`

## Validation

```bash
python3 -m py_compile main.py
python3 main.py --config config.json --signalk --dry
```

## License

Apache-2.0
