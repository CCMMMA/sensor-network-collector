# sensor-network-collector

Threaded MQTT collector for weather/sensor networks, aligned with `vantage-publisher` configuration style.

## Features

- MQTT subscriber with auto-reconnect
- Multiple output sinks:
  - InfluxDB
  - Signal K websocket deltas
  - hourly CSV storage
- Dry mode for safe runtime validation (`--dry`)
- Web GUI for data download with authentication and access policies
- Map-based station discovery on the home page
- Data browsing per station with charts and tables
- Live station trend pages (auto-updating charts and latest table)
- Public sensor network dashboard with auto-updating status and alarms
- Watchdog anomaly detection with persisted anomaly log (SQLite)
- SMTP notifications (welcome, registration, alarms with fast-login link)
- Admin-enforced password change and user self-service password update
- Web app logo and per-station logo upload/display

## Configuration Style (Homogeneous with `vantage-publisher`)

The collector now accepts the same naming style used in `vantage-publisher` (`mqttBroker`, `mqttPort`, `signalkServerUrl`, `pathStorage`, `httpEnabled`, etc.).

Backward-compatible snake_case keys are still accepted.

## Requirements

- Python 3.9+
- MQTT broker
- Optional:
  - InfluxDB v2
  - Signal K server

Install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## CLI

```bash
python3 main.py --config config.json
```

Options:

- `--config <path>` config file path (default `config.json`)
- `--signalk true|false` override config `signalk`
- `--influxdb true|false` override config `influxdb`
- `--storage true|false|<path>`
  - `true/false`: enable/disable storage (uses configured `pathStorage`)
  - `<path>`: enable storage and override `pathStorage`
- `--http true|false` override config `httpEnabled`
- `--dry` dry mode (no actual sink writes; log generated outputs)

Examples:

```bash
# Normal run (uses config booleans)
python3 main.py --config config.json

# Force only Signal K + storage path override
python3 main.py --config config.json --influxdb false --signalk true --storage /data/collector

# Dry validation of all enabled sinks
python3 main.py --config config.json --dry
```

## Documentation

Detailed documentation has been split into dedicated pages:

- Docker deployment: [link](docs/docker.md)
- Production WSGI setup: [link](docs/wsgi.md)
- Configuration reference: [link](docs/configuration.md)
- Signal K forwarding: [link](docs/signalk.md)
- CSV storage: [link](docs/storage.md)
- Web GUI and policies: [link](docs/webgui.md)
- InfluxDB sink: [link](docs/influxdb.md)

## Architecture

Components:

1. MQTT intake (`paho-mqtt`)
2. payload normalization (flat JSON / GeoJSON)
3. timestamp resolver (`Datetime` -> `DatetimeWS` -> current UTC)
4. sink fan-out:
  - InfluxDB writer
  - Signal K websocket publisher
  - CSV hourly storage
5. optional Flask web GUI:
  - SQLite auth/policy store
  - anomaly store + silence state
  - watchdog notifications
  - access-controlled downloads
  - live station trend pages
  - public network dashboard
  - account emails + fast-login links
  - branding (app logo + station logo)

## Data Flow

```text
MQTT broker
  -> sensor-network-collector
    -> normalize payload + timestamp
      -> InfluxDB sink (optional)
      -> Signal K sink (optional)
      -> CSV sink (optional)

CSV sink (pathStorage)
  -> web GUI download portal (optional)
    -> policy checks (open/account/restricted)
      -> ZIP download for selected instruments/date range
  -> station trend API (optional)
    -> auto-updating chart/table in browser
  -> public dashboard API (optional)
    -> auto-updating network status and alarms
  -> watchdog/anomaly detector (optional)
    -> anomaly DB log + notification emails + silence window
```

## Example: Many `vantage-publisher` -> flat MQTT -> Signal K

Assume many publishers send flat data on topics:

- `it.uniparthenope.meteo.ws1`
- `it.uniparthenope.meteo.ws2`
- `it.uniparthenope.meteo.ws3`

Example config:

```json
{
  "mqttBroker": "mqtt-broker.local",
  "mqttPort": 1883,
  "mqttTopic": "it.uniparthenope.meteo/#",

  "storage": true,
  "pathStorage": "/data/sensor-network-collector",

  "influxdb": false,

  "signalk": true,
  "signalkServerUrl": "ws://signalk.local:3000/signalk/v1/stream",
  "signalkContextPrefix": "meteo",
  "signalkPathMap": {
    "TempOut": "environment.outside.temperature",
    "HumOut": "environment.outside.relativeHumidity",
    "Barometer": "environment.outside.pressure",
    "WindSpeed": "environment.wind.speedApparent",
    "WindDir": "environment.wind.angleApparent"
  },

  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "authDbPath": "/data/sensor-network-collector/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "change-me"
}
```

Run:

```bash
python3 main.py --config config.json
```

Context examples:

- topic `it.uniparthenope.meteo.ws1` -> `meteo.it.uniparthenope.meteo.ws1`
- topic `it/uniparthenope/meteo/ws2` -> `meteo.it.uniparthenope.meteo.ws2`

## Validation

```bash
python3 -m py_compile main.py
python3 main.py --config config.json --dry
```

## Security notes

- Change `adminPassword` immediately.
- Set a strong `webSessionSecret` in production.
- Serve GUI behind TLS/reverse proxy in production deployments.

## License

Apache-2.0
