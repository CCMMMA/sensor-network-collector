# sensor-network-collector

Threaded MQTT collector for weather/sensor networks, aligned with `vantage-publisher` configuration style.

## Features

- MQTT subscriber with auto-reconnect
- Multiple output sinks:
  - InfluxDB
  - Signal K websocket deltas with retry backoff on transient gateway failures
  - hourly CSV storage
- Dry mode for safe runtime validation (`--dry`)
- Web GUI for data download with authentication and access policies
- Map-based station discovery on the home page
- Data browsing per station with configurable charts and tables
- Station browser table supports per-column header toggles and window statistics
- Live station trend pages (auto-updating charts and latest table)
- Public sensor network dashboard with auto-updating status and alarms
- Per-station public trend-chart Y-axis configuration with controller/admin management
- JSON export/import for station trend-chart setup
- Public station trend charts update incrementally, support full-screen focus with branding, and show per-window min/max/current widgets
- Public station dashboard includes a combined Wind trend chart with wind direction and wind speed on separate Y-axes
- Watchdog anomaly detection with persisted anomaly log (SQLite)
- SMTP notifications (welcome, registration, alarms with fast-login link)
  - if `smtpHost` is not configured, emails are skipped
  - if `smtpPort`/`smtpUser`/`smtpPass` are omitted, SMTP defaults to unauthenticated port `25`
- Admin-enforced password change and user self-service password update
- Self-service forgot-password and reset-by-email flow
- Web app logo and per-station logo upload/display
- Configurable home-page external links for logo and info

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
- How to join with a Vantage Pro2 edge node: [link](docs/how_to_join.md)

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
  - station chart settings + chart-control rights
  - watchdog notifications
  - access-controlled downloads
  - live station trend pages
  - public network dashboard
  - public station dashboard with selectable trend windows persisted in cookies and per-station Y-axis settings
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
  -> station chart settings store (optional)
    -> per-station y-axis min/max/step overrides + JSON import/export
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

- topic `it.uniparthenope.meteo.ws1` -> `meteo.it_uniparthenope_meteo_ws1`
- topic `it/uniparthenope/meteo/ws2` -> `meteo.it.uniparthenope.meteo.ws2`
- if payload contains `name`, it is also sent to Signal K as path `name` inside the computed context

## Validation

```bash
python3 -m py_compile main.py
python3 main.py --config config.json --dry
```

## Units and normalization

- CSV storage and InfluxDB keep the raw publisher units as received on MQTT.
- Signal K output applies protocol-oriented conversions before publish:
  - temperature -> Kelvin
  - pressure -> Pa
  - humidity percent -> ratio
  - angular values -> radians
  - rain totals / evapotranspiration totals -> meters
  - rain rate -> meters per second

See:

- storage units and CSV behavior: [link](docs/storage.md)
- InfluxDB record behavior: [link](docs/influxdb.md)
- Signal K conversion rules: [link](docs/signalk.md)

## Web UI notes

- `baseUrl` is used to compose externally visible links in email messages, including fast-login, onboarding, and password-reset URLs.
- Account requests are handled in two steps: request by email/reason, then admin approval, then onboarding with a unique username and strong password.
- Public station dashboard trend-window choice is remembered in browser cookies.
- Station dashboard trend interval choice is also remembered in browser cookies.
- Station browser chart and table preferences are persisted per station and can be exported/imported as JSON.
- The auth SQLite database also stores chart-control rights and per-station trend-chart axis settings used by the Public Station Dashboard.
- Public station dashboard supports `focus=<chart_key>` in the query string to open a specific trend chart in focused full-screen mode.
- Public station dashboard trend charts show the full date on the x-axis when the displayed day changes.
- Home-page logo can link to `webAppLink`, and the unauthenticated nav can show `Info` from `webInfoLink`.
- In focused mode, the chart title includes the selected trend window and the header shows the station name without UUID.

## Security notes

- Change `adminPassword` immediately.
- Set a strong `webSessionSecret` in production.
- Serve GUI behind TLS/reverse proxy in production deployments.

## License

Apache-2.0
