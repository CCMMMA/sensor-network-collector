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

## Deployment with Docker

Use the provided:

- [`Dockerfile`](/Users/raffaelemontella/Documents/New project/sensor-network-collector/Dockerfile)
- [`docker-compose.yml`](/Users/raffaelemontella/Documents/New project/sensor-network-collector/docker-compose.yml)

### Step 1: Prerequisites

Install:

- Docker Engine
- Docker Compose v2 (`docker compose`)

Verify:

```bash
docker --version
docker compose version
```

### Step 2: Prepare local files

From the project root:

```bash
mkdir -p data
cp config.json config.local.json
```

Then edit `config.local.json` for your environment.

Recommended values for Docker:

```json
{
  "mqttBroker": "your-mqtt-host",
  "mqttPort": 1883,
  "pathStorage": "/data/storage",
  "authDbPath": "/data/collector_auth.sqlite",
  "httpHost": "0.0.0.0",
  "httpPort": 8080
}
```

Important networking note:

- Do not use `localhost` for remote MQTT/InfluxDB services.
- Use a reachable hostname/IP from inside the container.

### Step 3: Point compose to your config

Edit [`docker-compose.yml`](/Users/raffaelemontella/Documents/New project/sensor-network-collector/docker-compose.yml) volume line:

```yaml
- ./config.local.json:/app/config.json:ro
```

This keeps your customized config separate from the default example.

### Step 4: Build and start

```bash
docker compose up -d --build
```

### Step 5: Verify service health

Check container status:

```bash
docker compose ps
```

Check live logs:

```bash
docker compose logs -f collector
```

Check web GUI (if `httpEnabled=true`):

- [http://localhost:8080](http://localhost:8080)

### Step 6: Daily operations

Stop:

```bash
docker compose down
```

Restart:

```bash
docker compose restart collector
```

Update after code/config changes:

```bash
docker compose up -d --build
```

### Step 7: Persistence and backup

Persisted data is in local `./data` (mounted to `/data` in container), including:

- CSV storage files
- auth/policy SQLite DB (if enabled)

Backup:

```bash
tar -czf sensor-network-collector-data-backup.tgz data/
```

## Configuration (`config.json`)

Example:

```json
{
  "logLevel": "INFO",
  "mqttBroker": "localhost",
  "mqttPort": 1883,
  "mqttUser": "",
  "mqttPass": "",
  "mqttTopic": "#",
  "skipEmptyFields": true,

  "storage": true,
  "pathStorage": "/tmp/sensor-network-collector",

  "influxdb": true,
  "influxdbUrl": "http://localhost:8086",
  "influxdbToken": "token",
  "influxdbOrg": "org",
  "influxdbBucket": "bucket",
  "influxMeasurement": "mqtt_data",

  "signalk": false,
  "signalkServerUrl": "ws://localhost:3000/signalk/v1/stream",
  "signalkToken": "",
  "signalkContextPrefix": "meteo",
  "signalkSourceLabel": "sensor-network-collector",
  "signalkPathMap": {},

  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "authDbPath": "/tmp/sensor-network-collector/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "change-me"
}
```

## Signal K forwarding (`--signalk`)

The collector forwards Signal K deltas to `/signalk/v1/stream`.

Rules:

- MQTT topic -> `context`
  - `context = <signalkContextPrefix>.<topic with '/' mapped to '.'>`
- data keys -> `values[].path`
  - from `signalkPathMap` if present
  - else standard mappings (same as `vantage-publisher`)
  - else fallback `environment.<sanitized_key>`

Standard conversions:

- `TempOut`, `TempIn`: Celsius -> Kelvin
- `Barometer`: hPa -> Pa
- `HumOut`, `HumIn`: percent -> ratio
- `WindDir`: degrees -> radians

If coordinates are present (`position` or GeoJSON), it emits `navigation.position`.

## CSV storage (`--storage`)

When enabled, incoming data is written to hourly files:

`/UUID/YYYY/MM/DD/UUID_YYYYMMDDZHH00.csv`

- `UUID`: instrument UUID (`uuid` field) or topic-derived fallback
- rotated every hour
- if collector restarts during a valid hour, it appends to the same file

## Web GUI and Policies

Enable web GUI with `httpEnabled=true` or `--http true`.

Features:

- login/logout
- account request form for new users
- admin panel to:
  - create users
  - approve/reject account requests
  - define per-instrument policy
  - grant/revoke per-user access for restricted instruments
- authenticated or anonymous downloads depending on policy

Policy levels (per instrument):

- `open`: free download without authentication
- `account`: authenticated user required
- `restricted`: authenticated + explicitly assigned user (admins always allowed)

Downloads:

- user can select one or more instruments
- optional date range filters
- output is a ZIP with matching CSV files from `pathStorage`

### Web access configuration examples for `pathStorage`

### 1) Open data portal (anonymous download allowed for selected instruments)

Use this when you want public download access for some instruments.

```json
{
  "storage": true,
  "pathStorage": "/data/sensor-network-collector",
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

Then:

1. Login as admin at `http://<host>:8080/admin`
2. Set policy `open` for instruments that must be downloadable without login
3. Keep other instruments as `account` or `restricted`

### 2) Account-required access to all stored data

Use this when every download must be authenticated.

```json
{
  "storage": true,
  "pathStorage": "/srv/collector/storage",
  "httpEnabled": true,
  "httpHost": "127.0.0.1",
  "httpPort": 8080,
  "authDbPath": "/srv/collector/storage/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "change-me"
}
```

Then set each instrument policy to `account` in the admin panel.
Users can request accounts from `/request-account`; admin approves them in `/admin`.

### 3) Restricted access by user and instrument

Use this when only specific users may download specific instruments.

```json
{
  "storage": true,
  "pathStorage": "/srv/collector/storage",
  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "authDbPath": "/srv/collector/storage/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "change-me"
}
```

Then in `/admin`:

1. Set instrument policy to `restricted`
2. Create/approve user accounts
3. Grant per-user access to instrument UUIDs in the “User access” section

### 4) Override storage path and web enablement from CLI

```bash
# Override only storage path, keep web gui enabled from config
python3 main.py --config config.json --storage /mnt/archive/collector

# Force-enable web gui and storage at runtime
python3 main.py --config config.json --http true --storage true
```

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
  - access-controlled downloads

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
    "HumOut": "environment.outside.humidity",
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
