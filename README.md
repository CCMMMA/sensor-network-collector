# sensor-network-collector

Threaded MQTT collector for weather/sensor networks, aligned with `vantage-publisher` configuration style.

## Features

- MQTT subscriber with auto-reconnect
- Multiple output sinks:
  - InfluxDB
  - Signal K websocket deltas with retry backoff on transient gateway failures
  - hourly CSV storage with path validation, protected collector metadata, and atomic schema expansion
- Dry mode for safe runtime validation (`--dry`)

The web application (map, station browser, dashboards, downloads, accounts, watchdog
alarms and email notifications) is no longer part of this project: it lives in
[`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa) and reads the CSV storage, and optionally the InfluxDB bucket, written by this
collector.

## Configuration Style (Homogeneous with `vantage-publisher`)

The collector now accepts the same naming style used in `vantage-publisher` (`mqttBroker`, `mqttPort`, `signalkServerUrl`, `pathStorage`, etc.).

Backward-compatible snake_case keys are still accepted.

## Requirements

- Python 3.10+ (CI checks 3.10–3.14; Docker uses 3.11)
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

Run the regression checks after installing dependencies (startup uses mocked MQTT):

```bash
python3 -m unittest discover -s tests -v
```

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
- `--http` deprecated and ignored (the web application moved to `sensor-network-pwa`); a warning is logged
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

- [Documentation index](docs/README.md)
- [Development and validation](docs/development.md)
- [Operations and troubleshooting](docs/operations.md)
- [Upgrading a deployment that still includes the web GUI](docs/upgrading.md)

- Docker deployment: [link](docs/docker.md)
- Continuous integration and image delivery: [link](docs/ci-cd.md)
- Configuration reference: [link](docs/configuration.md)
- Signal K forwarding: [link](docs/signalk.md)
- CSV storage: [link](docs/storage.md)
- Web application (separate project): [`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa)
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
The web application is a separate process from a separate project ([`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa)).

## Data Flow

```text
MQTT broker
  -> sensor-network-collector
    -> normalize payload + timestamp
      -> InfluxDB sink (optional)
      -> Signal K sink (optional)
      -> CSV sink (optional)

CSV sink (pathStorage) / InfluxDB bucket
  -> sensor-network-pwa (separate project, reads the collected data)
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
  }
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

## Web application

Point [`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa) at the same `pathStorage` directory (and, optionally, the same InfluxDB
bucket). The collector ignores the web, SMTP, and watchdog keys of an older shared
`config.json`, so such a file keeps working here; `httpEnabled: true` and `--http`
only log a warning. To update a running deployment that still serves the web GUI from
this repository, follow [the upgrade guide](docs/upgrading.md).

## License

Apache-2.0
