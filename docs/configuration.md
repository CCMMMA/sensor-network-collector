# Configuration Reference

This page documents `config.json` keys with contextualized examples.

The web application is configured separately; see the configuration reference of
[`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa). Web, SMTP, and watchdog keys (`httpHost`, `authDbPath`, `smtpHost`,
`watchdogIntervalSec`, ...) found in an older shared file are ignored by the
collector. `httpEnabled: true` and the `--http` option are ignored with a warning.

## Minimal profile: storage only

```json
{
  "mqttBroker": "mqtt.local",
  "mqttPort": 1883,
  "mqttTopic": "it.uniparthenope.meteo/#",
  "influxdb": false,
  "storage": true,
  "pathStorage": "/data/storage"
}
```

## Key groups

### Logging and MQTT

- `logLevel`: `DEBUG|INFO|WARNING|ERROR`
- `mqttBroker`: MQTT host/IP
- `mqttPort`: MQTT port
- `mqttUser`, `mqttPass`: optional auth
- `mqttTopic`: subscription filter (`#` for all)
- `skipEmptyFields`: drop payloads without writable fields

Example:

```json
{
  "logLevel": "INFO",
  "mqttBroker": "broker.example.net",
  "mqttPort": 1883,
  "mqttUser": "collector",
  "mqttPass": "secret",
  "mqttTopic": "it.uniparthenope.meteo/#",
  "skipEmptyFields": true
}
```

### Storage

- `storage`: enable CSV sink
- `pathStorage`: root directory for hourly station files; the web application needs read access to the same directory, plus write access to its `_logos` subdirectory for station logo uploads

Storage units:

- CSV files keep raw publisher units and values as received on MQTT.
- No Signal K-style normalization is applied to file storage.

Example:

```json
{
  "storage": true,
  "pathStorage": "/srv/collector/storage"
}
```

### InfluxDB

- `influxdb`: enable Influx sink
- `influxdbUrl`, `influxdbToken`, `influxdbOrg`, `influxdbBucket`
- `influxMeasurement`

Influx units:

- InfluxDB fields keep raw publisher units and values as received on MQTT.
- Signal K conversions are not reused for Influx writes.

Example:

```json
{
  "influxdb": true,
  "influxdbUrl": "http://influxdb:8086",
  "influxdbToken": "token",
  "influxdbOrg": "org",
  "influxdbBucket": "weather",
  "influxMeasurement": "mqtt_data"
}
```

### Signal K

- `signalk`: enable Signal K sink
- `signalkServerUrl`, `signalkToken`
- `signalkClientId`, `signalkAccessDescription`
- `signalkAccessPollSec`, `signalkAccessTimeoutSec`
- `signalkContextPrefix`, `signalkSourceLabel`
- `signalkPathMap`
- empty `meta` keys or empty-string `meta` values are ignored before sending deltas

Example:

```json
{
  "signalk": true,
  "signalkServerUrl": "ws://signalk.local:3000/signalk/v1/stream",
  "signalkToken": "",
  "signalkClientId": "9d95a6f4-7c8e-4a2f-9fd8-8a190f8f24be",
  "signalkAccessDescription": "sensor-network-collector",
  "signalkContextPrefix": "meteo"
}
```

Context rule:

- MQTT topic is split on `/`
- inside each segment, `.` is converted to `_`
- segments are then joined with `.`
- payload `name` is emitted as Signal K path `name`

Example:

```text
topic: it.uniparthenope.meteo.ws1
context: meteo.it_uniparthenope_meteo_ws1
name path: meteo.it_uniparthenope_meteo_ws1.name
```

Signal K value normalization:

- temperature -> Kelvin
- pressure -> Pa
- humidity percent -> ratio
- angular values -> radians
- rain totals / evapotranspiration totals -> meters
- rain rate -> meters per second

## Full integrated example

```json
{
  "logLevel": "INFO",
  "mqttBroker": "mqtt.local",
  "mqttPort": 1883,
  "mqttTopic": "it.uniparthenope.meteo/#",
  "skipEmptyFields": true,

  "storage": true,
  "pathStorage": "/data/storage",

  "influxdb": true,
  "influxdbUrl": "http://influxdb:8086",
  "influxdbToken": "token",
  "influxdbOrg": "org",
  "influxdbBucket": "weather",
  "influxMeasurement": "mqtt_data",

  "signalk": true,
  "signalkServerUrl": "ws://signalk.local:3000/signalk/v1/stream",
  "signalkToken": "",
  "signalkClientId": "9d95a6f4-7c8e-4a2f-9fd8-8a190f8f24be",
  "signalkAccessDescription": "sensor-network-collector",
  "signalkAccessPollSec": 10,
  "signalkAccessTimeoutSec": 10,
  "signalkContextPrefix": "meteo",
  "signalkSourceLabel": "sensor-network-collector"
}
```
