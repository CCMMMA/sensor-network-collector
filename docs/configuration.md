# Configuration Reference

This page documents `config.json` keys with contextualized examples.

## Minimal profile: storage + web GUI

```json
{
  "mqttBroker": "mqtt.local",
  "mqttPort": 1883,
  "mqttTopic": "it.uniparthenope.meteo/#",
  "storage": true,
  "pathStorage": "/data/storage",
  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "authDbPath": "/data/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "change-me"
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
- `pathStorage`: root directory for hourly station files

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

### Web GUI and security

- `httpEnabled`, `httpHost`, `httpPort`
- `baseUrl`: public URL used in email links
- `authDbPath`: SQLite file for users/policies/anomalies
- `webSessionSecret`: required strong random secret
- `adminUser`, `adminPassword`
- `webAppLogo`: optional absolute path to app logo image

Example:

```json
{
  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "baseUrl": "https://collector.example.org",
  "authDbPath": "/data/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "replace-with-strong-password",
  "webAppLogo": "/data/branding/app-logo.png"
}
```

`baseUrl` is used for:

- fast-login links in welcome, approval, and anomaly emails
- password-reset links from the forgot-password flow

### SMTP and notifications

- `smtpEnabled`
- `smtpHost`, `smtpPort`
- `smtpUser`, `smtpPass`
- `smtpFrom`
- `smtpUseTls`

Behavior:

- if `smtpHost` is not set, the application does not try to send emails
- if `smtpPort`, `smtpUser`, and `smtpPass` are omitted, SMTP defaults to unauthenticated port `25`
- if `smtpUser` is set, SMTP login is attempted
- `smtpUseTls` can still explicitly force TLS behavior; default logic follows the port/auth fallback
- forgot-password and reset emails require both SMTP configuration and a correct `baseUrl`

Example:

```json
{
  "smtpEnabled": true,
  "smtpHost": "smtp.mailprovider.net",
  "smtpFrom": "noreply@example.org",
  "smtpPort": 25
}
```

Authenticated SMTP example:

```json
{
  "smtpEnabled": true,
  "smtpHost": "smtp.mailprovider.net",
  "smtpPort": 587,
  "smtpUser": "noreply@example.org",
  "smtpPass": "smtp-password",
  "smtpFrom": "noreply@example.org",
  "smtpUseTls": true
}
```

### Watchdog

- `watchdogIntervalSec`: scan period for anomaly detection

Example:

```json
{
  "watchdogIntervalSec": 60
}
```

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
  "signalkSourceLabel": "sensor-network-collector",

  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "baseUrl": "https://collector.example.org",
  "webAppLogo": "/data/branding/app-logo.png",
  "authDbPath": "/data/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-secret",
  "adminUser": "admin",
  "adminPassword": "replace-with-strong-password",

  "smtpEnabled": true,
  "smtpHost": "smtp.mailprovider.net",
  "smtpPort": 587,
  "smtpUser": "noreply@example.org",
  "smtpPass": "smtp-password",
  "smtpFrom": "noreply@example.org",
  "smtpUseTls": true,

  "watchdogIntervalSec": 60
}
```
