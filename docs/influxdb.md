# InfluxDB (`--influxdb`)

## Purpose

Write normalized MQTT payloads to InfluxDB v2 for time-series analytics.

## Configuration keys

Required when `influxdb=true`:

- `influxdbUrl`
- `influxdbToken`
- `influxdbOrg`
- `influxdbBucket`

Optional:

- `influxMeasurement` (default: `mqtt_data`)

## Record shape

For each message:

- measurement: `influxMeasurement`
- tags:
  - `topic`
  - `uuid`
  - extracted metadata tags
- fields:
  - flattened scalar payload fields
- timestamp:
  - payload `Datetime` / `DatetimeWS` if valid, else current UTC

## Field type conflict handling (422)

InfluxDB rejects writes when a field changes type in same measurement.

Collector behavior:

1. parse conflict from error payload
2. retry with field coercion to existing type (int/float/string/bool)
3. if coercion impossible, drop only conflicting field and retry
4. abort only if no fields remain or retries exhausted

This allows continued ingestion without dropping full records.

## CLI overrides

```bash
python3 main.py --config config.json --influxdb true
python3 main.py --config config.json --influxdb false
```

## Dry-run support

With `--dry`, Influx writes are logged but not sent.

## Performance considerations

- write API uses synchronous mode for reliability
- for very high throughput, consider batching and retention policies in Influx

## Troubleshooting

### Authentication errors

- verify token/org/bucket
- verify URL reachability from runtime environment

### 422 type conflicts

- check producer consistency for field types
- consider separating heterogeneous metrics into different measurements

### Connection failures

- verify network routing and TLS/proxy settings
