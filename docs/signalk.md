# Signal K Forwarding (`--signalk`)

## Transport

- collector publishes deltas over websocket to Signal K stream endpoint
- default endpoint format: `ws://host:3000/signalk/v1/stream`

## Context mapping

`context` is derived from MQTT topic:

```text
<contextPrefix>.<topic segments joined with '.', with '.' inside each segment converted to '_'>
```

Example:

- topic `it/uniparthenope/meteo/ws1`
- prefix `meteo`
- context `meteo.it.uniparthenope.meteo.ws1`

If topic is flat MQTT with dots inside a single segment:

- topic `it.uniparthenope.meteo.ws1`
- prefix `meteo`
- context `meteo.it_uniparthenope_meteo_ws1`

## Path mapping

`signalkPathMap` supports:

1. legacy string mapping
2. object mapping with `path` and optional `meta`

Example:

```json
{
  "TempOut": "environment.outside.temperature",
  "BarTrend": {
    "path": "environment.outside.pressureTrend",
    "meta": {
      "displayName": "Pressure Trend",
      "units": "Pa/s"
    }
  }
}
```

If `meta` is present it is sent as Signal K metadata delta for that path.

The collector also emits payload `name` as Signal K value path `name` inside the computed context.

Example:

```text
context: meteo.it_uniparthenope_meteo_ws1
path: name
full Signal K address: meteo.it_uniparthenope_meteo_ws1.name
```

## Unit conversion rules

Before sending to Signal K, collector converts to SI-friendly conventions:

- temperature -> Kelvin
- pressure (hPa-like values) -> Pa
- humidity percent -> ratio
- angular/heading/direction-like values -> radians

## Access request flow

If `signalkToken` is empty or invalid:

1. collector submits access request (`clientId`, `description`)
2. polls request status until approved
3. continues other sinks while waiting
4. stores approved token back into config JSON
5. resumes Signal K forwarding automatically

## Configuration keys

- `signalk` (`true|false`)
- `signalkServerUrl`
- `signalkToken`
- `signalkClientId`
- `signalkAccessDescription`
- `signalkAccessPollSec`
- `signalkAccessTimeoutSec`
- `signalkContextPrefix`
- `signalkSourceLabel`
- `signalkPathMap`

## CLI override

```bash
python3 main.py --config config.json --signalk true
python3 main.py --config config.json --signalk false
```
