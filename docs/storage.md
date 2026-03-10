# CSV Storage (`--storage`)

## Purpose

Persist inbound MQTT payloads as hourly CSV partitions for replay, download, and web browsing.

## File layout

Pattern:

```text
/UUID/YYYY/MM/DD/UUID_YYYYMMDDZHH00.csv
```

Where:

- `UUID`: instrument identifier from payload (`uuid`/`UUID`/`station_uuid`) or topic fallback
- one file per hour (UTC timestamp bucket)

## Rotation and append behavior

- file rotates every hour
- if collector restarts during same hour, it appends to current hourly file
- schema evolves forward: when new fields appear, header is extended and prior rows are rewritten with blank new columns

## CLI and config

Enable in config:

```json
{
  "storage": true,
  "pathStorage": "/data/storage"
}
```

Enable/override via CLI:

```bash
python3 main.py --config config.json --storage true
python3 main.py --config config.json --storage /mnt/archive/collector
python3 main.py --config config.json --storage false
```

## Data model notes

- scalar values (`bool`, `int`, `float`, `str`) stored directly
- nested objects/arrays serialized as JSON strings
- `timestamp`, `topic`, `uuid` and flattened fields are included

## Operational recommendations

- use SSD-backed storage for high write rates
- protect volume with regular backups
- avoid manual edits in active hourly files

## Troubleshooting

No files written:

- verify `storage=true`
- verify `pathStorage` exists and is writable
- check logs for `Storage write failed`

Unexpected UUID directories:

- verify payload includes consistent UUID field
- verify topic naming conventions for fallback ID
