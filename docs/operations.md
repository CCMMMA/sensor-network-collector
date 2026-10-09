# Operations

## Verify a deployment

For the Compose deployment:

```bash
docker compose ps
docker compose logs --tail=100 collector
```

Confirm the collector connects to MQTT and subscribes to the intended filter.
After a known station publishes, check enabled sinks:

- CSV: a new row under `pathStorage/<identifier>/YYYY/MM/DD/` with the expected
  collector timestamp, topic, and identifier.
- InfluxDB: a record in the configured bucket and measurement, with the station
  identifier and publisher units preserved.
- Signal K: access approved and the expected context/path updating on the server.
- Web application ([`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa), if deployed): the station appears with fresh data.

A running container alone does not establish data flow.

For Python deployments, run `python main.py --config config.json` using the
configured service account and inspect its process logs. The automated startup test
uses mocked MQTT; it does not replace these live checks.

## Configuration and process roles

The collector handles MQTT and outputs only. The web application, its SQLite
database, and the anomaly watchdog with its email notifications run from [`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa),
which reads the storage root written here. An upgrade from a version that embedded
the web GUI must deploy that project to keep the GUI and the watchdog notifications;
see [upgrading](upgrading.md).

Signal K approval persists its token into the collector's config JSON. The collector
must have write access to that file, including its bind mount. See [Signal K](signalk.md).

## Backup and restore

Back up configuration separately from persistent data, and keep both private:
configuration contains tokens and passwords. For a consistent backup of active CSV
files, stop the collector (and the web application services sharing the directory)
before copying the mounted data directory. For the documented `./data:/data` layout:

```bash
docker compose stop collector
tar -czf collector-data-backup.tgz data/
docker compose start collector
```

Store the archive outside the served data tree. Record the deployed image digest or
commit tag with each backup. To restore, stop the services, retain a copy of the
current data, restore the saved `data/` directory and matching configuration, check
ownership/permissions, and start the services. Recheck station freshness.

An image rollback does not roll back stored observations.
Review compatibility before running an older version against current data; use a
matching data backup when necessary. See [image rollout and rollback](ci-cd.md).

## Troubleshooting

| Symptom | Checks |
| --- | --- |
| Startup asks for InfluxDB credentials | InfluxDB defaults to enabled; set `influxdb=false` explicitly for a storage-only deployment. |
| `No outputs enabled` | Enable at least one of storage, InfluxDB, or Signal K. |
| Connected but no samples | Check MQTT topic filter, publisher activity, and invalid JSON/non-object payload warnings. |
| `Storage write failed` | Check free space, permissions, identifier validation, and symlinks leaving the storage root. Other enabled sinks still process a CSV rejection. |
| Signal K approval repeats or cannot save | Check approval status and config-file write access; inspect access-manager retry logs. |
| Web GUI no longer reachable after an upgrade | The GUI moved to `sensor-network-pwa`; `httpEnabled`/`--http` are ignored with a warning. Deploy that project against the same `pathStorage`; see [upgrading](upgrading.md). |
| Web service fails with `exec: "gunicorn": executable file not found` | It was rebuilt from this repository, whose image no longer contains the web application. See [upgrading](upgrading.md#if-the-web-service-was-already-rebuilt). |
| `updater.sh` reports services that were not updated | The Compose file still defines a `webapp_wsgi` service; it is left running on its old image. See [upgrading](upgrading.md). |
| `Unhandled error while processing topic=...` | One message could not be processed; the collector keeps running. Inspect the traceback and the publisher payload. |

See [storage](storage.md) and
[CI/CD troubleshooting](ci-cd.md#troubleshooting) for more detail.
