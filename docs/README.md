# Documentation

## Start here

- [Configuration](configuration.md): MQTT and output sinks.
- [Development and validation](development.md): local setup, tests, and change workflow.
- [Docker deployment](docker.md): collector service with persistent volumes.
- [CI/CD](ci-cd.md): checks, GHCR images, rollout, and rollback.
- [Operations](operations.md): runtime verification, backups, and failure diagnosis.
- [Upgrading](upgrading.md): updating a deployment that still serves the web GUI from this repository.

## Features and integrations

- [CSV storage](storage.md): identifiers, hourly partitions, schema changes, and units.
- [InfluxDB](influxdb.md): records, timestamps, and field-type conflict handling.
- [Signal K](signalk.md): mappings, unit conversion, and token approval.
- [Joining the network](how_to_join.md): Vantage Pro2 edge-node setup.

The collector entry point is `main.py`. The web application (accounts, access
policies, dashboards, charts, downloads, watchdog alarms) is the separate
[`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa) project: it reads the CSV storage and, optionally, the InfluxDB bucket
written here, and never ingests MQTT messages.

For a new deployment, configure MQTT and at least one sink, choose Docker or a
Python service deployment, then follow the operations checks. CI publishes images;
rolling them out to a host is a separate operator action.
