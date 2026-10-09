# Docker Deployment

This document describes production-grade Docker deployment for `sensor-network-collector`.

## Files

- `Dockerfile`
- `docker-compose.yml.sample`
- `config.json.sample`
- `.dockerignore` restricts the build context to runtime sources and dependency metadata

GitHub Actions tests builds and publishes successful `main` images to GHCR. See
[CI/CD and registry-based rollouts](ci-cd.md) for image tags, permissions, and rollout
commands. Local builds remain supported. Images do not contain runtime configuration
or data; provide these through volume mounts.

## Runtime model

The container runs `python main.py --config /app/config.json` for MQTT ingest and
sinks, and writes CSV storage under the mounted persistent volume. It listens on no
port. The web application is deployed from [`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa) with its own image, mounting the
same data directory.

## Prerequisites

- Docker Engine 24+
- Docker Compose v2

Check:

```bash
docker --version
docker compose version
```

## Recommended directory layout

```text
/opt/sensor-network-collector/
  Dockerfile
  docker-compose.yml
  config.json
  data/
    storage/
```

## Compose setup

Start from sample:

```bash
cp docker-compose.yml.sample docker-compose.yml
cp config.json.sample config.json
mkdir -p data
```

Mount persistent data and allow the collector to save approved Signal K tokens in its config:

```yaml
services:
  collector:
    build: .
    restart: unless-stopped
    volumes:
      - ./config.json:/app/config.json
      - ./data:/data
```

## Required config adaptations in Docker

Use paths inside container:

```json
{
  "pathStorage": "/data/storage"
}
```

Important:

- Use network-reachable hostnames for MQTT/InfluxDB/Signal K (not `localhost` unless the service runs in the same container).
- Mount the host `./data` directory into the web application containers too, so they read the CSV files written here.

## Build and start

```bash
docker compose up -d --build
```

This starts `collector` for MQTT ingest, storage, InfluxDB, and Signal K.

## Verify health

```bash
docker compose ps
docker compose logs -f collector
```

## Operational commands

Stop:

```bash
docker compose down
```

Restart:

```bash
docker compose restart collector
```

Rollout after config/code updates:

```bash
docker compose up -d --build
```

Or run `./updater.sh`, which pulls the repository (fast-forward only), builds the
image, and restarts the services. If `docker-compose.yml` still defines a Gunicorn
`webapp_wsgi` service from before the web application moved out, the script leaves
that service running on its old image and updates only the others; see
[upgrading](upgrading.md).

## Data persistence and backup

Everything under `/data` should be persistent. The collector writes the station CSV
files there; the web application keeps its SQLite database and logo uploads in the
same directory when deployed as documented in its own project.

Stop every writer (this collector and the web application services) before copying
active files. Back up configuration separately and securely; see
[operations](operations.md#backup-and-restore).

```bash
docker compose stop collector
tar -czf sensor-network-collector-backup.tgz data/
docker compose start collector
```

Restore:

```bash
tar -xzf sensor-network-collector-backup.tgz
```

## Security hardening checklist

- The collector needs outbound access only (MQTT, InfluxDB, Signal K); publish no ports
- Keep `config.json` out of public repositories: it holds MQTT, InfluxDB, and Signal K credentials
