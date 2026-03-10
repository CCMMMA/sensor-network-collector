# Docker Deployment

This document describes production-grade Docker deployment for `sensor-network-collector`.

## Files

- `Dockerfile`
- `docker-compose.yml.sample`
- `config.json.sample`

## Runtime model

Container responsibilities:

- run `python3 main.py --config /app/config.json`
- write CSV storage and auth DB under mounted persistent volume
- expose web GUI when `httpEnabled=true`

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
  config.local.json
  data/
    storage/
    collector_auth.sqlite
    logos/
```

## Compose setup

Start from sample:

```bash
cp docker-compose.yml.sample docker-compose.yml
cp config.json.sample config.local.json
mkdir -p data
```

Bind your config read-only and mount persistent data:

```yaml
services:
  collector:
    build: .
    restart: unless-stopped
    ports:
      - "8080:8080"
    volumes:
      - ./config.local.json:/app/config.json:ro
      - ./data:/data
```

## Required config adaptations in Docker

Use paths inside container:

```json
{
  "pathStorage": "/data/storage",
  "authDbPath": "/data/collector_auth.sqlite",
  "httpHost": "0.0.0.0",
  "httpPort": 8080
}
```

Important:

- Use network-reachable hostnames for MQTT/InfluxDB/SMTP (not `localhost` unless service runs in same container).
- Use a strong `webSessionSecret`.
- Set strong `adminPassword`.
- If email delivery is not needed, omit `smtpHost` and the application will skip email sending.
- For plain unauthenticated relay inside trusted networks, you can omit `smtpPort`, `smtpUser`, and `smtpPass`; SMTP falls back to port `25`.

## Build and start

```bash
docker compose up -d --build
```

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

## Data persistence and backup

Everything under `/data` should be persistent:

- station CSV files
- SQLite auth/anomaly DB
- station logo uploads

Backup:

```bash
tar -czf sensor-network-collector-backup.tgz data/
```

Restore:

```bash
tar -xzf sensor-network-collector-backup.tgz
```

## Networking and reverse proxy

If exposed publicly, run behind Nginx/Caddy/Traefik with TLS.

Forward only HTTP port from reverse proxy to container.

## Security hardening checklist

- Set non-default `adminPassword`
- Set strong `webSessionSecret`
- Restrict host firewall to required ports
- Configure SMTP credentials via secrets manager or protected file
- Keep `config.local.json` out of public repositories
