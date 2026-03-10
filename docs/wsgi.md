# Production WSGI Setup

This document covers production deployment of the web app via WSGI (Gunicorn).

## Scope

- serves Flask web GUI (`webapp_wsgi.py`)
- does not replace collector process for MQTT ingest; run collector separately if needed

## Prerequisites

- Python 3.9+
- virtualenv
- gunicorn

Install:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install gunicorn
```

## Configuration

Create production config:

```bash
cp config.json.sample config.prod.json
```

Required keys:

- `httpEnabled: true`
- `pathStorage`
- `authDbPath`
- `webSessionSecret`
- `adminPassword`
- `baseUrl` (must match public URL for email fast-login links)

Optional but commonly relevant:

- `webAppLogo`
- `smtpHost` / `smtpFrom` for account and alarm emails
- if `smtpHost` is omitted, email features remain inactive

## Run Gunicorn

```bash
source .venv/bin/activate
COLLECTOR_CONFIG=/absolute/path/config.prod.json \
  gunicorn --workers 2 --bind 127.0.0.1:8080 webapp_wsgi:app
```

Tuning guidance:

- workers: `2` to `2 * CPU + 1` depending on load
- behind reverse proxy, keep bind local (`127.0.0.1`)

## systemd service

`/etc/systemd/system/sensor-network-web.service`:

```ini
[Unit]
Description=Sensor Network Collector Web (Gunicorn)
After=network.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=/opt/sensor-network-collector
Environment=COLLECTOR_CONFIG=/opt/sensor-network-collector/config.prod.json
ExecStart=/opt/sensor-network-collector/.venv/bin/gunicorn --workers 2 --bind 127.0.0.1:8080 webapp_wsgi:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Enable:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now sensor-network-web.service
sudo systemctl status sensor-network-web.service
```

## Nginx reverse proxy

`/etc/nginx/sites-available/sensor-network-web`:

```nginx
server {
    listen 80;
    server_name your-domain.example;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Then add TLS (certbot recommended).

## Operational checks

- Verify app reachable at public URL
- Verify login/session persistence across restarts
- Verify anomalies page and admin dashboard load
- Verify fast-login links in email use correct host (`baseUrl`)
