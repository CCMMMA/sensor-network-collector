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
- `baseUrl` (must match public URL for email fast-login and password-reset links)

Optional but commonly relevant:

- `webAppLogo`
- `smtpHost` / `smtpFrom` for account and alarm emails
- if `smtpHost` is omitted, email features remain inactive

## Run Gunicorn

```bash
source .venv/bin/activate
COLLECTOR_CONFIG=/absolute/path/config.prod.json \
  gunicorn --workers 2 --bind 127.0.0.1:8080 --worker-tmp-dir /tmp webapp_wsgi:app
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
ExecStart=/opt/sensor-network-collector/.venv/bin/gunicorn --workers 2 --bind 127.0.0.1:8080 --worker-tmp-dir /tmp webapp_wsgi:app
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

## Docker Compose + Nginx on port 8087

This pattern is useful when:

- `sensor-network-collector` runs in containers
- Nginx is used as the local frontend for the web GUI
- another external reverse proxy already exists in front of this host
- the local Nginx should listen on a non-standard port such as `8087`

Example traffic flow:

```text
public reverse proxy (TLS / public DNS)
  -> host:8087
    -> nginx container
      -> gunicorn container
```

### 1. Prepare host directories

Example deployment root:

```text
/opt/sensor-network-collector/
  docker-compose.yml
  config.prod.json
  nginx/
    default.conf
  data/
    storage/
    collector_auth.sqlite
```

Create it:

```bash
sudo mkdir -p /opt/sensor-network-collector/nginx
sudo mkdir -p /opt/sensor-network-collector/data/storage
cd /opt/sensor-network-collector
```

### 2. Create `config.prod.json`

Start from the repository sample and adapt it for container paths:

```json
{
  "mqttBroker": "mqtt.example.net",
  "mqttPort": 1883,
  "mqttTopic": "it.uniparthenope.meteo/#",
  "storage": true,
  "pathStorage": "/data/storage",
  "httpEnabled": true,
  "httpHost": "0.0.0.0",
  "httpPort": 8080,
  "authDbPath": "/data/collector_auth.sqlite",
  "webSessionSecret": "replace-with-strong-random-secret",
  "adminUser": "admin",
  "adminPassword": "replace-with-strong-password",
  "baseUrl": "https://public.example.org",
  "smtpEnabled": true,
  "smtpHost": "smtp.example.net",
  "smtpFrom": "collector@example.org"
}
```

Important:

- `pathStorage` and `authDbPath` must point to writable paths inside the container
- `httpHost` must be `0.0.0.0`
- `httpPort` stays `8080` inside the application container
- `baseUrl` must match the public external URL seen by users, not `http://host:8087`
- if the external reverse proxy terminates TLS, `baseUrl` should still use the final public `https://...` URL

### 3. Create `docker-compose.yml`

This Compose setup runs:

- `collector`: MQTT collector + sinks/background tasks
- `web`: Gunicorn serving `webapp_wsgi:app`
- `nginx`: local reverse proxy listening on host port `8087`

```yaml
services:
  collector:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: sensor-network-collector
    restart: unless-stopped
    command: ["python3", "main.py", "--config", "/app/config.json"]
    volumes:
      - ./config.prod.json:/app/config.json:ro
      - ./data:/data

  web:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: sensor-network-web
    restart: unless-stopped
    depends_on:
      - collector
    command: ["gunicorn", "--workers", "2", "--bind", "0.0.0.0:8080", "--worker-tmp-dir", "/tmp", "webapp_wsgi:app"]
    environment:
      COLLECTOR_CONFIG: /app/config.json
    expose:
      - "8080"
    volumes:
      - ./config.prod.json:/app/config.json:ro
      - ./data:/data

  nginx:
    image: nginx:1.27-alpine
    container_name: sensor-network-nginx
    restart: unless-stopped
    depends_on:
      - web
    ports:
      - "8087:8087"
    volumes:
      - ./nginx/default.conf:/etc/nginx/conf.d/default.conf:ro
    tmpfs:
      - /var/cache/nginx
      - /var/run
      - /tmp
```

Notes:

- `collector` is only exposed to the internal Compose network
- only Nginx publishes a host port
- if you also need MQTT ingest, InfluxDB, or SMTP by hostname, ensure the container can resolve and reach those services from the Docker network
- The sample Gunicorn command uses `--worker-tmp-dir /tmp` to avoid permission errors for Gunicorn internals in hardened containers.
- The sample Nginx config writes temp files under `/tmp`; the Compose sample mounts writable `tmpfs` paths for Nginx runtime/cache directories.
- If you force `user: "<uid>:<gid>"` on the standard `nginx:alpine` image, startup can fail with permission errors on `/var/cache/nginx`. In that case either remove the `user` override for Nginx or use an unprivileged Nginx image and keep temp paths under `/tmp`.

### 4. Create `nginx/default.conf`

This Nginx config listens on port `8087` and forwards requests to the Gunicorn `web` service on internal port `8080`:

```nginx
server {
    listen 8087;
    server_name _;

    client_max_body_size 25m;
    client_body_temp_path /tmp/client_temp;
    proxy_temp_path /tmp/proxy_temp;
    fastcgi_temp_path /tmp/fastcgi_temp;
    uwsgi_temp_path /tmp/uwsgi_temp;
    scgi_temp_path /tmp/scgi_temp;

    location / {
        proxy_pass http://web:8080;
        proxy_http_version 1.1;

        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header X-Forwarded-Host $host;
        proxy_set_header X-Forwarded-Port $server_port;
    }
}
```

Why these headers matter:

- `Host` and `X-Forwarded-*` preserve the original request context
- this helps Flask generate correct redirects and keeps reverse-proxy behavior predictable
- email links are still controlled by `baseUrl`, not by forwarded headers

### 5. Build and start

From `/opt/sensor-network-collector`:

```bash
docker compose up -d --build
```

Check status:

```bash
docker compose ps
docker compose logs -f collector
docker compose logs -f web
docker compose logs -f nginx
```

### 6. Configure the external reverse proxy

If another reverse proxy already fronts this host, forward requests to:

```text
http://<host-or-container-hostname>:8087
```

Typical outer proxy responsibilities:

- public TLS termination
- public hostname routing
- optional access logging and rate limiting

The outer proxy should preserve at least:

- `Host`
- `X-Forwarded-For`
- `X-Forwarded-Proto`

### 7. Validate end to end

Check the deployment in this order:

1. Local Nginx on the Docker host:

```bash
curl -I http://127.0.0.1:8087/
```

2. Application response through Nginx:

```bash
curl -L http://127.0.0.1:8087/login
```

3. Public URL through the outer reverse proxy:

```bash
curl -I https://public.example.org/
```

4. Web GUI behavior in browser:

- home page loads
- login works
- admin panel opens
- public station pages open

5. Email-link correctness:

- forgot-password email uses the `baseUrl` host
- fast-login email uses the `baseUrl` host
- onboarding links from account approval use the `baseUrl` host

### 8. Restart and update workflow

After config or code changes:

```bash
docker compose up -d --build
```

To restart only Nginx:

```bash
docker compose restart nginx
```

To restart only the collector:

```bash
docker compose restart collector
```

### 9. Common mistakes

- `baseUrl` set to `http://host:8087` instead of the final public URL
- `authDbPath` mounted read-only
- `pathStorage` not mounted to persistent storage
- publishing `8080` directly and bypassing Nginx unintentionally
- forgetting to forward `X-Forwarded-Proto` from the outer reverse proxy

## Operational checks

- Verify app reachable at public URL
- Verify login/session persistence across restarts
- Verify anomalies page and admin dashboard load
- Verify fast-login links in email use correct host (`baseUrl`)
- Verify forgot-password emails generate reset links with the same public host
