# Upgrading a deployment that still includes the web GUI

Earlier versions of this repository also contained the web application: an embedded
Flask GUI (`httpEnabled`), a Gunicorn entry point (`webapp_wsgi.py`), the anomaly
watchdog, and an Nginx sample. All of that now lives in
[`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa). This page
describes how to update a running deployment without losing data collection or the
web portal.

## What stays the same

- `config.json` needs no edit. Keys of the web application (`httpEnabled`,
  `httpHost`, `authDbPath`, `adminPassword`, `smtp*`, `watchdogIntervalSec`, ...) are
  ignored; `httpEnabled: true` and `--http` only log a warning.
- Environment variables, the other command-line options, and the entry point
  (`python main.py --config ...`) are unchanged.
- CSV files are written to the same `pathStorage` layout, and InfluxDB and Signal K
  records are unchanged. The Signal K token saved in the configuration is reused.
- The SQLite database (`authDbPath`) is not touched by the collector. The web
  application keeps using it, with the existing accounts and policies.

## What the collector no longer does

| Before | Now |
| --- | --- |
| `httpEnabled: true` served the GUI from the collector process | Run `sensor-network-pwa` |
| The collector process ran the anomaly watchdog and sent alarm emails when `httpEnabled` was true | Run the `sensor-network-pwa` watchdog (`--watchdog-only`) |
| The image contained Gunicorn and `webapp_wsgi.py` | The image only runs the collector |
| `nginx/default.conf` sample | Moved to `sensor-network-pwa` |

Alarm emails stop as soon as the collector is updated and resume when the
`sensor-network-pwa` watchdog is started.

## Docker Compose deployment (collector + Gunicorn `web` service)

### 1. Update the collector

Run the first update by hand. The `updater.sh` already on the host is the old one: it
starts with `docker compose down` and can restart the web service without the files
it needs.

If `nginx/default.conf` was edited on the host, copy it aside first; `git pull`
refuses to continue while it has local changes. Discard the edit with
`git checkout nginx/default.conf`, pull, and put the copy back.

```bash
cd /opt/sensor-network-collector
git pull --ff-only
./updater.sh
```

The new `updater.sh` detects Compose services that still run `webapp_wsgi` and:

- rebuilds and restarts every other service (the collector), without touching them;
- leaves those web services running on the image they already have;
- restores `nginx/default.conf` as a local, untracked file when the Compose file
  mounts it;
- prints a warning naming the services it skipped.

The portal keeps working on its previous code while the collector runs the new one.
Until step 2 is done, do not run `docker compose up --build`, `docker compose build`,
or `docker compose down` in this directory: a web service rebuilt from this checkout
cannot start.

### 2. Move the portal to `sensor-network-pwa`

Follow the Docker guide of
[`sensor-network-pwa`](https://github.com/CCMMMA/sensor-network-pwa): start its `web`
and `watchdog` services with the same configuration file and the same data
directory, then point the reverse proxy at the new `web` service.

Afterwards remove the old web service (and its Nginx service, if the proxy moved too)
from this directory's `docker-compose.yml`, and remove the stopped containers:

```bash
docker compose up -d --remove-orphans
```

From then on `./updater.sh` updates the whole stack again.

### If the web service was already rebuilt

A web service recreated from the new checkout fails with
`exec: "gunicorn": executable file not found`. Build the last version that contained
the web application and run the service from that image until the portal is moved:

```bash
last_web="$(git rev-list -n 1 HEAD -- webapp_wsgi.py)^"
git archive "$last_web" | docker build -t sensor-network-collector:with-web -
```

In `docker-compose.yml`, replace the `build:` block of the web service with
`image: sensor-network-collector:with-web`, then `docker compose up -d --no-deps <web service>`.

## Registry images

`ghcr.io/ccmmma/sensor-network-collector:latest` no longer contains Gunicorn. A web
service that uses this image must stay on the `sha-<commit>` tag it runs now until it
is replaced by a `sensor-network-pwa` image. The collector service can move to the new
image at once.

## Single container or `python main.py` with `httpEnabled: true`

Here the GUI is served by the collector process itself, so it disappears when the
collector is updated. Start `sensor-network-pwa` first, on the same `pathStorage` and
`authDbPath` (and on another port while both run), switch users or the reverse proxy
to it, then update the collector.

A Gunicorn service started from this checkout (`webapp_wsgi:app`) keeps running after
`git pull` but cannot be restarted, because `webapp_wsgi.py` is gone. Move it to a
`sensor-network-pwa` checkout before pulling.

For the collector itself:

```bash
git pull --ff-only
pip install -r requirements.txt
```

and restart the service. Flask and Gunicorn are no longer required and may be removed
from the virtual environment.

## Verify

```bash
docker compose ps
docker compose logs --tail=50 collector
```

The collector logs `Connected to MQTT broker` and `Subscribed to topic`, then a
`Storage write` (or `Influx write` / `Signal K write`) line for each message. With a
legacy configuration it also logs once that `--http/httpEnabled is ignored`. New rows
appear under `pathStorage/<identifier>/YYYY/MM/DD/`, and the portal shows fresh data.
