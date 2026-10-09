#!/bin/bash
# Stop at the first failure so a failed pull or build never restarts the stack.
set -euo pipefail
cd "$(dirname "$0")"

git pull --ff-only
make build

# Deployments created before the web application moved to sensor-network-pwa may
# still define a Gunicorn service (webapp_wsgi:app) built from this repository.
# The current image cannot run it, so those services are left untouched on the
# image they already run and only the other services are updated.
# See docs/upgrading.md.
compose_config="$(docker compose config)"
services="$(printf '%s\n' "$compose_config" | awk '
  /^[^ ]/ { in_services = ($0 == "services:"); next }
  in_services && /^  [^ ]/ { name = $1; sub(/:$/, "", name); order[++n] = name; next }
  in_services && /webapp_wsgi/ { legacy[name] = 1 }
  END { for (i = 1; i <= n; i++) print (order[i] in legacy ? "legacy " : "current ") order[i] }
')"
legacy_services="$(printf '%s\n' "$services" | sed -n 's/^legacy //p')"
current_services="$(printf '%s\n' "$services" | sed -n 's/^current //p')"

if [ -z "$legacy_services" ]; then
  docker compose up -d --build
else
  # A reverse proxy of such a deployment may bind-mount nginx/default.conf, which
  # is no longer tracked here: put the last tracked version back as a local file.
  if printf '%s\n' "$compose_config" | grep -q 'nginx/default\.conf'; then
    if [ -d nginx/default.conf ]; then
      rmdir nginx/default.conf
    fi
    if [ ! -e nginx/default.conf ]; then
      removed_in="$(git rev-list -n 1 HEAD -- nginx/default.conf)"
      if [ -n "$removed_in" ]; then
        mkdir -p nginx
        git show "${removed_in}:nginx/default.conf" > nginx/default.conf 2>/dev/null \
          || git show "${removed_in}^:nginx/default.conf" > nginx/default.conf
        echo "Restored nginx/default.conf as a local, untracked file."
      fi
    fi
  fi

  if [ -n "$current_services" ]; then
    # shellcheck disable=SC2086
    docker compose up -d --build --no-deps $current_services
  fi

  echo
  echo "WARNING: the web application is no longer part of this repository."
  echo "Not updated, still running the image they already had:" $legacy_services
  echo "Do not rebuild or recreate them from this checkout: deploy sensor-network-pwa"
  echo "and remove them from docker-compose.yml (docs/upgrading.md)."
fi

if grep -qs '"httpEnabled"[[:space:]]*:[[:space:]]*true' config.json; then
  echo
  echo "NOTE: config.json sets httpEnabled, which the collector now ignores: it no"
  echo "longer serves the web GUI or runs the anomaly watchdog (docs/upgrading.md)."
fi
