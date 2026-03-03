#!/bin/bash
docker-compose down
git pull
chmod +x updater.sh
make build
docker compose up -d
