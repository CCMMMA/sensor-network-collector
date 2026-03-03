#!/bin/bash
docker compose down
git pull
make build
docker compose up -d
