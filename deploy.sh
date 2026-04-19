#!/bin/bash

cd /home/sasha/parser || exit

echo "Pulling latest changes..."
git pull

echo "Running docker compose..."
docker compose down
docker image prune -f

docker compose pull
docker compose up -d --build

echo "Deploy completed!"
docker compose ps
