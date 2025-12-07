#!/bin/bash

echo "[1] Starting Python Server..."
python server.py &

echo "[2] Starting Celery Worker..."
celery -A worker.celery_app worker --loglevel=info -P solo &

echo "[3] Starting Celery Flower..."
celery -A worker.celery_app flower --port=5556 &

echo "All services started!"
