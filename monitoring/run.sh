#!/bin/bash

# Ensure we are currently in the script's directory to make relative paths work
cd "$(dirname "$0")"

SCRIPT_DIR=$(pwd)
echo "Working directory: $SCRIPT_DIR"

# Create network if it doesn't exist
docker network create monitoring 2>/dev/null

# Create volume if it doesn't exist
docker volume create grafana-data 2>/dev/null

# Remove existing containers to avoid conflicts
docker rm -f prometheus grafana 2>/dev/null

echo "Starting Prometheus..."
docker run -d \
  --name prometheus \
  --restart unless-stopped \
  --network monitoring \
  -p 9090:9090 \
  -v "$SCRIPT_DIR/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml" \
  --add-host host.docker.internal:host-gateway \
  prom/prometheus:v2.54.0 \
  --config.file=/etc/prometheus/prometheus.yml \
  --storage.tsdb.retention.time=15d \
  --web.console.libraries=/usr/share/prometheus/console_libraries \
  --web.console.templates=/usr/share/prometheus/consoles

echo "Starting Grafana..."
docker run -d \
  --name grafana \
  --restart unless-stopped \
  --network monitoring \
  -p 3000:3000 \
  -e GF_SECURITY_ADMIN_USER=admin \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -e GF_USERS_ALLOW_SIGN_UP=false \
  -v grafana-data:/var/lib/grafana \
  -v "$SCRIPT_DIR/grafana/provisioning:/etc/grafana/provisioning" \
  -v "$SCRIPT_DIR/grafana/dashboards:/etc/grafana/dashboards" \
  grafana/grafana-oss:11.0.0

echo "Monitoring stack started!"
echo "Prometheus: http://localhost:9090"
echo "Grafana:    http://localhost:3000"
