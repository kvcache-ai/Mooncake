#!/bin/bash
# Mooncake CI Local Test Prerequisites Check
# Usage: bash check-prerequisites.sh
# This script checks and auto-fixes all prerequisites for running Mooncake CI tests locally.

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "🔍 Checking Mooncake CI test prerequisites..."

# 1. Check build directory
if [ ! -f build/CMakeCache.txt ]; then
  echo -e "${RED}✗ Build directory not found or not built${NC}"
  echo "  → Run: mkdir build && cd build && cmake .. && cmake --build ."
  exit 1
fi
echo -e "${GREEN}✓ Build exists${NC}"

# 2. Check mooncake installation
if ! python -c "import mooncake" 2>/dev/null; then
  echo -e "${RED}✗ mooncake package not installed${NC}"
  echo "  → Fixing: Installing mooncake package..."
  cd build && sudo cmake --install . && cd - >/dev/null
  if ! python -c "import mooncake" 2>/dev/null; then
    echo "  → Alternative: pip install mooncake-wheel/dist/*.whl"
    exit 1
  fi
  echo -e "${GREEN}✓ mooncake package installed${NC}"
else
  echo -e "${GREEN}✓ mooncake package already installed${NC}"
fi

# 3. Check ctest availability
if ! command -v ctest &> /dev/null; then
  echo -e "${RED}✗ ctest not found${NC}"
  exit 1
fi
echo -e "${GREEN}✓ ctest available${NC}"

# 4. Kill and restart services (safest approach for local testing)
echo -e "\n${YELLOW}Cleaning up and restarting services...${NC}"
pkill -f "^etcd" || true
pkill -f bootstrap_server.py || true
pkill -f mooncake_http_metadata_server || true
sleep 1

# 5. Start etcd
if ! command -v etcd &> /dev/null; then
  echo -e "${YELLOW}⚠ etcd not found, installing...${NC}"
  ETCD_VER=v3.6.1
  OS=$(uname -s | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)
  [ "$ARCH" = "x86_64" ] && ARCH="amd64"
  DOWNLOAD_URL="https://github.com/etcd-io/etcd/releases/download/${ETCD_VER}/etcd-${ETCD_VER}-${OS}-${ARCH}.tar.gz"
  echo "  Downloading from: $DOWNLOAD_URL"
  cd /tmp
  wget -q "$DOWNLOAD_URL" && tar xzf "etcd-${ETCD_VER}-${OS}-${ARCH}.tar.gz" && \
    sudo mv "etcd-${ETCD_VER}-${OS}-${ARCH}"/etcd* /usr/local/bin/
  cd - >/dev/null
  echo -e "${GREEN}✓ etcd installed${NC}"
fi

etcd --advertise-client-urls http://127.0.0.1:2379 --listen-client-urls http://127.0.0.1:2379 >/dev/null 2>&1 &
ETCD_PID=$!
sleep 2
if ! etcdctl --endpoints=http://127.0.0.1:2379 endpoint health &>/dev/null; then
  echo -e "${RED}✗ etcd failed to start${NC}"
  kill $ETCD_PID 2>/dev/null || true
  exit 1
fi
echo -e "${GREEN}✓ etcd running (PID: $ETCD_PID)${NC}"

# 6. Start HTTP metadata server
if [ -f "mooncake-transfer-engine/example/http-metadata-server-python/bootstrap_server.py" ]; then
  cd mooncake-transfer-engine/example/http-metadata-server-python
  pip install -q aiohttp 2>/dev/null || true
  python ./bootstrap_server.py >/dev/null 2>&1 &
  METADATA_PID=$!
  cd - >/dev/null
  sleep 1
  if curl -s http://127.0.0.1:8080/metadata > /dev/null 2>&1; then
    echo -e "${GREEN}✓ HTTP Metadata server running (PID: $METADATA_PID)${NC}"
  else
    echo -e "${RED}✗ HTTP Metadata server failed to start${NC}"
    kill $METADATA_PID $ETCD_PID 2>/dev/null || true
    exit 1
  fi
else
  echo -e "${YELLOW}⚠ Metadata server script not found, skipping${NC}"
fi

echo -e "\n${GREEN}✅ All prerequisites ready!${NC}"
echo "Service PIDs: etcd=$ETCD_PID"
[ -n "$METADATA_PID" ] && echo "Metadata server PID: $METADATA_PID"
echo -e "\n${YELLOW}To kill services:${NC}"
echo "  pkill -f '^etcd'"
echo "  pkill -f bootstrap_server"
