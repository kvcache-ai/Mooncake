#!/usr/bin/env bash
set -e -o pipefail

cd mooncake-transfer-engine/example/http-metadata-server-python
pip install aiohttp
python ./bootstrap_server.py &
echo "NIGHTLY_METADATA_SERVER_PID=$!" >> "$GITHUB_ENV"
sleep 2
