#!/bin/bash
# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail

GO_VERSION="${GO_VERSION:-1.23.8}"
ARCH="$(uname -m)"

if [ "$ARCH" = "aarch64" ]; then
    GOARCH="arm64"
elif [ "$ARCH" = "x86_64" ]; then
    GOARCH="amd64"
else
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
fi

wget -q -O /tmp/go.tar.gz "https://go.dev/dl/go${GO_VERSION}.linux-${GOARCH}.tar.gz"
tar -C /usr/local -xzf /tmp/go.tar.gz
rm -f /tmp/go.tar.gz
