#!/bin/sh

set -e
set -u

MOONCAKE_TOOLCHAIN=/opt/mooncake-toolchain
GITHUB_MIRROR=${ASCEND_GITHUB_MIRROR_URLS:-}
AUDIT_GO_WHEEL=${AUDIT_GO_WHEEL:-}

PATH=${MOONCAKE_TOOLCHAIN}/bin:$PATH

# ensure python3 with venv module
command -v apt-get && apt-get update && apt-get install -y python3-venv || :
command -v yum && yum makecache && yum install -y python3 || :
command -v python3 || exit 1

python3 -m venv ${MOONCAKE_TOOLCHAIN}
test -z "$GITHUB_MIRROR" || python3 -m pip config --user set \
  global.index-url https://mirrors.huaweicloud.com/repository/pypi/simple
python3 -m pip install --upgrade pip cmake ninja
python3 -m pip install go-bin==1.27.0

test -z "$AUDIT_GO_WHEEL" && exit

cd $(find ${MOONCAKE_TOOLCHAIN} -type d -name go)
find -type f -exec sha256sum {} \; | tee /tmp/wheel.sha256sum
wget -q -O - https://go.dev/dl/go1.27.0.linux-amd64.tar.gz | tar -C /tmp -xzf -
cd /tmp/go
find -type f -exec sha256sum {} \; | tee /tmp/tar.sha256sum
diff -u /tmp/tar.sha256sum /tmp/wheel.sha256sum | tee /tmp/audit.diff
