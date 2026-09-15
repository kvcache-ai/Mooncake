#!/bin/sh

set -e
set -u

MOONCAKE_TOOLCHAIN="/opt/mooncake-toolchain"

GO_DL_HOST="go.dev"
GO_ARCH="unknown"
GO_VERSION="go1.27.0"

GITHUB_MIRROR=${ASCEND_GITHUB_MIRROR_URLS:-}

PATH=${MOONCAKE_TOOLCHAIN}/bin:$PATH

# ensure python3 with venv module
command -v apt-get && apt-get update && apt-get install -y python3-venv || :
command -v yum && yum makecache && yum install -y python3 || :
command -v python3 || exit 1

python3 -m venv ${MOONCAKE_TOOLCHAIN}
test -z "$GITHUB_MIRROR" || python3 -m pip config --user set \
  global.index-url https://mirrors.huaweicloud.com/repository/pypi/simple
python3 -m pip install --upgrade pip cmake ninja

# dirty hack for sudo
ln -f -s ${MOONCAKE_TOOLCHAIN}/bin/cmake /usr/local/bin/cmake

command -v go && test "$(go env GOVERSION)" = "$GO_VERSION" && exit
ARCH=$(uname -m)
test "$ARCH" = "x86_64" && GO_ARCH="amd64" || :
test "$ARCH" = "aarch64" && GO_ARCH="arm64" || :
test "$GO_ARCH" = "unknown" && echo "Unsupported architecture: $ARCH" && exit 1
test -z "$GITHUB_MIRROR" || GO_DL_HOST="golang.google.cn"
wget -q -O - "https://$GO_DL_HOST/dl/$GO_VERSION.linux-$GO_ARCH.tar.gz" | \
  tar -C ${MOONCAKE_TOOLCHAIN} -x -z -f -
ln -s ../go/bin/go ${MOONCAKE_TOOLCHAIN}/bin/go
ln -s ../go/bin/gofmt ${MOONCAKE_TOOLCHAIN}/bin/gofmt
