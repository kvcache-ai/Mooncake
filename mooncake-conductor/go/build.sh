#!/bin/bash
# SPDX-License-Identifier: Apache-2.0

set -euo pipefail
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 CONDUCTOR_BUILD_DIR OUTPUT_BINARY" >&2
    exit 1
fi
conductor_build=$(cd "$1" && pwd)
output_binary=$2
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Match Store's cgo build: provide native header and library paths explicitly.
export CGO_CFLAGS="-I${script_dir}/../include${CGO_CFLAGS:+ $CGO_CFLAGS}"
export CGO_LDFLAGS="-L${conductor_build} -lmooncake_conductor_client${CGO_LDFLAGS:+ $CGO_LDFLAGS}"
export CGO_ENABLED=1
cd "$script_dir"
"${GO:-go}" build -o "$output_binary" ./examples/basic
