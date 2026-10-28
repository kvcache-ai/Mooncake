#!/usr/bin/env bash
# SOURCE must be a disposable copy of the archived notify-stage source.
set -euo pipefail
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SOURCE=${1:?absolute archived notify-stage source directory}
(cd "$SOURCE" && sha256sum -c "$HERE/base-files.sha256")
for name in pin fast source-prefetch; do
  patch --batch --forward -d "$SOURCE" -p1 < "$HERE/$name.patch"
done
