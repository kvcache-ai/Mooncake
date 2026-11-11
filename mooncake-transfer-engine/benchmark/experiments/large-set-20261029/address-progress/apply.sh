#!/usr/bin/env bash
set -euo pipefail
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
SOURCE=${1:?disposable archived source AFTER parent pin/fast/source-prefetch series}
STAGE=${2:-coarse}
case "$STAGE" in address|progress|coarse) ;; *) exit 2 ;; esac
(cd "$SOURCE" && sha256sum -c "$HERE/base-files.sha256")
for name in address progress coarse; do
  patch --batch --forward -d "$SOURCE" -p1 < "$HERE/$name.patch"
  [[ "$name" != "$STAGE" ]] || break
done
if [[ "$STAGE" == coarse ]]; then
  (cd "$SOURCE" && sha256sum -c "$HERE/final-files.sha256")
fi
