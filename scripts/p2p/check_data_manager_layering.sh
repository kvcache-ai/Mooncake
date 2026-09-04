#!/usr/bin/env bash
# Enforces the layering rules from remake_kvbm/new_data_manager.md
# (acceptance items 11 and 12):
#   1. No header under p2p/client/v2/ may include tiered_backend.h or anything
#      under tiered_cache/. "V2 does not reuse TieredBackend" has to be a
#      compile-time fact, not a comment.
#   2. The base mooncake_store source list must not pull in v1/ or v2/ sources.
#
# Usage: scripts/p2p/check_data_manager_layering.sh [repo_root]
set -euo pipefail

root="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
store="${root}/mooncake-store"
status=0

v2_include_dir="${store}/include/p2p/client/v2"
if [[ -d "${v2_include_dir}" ]]; then
    while IFS= read -r -d '' header; do
        if grep -nE '^[[:space:]]*#include[[:space:]]*[<"].*(tiered_backend\.h|tiered_cache/)' \
               "${header}"; then
            echo "ERROR: ${header#${root}/} includes the tiered-cache tree" >&2
            status=1
        fi
    done < <(find "${v2_include_dir}" -name '*.h' -print0)
fi

# Everything under v2/ must live in namespace mooncake::v2. V1's tiered-cache
# tree and V2 both define a type called MovementRequest; with both in namespace
# mooncake that is an ODR violation the linker resolves silently, and the
# symptom is a crash in unrelated V1 code. The nested namespace makes the
# collision impossible rather than merely unlikely.
for dir in "${store}/include/p2p/client/v2" "${store}/src/p2p/client/v2"; do
    [[ -d "${dir}" ]] || continue
    while IFS= read -r -d '' source; do
        if ! grep -q '^namespace mooncake::v2 {' "${source}"; then
            echo "ERROR: ${source#${root}/} must declare namespace mooncake::v2" >&2
            status=1
        fi
    done < <(find "${dir}" \( -name '*.h' -o -name '*.cpp' \) -print0)
done

# data_manager_types.h must be self-contained: its own translation unit
# includes nothing else, so a successful build proves it compiles standalone.
types_cpp="${store}/src/p2p/client/data_manager_types.cpp"
if [[ -f "${types_cpp}" ]]; then
    extra_includes="$(grep -E '^[[:space:]]*#include' "${types_cpp}" \
        | grep -v 'p2p/client/data_manager_types.h' || true)"
    if [[ -n "${extra_includes}" ]]; then
        echo "ERROR: data_manager_types.cpp must include only its own header," >&2
        echo "       so that header is proven self-contained. Found:" >&2
        echo "${extra_includes}" >&2
        status=1
    fi
fi

# The base library must stay free of DataManager implementations.
cmake_file="${store}/src/CMakeLists.txt"
base_block="$(awk '/^set\(MOONCAKE_STORE_SOURCES/,/^\)/' "${cmake_file}")"
if grep -qE 'p2p/client/v[12]/' <<<"${base_block}"; then
    echo "ERROR: MOONCAKE_STORE_SOURCES lists a p2p/client/v1|v2 source" >&2
    status=1
fi

if [[ ${status} -eq 0 ]]; then
    echo "data manager layering: OK"
fi
exit ${status}
