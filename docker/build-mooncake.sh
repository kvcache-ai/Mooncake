#!/usr/bin/env bash
# Export http_proxy/https_proxy (or their uppercase forms) before invoking.
set -euo pipefail

# The Dockerfile uses RUN --mount, which requires BuildKit.
export DOCKER_BUILDKIT=1

cd "$(dirname "${BASH_SOURCE[0]}")/.."
short_sha=$(git rev-parse --short HEAD)
commit=$(sed -n 's/^set(YLT_COMMIT \(.*\))/\1/p' mooncake-common/FindYLT.cmake)
sha256=$(sed -n 's/.*SHA256=\([0-9a-f]*\).*/\1/p' mooncake-common/FindYLT.cmake)
[[ -n "$commit" && -n "$sha256" ]]
cache_dir="$PWD/.cache/yalantinglibs"
archive="$cache_dir/$commit.tar.gz"
mkdir -p "$cache_dir"

if ! printf '%s  %s\n' "$sha256" "$archive" | sha256sum --check --status 2>/dev/null; then
    temporary=$(mktemp "$archive.XXXXXX")
    trap 'rm -f "$temporary"' EXIT
    curl --fail --location --retry 3 --connect-timeout 30 --max-time 600 \
        "https://github.com/alibaba/yalantinglibs/archive/$commit.tar.gz" \
        --output "$temporary"
    printf '%s  %s\n' "$sha256" "$temporary" | sha256sum --check
    mv "$temporary" "$archive"
    trap - EXIT
else
    echo "Using verified yalantinglibs archive: $archive"
fi

# Docker provides these proxy build arguments without ARG/ENV declarations.
exec docker build -f docker/mooncake.Dockerfile \
    -t mooncake:from-source -t "mooncake:${short_sha}" \
    --build-arg http_proxy --build-arg https_proxy \
    --build-arg HTTP_PROXY --build-arg HTTPS_PROXY \
    --build-arg no_proxy --build-arg NO_PROXY "$@" .
