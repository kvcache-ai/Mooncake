#!/usr/bin/env bash
# Export http_proxy/https_proxy (or their uppercase forms) before invoking.
set -euo pipefail

# The Dockerfile uses RUN --mount, which requires BuildKit.
export DOCKER_BUILDKIT=1

cd "$(dirname "${BASH_SOURCE[0]}")/.."
short_sha=$(git rev-parse --short HEAD)

# Docker provides these proxy build arguments without ARG/ENV declarations.
exec docker build -f docker/mooncake.Dockerfile \
    -t mooncake:from-source -t "mooncake:${short_sha}" \
    --build-arg "GH_MIRROR=${GH_MIRROR:-}" \
    --build-arg http_proxy --build-arg https_proxy \
    --build-arg HTTP_PROXY --build-arg HTTPS_PROXY \
    --build-arg no_proxy --build-arg NO_PROXY "$@" .
