#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(git -C "${SCRIPT_DIR}" rev-parse --show-toplevel)
# shellcheck disable=SC1091
source "${REPO_ROOT}/scripts/lib/common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/tests/route-migration/test-route-migration-admin-e2e.sh

Run the route-migration admin/operator in-process E2E checks that belong to the
admin PR layer.

This script validates:

- admin HTTP accepts a route-migration task and exposes status
- the operator CLI HTTP client submits/list/gets route-migration tasks

The host environment is expected to satisfy the repository-standard Rust build
requirements.
EOF
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

mc_scripts_require_command cargo
mc_scripts_require_command python3

UPSTREAM_BUILD_DIR=$(mc_scripts_resolve_upstream_build_dir "${REPO_ROOT}")
mc_scripts_setup_upstream_runtime_env "${REPO_ROOT}" python "${UPSTREAM_BUILD_DIR}"
export PYTHONDONTWRITEBYTECODE=1

cd "${REPO_ROOT}"

echo "==> route migration admin e2e: admin HTTP server status flow"
cargo test -p mooncake-store-py \
  admin_http_server_accepts_route_migration_tasks_and_reports_status \
  -- --nocapture

echo "==> route migration admin e2e: operator CLI HTTP client flow"
cargo test -p mooncake-store-py --bin mooncake-store-admin \
  route_migration_http_client_ \
  -- --nocapture

echo "route migration admin e2e ok"
