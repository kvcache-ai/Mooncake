#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=${MOONCAKE_STORE_RS_DIR:-}
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/../../lib/common.sh"

usage() {
  cat <<'EOF'
Usage: scripts/tests/route-migration/test-route-migration-runtime-e2e.sh

Run the route-migration runtime/control-plane in-process E2E checks that belong
to the runtime PR layer.

This script intentionally stays below the admin/operator surface. It validates:

- control-plane submit -> executor worker -> route publish
- executor panic recovery for a subsequent task

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

mc_scripts_setup_upstream_runtime_env python
export PYTHONDONTWRITEBYTECODE=1

cd "${REPO_ROOT}"

echo "==> route migration runtime e2e: explicit move through control plane"
cargo test -p mooncake-store-client \
  control_plane_submit_migration_task_executes_explicit_move \
  -- --nocapture

echo "==> route migration runtime e2e: worker survives panic and keeps serving"
cargo test -p mooncake-store-client \
  control_plane_submit_migration_task_recovers_after_executor_panic \
  -- --nocapture

echo "route migration runtime e2e ok"
