#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

source "${HOME}/.cargo/env" 2>/dev/null || true
for command in cargo pkg-config ldd sha256sum; do
  command -v "${command}" >/dev/null || {
    echo "${command} is required; source the canonical Rust/SPDK environment first" >&2
    exit 1
  }
done

find_spdk_prefix() {
  local candidate pc
  for candidate in "${MOONCAKE_SPDK_PREFIX:-}"; do
    [[ -n "${candidate}" ]] || continue
    for pc in \
      "${candidate}/install/lib/pkgconfig" \
      "${candidate}/lib/pkgconfig" \
      "${candidate}/build/lib/pkgconfig" \
      "${candidate}/build/lib64/pkgconfig"; do
      if [[ -f "${pc}/spdk_nvme.pc" && -f "${pc}/spdk_env_dpdk.pc" && -f "${pc}/spdk_syslibs.pc" ]]; then
        printf '%s\n' "${candidate}"
        return 0
      fi
    done
  done
  return 1
}

SPDK_PREFIX="$(find_spdk_prefix)" || {
  cat >&2 <<'MSG'
SPDK development package not found.
Set MOONCAKE_SPDK_PREFIX to an SPDK prefix that contains spdk_nvme.pc,
spdk_env_dpdk.pc and spdk_syslibs.pc under install/lib/pkgconfig,
lib/pkgconfig, build/lib/pkgconfig, or build/lib64/pkgconfig.
MSG
  exit 1
}

SPDK_PC_DIR=""
for candidate in \
  "${SPDK_PREFIX}/install/lib/pkgconfig" \
  "${SPDK_PREFIX}/lib/pkgconfig" \
  "${SPDK_PREFIX}/build/lib/pkgconfig" \
  "${SPDK_PREFIX}/build/lib64/pkgconfig"; do
  if [[ -f "${candidate}/spdk_nvme.pc" && -f "${candidate}/spdk_env_dpdk.pc" && -f "${candidate}/spdk_syslibs.pc" ]]; then
    SPDK_PC_DIR="${candidate}"
    break
  fi
done
export MOONCAKE_SPDK_PREFIX="${SPDK_PREFIX}"
export PKG_CONFIG_PATH="${SPDK_PC_DIR}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"

pkg-config --exists spdk_nvme spdk_env_dpdk spdk_syslibs || {
  echo "SPDK pkg-config validation failed: ${SPDK_PC_DIR}" >&2
  exit 1
}
SPDK_LIB_DIR="$(pkg-config --variable=libdir spdk_nvme 2>/dev/null || true)"
if [[ -z "${SPDK_LIB_DIR}" || ! -d "${SPDK_LIB_DIR}" ]]; then
  SPDK_LIB_DIR="$(cd "${SPDK_PC_DIR}/.." && pwd)"
fi
for library in libspdk_nvme.so libspdk_env_dpdk.so; do
  [[ -e "${SPDK_LIB_DIR}/${library}" ]] || {
    echo "${SPDK_LIB_DIR}/${library} is required; static-only SPDK packages are not supported by this test build" >&2
    exit 1
  }
done

# Keep the multi-node build reproducible from the locked dependency graph.
# Initiators are CPU-only by default. Do not let CUDA headers in the build
# container enable CUDA code paths that require a GPU at runtime. Set
# MOONCAKE_ENABLE_CUDA=1 explicitly only for a GPU-capable validation host.
export MOONCAKE_ENABLE_CUDA="${MOONCAKE_ENABLE_CUDA:-0}"
NOF_LINKER="${NOF_LINKER:-${ROOT_DIR}/scripts/e2e/nof-spdk-linker.sh}"
[[ -x "${NOF_LINKER}" ]] || {
  echo "NoF linker wrapper is not executable: ${NOF_LINKER}" >&2
  exit 1
}
export CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER="${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER:-${NOF_LINKER}}"
# Unit tests and the final binary use the same SPDK shared-library runtime.
export LD_LIBRARY_PATH="${SPDK_LIB_DIR}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
PROFILE="${NOF_BUILD_PROFILE:-debug}"
TARGET_DIR="${CARGO_TARGET_DIR:-${ROOT_DIR}/target}"
case "${PROFILE}" in
  debug) PROFILE_ARGS=() ; BINARY="${TARGET_DIR}/debug/nof_multi_client" ;;
  release) PROFILE_ARGS=(--release) ; BINARY="${TARGET_DIR}/release/nof_multi_client" ;;
  *) echo "NOF_BUILD_PROFILE must be debug or release, got ${PROFILE}" >&2; exit 2 ;;
esac

cd "${ROOT_DIR}"
if [[ -n "$(git status --porcelain --untracked-files=all)" ]]; then
  echo "NoF E2E build requires a clean source tree so the artifact matches the reported commit" >&2
  git status --short >&2
  exit 1
fi
BUILD_COMMIT="$(git rev-parse HEAD)"
EXPECTED_COMMIT="${NOF_EXPECTED_COMMIT:-${BUILD_COMMIT}}"
if [[ "${BUILD_COMMIT}" != "${EXPECTED_COMMIT}" ]]; then
  echo "source HEAD ${BUILD_COMMIT} does not match NOF_EXPECTED_COMMIT ${EXPECTED_COMMIT}" >&2
  exit 1
fi
cargo fmt --all -- --check
cargo build --locked "${PROFILE_ARGS[@]}" -p mooncake-store-e2e --bin nof_multi_client \
  --features nof-spdk

if [[ "${NOF_RUN_UNIT_TESTS:-0}" == 1 ]]; then
  cargo test --locked -p mooncake-store-client --lib --features nof-spdk nof
  cargo test --locked -p mooncake-store-client --lib --features nof-spdk \
    client::cold_tier::owner::tests::
  cargo test --locked -p mooncake-store-e2e --bin nof_multi_client --features nof-spdk
fi

[[ -x "${BINARY}" ]] || { echo "built binary not found: ${BINARY}" >&2; exit 1; }
RUNTIME_DEPS="$(ldd -r "${BINARY}" 2>&1 || true)"
if grep -Eq 'not found|undefined symbol:' <<<"${RUNTIME_DEPS}"; then
  echo "SPDK runtime dependencies are unresolved for ${BINARY}" >&2
  printf '%s\n' "${RUNTIME_DEPS}" >&2
  exit 1
fi
if [[ "$(git rev-parse HEAD)" != "${BUILD_COMMIT}" || -n "$(git status --porcelain --untracked-files=all)" ]]; then
  echo "source tree changed while building ${BUILD_COMMIT}; refusing to publish the artifact" >&2
  exit 1
fi
BINARY_SHA256="$(sha256sum "${BINARY}" | awk '{print $1}')"
printf 'build_commit=%s\nbinary_sha256=%s\n' "${BUILD_COMMIT}" "${BINARY_SHA256}" \
  >"${BINARY}.build-manifest"

printf 'built=%s\nspdk_prefix=%s\nspdk_lib_dir=%s\nlinker=%s\n' \
  "${BINARY}" "${SPDK_PREFIX}" "${SPDK_LIB_DIR}" "${CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER}"
printf 'commit=%s\ncargo=%s\nrustc=%s\nspdk_nvme=%s\n' \
  "${BUILD_COMMIT}" "$(cargo --version)" "$(rustc --version)" \
  "$(pkg-config --modversion spdk_nvme)"
printf '%s  %s\n' "${BINARY_SHA256}" "${BINARY}"
