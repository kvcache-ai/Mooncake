#!/bin/bash

set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
BUILD_DIR=${BUILD_DIR:-"${ROOT_DIR}/build-rust-release"}
OUTPUT_DIR=${OUTPUT_DIR:-"${ROOT_DIR}/dist-rust"}
VERSION=${VERSION:-${GITHUB_REF_NAME:-dev}}
ARCH=${ARCH:-$(uname -m)}
PACKAGE_BASENAME=${PACKAGE_BASENAME:-"mooncake-transfer-engine-rust-${VERSION}-linux-${ARCH}"}
PACKAGE_DIR="${OUTPUT_DIR}/${PACKAGE_BASENAME}"
ARCHIVE_PATH="${OUTPUT_DIR}/${PACKAGE_BASENAME}.tar.gz"
CHECKSUM_PATH="${ARCHIVE_PATH}.sha256"

mkdir -p "${OUTPUT_DIR}"

cmake -S "${ROOT_DIR}" -B "${BUILD_DIR}" \
  -DBUILD_UNIT_TESTS=OFF \
  -DBUILD_EXAMPLES=OFF \
  -DWITH_TE=ON \
  -DWITH_STORE=OFF \
  -DWITH_P2P_STORE=OFF \
  -DWITH_EP=OFF \
  -DWITH_RUST_EXAMPLE=ON \
  -DUSE_HTTP=ON \
  -DUSE_ETCD=ON \
  -DCMAKE_BUILD_TYPE=Release \
  "$@"

cmake --build "${BUILD_DIR}" --target build_transfer_engine_rust -j"$(nproc)"

RUST_BINARY="${BUILD_DIR}/mooncake-transfer-engine/rust/release/transfer_engine_rust"

if [[ ! -x "${RUST_BINARY}" ]]; then
  echo "Rust binary not found at ${RUST_BINARY}" >&2
  exit 1
fi

rm -rf "${PACKAGE_DIR}" "${ARCHIVE_PATH}" "${CHECKSUM_PATH}"
mkdir -p "${PACKAGE_DIR}"

cp "${RUST_BINARY}" "${PACKAGE_DIR}/"

cat > "${PACKAGE_DIR}/README.txt" <<EOF
Mooncake Rust Release Asset
===========================

Package: ${PACKAGE_BASENAME}
Binary: transfer_engine_rust
Version: ${VERSION}

This package contains the Rust Transfer Engine benchmark binary built from the
Mooncake repository. The binary currently wraps the Transfer Engine C API and
follows the same metadata/runtime prerequisites as the C++ benchmark.

Runtime notes:
- Linux ${ARCH}
- Requires Mooncake runtime dependencies installed on the target host
- Metadata backends are configured at runtime through the binary flags

Quick start:
1. Extract this archive
2. Run ./transfer_engine_rust --help
3. Start the required metadata service before running benchmarks

For full setup instructions, see the Mooncake README and project documentation.
EOF

tar -C "${OUTPUT_DIR}" -czf "${ARCHIVE_PATH}" "${PACKAGE_BASENAME}"

(
  cd "${OUTPUT_DIR}"
  sha256sum "${PACKAGE_BASENAME}.tar.gz" > "$(basename "${CHECKSUM_PATH}")"
)

echo "Created Rust release archive: ${ARCHIVE_PATH}"
echo "Created checksum file: ${CHECKSUM_PATH}"