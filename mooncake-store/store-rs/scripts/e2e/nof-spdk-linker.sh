#!/usr/bin/env bash
set -euo pipefail

# Cargo emits dependency-provided -l flags after its default --as-needed flag.
# Keep the SPDK/DPDK shared libraries live because the SPDK package in the
# supported environment omits part of its transitive DT_NEEDED metadata. Keep
# the system-library tail under --as-needed, but explicitly retain OpenSSL,
# which SPDK references through unresolved symbols.
args=()
spdk_group=0
system_tail=0
for arg in "$@"; do
  if [[ "$arg" == "-lspdk_nvme" && "$spdk_group" == 0 ]]; then
    args+=("-Wl,--no-as-needed")
    spdk_group=1
  elif [[ "$arg" == "-lrt" && "$system_tail" == 0 ]]; then
    args+=("-Wl,--as-needed" "-Wl,-u,OPENSSL_init_ssl" "-Wl,-u,EVP_MD_CTX_free")
    system_tail=1
  fi
  args+=("$arg")
done
exec "${CC:-cc}" "${args[@]}"
