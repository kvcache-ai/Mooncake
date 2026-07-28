# syntax=docker/dockerfile:1

# Keep docker/master.Dockerfile and docker/master-cuda13.Dockerfile in sync: they must
# differ ONLY in the three CUDA-flavor lines (the cudalibs FROM, the libcudart COPY and
# the pip package name). The publish workflow diffs them with those lines normalized.

# Stage cudalibs: take stub libcuda and libcudart from the CUDA devel image
FROM nvidia/cuda:13.0.3-devel-ubuntu22.04@sha256:3869b846a8cc495ce11c172d87cfc0da8874b910d14a9810bec6b6182e9ee9f8 AS cudalibs

# Final image. Must be trixie (glibc 2.41): the aarch64 wheel is manylinux_2_39
# (needs glibc >= 2.39), which bookworm (2.36) cannot satisfy.
FROM python:3.12-slim-trixie@sha256:d764629ce0ddd8c71fd371e9901efb324a95789d2315a47db7e4d27e78f1b0e9

# Build args: mooncake version, pip index URL
ARG MOONCAKE_VERSION
ARG PIP_INDEX_URL=https://pypi.org/simple

# Install runtime system libraries and tini.
# ibverbs-providers ships /usr/lib/<triplet>/libmlx5.so.1 plus the libibverbs provider
# plugins, and both are required:
#   - at load time, because since 0.3.12 engine.so / store.so / mooncake_master carry a
#     hard DT_NEEDED on libmlx5.so.1 (the transport links mlx5 for the IBGDA / mlx5dv
#     DevX path) and auditwheel deliberately does not vendor RDMA libraries into the
#     wheel -- without it `import mooncake.engine` fails outright, even for TCP-only use;
#   - at run time, because libibverbs claims devices through those provider plugins:
#     without the package ibv_get_device_list() returns 0 devices even when the mlx5
#     devices are visible in /sys/class/infiniband and /dev/infiniband is passed in.
# It must come from apt next to libibverbs1: both are built from the rdma-core source
# package and share a private provider ABI, so their versions have to match.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libibverbs1 ibverbs-providers libnuma1 libcurl4t64 libstdc++6 tini \
    && rm -rf /var/lib/apt/lists/*

# Copy stub libcuda and libcudart into the loader's default path, refresh the link cache
COPY --from=cudalibs /usr/local/cuda/lib64/stubs/libcuda.so /usr/local/lib/libcuda.so.1
COPY --from=cudalibs /usr/local/cuda/lib64/libcudart.so.13  /usr/local/lib/libcudart.so.13
RUN ldconfig

# Install mooncake, remove torch EP/PG extensions (ep_*/pg_*), chown the package dir to uid 65532 (all in one layer)
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" \
        mooncake-transfer-engine-cuda13==${MOONCAKE_VERSION} \
    && PKG="$(python3 -c 'import mooncake,os;print(os.path.dirname(mooncake.__file__))')" \
    && rm -f "$PKG"/ep_*.so "$PKG"/pg_*.so \
    && chown -R 65532:65532 "$PKG"

# Create a HOME owned by uid 65532 and set it as WORKDIR
ENV HOME=/home/nonroot
RUN mkdir -p /home/nonroot && chown 65532:65532 /home/nonroot
WORKDIR /home/nonroot

USER 65532:65532

# tini as PID 1 to forward signals; default into bash
ENTRYPOINT ["tini", "-g", "--"]
CMD ["bash"]
