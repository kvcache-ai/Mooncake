# syntax=docker/dockerfile:1

# Stage cudalibs: take stub libcuda and libcudart from the CUDA devel image
FROM nvidia/cuda:12.8.1-devel-ubuntu22.04@sha256:a99a1860ba8e2916e5c3e73b72ec4c4301653a84586e05bfc9a2aa2d58027e97 AS cudalibs

# Final image. Must be trixie (glibc 2.41): the aarch64 wheel is manylinux_2_39
# (needs glibc >= 2.39), which bookworm (2.36) cannot satisfy.
FROM python:3.12-slim-trixie@sha256:d764629ce0ddd8c71fd371e9901efb324a95789d2315a47db7e4d27e78f1b0e9

# Build args: mooncake version, pip index URL
ARG MOONCAKE_VERSION
ARG PIP_INDEX_URL=https://pypi.org/simple

# Install runtime system libraries and tini
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates libibverbs1 libnuma1 libcurl4t64 libstdc++6 tini \
    && rm -rf /var/lib/apt/lists/*

# Copy stub libcuda and libcudart into the loader's default path, refresh the link cache
COPY --from=cudalibs /usr/local/cuda/lib64/stubs/libcuda.so /usr/local/lib/libcuda.so.1
COPY --from=cudalibs /usr/local/cuda/lib64/libcudart.so.12  /usr/local/lib/libcudart.so.12
RUN ldconfig

# Install mooncake, remove torch EP/PG extensions (ep_*/pg_*), chown the package dir to uid 65532 (all in one layer)
RUN pip install --no-cache-dir --index-url "${PIP_INDEX_URL}" \
        mooncake-transfer-engine==${MOONCAKE_VERSION} \
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
