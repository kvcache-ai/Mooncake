# syntax=docker/dockerfile:1.7
#
# Mooncake Store 快速构建镜像 — 使用本地预编译的 wheel，跳过 C++ 编译
# 使用方式:
#   bash scripts/build_wheel.sh        # 编译本地 wheel
#   docker build -f docker/mooncake-store-fast.Dockerfile -t mooncake/mooncake-store:latest .
#   kind load docker-image mooncake/mooncake-store:latest

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 安装运行时依赖（无需编译工具链）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libnuma1 \
        liburing-dev \
        libyaml-cpp0.8 \
        libjsoncpp25 \
        libgflags2.2 \
        libgoogle-glog0v6 \
        libunwind-dev \
        libcurl4 \
        libibverbs1 \
        ibverbs-providers \
        python3 \
        python3-pip && \
    rm -rf /var/lib/apt/lists/*

# 复制预下载的 wheel 和依赖
COPY docker-pip-cache/ /tmp/pip-cache/

# 从本地缓存安装 wheel 和所有依赖
RUN python3 -m pip install --break-system-packages --no-index --find-links=/tmp/pip-cache/ --no-cache-dir /tmp/pip-cache/mooncake_transfer_engine*.whl && \
    rm -rf /tmp/pip-cache /root/.cache/pip

# Override mooncake_master with our exact locally-compiled binary.
COPY mooncake_master.bin /usr/local/lib/python3.12/dist-packages/mooncake/mooncake_master

# Copy the K8s lease wrapper shared library (built from Go via cgo).
COPY libk8s_lease_wrapper.bin /usr/local/lib/python3.12/dist-packages/mooncake/libk8s_lease_wrapper.so

# Add mooncake Python package dir to library path
RUN ldconfig /usr/local/lib/python3.12/dist-packages/mooncake/

# 复制可选的辅助脚本
COPY scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN chmod 755 /usr/local/bin/mooncake-hicache-sizing

CMD ["/bin/bash"]
