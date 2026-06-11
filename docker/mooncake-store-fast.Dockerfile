# syntax=docker/dockerfile:1.7
#
# Mooncake Store 快速构建镜像 — 使用本地预编译的 wheel，跳过 C++ 编译
# 使用方式:
#   bash scripts/build_wheel.sh        # 编译本地 wheel
#   docker build -f docker/mooncake-store-fast.Dockerfile -t mooncake/mooncake-store:latest .
#   kind load docker-image mooncake/mooncake-store:latest
#
# 对比 docker/mooncake.Dockerfile（全量编译）:
#   - 全量版: 在 Docker 内用 --parallel 2 编译 C++，耗时 20-40 分钟
#   - 快速版: 复制本机预编译的 wheel，耗时 <2 分钟
#   前提条件：本机 build/ 目录已有编译产物，且 ccache 已配置

FROM ubuntu:22.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 安装运行时依赖（无需编译工具链）
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates \
        libnuma1 \
        liburing2 \
        libyaml-cpp0.7 \
        libjsoncpp25 \
        libgflags2.2 \
        libgoogle-glog0v5 \
        libunwind-15 \
        libcurl4 \
        libibverbs1 \
        python3 \
        python3-pip && \
    rm -rf /var/lib/apt/lists/*

# 复制本机预编译的 wheel（由 scripts/build_wheel.sh 生成）
COPY dist/mooncake_transfer_engine*.whl /tmp/mooncake-wheel/

# 安装 wheel（无需重新编译）
RUN python3 -m pip install --no-cache-dir /tmp/mooncake-wheel/*.whl && \
    rm -rf /tmp/mooncake-wheel /root/.cache/pip

# Override mooncake_master with our exact locally-compiled binary.
# The wheel's auditwheel step modifies PIE executables, potentially
# altering behavior; this COPY ensures the precise binary we built runs.
COPY mooncake_master.bin /usr/local/lib/python3.10/dist-packages/mooncake/mooncake_master

# Copy the K8s lease wrapper shared library (built from Go via cgo).
# Required for HA mode (k8s lease-based leader election).
COPY libk8s_lease_wrapper.bin /usr/local/lib/python3.10/dist-packages/mooncake/libk8s_lease_wrapper.so

# Add mooncake Python package dir to library path for libk8s_lease_wrapper.so
# (mooncake_master was compiled with absolute build-machine RPATH entries)
RUN ldconfig /usr/local/lib/python3.10/dist-packages/mooncake/

# 复制可选的辅助脚本
COPY scripts/check_hicache_hugepage_requirements.py /usr/local/bin/mooncake-hicache-sizing
RUN chmod 755 /usr/local/bin/mooncake-hicache-sizing

CMD ["/bin/bash"]
