#!/bin/bash
# Mooncake Store 集群部署脚本
# 目标：在 SKV RDMA 集群 (skv-node1~5) 上编译部署 Mooncake Store
#
# 使用方法：
#   1. 先在 skv-node1 上运行: bash scripts/deploy_cluster.sh build
#   2. 启动 master:          bash scripts/deploy_cluster.sh master
#   3. 在其他节点启动 client:  bash scripts/deploy_cluster.sh client
#   4. 跑 benchmark:          bash scripts/deploy_cluster.sh bench

set -e

# ============================================================
# 集群配置
# ============================================================
WORK_DIR="/home/kvgroup/chaomei/Mooncake"
BUILD_DIR="${WORK_DIR}/build"

# 节点 RDMA IP
NODE1_RDMA="10.0.0.61"
NODE2_RDMA="10.0.0.62"
NODE3_RDMA="10.0.0.63"
NODE4_RDMA="10.0.0.64"
NODE5_RDMA="10.0.0.65"

# 节点千兆 IP（用于管理）
NODE1_MGMT="192.168.1.116"
NODE2_MGMT="192.168.1.102"
NODE3_MGMT="192.168.1.103"
NODE4_MGMT="192.168.1.117"
NODE5_MGMT="192.168.1.141"

# Master 配置
MASTER_PORT=50051
MASTER_ADDR="${NODE1_RDMA}:${MASTER_PORT}"

# ============================================================
# 函数定义
# ============================================================

check_rdma() {
    echo "=== 检查 RDMA 设备 ==="
    if command -v ibstat &>/dev/null; then
        ibstat | head -20
    else
        echo "ibstat 未安装，尝试 ibv_devinfo..."
        ibv_devinfo 2>/dev/null | head -30 || echo "无 RDMA 设备信息"
    fi
    echo ""
    echo "=== 检查 RDMA 网络连通性 ==="
    for ip in $NODE1_RDMA $NODE2_RDMA $NODE3_RDMA $NODE4_RDMA $NODE5_RDMA; do
        ping -c 1 -W 1 $ip &>/dev/null && echo "  $ip: OK" || echo "  $ip: UNREACHABLE"
    done
}

install_deps() {
    echo "=== 安装依赖 ==="
    cd "$WORK_DIR"
    sudo bash dependencies.sh -y
}

build() {
    echo "=== 编译 Mooncake Store ==="
    cd "$WORK_DIR"

    # 初始化 submodules
    git submodule update --init --recursive

    mkdir -p "$BUILD_DIR"
    cd "$BUILD_DIR"

    # 编译选项：
    # - WITH_STORE=ON: 编译 Mooncake Store
    # - WITH_TE=ON: 编译 Transfer Engine (Store 依赖)
    # - USE_HTTP=ON: 使用 HTTP metadata server（简单部署，无需 etcd）
    # - BUILD_UNIT_TESTS=ON: 编译单元测试
    cmake .. \
        -DWITH_STORE=ON \
        -DWITH_TE=ON \
        -DUSE_HTTP=ON \
        -DBUILD_UNIT_TESTS=ON \
        -DCMAKE_BUILD_TYPE=Release

    make -j$(nproc)

    echo "=== 编译完成 ==="
    echo "可执行文件:"
    ls -la mooncake-store/mooncake_master 2>/dev/null || echo "  master: 未找到"
    ls -la mooncake-store/benchmarks/master_bench 2>/dev/null || echo "  master_bench: 未找到"
    ls -la mooncake-store/benchmarks/allocator_bench 2>/dev/null || echo "  allocator_bench: 未找到"
    ls -la mooncake-store/benchmarks/allocation_strategy_bench 2>/dev/null || echo "  allocation_strategy_bench: 未找到"
    ls -la mooncake-store/benchmarks/storage_backend_bench 2>/dev/null || echo "  storage_backend_bench: 未找到"
}

start_master() {
    echo "=== 启动 Mooncake Master (node1) ==="
    cd "$BUILD_DIR"

    # 检查是否已有 master 在运行
    if pgrep -f mooncake_master &>/dev/null; then
        echo "Master 已在运行，先停止..."
        pkill -f mooncake_master || true
        sleep 1
    fi

    # 启动 master（后台运行）
    ./mooncake-store/mooncake_master \
        --rpc_port=${MASTER_PORT} \
        --memory_allocator=offset \
        --eviction_strategy=lru \
        --eviction_high_watermark_ratio=0.95 \
        2>&1 | tee /tmp/mooncake_master.log &

    sleep 2
    if pgrep -f mooncake_master &>/dev/null; then
        echo "Master 启动成功，监听 ${MASTER_ADDR}"
    else
        echo "Master 启动失败，查看日志: /tmp/mooncake_master.log"
        tail -20 /tmp/mooncake_master.log
    fi
}

stop_master() {
    echo "=== 停止 Mooncake Master ==="
    pkill -f mooncake_master || echo "没有运行中的 master"
}

run_unit_tests() {
    echo "=== 运行单元测试 ==="
    cd "$BUILD_DIR"
    ctest --test-dir mooncake-store --output-on-failure -j$(nproc) 2>&1 | tee /tmp/mooncake_test.log
}

run_master_bench() {
    echo "=== 运行 Master Benchmark ==="
    echo "测试 PutStart/PutEnd/Query/Remove 吞吐量"
    cd "$BUILD_DIR"

    # 确保 master 在运行
    if ! pgrep -f mooncake_master &>/dev/null; then
        echo "错误: Master 未运行，请先执行 'bash scripts/deploy_cluster.sh master'"
        exit 1
    fi

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    RESULT_FILE="/tmp/master_bench_${TIMESTAMP}.log"

    echo "--- BatchPut (默认参数) ---"
    ./mooncake-store/benchmarks/master_bench \
        --master_server=${MASTER_ADDR} \
        --operation=BatchPut \
        --num_segments=16 \
        --num_clients=4 \
        --num_threads=4 \
        --value_size=4096 \
        --batch_size=128 \
        --duration=30 \
        2>&1 | tee -a "$RESULT_FILE"

    echo ""
    echo "--- BatchGet ---"
    ./mooncake-store/benchmarks/master_bench \
        --master_server=${MASTER_ADDR} \
        --operation=BatchGet \
        --num_segments=16 \
        --num_clients=4 \
        --num_threads=4 \
        --value_size=4096 \
        --batch_size=128 \
        --num_keys=10000 \
        --duration=30 \
        2>&1 | tee -a "$RESULT_FILE"

    echo ""
    echo "--- Remove ---"
    ./mooncake-store/benchmarks/master_bench \
        --master_server=${MASTER_ADDR} \
        --operation=Remove \
        --num_segments=16 \
        --num_clients=4 \
        --num_threads=4 \
        --value_size=4096 \
        --num_keys=10000 \
        --duration=30 \
        2>&1 | tee -a "$RESULT_FILE"

    echo ""
    echo "结果保存至: $RESULT_FILE"
}

run_allocator_bench() {
    echo "=== 运行 Allocator Benchmark ==="
    cd "$BUILD_DIR"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    ./mooncake-store/benchmarks/allocator_bench 2>&1 | tee "/tmp/allocator_bench_${TIMESTAMP}.log"
}

run_allocation_strategy_bench() {
    echo "=== 运行 Allocation Strategy Benchmark ==="
    cd "$BUILD_DIR"

    TIMESTAMP=$(date +%Y%m%d_%H%M%S)
    ./mooncake-store/benchmarks/allocation_strategy_bench 2>&1 | tee "/tmp/allocation_strategy_bench_${TIMESTAMP}.log"
}

run_all_benchmarks() {
    echo "========================================="
    echo "  Mooncake Store Baseline Benchmark"
    echo "  $(date)"
    echo "========================================="

    run_allocator_bench
    echo ""
    run_allocation_strategy_bench
    echo ""
    run_master_bench
}

sync_to_cluster() {
    echo "=== 同步代码到集群所有节点 ==="
    for node in $NODE2_MGMT $NODE3_MGMT $NODE4_MGMT $NODE5_MGMT; do
        echo "同步到 $node ..."
        rsync -az --delete \
            --exclude='.git' \
            --exclude='build' \
            "$WORK_DIR/" "kvgroup@${node}:${WORK_DIR}/"
    done
    echo "同步完成"
}

# ============================================================
# 主入口
# ============================================================

case "${1:-help}" in
    check)
        check_rdma
        ;;
    deps)
        install_deps
        ;;
    build)
        build
        ;;
    master)
        start_master
        ;;
    stop)
        stop_master
        ;;
    test)
        run_unit_tests
        ;;
    bench)
        run_all_benchmarks
        ;;
    bench-master)
        run_master_bench
        ;;
    bench-alloc)
        run_allocator_bench
        ;;
    bench-strategy)
        run_allocation_strategy_bench
        ;;
    sync)
        sync_to_cluster
        ;;
    help|*)
        echo "Mooncake Store 集群部署工具"
        echo ""
        echo "用法: bash scripts/deploy_cluster.sh <command>"
        echo ""
        echo "命令:"
        echo "  check          检查 RDMA 设备和网络连通性"
        echo "  deps           安装编译依赖"
        echo "  build          编译 Mooncake Store"
        echo "  master         启动 Master 服务"
        echo "  stop           停止 Master 服务"
        echo "  test           运行单元测试"
        echo "  bench          运行全部 benchmark（baseline）"
        echo "  bench-master   仅运行 Master benchmark"
        echo "  bench-alloc    仅运行 Allocator benchmark"
        echo "  bench-strategy 仅运行 Allocation Strategy benchmark"
        echo "  sync           同步代码到集群所有节点"
        echo ""
        echo "典型工作流:"
        echo "  1. ssh SKV_1"
        echo "  2. cd ${WORK_DIR}"
        echo "  3. bash scripts/deploy_cluster.sh check   # 检查环境"
        echo "  4. bash scripts/deploy_cluster.sh deps    # 安装依赖（首次）"
        echo "  5. bash scripts/deploy_cluster.sh build   # 编译"
        echo "  6. bash scripts/deploy_cluster.sh master  # 启动 master"
        echo "  7. bash scripts/deploy_cluster.sh bench   # 跑 baseline"
        ;;
esac
