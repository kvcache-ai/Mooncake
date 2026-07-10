#!/usr/bin/env bash
# Mooncake 快速 K8s 部署工具
# 所有命令最终都将变更部署到 K8s 集群
# 使用: bash scripts/dev-cycle.sh <command>
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$(readlink -f "$0")")/.." && pwd)"
OPERATOR_DIR="${ROOT_DIR}/mooncake-operator"
UI_DIR="${ROOT_DIR}/mooncake-operator/ui"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()  { echo -e "${BLUE}[INFO]${NC} $*"; }
ok()    { echo -e "${GREEN}[OK]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*" >&2; }

# ==============================================================
# 批量重启所有用到 mooncake-store 镜像的 Pod
# ==============================================================
restart-store-pods() {
    info "重启所有使用 mooncake-store 镜像的工作负载..."
    local ns="${1:-}"
    if [[ -z "$ns" ]]; then
        # 找到所有包含 mooncake-store 相关 CR 的 namespace
        ns=$(kubectl get mooncakeclusters.mooncake.io --all-namespaces -o jsonpath='{.items[*].metadata.namespace}' 2>/dev/null || true)
    fi
    if [[ -n "$ns" ]]; then
        for n in $ns; do
            # Restart statefulsets and deployments directly by name pattern instead of label selector
            # (statefulset labels may not match template labels, causing selector-based restart to fail)
            kubectl get statefulset -n "$n" -o name 2>/dev/null | while read -r sts; do
                kubectl rollout restart "$sts" -n "$n"
            done
            kubectl get deployment -n "$n" -o name 2>/dev/null | while read -r deploy; do
                kubectl rollout restart "$deploy" -n "$n"
            done
            kubectl rollout status statefulset -n "$n" --timeout=120s 2>/dev/null || true
            kubectl rollout status deployment -n "$n" --timeout=120s 2>/dev/null || true
        done
    else
        info "未找到 MooncakeCluster CR，跳过 Pod 重启"
        info "提示: 创建一个 MooncakeCluster CR 后，新的 pods 会自动使用更新后的镜像"
    fi
    ok "Store Pod 重启完成"
}

# ==============================================================
# 1. Operator → K8s（只改 Go 代码时用）
#    流程: go build → docker → kind load → rollout restart
#    耗时: ~30 秒
# ==============================================================
deploy-operator() {
    info "=== 快速部署 Operator 到 K8s ==="
    cd "${OPERATOR_DIR}"
    go build -o bin/manager ./cmd/main.go && ok "Go 编译完成"
    docker build -t mooncake-operator:latest . && ok "Docker 镜像构建完成"
    local kind_cluster="${KIND_CLUSTER:-three-node-cluster}"
    kind load docker-image --name "$kind_cluster" mooncake-operator:latest && ok "镜像加载到 kind"
    kubectl rollout restart deployment -n mooncake-operator-system mooncake-operator-controller-manager
    kubectl rollout status deployment -n mooncake-operator-system mooncake-operator-controller-manager --timeout=60s
    ok "Operator 部署完成！"
}

# ==============================================================
# 2. UI → K8s（只改前端 TypeScript 代码时用）
#    流程: npm run build → docker build (hostbuild) → kind load → rollout restart → port-forward
#    耗时: ~1 分钟
# ==============================================================
deploy-ui() {
    info "=== 快速部署 UI 到 K8s ==="
    cd "${UI_DIR}"
    [[ ! -d node_modules ]] && npm install
    npm run build && ok "Next.js 构建完成"
    docker build -f Dockerfile.hostbuild -t mooncake-operator-ui:latest . && ok "Docker 镜像构建完成"
    local kind_cluster="${KIND_CLUSTER:-three-node-cluster}"
    kind load docker-image --name "$kind_cluster" mooncake-operator-ui:latest && ok "镜像加载到 kind"
    kubectl rollout restart deployment -n mooncake-operator-system mooncake-operator-ui
    kubectl rollout status deployment -n mooncake-operator-system mooncake-operator-ui --timeout=60s
    # 重启端口转发
    kill $(lsof -ti:8801 2>/dev/null) 2>/dev/null || true
    kubectl port-forward svc/mooncake-operator-ui 8801:3000 -n mooncake-operator-system --address 0.0.0.0 &
    ok "UI 部署完成！访问 http://localhost:8801"
}

# ==============================================================
# 3. Store（C++）→ K8s（改 C++ 核心代码时用）
#    流程: cmake build (本地, ccache) → build_wheel.sh → docker build (快速) → kind load → restart pods
#    耗时: ~3-5 分钟（对比全量 Docker 编译: 20-40 分钟）
#    前提: ccache 已配置，build/ 目录已有编译产物
# ==============================================================
deploy-store() {
    info "=== 快速部署 Store（C++）到 K8s ==="

    # Step 1: 增量编译 C++
    local jobs="${MAX_JOBS:-2}"
    cd "${ROOT_DIR}/build"
    if [[ ! -f CMakeCache.txt ]]; then
        info "CMake 尚未配置，执行 cmake..."
        cmake .. -DCMAKE_BUILD_TYPE=Release \
            -DUSE_CUDA=OFF -DWITH_EP=OFF \
            -DSTORE_USE_K8S_LEASE=ON -DUSE_HTTP=ON \
            -DBUILD_UNIT_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF
    fi
    info "增量编译 C++（并行度: ${jobs}）..."
    cmake --build . --parallel "${jobs}" && ok "C++ 编译完成"

    # Step 2: 编译 Go K8s lease wrapper（如果还没编译）
    if [[ ! -f "${ROOT_DIR}/build/mooncake-common/k8s-lease/libk8s_lease_wrapper.so" ]]; then
        info "编译 K8s lease wrapper..."
        cmake --build . --parallel "${jobs}" --target build_k8s_lease_wrapper 2>/dev/null || true
    fi

    # Step 3: 生成 Python wheel
    # Fast path: if dist/ already has a wheel, only update the binaries inside
    # it (unpack → replace .so/.bin → repack). Skips python -m build + auditwheel.
    # This cuts wheel step from ~130s to ~10s when only C++ code changed.
    info "生成 wheel 包..."
    cd "${ROOT_DIR}"
    if ls dist/*.whl 2>/dev/null | head -1 | grep -q .; then
        info "已有 wheel，使用快速更新模式 (WHEEL_REUSE=1)..."
        WHEEL_REUSE=1 bash scripts/build_wheel.sh && ok "Wheel 包快速更新完成"
    else
        bash scripts/build_wheel.sh && ok "Wheel 包构建完成"
    fi

    # Ensure dist/ has a wheel for the Dockerfile to copy
    if [ ! -f dist/*.whl ]; then
        info "dist/ 缺少 wheel，从 build_wheel.sh 输出目录复制..."
        # build_wheel.sh already put the wheel in dist/ via its own OUTPUT_DIR
    fi

    # Step 4: 复制 wheel 到 docker-pip-cache（供 Dockerfile 安装）
    info "复制 wheel 到 docker-pip-cache..."
    # build_wheel.sh 将 wheel 输出到 mooncake-wheel/dist/
    WHL_SRC=$(ls mooncake-wheel/dist/mooncake_transfer_engine-*.whl 2>/dev/null | head -1 || true)
    if [[ -n "$WHL_SRC" ]]; then
        cp "$WHL_SRC" docker-pip-cache/
        ok "Wheel 已复制到 docker-pip-cache/"
    else
        WHL_SRC=$(ls dist/mooncake_transfer_engine-*.whl 2>/dev/null | head -1 || true)
        if [[ -n "$WHL_SRC" ]]; then
            cp "$WHL_SRC" docker-pip-cache/
            ok "Wheel 已复制到 docker-pip-cache/"
        else
            warn "未找到 wheel 文件，Docker 可能使用缓存的旧 wheel"
        fi
    fi

    # Step 5: 构建快速 Docker 镜像（使用预编译 wheel，跳过 C++ 编译）
    info "构建 Store Docker 镜像（快速模式，无 C++ 编译）..."
    # Update mooncake_master.bin from build output (Dockerfile copies this into the image)
    cp "${ROOT_DIR}/build/mooncake-store/src/mooncake_master" "${ROOT_DIR}/mooncake_master.bin" && \
        ok "mooncake_master.bin 已更新"
    docker build --no-cache -f docker/mooncake-store-fast.Dockerfile \
        -t mooncake/mooncake-store:latest \
        -t mooncake-store:latest . && ok "Docker 镜像构建完成"

    # Step 5: 加载到 kind（两个 tag，适配不同 CR 配置）
    local kind_cluster="${KIND_CLUSTER:-three-node-cluster}"
    kind load docker-image --name "$kind_cluster" mooncake/mooncake-store:latest && ok "镜像(mooncake/mooncake-store)加载到 kind"
    kind load docker-image --name "$kind_cluster" mooncake-store:latest 2>&1 | grep -v "already present" || true

    # Step 6: 重启 Pod
    restart-store-pods

    ok "Store 部署完成！"
}

# ==============================================================
# 4. 全量部署（Operator + UI + Store 全部更新到 K8s）
# ==============================================================
deploy-all() {
    deploy-operator
    deploy-ui
    deploy-store
    ok "全部部署完成！"
}

# ==============================================================
# 5. C++ 编译缓存（ccache）一次性配置
# ==============================================================
setup-ccache() {
    if ! command -v ccache &>/dev/null; then
        info "安装 ccache..."
        apt-get update -qq && apt-get install -y -qq ccache
    fi
    ok "ccache 版本: $(ccache --version | head -1)"
    ccache -M 10G && ccache -z
    mkdir -p ~/.ccache
    cat > ~/.ccache/ccache.conf << 'CCACHE_CONF'
max_size = 10.0G
cache_dir = /root/.ccache
compression = true
compression_level = 6
CCACHE_CONF
    # 将 ccache 加入 PATH 最前面
    mkdir -p /usr/local/lib/ccache/bin
    for compiler in gcc g++; do
        local path; path=$(which "$compiler" 2>/dev/null || true)
        if [[ -n "$path" ]] && ! readlink "$path" | grep -q ccache; then
            ln -sf "$(which ccache)" "/usr/local/lib/ccache/bin/${compiler}"
        fi
    done
    export PATH="/usr/local/lib/ccache/bin:$PATH"
    if ! grep -q "ccache/bin" ~/.bashrc 2>/dev/null; then
        echo 'export PATH="/usr/local/lib/ccache/bin:$PATH"' >> ~/.bashrc
    fi
    ok "ccache 配置完成（最大缓存 10GB）"
    info "下次 cmake 重新配置时会自动检测到 ccache"
}

# ==============================================================
# 6. 纯 C++ 增量编译（不部署，用于检查编译是否能通过）
# ==============================================================
build-cpp() {
    local target="${1:-}"
    local jobs="${MAX_JOBS:-2}"
    cd "${ROOT_DIR}/build"
    if [[ ! -f CMakeCache.txt ]]; then
        info "CMake 尚未配置，执行 cmake..."
        cmake .. -DCMAKE_BUILD_TYPE=Release \
            -DUSE_CUDA=OFF -DWITH_EP=OFF \
            -DSTORE_USE_K8S_LEASE=ON -DUSE_HTTP=ON \
            -DBUILD_UNIT_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF
    fi
    info "增量编译 C++（并行度: ${jobs}）..."
    if [[ -n "$target" ]]; then
        cmake --build . --target "${target}" --parallel "${jobs}"
    else
        cmake --build . --parallel "${jobs}"
    fi
    ok "C++ 编译完成"
}

# ==============================================================
# 7. 重新配置 CMake 使用 Ninja（更快增量编译）
# ==============================================================
reconfigure-ninja() {
    if ! command -v ninja &>/dev/null; then
        info "安装 ninja-build..."
        apt-get install -y -qq ninja-build
    fi
    info "配置 CMake 使用 Ninja 生成器..."
    mkdir -p "${ROOT_DIR}/build-ninja"
    cd "${ROOT_DIR}/build-ninja"
    cmake .. -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DUSE_CUDA=OFF -DWITH_EP=OFF \
        -DSTORE_USE_K8S_LEASE=ON -DUSE_HTTP=ON \
        -DBUILD_UNIT_TESTS=OFF -DBUILD_EXAMPLES=OFF -DBUILD_BENCHMARK=OFF
    ok "Ninja 构建目录已配置: ${ROOT_DIR}/build-ninja"
    info "使用: cd build-ninja && ninja"
    info "Ninja 优势: 增量更快 + 自动 job pool（编译多核/链接少核防 OOM）"
}

# ==============================================================
# 主入口
# ==============================================================
main() {
    local cmd="${1:-help}"
    shift 2>/dev/null || true
    case "$cmd" in
        deploy-operator)   deploy-operator;;
        deploy-ui)         deploy-ui;;
        deploy-store)      deploy-store;;
        deploy-all)        deploy-all;;
        setup-ccache)      setup-ccache;;
        build-cpp)         build-cpp "$@";;
        reconfigure-ninja) reconfigure-ninja;;
        *)
            echo "Mooncake 快速 K8s 部署工具"
            echo ""
            echo "用法: bash scripts/dev-cycle.sh <command>"
            echo ""
            echo "--- K8s 部署命令（修改 → 自动推送至 K8s） ---"
            echo "  deploy-operator  改 Go 代码后: 编译 → Docker → kind → 重启 pod (~30s)"
            echo "  deploy-ui        改 UI 代码后: npm build → Docker → kind → 重启 pod (~1min)"
            echo "  deploy-store     改 C++ 代码后: 增量编译 → wheel → docker → kind → 重启 (~3min)"
            echo "  deploy-all       一次部署全部组件"
            echo ""
            echo "--- 辅助命令 ---"
            echo "  setup-ccache      安装配置 C++ 编译缓存（一次性的，已执行）"
            echo "  build-cpp [目标]  纯 C++ 增量编译（不部署，用于检查）"
            echo "  reconfigure-ninja 切换到 Ninja 构建系统"
            ;;
    esac
}
main "$@"
