#!/usr/bin/env bash
#
# reproduce_crash.sh — 复现 mooncake-store 同时杀掉 master + client 的 hang/crash bug
#
# 用法:
#   chmod +x reproduce_crash.sh
#   ./reproduce_crash.sh [--rounds N] [--strategy STRATEGY] [--timeout SECONDS]
#
# 策略 (--strategy):
#   kill9_master_then_term_client  — 先 kill -9 master, 100ms 后 SIGTERM client (默认, 最易触发)
#   term_both                      — 同时 SIGTERM master 和 client
#   kill9_both                     — 同时 kill -9 master 和 client
#   term_master_then_term_client   — SIGTERM master, 100ms 后 SIGTERM client
#   stop_master_then_term_client   — SIGSTOP master (冻结), 100ms 后 SIGTERM client (最佳 hang 复现)
#
# 示例:
#   ./reproduce_crash.sh --rounds 5 --strategy kill9_master_then_term_client --timeout 10
#

set -uo pipefail

# ======================== 配置 ========================
MOONCAKE_BUILD="/home/ljh/develop/Mooncake/build"
MASTER_BIN="${MOONCAKE_BUILD}/mooncake-store/src/mooncake_master"
CLIENT_BIN="${MOONCAKE_BUILD}/mooncake-store/src/mooncake_client"
PYTHON_PATH="${MOONCAKE_BUILD}/mooncake-integration"

RPC_PORT=30051
HTTP_META_PORT=8090
METADATA_SERVER="http://127.0.0.1:${HTTP_META_PORT}/metadata"
MASTER_ADDR="127.0.0.1:${RPC_PORT}"
LOCAL_HOST="127.0.0.1"

LOG_DIR="/home/ljh/bug_reproduce/logs"
ROUND_TOTAL=3
STRATEGY="stop_master_then_term_client"
EXIT_TIMEOUT=10   # client 进程退出的超时秒数

# ======================== 参数解析 ========================
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rounds)    ROUND_TOTAL="$2"; shift 2 ;;
        --strategy)  STRATEGY="$2";    shift 2 ;;
        --timeout)   EXIT_TIMEOUT="$2"; shift 2 ;;
        --help|-h)
            head -18 "$0" | tail -16
            exit 0
            ;;
        *) echo "未知参数: $1"; exit 1 ;;
    esac
done

mkdir -p "${LOG_DIR}"

# ======================== 工具函数 ========================

log() { >&2 echo "[$(date '+%H:%M:%S')] $*"; }

cleanup_procs() {
    # 先 SIGCONT 被冻结的进程，再强制清理
    pkill -CONT -f "mooncake_master.*--rpc_port=${RPC_PORT}" 2>/dev/null || true
    pkill -9 -f "mooncake_master.*--rpc_port=${RPC_PORT}" 2>/dev/null || true
    pkill -9 -f "mooncake_client.*--master_server_address=${MASTER_ADDR}" 2>/dev/null || true
    pkill -9 -f "reproduce_client_worker.py" 2>/dev/null || true
    sleep 0.5
}

wait_for_port() {
    local port=$1 max_wait=$2
    for ((i=0; i<max_wait; i++)); do
        if ss -tlnp 2>/dev/null | grep -q ":${port} " || \
           netstat -tlnp 2>/dev/null | grep -q ":${port} "; then
            return 0
        fi
        sleep 1
    done
    return 1
}

start_master() {
    local logfile="${LOG_DIR}/master_round${1}.log"
    log "启动 master (rpc_port=${RPC_PORT}, http_meta_port=${HTTP_META_PORT})"
    "${MASTER_BIN}" \
        --rpc_port="${RPC_PORT}" \
        --enable_http_metadata_server=true \
        --http_metadata_server_port="${HTTP_META_PORT}" \
        --rpc_conn_timeout_seconds=0 \
        --logtostderr=true \
        > "${logfile}" 2>&1 &
    MASTER_PID=$!
    log "master PID=${MASTER_PID}"

    if ! wait_for_port "${RPC_PORT}" 10; then
        log "ERROR: master 未能在 10s 内启动"
        return 1
    fi
    log "master 已就绪"
}

start_client() {
    local logfile="${LOG_DIR}/client_round${1}.log"
    log "启动 client (master_addr=${MASTER_ADDR})"
    "${CLIENT_BIN}" \
        --master_server_address="${MASTER_ADDR}" \
        --metadata_server="${METADATA_SERVER}" \
        --host="${LOCAL_HOST}" \
        --protocol="tcp" \
        --global_segment_size="256 MB" \
        --logtostderr=true \
        > "${logfile}" 2>&1 &
    CLIENT_PID=$!
    log "client PID=${CLIENT_PID}"
    sleep 3  # 等待 client 完成 setup、注册 segment、启动 heartbeat
    if ! kill -0 "${CLIENT_PID}" 2>/dev/null; then
        log "ERROR: client 进程已退出"
        return 1
    fi
    log "client 已就绪"
}

# 通过 Python 写入测试数据，增加 mounted segment 数量
write_test_data() {
    local round=$1
    log "通过 Python 客户端写入测试数据..."
    PYTHONPATH="${PYTHON_PATH}" python3 << PYEOF > "${LOG_DIR}/python_round${round}.log" 2>&1 || true
import store
import os, sys, time

try:
    s = store.MooncakeDistributedStore()
    rc = s.setup(
        local_hostname="127.0.0.1",
        metadata_server="${METADATA_SERVER}",
        global_segment_size=256 * 1024 * 1024,
        local_buffer_size=64 * 1024 * 1024,
        protocol="tcp",
        rdma_devices="",
        master_server_addr="${MASTER_ADDR}"
    )
    if rc != 0:
        print(f"setup failed: {rc}")
        sys.exit(1)

    # 写入多个 key，让 client 有更多 mounted segments 和活跃状态
    for i in range(10):
        key = f"test_crash_key_{i}"
        data = b"x" * (1024 * 1024)  # 1MB per key
        rc = s.put(key, data)
        print(f"put({key}): {rc}")

    print("数据写入完成，等待 2 秒让 heartbeat 建立...")
    time.sleep(2)

    # 不调用 close()，让 Python 进程自然退出
    # close 会触发正常的清理流程
    s.close()
    print("Python client done")
except Exception as e:
    print(f"Python 客户端异常: {e}")
    sys.exit(1)
PYEOF
    log "Python 客户端数据写入完成"
}

# 执行 kill 策略
execute_kill_strategy() {
    local strategy=$1
    log "执行 kill 策略: ${strategy}"

    case "${strategy}" in
        kill9_master_then_term_client)
            # 最容易触发的场景：master 瞬间死亡，client 尝试优雅退出
            log "  kill -9 master (PID=${MASTER_PID})"
            kill -9 "${MASTER_PID}" 2>/dev/null || true
            sleep 0.1
            log "  kill -TERM client (PID=${CLIENT_PID})"
            kill -TERM "${CLIENT_PID}" 2>/dev/null || true
            ;;
        term_both)
            log "  kill -TERM master+client 同时"
            kill -TERM "${MASTER_PID}" "${CLIENT_PID}" 2>/dev/null || true
            ;;
        kill9_both)
            log "  kill -9 master+client 同时"
            kill -9 "${MASTER_PID}" "${CLIENT_PID}" 2>/dev/null || true
            ;;
        term_master_then_term_client)
            log "  kill -TERM master"
            kill -TERM "${MASTER_PID}" 2>/dev/null || true
            sleep 0.1
            log "  kill -TERM client"
            kill -TERM "${CLIENT_PID}" 2>/dev/null || true
            ;;
        stop_master_then_term_client)
            # SIGSTOP 冻结 master: 进程存活但不响应任何 RPC
            # TCP 连接保持 open, syncAwait 永远等不到回复 → 完美模拟 hang
            log "  kill -STOP master (PID=${MASTER_PID}) — 冻结，不杀死"
            kill -STOP "${MASTER_PID}" 2>/dev/null || true
            sleep 0.1
            log "  kill -TERM client (PID=${CLIENT_PID})"
            kill -TERM "${CLIENT_PID}" 2>/dev/null || true
            ;;
        *)
            log "ERROR: 未知策略 ${strategy}"
            return 1
            ;;
    esac
}

# 监控 client 退出状况
monitor_client_exit() {
    local timeout=$1 round=$2
    local elapsed=0
    local strace_pid=""

    log "等待 client (PID=${CLIENT_PID}) 退出，超时=${timeout}s..."

    # 尝试附加 strace（可能因 ptrace 权限失败，非致命）
    if command -v strace &>/dev/null; then
        strace -p "${CLIENT_PID}" -f -tt -e trace=futex,connect,sendto,recvfrom             -o "${LOG_DIR}/strace_round${round}.log" 2>/dev/null &
        strace_pid=$!
        if kill -0 "${strace_pid}" 2>/dev/null; then
            log "已附加 strace (PID=${strace_pid}) 到 client"
        else
            strace_pid=""
            log "strace 附加失败(权限不足)，跳过系统调用追踪"
        fi
    fi

    while (( elapsed < timeout )); do
        if ! kill -0 "${CLIENT_PID}" 2>/dev/null; then
            wait "${CLIENT_PID}" 2>/dev/null
            local exit_code=$?
            log "client 在 ${elapsed}s 内退出 (exit_code=${exit_code})"
            if [[ -n "${strace_pid}" ]]; then kill "${strace_pid}" 2>/dev/null || true; fi
            echo "OK:${elapsed}s:exit${exit_code}"
            return 0
        fi
        sleep 1
        ((elapsed++))
    done

    # 超时 — bug 已触发
    log "!!! CLIENT HANG 检测到 !!! 进程在 ${timeout}s 后仍未退出"

    # 收集诊断信息
    log "收集诊断信息..."
    {
        echo "=== /proc/${CLIENT_PID}/status ==="
        cat "/proc/${CLIENT_PID}/status" 2>/dev/null || echo "(无法读取)"
        echo ""
        echo "=== /proc/${CLIENT_PID}/stack (内核栈) ==="
        cat "/proc/${CLIENT_PID}/stack" 2>/dev/null || echo "(需要 root 权限)"
        echo ""
        echo "=== /proc/${CLIENT_PID}/task/*/status (所有线程) ==="
        for tid_dir in /proc/${CLIENT_PID}/task/*/; do
            tid=$(basename "$tid_dir")
            echo "--- Thread ${tid} ---"
            cat "${tid_dir}/status" 2>/dev/null | grep -E "^(Name|State|voluntary)" || true
        done
        echo ""
        echo "=== pstack/gdb backtrace ==="
        if command -v gdb &>/dev/null; then
            gdb -batch -ex "thread apply all bt" -p "${CLIENT_PID}" 2>/dev/null || echo "(gdb 附加失败)"
        elif command -v pstack &>/dev/null; then
            pstack "${CLIENT_PID}" 2>/dev/null || echo "(pstack 失败)"
        else
            echo "(gdb/pstack 不可用)"
        fi
    } > "${LOG_DIR}/diag_round${round}.log" 2>&1

    if [[ -n "${strace_pid}" ]]; then kill "${strace_pid}" 2>/dev/null || true; fi

    # 强杀 client
    log "强制杀掉 client (kill -9)"
    kill -9 "${CLIENT_PID}" 2>/dev/null || true
    wait "${CLIENT_PID}" 2>/dev/null || true

    echo "HANG:${timeout}s"
    return 1
}

# ======================== 主流程 ========================

log "======================================================"
log "Mooncake Store 崩溃复现脚本"
log "策略: ${STRATEGY}"
log "轮次: ${ROUND_TOTAL}"
log "超时: ${EXIT_TIMEOUT}s"
log "======================================================"

# 确保环境干净
cleanup_procs

hang_count=0
crash_count=0
ok_count=0

for ((round=1; round<=ROUND_TOTAL; round++)); do
    log ""
    log "==================== 第 ${round}/${ROUND_TOTAL} 轮 ===================="

    # 1. 启动 master
    if ! start_master "${round}"; then
        log "master 启动失败，跳过本轮"
        cleanup_procs
        continue
    fi

    # 2. 启动 client
    if ! start_client "${round}"; then
        log "client 启动失败，跳过本轮"
        cleanup_procs
        continue
    fi

    # 3. 写入测试数据（可选，增加 segment 注册量）
    write_test_data "${round}"

    # 4. 等待 heartbeat 稳定
    log "等待 2s 让 heartbeat 稳定运行..."
    sleep 2

    # 5. 确认进程仍存活
    if ! kill -0 "${MASTER_PID}" 2>/dev/null; then
        log "WARN: master 已提前退出"
        cleanup_procs
        continue
    fi
    if ! kill -0 "${CLIENT_PID}" 2>/dev/null; then
        log "WARN: client 已提前退出"
        cleanup_procs
        continue
    fi

    # 6. 执行 kill 策略
    execute_kill_strategy "${STRATEGY}"

    # 7. 监控 client 退出
    result=$(monitor_client_exit "${EXIT_TIMEOUT}" "${round}" || true)

    if [[ "${result}" == HANG* ]]; then
        ((hang_count++))
        log "第 ${round} 轮结果: HANG (bug 触发)"
    elif [[ "${result}" == OK* ]]; then
        exit_time=$(echo "${result}" | cut -d: -f2)
        exit_code=$(echo "${result}" | cut -d: -f3)
        if [[ "${exit_code}" != "exit0" ]]; then
            ((crash_count++))
            log "第 ${round} 轮结果: CRASH (${exit_code}, 耗时 ${exit_time})"
        else
            ((ok_count++))
            log "第 ${round} 轮结果: OK (正常退出, 耗时 ${exit_time})"
        fi
    else
        ((crash_count++))
        log "第 ${round} 轮结果: UNKNOWN"
    fi

    # 清理残留进程，为下一轮准备
    cleanup_procs
    sleep 1
done

# ======================== 汇总 ========================
log ""
log "======================================================"
log "测试完成 — 共 ${ROUND_TOTAL} 轮"
log "  HANG (bug 触发):  ${hang_count}"
log "  CRASH (异常退出): ${crash_count}"
log "  OK (正常退出):    ${ok_count}"
log "======================================================"

if (( hang_count > 0 )); then
    log ""
    log "Bug 已复现! 详见日志目录: ${LOG_DIR}"
    log "  - strace_round*.log:  系统调用追踪 (卡在 futex/connect)"
    log "  - diag_round*.log:    进程诊断 (线程状态, backtrace)"
    log "  - client_round*.log:  client 标准输出日志"
    log "  - master_round*.log:  master 标准输出日志"
    exit 1
fi

exit 0
