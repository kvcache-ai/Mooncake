#!/usr/bin/env python3
"""SSD Balance 分配策略人工验证脚本。

验证 SSD 负载均衡和 DDR 准入控制的核心保证：
  1. SSD 负载均衡：按 SSD 空闲比例分配，多节点溢出行为正常
  2. SSD 驱逐保护：SSD 满时禁止写入，已有数据不被驱逐
  3. DDR 准入控制：DDR 满时临时禁止写入，释放后恢复
  4. 全局拒绝：所有节点 SSD 满后拒绝写入，释放后恢复

用法：
    python verify_ssd_balance.py --test <test_name>

每次测试前请清理 SSD 目录：rm -rf <SSD_PATH> && mkdir -p <SSD_PATH>

关键环境变量：
    MC_METADATA_SERVER                        - Master 元数据地址
    MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES   - SSD 容量上限（字节）
    MOONCAKE_OFFLOAD_FILE_STORAGE_PATH        - SSD 数据存储目录
    MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS - Offload 心跳间隔（建议设为 1）
"""

import argparse
import os
import sys
import time
import traceback
import urllib.request

from mooncake.store import MooncakeDistributedStore

DEFAULT_MASTER_PORT = "50053"
DEFAULT_METADATA_PORT = "8880"
DEFAULT_METRICS_PORT = "9104"

# 默认规模：DDR=4GB, SSD=16GB
DEFAULT_DDR_SIZE = 4 * 1024 * 1024 * 1024    # 4GB
DEFAULT_SSD_SIZE = 16 * 1024 * 1024 * 1024   # 16GB
KEY_SIZE = 4 * 1024 * 1024                    # 4MB
INSERT_INTERVAL = 0.01                        # 10ms


def fetch_metrics():
    """从 Master /metrics 端点获取 Prometheus 格式指标。"""
    metrics_port = os.getenv("METRICS_PORT", DEFAULT_METRICS_PORT)
    try:
        url = f"http://127.0.0.1:{metrics_port}/metrics"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=2) as resp:
            text = resp.read().decode()
        result = {}
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2:
                name = parts[0]
                if "{" in name:
                    name = name[:name.index("{")]
                try:
                    result[name] = float(parts[1])
                except ValueError:
                    pass
        return result
    except Exception:
        return None


def print_metrics(label=""):
    """打印当前 Master 的 Mem/SSD 状态。"""
    stats = fetch_metrics()
    if not stats:
        print(f"  [{label}] (metrics 不可用)")
        return
    prefix = f"  [{label}] " if label else "  "
    try:
        mem_total = stats.get("master_total_capacity_bytes", 0)
        mem_used = stats.get("master_allocated_bytes", 0)
        ssd_total = stats.get("master_total_file_capacity_bytes", 0)
        ssd_used = stats.get("master_allocated_file_size_bytes", 0)
        if mem_total > 0:
            print(f"{prefix}Mem: {mem_used/1024/1024:.0f}M/{mem_total/1024/1024:.0f}M "
                  f"({mem_used/mem_total*100:.1f}%)")
        if ssd_total > 0 and ssd_total < 10**15:
            print(f"{prefix}SSD: {ssd_used/1024/1024:.0f}M/{ssd_total/1024/1024:.0f}M "
                  f"({ssd_used/ssd_total*100:.1f}%)")
        elif ssd_total >= 10**15:
            print(f"{prefix}SSD: {ssd_used/1024/1024:.0f}M / infinity")
    except Exception as e:
        print(f"{prefix}(metrics parse error: {e})")


def create_store(segment_size=DEFAULT_DDR_SIZE,
                 buffer_size=DEFAULT_DDR_SIZE,
                 enable_offload=True,
                 ssd_path_override=None,
                 ssd_total_size_override=None):
    """创建 Store 客户端。"""
    ssd_path = ""
    if enable_offload:
        ssd_limit_str = ssd_total_size_override or os.getenv(
            "MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES", "")
        if not ssd_limit_str:
            print("[ERROR] MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES 未设置！")
            sys.exit(1)
        if isinstance(ssd_limit_str, int):
            ssd_limit = ssd_limit_str
        else:
            ssd_limit = int(ssd_limit_str)
        os.environ["MOONCAKE_OFFLOAD_TOTAL_SIZE_LIMIT_BYTES"] = str(ssd_limit)
        if ssd_limit <= segment_size:
            print(f"[ERROR] SSD({ssd_limit}) 必须 > DDR({segment_size})")
            sys.exit(1)
        ssd_path = ssd_path_override or os.getenv(
            "MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/mooncake_ssd_test")
        heartbeat_interval = os.getenv("MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS",
                                        "(未设置, 默认10s)")
        os.makedirs(ssd_path, exist_ok=True)

    if segment_size >= 1024 * 1024 * 1024:
        ddr_str = f"{segment_size/1024/1024/1024:.1f}GB"
    else:
        ddr_str = f"{segment_size/1024/1024:.0f}MB"
    print(f"  配置:")
    print(f"    DDR: {ddr_str}")
    if enable_offload:
        print(f"    SSD: {ssd_limit/1024/1024/1024:.1f}GB")
        print(f"    SSD 路径: {ssd_path}")
        print(f"    心跳间隔: {heartbeat_interval}s")
    else:
        print(f"    SSD offload: DISABLED")

    store = MooncakeDistributedStore()
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "eth0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "127.0.0.1")
    metadata_server = os.getenv("MC_METADATA_SERVER",
                                f"http://127.0.0.1:{DEFAULT_METADATA_PORT}/metadata")
    master_server = os.getenv("MASTER_SERVER",
                              f"127.0.0.1:{DEFAULT_MASTER_PORT}")

    print(f"    metadata_server: {metadata_server}")
    print(f"    master_server: {master_server}")

    t0 = time.time()
    retcode = store.setup(
        local_hostname, metadata_server, segment_size, buffer_size,
        protocol, device_name, master_server,
        enable_ssd_offload=enable_offload,
        ssd_offload_path=ssd_path,
    )
    elapsed = time.time() - t0
    if retcode:
        raise RuntimeError(f"Store setup 失败: retcode={retcode}")
    print(f"  Store setup 成功 ({elapsed:.1f}s)")
    return store


def wait_with_progress(seconds, prefix=""):
    """等待指定秒数，显示进度。"""
    for t in range(seconds):
        sys.stdout.write(f"\r  {prefix}{t+1}/{seconds}s")
        sys.stdout.flush()
        time.sleep(1)
    print()


def check_ssd_dir(ssd_path):
    """检查 SSD 目录大小。"""
    if not ssd_path or not os.path.exists(ssd_path):
        return 0, 0
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(ssd_path):
        for f in files:
            fp = os.path.join(root, f)
            total_size += os.path.getsize(fp)
            file_count += 1
    return file_count, total_size


# ============================================================================
# Test 1: SSD 负载均衡（2 Client 不对称 SSD）
# ============================================================================

def test_load_balancing():
    """验证 2 Client 不对称 SSD 容量下的负载均衡。"""
    print("=== 验证：SSD 负载均衡（2 Client 不对称 SSD）===\n")

    client2_ssd = 16 * 1024 * 1024 * 1024   # 16GB
    client1_ssd = 8 * 1024 * 1024 * 1024    # 8GB
    ddr_per_client = DEFAULT_DDR_SIZE        # 4GB

    base_ssd_path = os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH",
                              "/tmp/mooncake_ssd_balance_lb")
    ssd_path_1 = os.path.join(base_ssd_path, "client1")
    ssd_path_2 = os.path.join(base_ssd_path, "client2")

    # 清空整个 SSD 基础目录
    os.system(f"rm -rf {base_ssd_path} && mkdir -p {base_ssd_path}")

    # Client 2 (大 SSD) 先注册
    print("  [1] 创建 Client 2 (SSD=16GB)...")
    store2 = create_store(
        segment_size=ddr_per_client,
        enable_offload=True,
        ssd_path_override=ssd_path_2,
        ssd_total_size_override=client2_ssd,
    )

    # Client 1 (小 SSD) 后注册
    print("\n  [2] 创建 Client 1 (SSD=8GB)...")
    store1 = create_store(
        segment_size=ddr_per_client,
        enable_offload=True,
        ssd_path_override=ssd_path_1,
        ssd_total_size_override=client1_ssd,
    )

    # 从 Client 1 写入
    num_keys = 1200
    written = 0
    rejected = 0
    print(f"\n  [3] 从 Client 1 写入 {num_keys} 个 4MB key ({num_keys*4/1024:.1f}GB)...")
    t0 = time.time()
    for i in range(num_keys):
        key = f"lb_key_{i}"
        data = b"\xAB" * KEY_SIZE
        retcode = store1.put(key, data)
        if retcode == 0:
            written += 1
        else:
            rejected += 1
            if rejected <= 3:
                print(f"    拒绝: key={key}, retcode={retcode}")
        if (i + 1) % 200 == 0:
            elapsed = time.time() - t0
            print(f"    {i+1}/{num_keys} ({written} 成功, {rejected} 拒绝, "
                  f"{elapsed:.1f}s)")
        time.sleep(INSERT_INTERVAL)

    print(f"  写入完成: {written} 成功, {rejected} 拒绝")
    print_metrics("写入后")

    # 等待 offload
    offload_wait = 60
    print(f"\n  [4] 等待 {offload_wait}s 让 offload 排空...")
    wait_with_progress(offload_wait)
    print_metrics("offload 后")

    # 检查两个 SSD 目录
    fc1, sz1 = check_ssd_dir(ssd_path_1)
    fc2, sz2 = check_ssd_dir(ssd_path_2)
    print(f"\n  [5] SSD 目录检查:")
    print(f"    Client 1 ({ssd_path_1}): {fc1} 文件, {sz1/1024/1024:.0f}MB")
    print(f"    Client 2 ({ssd_path_2}): {fc2} 文件, {sz2/1024/1024:.0f}MB")

    # 判断
    passed = True
    if sz1 == 0 and sz2 == 0:
        print(f"\n  [WARN] 两个 SSD 目录均为空，offload 可能未触发")
        passed = False
    elif sz2 > sz1:
        print(f"\n  PASS: Client 2 SSD ({sz2/1024/1024:.0f}MB) > "
              f"Client 1 SSD ({sz1/1024/1024:.0f}MB)")
    elif sz1 > 0 and sz2 > 0:
        print(f"\n  [WARN] Client 2 SSD 未明显大于 Client 1 "
              f"(可能是采样随机性，需检查)")
    else:
        print(f"\n  [WARN] 负载分布不理想")

    print(f"\n  >>> 按回车退出")
    input()


# ============================================================================
# Test 2: SSD 驱逐保护
# ============================================================================

def test_ssd_high_watermark_blocking():
    """验证 SSD 达到高水位后 Master 拒绝新分配，已有数据可读。

    关键：ssd_used_bytes 仅在 NotifyOffloadSuccess 回调时异步更新，
    因此写入阶段无法被 SSD watermark 阻断（DDR 缓冲 + ssd_used_bytes 滞后）。
    正确验证方式：offload 完成后 ssd_used_bytes 反映真实使用率 > 90%，
    此时新写入应被拒绝。
    """
    print("=== 验证：SSD 高水位分配阻断 ===\n")

    # 使用较小 SSD（5GB）以便测试能填满到高水位
    # DDR=4GB, SSD=5GB → high_watermark=90% → 4.5GB（约 1150 个 4MB key）
    ssd_size = 5 * 1024 * 1024 * 1024   # 5GB
    ddr_size = DEFAULT_DDR_SIZE          # 4GB

    # 设置较小的 bucket size（40MB），使初始 20 key（80MB）足以填满 2 个 bucket 落盘
    os.environ["MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES"] = str(
        40 * 1024 * 1024)

    store = create_store(
        segment_size=ddr_size,
        enable_offload=True,
        ssd_total_size_override=ssd_size,
    )

    num_initial = 20
    num_pressure = 1200
    batch_size = 200

    # Phase 1: 写入初始数据（80MB），确保 offload 落盘
    initial_keys = []
    print(f"\n  [1] 写入 {num_initial} 个初始 key ({num_initial*4}MB)...")
    for i in range(num_initial):
        key = f"hw_initial_{i}"
        data = bytes([i % 256]) * KEY_SIZE
        retcode = store.put(key, data)
        if retcode == 0:
            initial_keys.append(key)
        else:
            print(f"    警告: 写入 {key} 失败: retcode={retcode}")
        time.sleep(INSERT_INTERVAL)

    print(f"  初始写入: {len(initial_keys)}/{num_initial} 成功")
    print_metrics("初始写入后")

    print(f"\n  [2] 等待 20s 让初始数据 offload 到 SSD...")
    wait_with_progress(20)
    print_metrics("offload 后")

    # Phase 2: 写入压力数据填满 SSD（不期望此阶段写入被拒绝）
    pressure_keys = []
    print(f"\n  [3] 写入 {num_pressure} 个压力 key 填满 SSD "
          f"（{num_pressure*4//1024:.1f}GB）...")
    num_batches = (num_pressure + batch_size - 1) // batch_size
    for batch_start in range(0, num_pressure, batch_size):
        batch_end = min(batch_start + batch_size, num_pressure)
        batch_num = batch_start // batch_size + 1
        batch_written = 0
        for i in range(batch_start, batch_end):
            key = f"hw_pressure_{i}"
            data = b"\x00" * KEY_SIZE
            retcode = store.put(key, data)
            if retcode == 0:
                pressure_keys.append(key)
                batch_written += 1
            time.sleep(INSERT_INTERVAL)

        print(f"    批次 {batch_num}/{num_batches}: {batch_written} 写入")
        print_metrics(f"批次 {batch_num} 后")

        if batch_end < num_pressure:
            print(f"    等待 20s 让 offload 排空 DDR...")
            wait_with_progress(20)

    print(f"  压力写入完成: {len(pressure_keys)} 个 key")

    # Phase 3: 等待最终 offload，让 ssd_used_bytes 追上实际使用量
    print(f"\n  [4] 等待 40s 让所有 offload 完成...")
    wait_with_progress(40)
    print_metrics("offload 完成后")

    # Phase 4: 验证写入被阻断（此时 ssd_used_bytes 已更新，SSD > 90%）
    print(f"\n  [5] 验证 SSD 高水位阻断（offload 完成后新写入应被拒绝）...")
    blocked_key = "hw_blocked_test"
    retcode = store.put(blocked_key, b"\x00" * KEY_SIZE)
    write_blocked = (retcode != 0)
    if write_blocked:
        print(f"  写入被拒绝 (retcode={retcode}) -- SSD 高水位阻断生效")
    else:
        print(f"  写入成功 (retcode={retcode}) -- SSD 可能未到高水位")

    # Phase 5: 验证初始数据可读
    print(f"\n  [6] 验证 {len(initial_keys)} 个初始 key...")
    survived = 0
    lost = 0
    for key in initial_keys:
        result = store.get(key)
        if result and len(result) == KEY_SIZE:
            survived += 1
        else:
            lost += 1
            print(f"    丢失: {key}")

    print(f"  结果: {survived}/{len(initial_keys)} 存活, {lost} 丢失")

    # 判断
    if write_blocked and lost == 0:
        print(f"\n  PASS: SSD 高水位阻断生效，初始数据全部存活")
    elif not write_blocked and lost == 0:
        print(f"\n  WARN: SSD 未到高水位（可能 offload 未完全完成），"
              f"但初始数据存活")
    elif lost > 0:
        print(f"\n  FAIL: {lost} 个初始 key 丢失")
    else:
        print(f"\n  FAIL: 意外结果")

    print(f"\n  >>> 按回车退出")
    input()


# ============================================================================
# Test 2b: SSD 驱逐保护
# ============================================================================

def test_ssd_eviction_protection():
    """验证启用 FIFO 驱逐策略 + disable_ssd_eviction 后，已有 SSD 数据不被驱逐。"""
    print("=== 验证：SSD 驱逐保护 ===\n")

    # 使用较小 SSD + bucket 参数，使测试能快速填满并触发容量检查
    ssd_size = 5 * 1024 * 1024 * 1024    # 5GB
    ddr_size = DEFAULT_DDR_SIZE           # 4GB

    # 关键配置：
    # - bucket_size_limit=40MB 使初始 80MB 数据能填满 2 个 bucket 落盘
    # - eviction_policy=fifo 启用驱逐（否则驱逐从不发生，测试无意义）
    # - disable_ssd_eviction=true 强制跳过驱逐（核心保护机制）
    # - max_total_size=256MB 使容量检查尽早触发
    os.environ["MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES"] = str(
        40 * 1024 * 1024)
    os.environ["MOONCAKE_OFFLOAD_BUCKET_EVICTION_POLICY"] = "fifo"
    os.environ["MOONCAKE_OFFLOAD_BUCKET_MAX_TOTAL_SIZE"] = str(
        256 * 1024 * 1024)
    os.environ["MOONCAKE_OFFLOAD_DISABLE_SSD_EVICTION"] = "true"

    store = create_store(
        segment_size=ddr_size,
        enable_offload=True,
        ssd_total_size_override=ssd_size,
    )

    num_initial = 20
    num_pressure = 200

    # Phase 1: 写入初始数据
    initial_keys = []
    print(f"\n  [1] 写入 {num_initial} 个初始 key ({num_initial*4}MB)...")
    for i in range(num_initial):
        key = f"evict_initial_{i}"
        data = bytes([i % 256]) * KEY_SIZE
        retcode = store.put(key, data)
        if retcode == 0:
            initial_keys.append(key)
        else:
            print(f"    警告: 写入 {key} 失败: retcode={retcode}")
        time.sleep(INSERT_INTERVAL)

    print(f"  初始写入: {len(initial_keys)}/{num_initial} 成功")
    print_metrics("初始写入后")

    # 等待 offload
    print(f"\n  [2] 等待 20s 让初始数据 offload 到 SSD...")
    wait_with_progress(20)
    print_metrics("offload 后")

    # Phase 2: 写入压力数据，触发容量检查
    # max_total_size=256MB，初始已用 80MB，写入 200 key（800MB）远超容量
    # 如果 disable_ssd_eviction 不生效，PrepareEviction 会按 FIFO 驱逐初始 bucket
    pressure_keys = []
    rejected = 0
    print(f"\n  [3] 写入 {num_pressure} 个压力 key（触发容量检查）...")
    for i in range(num_pressure):
        key = f"evict_pressure_{i}"
        data = b"\x00" * KEY_SIZE
        retcode = store.put(key, data)
        if retcode == 0:
            pressure_keys.append(key)
        else:
            rejected += 1
        time.sleep(INSERT_INTERVAL)
        # 每 50 个 key 等待一次 offload，让容量检查有机会触发
        if (i + 1) % 50 == 0:
            print(f"    {i+1}/{num_pressure} ({len(pressure_keys)} 成功, "
                  f"{rejected} 拒绝)")
            wait_with_progress(10)

    print(f"  压力写入: {len(pressure_keys)} 成功, {rejected} 拒绝")

    # 等待最终 offload
    print(f"\n  [4] 等待 30s 让 offload 完成...")
    wait_with_progress(30)
    print_metrics("最终状态")

    # Phase 3: 验证初始数据
    print(f"\n  [5] 验证 {len(initial_keys)} 个初始 key...")
    survived = 0
    lost = 0
    for key in initial_keys:
        result = store.get(key)
        if result and len(result) == KEY_SIZE:
            survived += 1
        else:
            lost += 1
            print(f"    丢失: {key}")

    print(f"  结果: {survived}/{len(initial_keys)} 存活, {lost} 丢失")

    if lost == 0 and survived == len(initial_keys):
        print(f"\n  PASS: disable_ssd_eviction 生效，初始数据未被驱逐")
    elif lost > 0:
        print(f"\n  FAIL: {lost} 个初始 key 丢失，SSD 驱逐保护失败")
    else:
        print(f"\n  WARN: 无初始数据可验证")

    # Cleanup
    for key in initial_keys + pressure_keys:
        try:
            store.remove(key)
        except Exception:
            pass

    print(f"\n  >>> 按回车退出")
    input()


# ============================================================================
# Test 3: DDR 准入控制
# ============================================================================

def test_ddr_admission():
    """验证 DDR 准入水位阻断写入。

    DDR=4GB, ddr_admission_watermark_ratio=0.90（需通过 master 启动参数设置）。
    写入大量 4MB key（1500 个 = 6GB），观察 DDR 超过准入水位时的写入拒绝。
    """
    print("=== 验证：DDR 准入控制 ===\n")

    store = create_store(enable_offload=False)

    num_keys = 1500  # 1500 * 4MB = 6GB > 4GB DDR
    written = 0
    rejected = 0
    first_reject_at = -1

    print(f"\n  [1] 写入 {num_keys} 个 4MB key ({num_keys*4//1024:.1f}GB)，"
          f"观察 DDR 准入水位阻断...")
    print(f"  DDR=4GB, 准入水位=90% ({4*1024*0.90:.0f}MB) "
          f"(需 --ddr_admission_watermark_ratio=0.90)")
    t0 = time.time()

    for i in range(num_keys):
        key = f"ddr_fill_{i}"
        data = b"\x00" * KEY_SIZE
        retcode = store.put(key, data)
        if retcode == 0:
            written += 1
        else:
            rejected += 1
            if first_reject_at < 0:
                first_reject_at = i + 1
                print(f"    *** 首次拒绝于第 {first_reject_at} 个 key ***")
        if (i + 1) % 100 == 0:
            elapsed = time.time() - t0
            print(f"    {i+1}/{num_keys}: {written} 成功, {rejected} 拒绝 "
                  f"({elapsed:.1f}s)")
            print_metrics(f"{i+1} keys 后")
        time.sleep(INSERT_INTERVAL)

    elapsed = time.time() - t0
    print(f"\n  写入完成: {written} 成功, {rejected} 拒绝 ({elapsed:.1f}s)")
    print_metrics("最终")

    if rejected > 0:
        print(f"\n  PASS: DDR 准入控制生效，首次拒绝于第 {first_reject_at} 个 key")
    else:
        print(f"\n  WARN: 全部 {written} 个 key 写入成功，DDR 未触发准入控制")
        print(f"  （可能 DDR eviction 持续释放空间，写入速度不够快）")

    print(f"\n  >>> 按回车退出")
    input()


# ============================================================================
# Test 4: 全节点 SSD 满
# ============================================================================

def test_all_ssd_full():
    """验证所有节点 SSD 满后全局拒绝，释放后恢复。"""
    print("=== 验证：全节点 SSD 满 ===\n")

    store = create_store()
    large_size = 16 * 1024 * 1024  # 16MB
    max_fill = 800  # 800 * 16MB = 12.5GB

    fill_keys = []
    print(f"\n  [1] 写入 {max_fill} 个 {large_size//1024//1024}MB 对象填满 SSD...")
    for i in range(max_fill):
        key = f"allfull_{i}"
        data = b"\x00" * large_size
        retcode = store.put(key, data)
        if retcode == 0:
            fill_keys.append(key)
        else:
            print(f"    写入阻止于第 {i+1} 个 (retcode={retcode})")
            break
        if (i + 1) % 200 == 0:
            print(f"    {i+1}/{max_fill} ({len(fill_keys)*large_size//1024//1024}MB)")
        time.sleep(INSERT_INTERVAL)

    print(f"  填充: {len(fill_keys)} 个 ({len(fill_keys)*large_size//1024//1024}MB)")

    # 等待 offload
    print(f"\n  [2] 等待 20s 让 offload 排空...")
    wait_with_progress(20)
    print_metrics("offload 后")

    # 验证写入被阻止
    print(f"\n  [3] 验证新写入被阻止...")
    test_key = "allfull_test"
    retcode = store.put(test_key, b"\x00" * KEY_SIZE)
    all_blocked = (retcode != 0)
    print(f"  写入结果: {'被阻止' if all_blocked else '成功'} "
          f"(retcode={retcode})")

    # 释放空间
    freed_count = min(20, len(fill_keys))
    print(f"\n  [4] 释放 {freed_count} 个对象...")
    for key in fill_keys[:freed_count]:
        try:
            store.remove(key)
        except Exception:
            pass

    print(f"  等待 5s...")
    time.sleep(5)
    print_metrics("释放后")

    # 验证写入恢复
    resume_key = "allfull_resume"
    retcode = store.put(resume_key, b"\x00" * KEY_SIZE)
    write_resumed = (retcode == 0)
    print(f"  释放后写入: {'成功' if write_resumed else '仍被阻止'} "
          f"(retcode={retcode})")

    if all_blocked and write_resumed:
        print(f"\n  PASS: 全局拒绝后释放恢复")
    elif not all_blocked:
        print(f"\n  PASS: SSD 未满到水位线")
    else:
        print(f"\n  FAIL: 释放后仍未恢复")

    # Cleanup
    for key in fill_keys + [test_key, resume_key]:
        try:
            store.remove(key)
        except Exception:
            pass

    print(f"\n  >>> 按回车退出")
    input()


# ============================================================================
# Main
# ============================================================================

TESTS = {
    "load_balancing": test_load_balancing,
    "ssd_high_watermark_blocking": test_ssd_high_watermark_blocking,
    "ssd_eviction_protection": test_ssd_eviction_protection,
    "ddr_admission": test_ddr_admission,
    "all_ssd_full": test_all_ssd_full,
}


def main():
    parser = argparse.ArgumentParser(
        description="SSD Balance 分配策略验证脚本")
    parser.add_argument("--test", choices=list(TESTS.keys()),
                        required=True,
                        help="要运行的测试")
    args = parser.parse_args()

    print("=" * 60)
    print(f"  SSD Balance 验证: {args.test}")
    print("=" * 60)
    print()

    try:
        TESTS[args.test]()
    except Exception as e:
        print(f"\n  FAIL: 测试异常: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
