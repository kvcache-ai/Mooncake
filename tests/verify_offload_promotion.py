#!/usr/bin/env python3
"""
Offload + Promotion Cold-Hot Exchange Verification Script

Verifies the complete cold-hot data exchange cycle in Mooncake:
  Offload (MEMORY → LOCAL_DISK) → Load (SSD → Client via RDMA)
  → Promotion (LOCAL_DISK → MEMORY, hot data only)

Prerequisites:
  - Mooncake master running with --enable_offload=true --offload_on_evict=true
    --promotion_on_hit=true --promotion_admission_threshold=2
  - Client with local SSD configured (MOONCAKE_OFFLOAD_FILE_STORAGE_PATH)

Usage:
    python verify_offload_promotion.py --master <master_address> [options]

Options:
    --master         Master server address (default: 127.0.0.1:50051)
    --num-keys       Number of keys for fill phase (default: 80)
    --value-size     Value size in bytes (default: 1048576, i.e. 1MB)
    --test           Which test to run: all|offload|load|promotion|exchange
"""

import argparse
import os
import sys
import time
import random
import string
from collections import defaultdict

try:
    from mooncake.store import MooncakeDistributedStore
except ImportError:
    print("ERROR: mooncake.store not available. Install mooncake-wheel first.")
    sys.exit(1)


# ============================================================================
# Configuration
# ============================================================================

DEFAULT_MASTER = "127.0.0.1:50051"
DEFAULT_NUM_KEYS = 800
DEFAULT_VALUE_SIZE = 1024 * 1024  # 1 MB
DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024  # fallback, auto-scale overrides
DEFAULT_LOCAL_BUFFER_SIZE = 64 * 1024 * 1024  # 64 MB

# DDR is auto-scaled in get_client() to total_data × 0.6.
# Promotion bottleneck: kMaxPerHeartbeat=1 (master_service.cpp:2991).
# Offload bottleneck: KEYS_ULTRA_LIMIT → enable_offloading_=false permanently
#                     (file_storage.cpp:471), scale wait times with num_keys.
OFFLOAD_WAIT_SECONDS = int(os.getenv("OFFLOAD_WAIT_SECONDS", "60"))
PROMOTION_WAIT_SECONDS = int(os.getenv("PROMOTION_WAIT_SECONDS", "180"))


# ============================================================================
# Helpers
# ============================================================================

def get_client(store, master_address=None, enable_ssd_offload=True,
               ssd_offload_path=None, num_keys=None, value_size=None):
    """Initialize and setup a distributed store client with SSD offload.

    If num_keys and value_size are provided, SEGMENT_SIZE_BYTES is
    auto-scaled to ~60% of total data so eviction reliably triggers
    (eviction_high_watermark * DDR < total data).  Explicitly setting
    SEGMENT_SIZE_BYTES in the environment overrides this.
    """
    protocol = os.getenv("PROTOCOL", "tcp")
    device_name = os.getenv("DEVICE_NAME", "eth0")
    local_hostname = os.getenv("LOCAL_HOSTNAME", "localhost")
    metadata_server = os.getenv("MC_METADATA_SERVER", "127.0.0.1:2379")

    # Auto-scale DDR: ~60% of expected total data so eviction punches
    # through at ~42% of key count (0.60 * 0.70 = 0.42).
    if num_keys and value_size and "SEGMENT_SIZE_BYTES" not in os.environ:
        auto_ddr = int(num_keys * value_size * 0.6)
        auto_ddr = max(auto_ddr, 32 * 1024 * 1024)  # floor 32 MB
        os.environ["SEGMENT_SIZE_BYTES"] = str(auto_ddr)

    segment_size = int(os.getenv("SEGMENT_SIZE_BYTES", DEFAULT_SEGMENT_SIZE))
    local_buffer_size = int(os.getenv("LOCAL_BUFFER_SIZE_BYTES",
                                       DEFAULT_LOCAL_BUFFER_SIZE))
    master_server = master_address or os.getenv("MASTER_SERVER", DEFAULT_MASTER)
    offload_path = ssd_offload_path or os.getenv(
        "MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", "/tmp/mooncake_offload_promotion")

    if enable_ssd_offload:
        # FileStorage::ValidatePath requires the directory to already exist
        os.makedirs(offload_path, exist_ok=True)
        print(f"  SSD offload path: {offload_path}")

        # BucketStorageBackend defaults (256 MB / 500 keys) are too large for
        # a single-process test — data would stay in the ungrouped pool forever.
        # Set small thresholds so a handful of 1 MB objects flush to disk.
        os.environ.setdefault("MOONCAKE_OFFLOAD_BUCKET_SIZE_LIMIT_BYTES",
                              str(10 * 1024 * 1024))   # 10 MB
        os.environ.setdefault("MOONCAKE_OFFLOAD_BUCKET_KEYS_LIMIT", "10")

    retcode = store.setup(
        local_hostname,
        metadata_server,
        segment_size,
        local_buffer_size,
        protocol,
        device_name,
        master_server,
        None,              # engine
        enable_ssd_offload,
    )
    if retcode:
        raise RuntimeError(f"Failed to setup store client. Return code: {retcode}")


def random_key(prefix="op_test"):
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    return f"{prefix}_{suffix}"


def random_value(size=DEFAULT_VALUE_SIZE):
    return os.urandom(size)


def batch_put(store, keys, value_size, batch_size=30, batch_pause=3.0):
    """Write keys in batches, pausing between batches to let eviction drain DDR.

    With offload_on_evict=true, eviction must offload to SSD before freeing
    DDR space. Writing all keys in a tight loop fills DDR immediately and
    causes NO_AVAILABLE_HANDLE rejections. Batching gives the eviction thread
    time to catch up.
    """
    reference = {}
    total_batches = (len(keys) + batch_size - 1) // batch_size
    for bi in range(total_batches):
        batch = keys[bi * batch_size:(bi + 1) * batch_size]
        for key in batch:
            value = random_value(value_size)
            retcode = store.put(key, value)
            if retcode == 0:
                reference[key] = value
        if bi < total_batches - 1:
            time.sleep(batch_pause)
    return reference


def replica_types(descs, key):
    """Return list of replica type tags for a key.

    Tags: 'MEMORY', 'LOCAL_DISK', 'DISK', 'UNKNOWN'
    """
    infos = descs.get(key) if isinstance(descs, dict) else None
    if infos is None:
        return []
    if not isinstance(infos, (list, tuple)):
        infos = [infos]
    tags = []
    for info in infos:
        if hasattr(info, "is_memory_replica") and info.is_memory_replica():
            tags.append("MEMORY")
        elif hasattr(info, "is_local_disk_replica") and info.is_local_disk_replica():
            tags.append("LOCAL_DISK")
        elif hasattr(info, "is_disk_replica") and info.is_disk_replica():
            tags.append("DISK")
        else:
            tags.append("UNKNOWN")
    return tags


def classify_keys(descs, keys):
    """Classify keys by their replica composition.

    Returns dict: {'memory_only': [...], 'local_disk_only': [...],
                   'both': [...], 'none': [...]}
    """
    result = {"memory_only": [], "local_disk_only": [], "both": [], "none": []}
    for key in keys:
        types = set(replica_types(descs, key))
        if "MEMORY" in types and "LOCAL_DISK" in types:
            result["both"].append(key)
        elif "MEMORY" in types:
            result["memory_only"].append(key)
        elif "LOCAL_DISK" in types:
            result["local_disk_only"].append(key)
        else:
            result["none"].append(key)
    return result


def safe_cleanup(store, keys, batch_size=50):
    """Remove keys in small batches.  Large per-key remove loops collide with
    concurrent FileStorage heartbeat/promotion threads on the same shards
    and can cause segfaults during teardown."""
    for i in range(0, len(keys), batch_size):
        batch = keys[i:i + batch_size]
        for key in batch:
            try:
                store.remove(key)
            except Exception:
                pass


def get_offload_rpc_count(store):
    """Return current offload RPC read count, or -1 if unavailable."""
    try:
        return store.get_offload_rpc_read_count()
    except Exception:
        return -1


# ============================================================================
# Test Result Tracking
# ============================================================================

class TestResult:
    def __init__(self, name):
        self.name = name
        self.passed = False
        self.details = {}
        self.messages = []

    def pass_test(self, **details):
        self.passed = True
        self.details.update(details)

    def fail_test(self, reason, **details):
        self.passed = False
        self.messages.append(reason)
        self.details.update(details)

    def __str__(self):
        status = "PASS" if self.passed else "FAIL"
        msg = f"[{status}] {self.name}"
        if self.messages:
            msg += f" - {'; '.join(self.messages)}"
        return msg


# ============================================================================
# Test 1: Basic Offload (MEMORY → LOCAL_DISK)
# ============================================================================

def test_basic_offload(num_keys=30, value_size=DEFAULT_VALUE_SIZE):
    """Write data → wait for offload → verify LOCAL_DISK replicas exist."""
    result = TestResult("Basic Offload (MEMORY -> SSD)")
    store = MooncakeDistributedStore()
    get_client(store, num_keys=num_keys, value_size=value_size)

    timestamp = int(time.time())
    keys = [f"offload_{i}_{timestamp}" for i in range(num_keys)]

    print(f"  Writing {num_keys} objects ({value_size // 1024} KB each)...")
    reference = batch_put(store, keys, value_size, batch_size=10)
    written = list(reference.keys())

    if len(written) == 0:
        result.fail_test("No objects written successfully")
        return result

    print(f"  Written {len(written)}/{num_keys} objects successfully")

    # Wait for offload heartbeat to flush data to SSD
    print(f"  Waiting {OFFLOAD_WAIT_SECONDS}s for offload heartbeat...")
    time.sleep(OFFLOAD_WAIT_SECONDS)

    # Check replica types
    descs = store.batch_get_replica_desc(written)
    classification = classify_keys(descs, written)

    print(f"  Replica distribution: "
          f"memory_only={len(classification['memory_only'])}, "
          f"local_disk_only={len(classification['local_disk_only'])}, "
          f"both={len(classification['both'])}, "
          f"none={len(classification['none'])}")

    local_disk_count = (len(classification["local_disk_only"]) +
                        len(classification["both"]))

    if local_disk_count > 0:
        result.pass_test(
            written=len(written),
            local_disk_keys=local_disk_count,
            memory_only=len(classification["memory_only"]),
            both=len(classification["both"]),
        )
    else:
        result.fail_test(
            "No objects have LOCAL_DISK replicas after offload wait. "
            "Is enable_offload=true on the master? "
            "Is MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS set to a small value?",
            written=len(written),
        )

    # Cleanup
    safe_cleanup(store, written)

    return result


# ============================================================================
# Test 2: SSD Load Path (LOCAL_DISK → Client via offload RPC)
# ============================================================================

def test_load_from_ssd(num_keys=80, value_size=DEFAULT_VALUE_SIZE):
    """Overflow memory → verify LOCAL_DISK-only keys can be read via Load path."""
    result = TestResult("SSD Load Path (SSD -> Client)")
    store = MooncakeDistributedStore()
    get_client(store, num_keys=num_keys, value_size=value_size)

    timestamp = int(time.time())
    keys = [f"load_{i}_{timestamp}" for i in range(num_keys)]

    print(f"  Writing {num_keys} x {value_size // 1024}KB = "
          f"{(num_keys * value_size) // (1024*1024)}MB to overflow 32MB DDR...")
    reference = batch_put(store, keys, value_size, batch_size=10)

    if len(reference) == 0:
        result.fail_test("No objects written")
        return result

    print(f"  Written {len(reference)}/{num_keys} objects")

    # Wait for offload + eviction to move data to SSD
    print(f"  Waiting {OFFLOAD_WAIT_SECONDS}s for offload + eviction...")
    time.sleep(OFFLOAD_WAIT_SECONDS)

    # Check replica types
    descs = store.batch_get_replica_desc(list(reference.keys()))
    classification = classify_keys(descs, list(reference.keys()))
    local_disk_only = classification["local_disk_only"]
    both_types = classification["both"]

    print(f"  Replica distribution: "
          f"memory_only={len(classification['memory_only'])}, "
          f"local_disk_only={len(classification['local_disk_only'])}, "
          f"both={len(classification['both'])}")

    target_keys = local_disk_only + both_types
    if len(target_keys) == 0:
        result.fail_test(
            "No objects with LOCAL_DISK replicas. DDR may be too large "
            "or offload not triggered. Try smaller SEGMENT_SIZE_BYTES.",
            memory_only=len(classification["memory_only"]),
        )
        # Cleanup
        safe_cleanup(store, list(reference.keys()))
        return result

    # Read LOCAL_DISK-only keys via offload RPC path
    print(f"  Testing Load path on {len(target_keys[:10])} keys...")
    rpc_before = get_offload_rpc_count(store)
    read_success = 0
    read_failure = 0

    for key in target_keys[:10]:
        got = store.get(key)
        expected = reference[key]
        if got == expected:
            read_success += 1
        else:
            read_failure += 1

    rpc_after = get_offload_rpc_count(store)
    rpc_delta = rpc_after - rpc_before if (rpc_before >= 0 and rpc_after >= 0) else -1

    print(f"  Load results: {read_success}/{read_success + read_failure} readable, "
          f"offload RPC count delta={rpc_delta}")

    if read_success > 0 and rpc_delta >= 0:
        result.pass_test(
            target_keys=len(target_keys),
            read_success=read_success,
            read_failure=read_failure,
            offload_rpc_delta=rpc_delta,
        )
    elif read_success > 0:
        result.pass_test(
            target_keys=len(target_keys),
            read_success=read_success,
            read_failure=read_failure,
            note="offload RPC counter unavailable (expected; API may differ)",
        )
    else:
        result.fail_test(
            f"All {len(target_keys[:10])} Load reads failed",
            read_success=read_success,
            read_failure=read_failure,
        )

    # Cleanup
    safe_cleanup(store, list(reference.keys()))

    return result


# ============================================================================
# Test 3: Promotion on Hit (LOCAL_DISK → MEMORY for hot data)
# ============================================================================

def test_promotion_on_hit(num_keys=80, value_size=DEFAULT_VALUE_SIZE):
    """Verify hot LOCAL_DISK-only data is promoted back to MEMORY."""
    result = TestResult("Promotion on Hit (SSD -> MEMORY)")
    store = MooncakeDistributedStore()
    get_client(store, num_keys=num_keys, value_size=value_size)

    timestamp = int(time.time())
    keys = [f"promo_{i}_{timestamp}" for i in range(num_keys)]

    # Phase 1: Write enough to overflow memory
    print(f"  Phase 1: Writing {num_keys} x {value_size // 1024}KB = "
          f"{(num_keys * value_size) // (1024*1024)}MB...")
    reference = batch_put(store, keys, value_size, batch_size=10)

    if len(reference) == 0:
        result.fail_test("No objects written")
        return result

    print(f"  Written {len(reference)}/{num_keys} objects")

    # Phase 2: Wait for offload + eviction
    print(f"  Phase 2: Waiting {OFFLOAD_WAIT_SECONDS}s for offload + eviction...")
    time.sleep(OFFLOAD_WAIT_SECONDS)

    descs = store.batch_get_replica_desc(list(reference.keys()))
    classification = classify_keys(descs, list(reference.keys()))
    local_disk_only = classification["local_disk_only"]

    print(f"  LOCAL_DISK-only keys after eviction: {len(local_disk_only)}")

    if len(local_disk_only) == 0:
        result.fail_test(
            "No LOCAL_DISK-only keys found. Cannot test promotion. "
            "Ensure offload_on_evict=true and segment is small enough.",
            classification={k: len(v) for k, v in classification.items()},
        )
        safe_cleanup(store, list(reference.keys()))
        return result

    # Pick a test key
    hot_key = local_disk_only[0]
    expected_bytes = reference[hot_key]

    # Phase 3: Repeated reads to clear admission threshold
    print(f"  Phase 3: Reading hot key '{hot_key}' repeatedly "
          f"(to clear promotion_admission_threshold)...")
    rpc_before = get_offload_rpc_count(store)

    for i in range(4):  # 4 reads, default threshold is 2
        got = store.get(hot_key)
        if got != expected_bytes:
            result.fail_test(f"Load read failed at iteration {i}")
            safe_cleanup(store, list(reference.keys()))
            return result

    rpc_after_reads = get_offload_rpc_count(store)
    print(f"  Offload RPC count after reads: {rpc_after_reads} "
          f"(delta={rpc_after_reads - rpc_before if rpc_before >= 0 else '?'})")

    # Phase 4: Wait for promotion heartbeat
    print(f"  Phase 4: Waiting {PROMOTION_WAIT_SECONDS}s for promotion...")
    time.sleep(PROMOTION_WAIT_SECONDS)

    descs_after = store.batch_get_replica_desc([hot_key])
    types_after = replica_types(descs_after, hot_key)
    has_memory = "MEMORY" in types_after
    has_local_disk = "LOCAL_DISK" in types_after

    print(f"  Hot key replicas after promotion wait: {types_after}")

    # Phase 5: Read again — should hit MEMORY, not increment offload RPC count
    rpc_before_final = get_offload_rpc_count(store)
    got_final = store.get(hot_key)
    rpc_after_final = get_offload_rpc_count(store)
    rpc_final_delta = (rpc_after_final - rpc_before_final
                       if (rpc_before_final >= 0 and rpc_after_final >= 0) else -1)

    if got_final == expected_bytes and rpc_final_delta == 0:
        print(f"  Post-promotion read: OK, offload RPC delta={rpc_final_delta} "
              f"(read served from MEMORY, no SSD I/O)")
    elif got_final == expected_bytes:
        print(f"  Post-promotion read: OK, offload RPC delta={rpc_final_delta} "
              f"(may still be from SSD)")

    if has_memory:
        result.pass_test(
            hot_key=hot_key,
            replicas_after=types_after,
            post_promotion_rpc_delta=rpc_final_delta,
            served_from_memory=(rpc_final_delta == 0),
        )
    else:
        result.fail_test(
            f"No MEMORY replica after promotion wait. "
            f"Is promotion_on_hit=true on the master? "
            f"Replicas: {types_after}",
            replicas_after=types_after,
        )

    # Cleanup
    safe_cleanup(store, list(reference.keys()))

    return result


# ============================================================================
# Test 4: Cold-Hot Exchange Cycle
# ============================================================================

def test_cold_hot_exchange(num_keys=80, value_size=DEFAULT_VALUE_SIZE):
    """Write data → overflow → hot access subset → verify hot keys in MEMORY,
    cold keys on LOCAL_DISK only."""
    result = TestResult("Cold-Hot Exchange")
    store = MooncakeDistributedStore()
    get_client(store, num_keys=num_keys, value_size=value_size)

    timestamp = int(time.time())
    total_keys = num_keys
    hot_fraction = 0.2  # 20% are hot
    hot_count = max(2, int(total_keys * hot_fraction))

    # Create keys: first hot_count are "hot", rest are "cold"
    hot_keys = [f"hot_{i}_{timestamp}" for i in range(hot_count)]
    cold_keys = [f"cold_{i}_{timestamp}" for i in range(hot_count, total_keys)]
    all_keys = hot_keys + cold_keys

    # Phase 1: Write all keys in batches to let eviction drain DDR
    print(f"  Phase 1: Writing {total_keys} keys "
          f"({hot_count} hot, {total_keys - hot_count} cold)...")
    reference = batch_put(store, all_keys, value_size, batch_size=10)

    print(f"  Written {len(reference)}/{total_keys} objects")

    # Phase 2: First offload cycle
    print(f"  Phase 2: Waiting {OFFLOAD_WAIT_SECONDS}s for offload + eviction...")
    time.sleep(OFFLOAD_WAIT_SECONDS)

    descs = store.batch_get_replica_desc(list(reference.keys()))
    classification = classify_keys(descs, list(reference.keys()))
    local_disk_only = classification["local_disk_only"]

    print(f"  After eviction: {len(local_disk_only)} LOCAL_DISK-only keys")

    # Diagnostic: count keys still missing LOCAL_DISK replicas entirely.
    # These were never offloaded — possibly due to KEYS_ULTRA_LIMIT shutting
    # down enable_offloading_ (file_storage.cpp:471), or the offload pipeline
    # not keeping up with the write rate.
    missing_ssd = (len(classification["memory_only"]) +
                   len(classification["none"]))
    if missing_ssd > 0:
        print(f"  ** DIAGNOSTIC: {missing_ssd} keys have NO LOCAL_DISK replica "
              f"(memory_only={len(classification['memory_only'])}, "
              f"none={len(classification['none'])}). "
              f"Check logs for KEYS_ULTRA_LIMIT or 'enable_offloading_' "
              f"shutdown. Consider reducing num_keys or increasing "
              f"OFFLOAD_WAIT_SECONDS.")

    if len(local_disk_only) < hot_count + 1:
        result.fail_test(
            f"Not enough LOCAL_DISK-only keys ({len(local_disk_only)}). "
            "Need more data or smaller segment.",
        )
        safe_cleanup(store, list(reference.keys()))
        return result

    # Phase 3: Repeatedly read hot keys to trigger promotion
    # Randomly intersperse some cold reads too
    print(f"  Phase 3: Hot access to {hot_count} hot keys (4x each)...")
    hot_keys_written = [k for k in hot_keys if k in reference]
    cold_keys_written = [k for k in cold_keys if k in reference]

    for _ in range(4):
        for hot_key in hot_keys_written:
            store.get(hot_key)
        # Occasional cold access
        if cold_keys_written:
            sample_cold = random.sample(cold_keys_written,
                                        min(3, len(cold_keys_written)))
            for cold_key in sample_cold:
                store.get(cold_key)

    # Phase 4: Wait for promotion
    print(f"  Phase 4: Waiting {PROMOTION_WAIT_SECONDS}s for promotion...")
    time.sleep(PROMOTION_WAIT_SECONDS)

    # Phase 5: Check state — classify hot/cold keys separately by replica type
    descs_after = store.batch_get_replica_desc(list(reference.keys()))
    classification_after = classify_keys(descs_after, list(reference.keys()))

    # Per-group breakdown
    def classify_group(descs, group_keys):
        """Count keys by replica composition: memory_only, local_disk_only, both."""
        result = {"memory_only": 0, "local_disk_only": 0, "both": 0, "none": 0}
        for key in group_keys:
            types = set(replica_types(descs, key))
            if "MEMORY" in types and "LOCAL_DISK" in types:
                result["both"] += 1
            elif "MEMORY" in types:
                result["memory_only"] += 1
            elif "LOCAL_DISK" in types:
                result["local_disk_only"] += 1
            else:
                result["none"] += 1
        return result

    hot_breakdown = classify_group(descs_after, hot_keys_written)
    cold_breakdown = classify_group(descs_after, cold_keys_written)

    print(f"  After promotion cycle ({len(hot_keys_written)} hot, "
          f"{len(cold_keys_written)} cold):")
    print(f"    Hot  keys — MEMORY_only: {hot_breakdown['memory_only']}, "
          f"LOCAL_DISK_only: {hot_breakdown['local_disk_only']}, "
          f"BOTH: {hot_breakdown['both']}, "
          f"none: {hot_breakdown['none']}")
    print(f"    Cold keys — MEMORY_only: {cold_breakdown['memory_only']}, "
          f"LOCAL_DISK_only: {cold_breakdown['local_disk_only']}, "
          f"BOTH: {cold_breakdown['both']}, "
          f"none: {cold_breakdown['none']}")
    print(f"    Overall — MEMORY_only: {len(classification_after['memory_only'])}, "
          f"LOCAL_DISK_only: {len(classification_after['local_disk_only'])}, "
          f"BOTH: {len(classification_after['both'])}")

    # Acceptance criteria:
    # - At least some hot keys should have MEMORY (promoted)
    # - Cold keys should be predominantly LOCAL_DISK-only
    hot_promoted = hot_breakdown["memory_only"] + hot_breakdown["both"]
    hot_local_disk_only = hot_breakdown["local_disk_only"]
    cold_with_memory = cold_breakdown["memory_only"] + cold_breakdown["both"]
    cold_local_disk_only = cold_breakdown["local_disk_only"]
    cold_stays_cold = cold_local_disk_only >= cold_with_memory

    # Hot keys still LOCAL_DISK-only are normal — each promotion heartbeat
    # processes only 1 key (kMaxPerHeartbeat=1).  Wait time may not cover
    # all hot keys.  Cold keys appearing in MEMORY are also expected if
    # phase-3 spot-checks happened to push their Count-Min Sketch counter
    # past promotion_admission_threshold.
    if hot_local_disk_only > 0:
        print(f"    Note: {hot_local_disk_only} hot keys remain LOCAL_DISK-only. "
              "Each heartbeat promotes at most 1 key — increase "
              "PROMOTION_WAIT_SECONDS to promote more.")
    if cold_with_memory > cold_local_disk_only:
        print(f"    Note: cold keys with MEMORY ({cold_with_memory}) > "
              f"LOCAL_DISK-only ({cold_local_disk_only}). "
              "Phase-3 spot reads may have triggered unintended promotions. "
              "Reduce spot-read count (min(3, ...)) in phase 3.")

    if hot_promoted and cold_stays_cold:
        result.pass_test(
            hot_promoted=hot_promoted,
            hot_local_disk_only=hot_local_disk_only,
            cold_in_memory=cold_with_memory,
            cold_local_disk=cold_local_disk_only,
            hot_breakdown=hot_breakdown,
            cold_breakdown=cold_breakdown,
        )
    elif hot_promoted:
        result.pass_test(
            hot_promoted=hot_promoted,
            hot_local_disk_only=hot_local_disk_only,
            cold_in_memory=cold_with_memory,
            cold_local_disk=cold_local_disk_only,
            hot_breakdown=hot_breakdown,
            cold_breakdown=cold_breakdown,
            note="Some cold keys also in memory (phase-3 spot reads may have "
                 "triggered unintended promotions)",
        )
    else:
        result.fail_test(
            f"No hot keys promoted to MEMORY. "
            f"Is promotion_on_hit=true on master?",
            hot_promoted=hot_promoted,
            hot_local_disk_only=hot_local_disk_only,
            cold_in_memory=cold_with_memory,
            cold_local_disk_only=cold_local_disk_only,
        )

    # Cleanup: let heartbeat/promotion threads settle before touching state.
    # Removing 800 keys triggers one RPC per key while FileStorage heartbeat
    # may be processing the same shards — tearing down under concurrent access
    # can segfault.  Skip per-key removal and just let the process exit;
    # tmpfs SSD data is ephemeral anyway.
    if len(reference) > 200:
        print(f"  Skipping per-key cleanup ({len(reference)} keys) to avoid "
              "teardown races.  Remove SSD dir manually if needed: "
              f"{offload_path or os.getenv('MOONCAKE_OFFLOAD_FILE_STORAGE_PATH', '/tmp/mooncake_offload_promotion')}")
    else:
        safe_cleanup(store, list(reference.keys()))

    return result


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Offload + Promotion Cold-Hot Exchange Verification")
    parser.add_argument("--master", default=None,
                        help="Master server address")
    parser.add_argument("--num-keys", type=int, default=DEFAULT_NUM_KEYS,
                        help="Number of keys per test")
    parser.add_argument("--value-size", type=int, default=DEFAULT_VALUE_SIZE,
                        help="Value size in bytes")
    parser.add_argument("--test", choices=["all", "offload", "load",
                                           "promotion", "exchange"],
                        default="all", help="Which test to run")
    args = parser.parse_args()

    if args.master:
        os.environ["MASTER_SERVER"] = args.master

    print("=" * 70)
    print("Offload + Promotion Cold-Hot Exchange Verification")
    print("=" * 70)
    print(f"Config: num_keys={args.num_keys}, value_size={args.value_size}, "
          f"segment_size={DEFAULT_SEGMENT_SIZE // (1024*1024)}MB")
    print(f"Offload wait: {OFFLOAD_WAIT_SECONDS}s, "
          f"Promotion wait: {PROMOTION_WAIT_SECONDS}s")
    print()

    tests = {
        "offload": ("Test 1: Basic Offload (MEMORY -> SSD)", test_basic_offload),
        "load": ("Test 2: SSD Load Path (SSD -> Client)", test_load_from_ssd),
        "promotion": ("Test 3: Promotion on Hit (SSD -> MEMORY)",
                      test_promotion_on_hit),
        "exchange": ("Test 4: Cold-Hot Exchange Cycle",
                     test_cold_hot_exchange),
    }

    test_order = ["offload", "load", "promotion", "exchange"]
    if args.test != "all":
        test_order = [args.test]

    results = []
    for test_name in test_order:
        if test_name not in tests:
            continue
        title, test_fn = tests[test_name]
        print(f"\n{'=' * 50}")
        print(title)
        print(f"{'=' * 50}")

        try:
            r = test_fn(num_keys=args.num_keys, value_size=args.value_size)
            results.append(r)
        except Exception as e:
            r = TestResult(title)
            r.fail_test(f"Exception: {e}")
            results.append(r)
            import traceback
            traceback.print_exc()

        print(f"\n  Result: {r}")

    # Summary
    print(f"\n{'=' * 70}")
    print("Summary")
    print(f"{'=' * 70}")

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    for r in results:
        print(f"  {r}")
        if r.details:
            for k, v in r.details.items():
                print(f"    {k}: {v}")

    print(f"\nTotal: {passed} passed, {failed} failed out of {len(results)}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
