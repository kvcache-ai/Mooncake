#!/usr/bin/env python3
"""
EngramStore backend benchmark.

Scenario:
  - 1 token per request (L=1)
  - Batch sizes: 1, 4, 16, 64, 128, 256
  - Benchmarks Mooncake's row-id lookup backend only

Usage:
  # Start master first:
  # mooncake_master --default_kv_lease_ttl=500 --enable_http_metadata_server=true

  export MOONCAKE_MASTER=127.0.0.1:50051
  export MOONCAKE_TE_META_DATA_SERVER=http://127.0.0.1:8080/metadata
  python scripts/bench_engram_store_27b.py

Optional:
  export ENGRAM_STORE_ALLOW_POPULATE_FALLBACK=1
"""

import ctypes
import os
import sys
import time
import uuid

import numpy as np

repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
build_dir = os.environ.get("MOONCAKE_BUILD_DIR", "build")
build_store = os.path.join(repo_root, build_dir, "mooncake-integration")
wheel_dir = os.path.join(repo_root, "mooncake-wheel")
for d in [build_store, wheel_dir]:
    if os.path.isdir(d) and d not in sys.path:
        sys.path.insert(0, d)

try:
    import store
    from mooncake.mooncake_config import MooncakeConfig
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

BATCH_SIZES = [1, 4, 16, 64, 128, 256]
NUM_WARMUP = 10
NUM_ITER = 50
TABLE_VOCAB_SIZES = [50000] * 8
EMBEDDING_DIM = 16
ALLOW_POPULATE_FALLBACK = (
    os.environ.get("ENGRAM_STORE_ALLOW_POPULATE_FALLBACK", "0") == "1"
)


def create_engram_store_config():
    cfg = store.EngramStoreConfig()
    cfg.table_vocab_sizes = TABLE_VOCAB_SIZES
    cfg.embedding_dim = EMBEDDING_DIM
    return cfg


def rollback_keys(store_obj, keys):
    for key in keys:
        rc = store_obj.remove(key, True)
        if rc not in (0, -704):
            print(f"Warning: failed to remove benchmark key {key}, rc={rc}")


def populate_store_via_cxl_segment(store_obj, keys, embedding_buffers):
    cxl_path = os.environ.get("MC_CXL_DEV_PATH")
    if not cxl_path:
        raise RuntimeError("MC_CXL_DEV_PATH is required for CXL fallback")

    real_cxl_path = os.path.realpath(cxl_path)
    base_addr = None
    limit_addr = None
    with open("/proc/self/maps", "r", encoding="utf-8") as fin:
        for line in fin:
            if real_cxl_path not in line:
                continue
            addr_range = line.split()[0]
            start_hex, end_hex = addr_range.split("-")
            base_addr = int(start_hex, 16)
            limit_addr = int(end_hex, 16)
            break

    if base_addr is None or limit_addr is None:
        raise RuntimeError(f"failed to find CXL mapping for {real_cxl_path}")

    page_size = 4096
    aligned_sizes = []
    total_aligned_bytes = 0
    for emb in embedding_buffers:
        nbytes = emb.nbytes
        aligned = (nbytes + page_size - 1) // page_size * page_size
        aligned_sizes.append(aligned)
        total_aligned_bytes += aligned

    cursor = limit_addr - total_aligned_bytes
    if cursor < base_addr:
        raise RuntimeError("CXL mapping is too small for staging region")

    published_keys = []
    try:
        for head_idx, emb in enumerate(embedding_buffers):
            nbytes = emb.nbytes
            aligned = aligned_sizes[head_idx]
            if cursor + aligned > limit_addr:
                raise RuntimeError("CXL mapping is too small for benchmark tables")
            ctypes.memmove(cursor, emb.ctypes.data, nbytes)
            key = keys[head_idx]
            rc = store_obj.put_from(key, cursor, nbytes)
            if rc != 0:
                raise RuntimeError(
                    f"CXL put_from fallback failed for {key}, rc={rc}"
                )
            published_keys.append(key)
            cursor += aligned
    except Exception:
        rollback_keys(store_obj, published_keys)
        raise


def populate_store(engram_store, store_obj):
    keys = engram_store.get_store_keys()
    embedding_buffers = []
    for vocab_size in engram_store.get_table_vocab_sizes():
        emb = np.random.randn(vocab_size, engram_store.get_embedding_dim()).astype(np.float32)
        embedding_buffers.append(emb)

    t0 = time.perf_counter()
    mode = "engram_store.populate"
    try:
        engram_store.populate(embedding_buffers)
    except RuntimeError:
        if not ALLOW_POPULATE_FALLBACK:
            raise
        protocol = os.environ.get("MOONCAKE_PROTOCOL", "")
        if protocol == "cxl":
            mode = "cxl put_from fallback"
            populate_store_via_cxl_segment(store_obj, keys, embedding_buffers)
        else:
            mode = "store.put fallback"
            published_keys = []
            try:
                for head_idx, emb in enumerate(embedding_buffers):
                    key = keys[head_idx]
                    rc = store_obj.put(key, emb)
                    if rc != 0:
                        raise RuntimeError(
                            f"fallback populate failed for {key}, rc={rc}"
                        )
                    published_keys.append(key)
            except Exception:
                rollback_keys(store_obj, published_keys)
                raise
    populate_ms = (time.perf_counter() - t0) * 1000
    return embedding_buffers, populate_ms, mode


def make_row_ids(engram_store, batch_size, seq_len):
    row_ids = []
    for b in range(batch_size):
        batch = []
        for l in range(seq_len):
            token_rows = []
            for head, vocab_size in enumerate(engram_store.get_table_vocab_sizes()):
                token_rows.append((b * seq_len + l + head) % vocab_size)
            batch.append(token_rows)
        row_ids.append(batch)
    return row_ids


def run_benchmark(engram_store, batch_size, num_warmup, num_iter):
    seq_len = 1
    row_ids = make_row_ids(engram_store, batch_size, seq_len)

    for _ in range(num_warmup):
        engram_store.lookup(row_ids)

    total_ms_list = []
    for _ in range(num_iter):
        t0 = time.perf_counter()
        engram_store.lookup(row_ids)
        t1 = time.perf_counter()
        total_ms_list.append((t1 - t0) * 1000)

    total_ms = np.array(total_ms_list)
    mean_total = np.mean(total_ms)
    p50_total = np.percentile(total_ms, 50)
    p99_total = np.percentile(total_ms, 99)

    tokens_per_sec = batch_size / (mean_total / 1000)
    bytes_per_request = (
        batch_size
        * seq_len
        * engram_store.get_num_heads()
        * engram_store.get_embedding_dim()
        * 4
    )
    gbps = (bytes_per_request / 1e9) / (mean_total / 1000)

    return {
        "batch_size": batch_size,
        "mean_total_ms": mean_total,
        "p50_ms": p50_total,
        "p99_ms": p99_total,
        "tokens_per_sec": tokens_per_sec,
        "gbps": gbps,
    }


def validate_lookup_correctness(engram_store, embedding_buffers):
    row_ids = make_row_ids(engram_store, batch_size=1, seq_len=1)
    output = np.asarray(engram_store.lookup(row_ids))

    max_abs_err = 0.0
    for head_idx in range(output.shape[2]):
        row_idx = row_ids[0][0][head_idx]
        expected = embedding_buffers[head_idx][row_idx]
        actual = output[0, 0, head_idx]
        max_abs_err = max(max_abs_err, float(np.max(np.abs(actual - expected))))
    return max_abs_err


def main():
    print("=" * 60)
    print("EngramStore Backend Benchmark")
    print("  Config: 1 token, Mooncake row-id lookup only")
    print("  Batch sizes: 1, 4, 16, 64, 128, 256")
    print(f"  Build dir: {build_dir}")
    print(f"  Protocol: {os.environ.get('MOONCAKE_PROTOCOL', '<unset>')}")
    print(f"  Allow populate fallback: {ALLOW_POPULATE_FALLBACK}")
    print("=" * 60)

    config = MooncakeConfig.load_from_env()
    store_obj = store.MooncakeDistributedStore()
    rc = store_obj.setup(
        config.local_hostname,
        config.metadata_server,
        config.global_segment_size,
        config.local_buffer_size,
        config.protocol,
        config.device_name,
        config.master_server_address,
    )
    if rc != 0:
        raise RuntimeError(f"Failed to setup Mooncake store, rc={rc}")

    engram_store = None
    try:
        cfg = create_engram_store_config()
        layer_id = uuid.uuid4().int & 0x7FFFFFFF
        engram_store = store.EngramStore(layer_id=layer_id, config=cfg, store=store_obj)
        embedding_buffers, populate_ms, mode = populate_store(engram_store, store_obj)
        print(f"Populate mode: {mode}, took {populate_ms:.2f} ms")

        max_abs_err = validate_lookup_correctness(engram_store, embedding_buffers)
        print(f"Max abs error: {max_abs_err:.6f}")

        print("\nResults:")
        print(
            f"{'Batch':>8} {'Mean(ms)':>10} {'P50(ms)':>10} {'P99(ms)':>10} {'Tok/s':>12} {'GB/s':>10}"
        )
        for batch_size in BATCH_SIZES:
            result = run_benchmark(engram_store, batch_size, NUM_WARMUP, NUM_ITER)
            print(
                f"{result['batch_size']:>8} {result['mean_total_ms']:>10.3f} {result['p50_ms']:>10.3f} "
                f"{result['p99_ms']:>10.3f} {result['tokens_per_sec']:>12.1f} {result['gbps']:>10.3f}"
            )
    finally:
        if engram_store is not None:
            try:
                engram_store.remove_from_store(force=True)
            except Exception as exc:
                print(f"Warning: failed to clean up benchmark layer: {exc}")
        store_obj.close()


if __name__ == "__main__":
    main()
