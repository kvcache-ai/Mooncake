#!/usr/bin/env python3
"""
Engram-27B Performance Benchmark

Scenario:
  - 1 token per request (L=1)
  - Batch sizes: 1, 4, 16, 64, 128, 256
  - Benchmarks the Mooncake-side hash + embedding query path

Usage:
  # Start master first (or use run_bench_engram.sh):
  # mooncake_master --default_kv_lease_ttl=500 --enable_http_metadata_server=true

  export MOONCAKE_MASTER=127.0.0.1:50051
  export MOONCAKE_TE_META_DATA_SERVER=http://127.0.0.1:8080/metadata
  python scripts/bench_engram_27b.py
"""

import os
import sys
import time
import ctypes
import numpy as np

# Setup paths
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

# Engram-27B-ish config used to benchmark data lookup only.
HC_MULT = 8
HIDDEN_SIZE = 80
BATCH_SIZES = [1, 4, 16, 64, 128, 256]
NUM_WARMUP = 10
NUM_ITER = 50


def create_engram_config():
    """Engram config approximating 27B model."""
    cfg = store.EngramConfig()
    cfg.tokenizer_name_or_path = ""
    cfg.engram_vocab_size = [50000, 50000]  # vocab per n-gram
    cfg.max_ngram_size = 3
    cfg.n_embed_per_ngram = 64  # embed_D = 64/4 = 16 per head, or n_head*embed_D
    cfg.n_head_per_ngram = 4
    cfg.layer_ids = [1, 15]
    cfg.pad_id = 2
    cfg.seed = 42
    cfg.kernel_size = 4

    bb = store.BackboneConfig()
    bb.hidden_size = HIDDEN_SIZE
    bb.hc_mult = HC_MULT
    bb.vocab_size = 128000
    bb.num_layers = 6

    return cfg, bb


def populate_store_via_cxl_segment(store_obj, embedding_buffers):
    """Populate keys by copying data into the mapped CXL file, then put_from."""
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

    # Keep the staging region away from the allocator's low-address growth to
    # avoid overlapping source and destination inside the same CXL mapping.
    cursor = limit_addr - total_aligned_bytes
    if cursor < base_addr:
        raise RuntimeError("CXL mapping is too small for staging region")

    for head_idx, emb in enumerate(embedding_buffers):
        raw = emb.tobytes(order="C")
        nbytes = len(raw)
        aligned = aligned_sizes[head_idx]
        if cursor + aligned > limit_addr:
            raise RuntimeError("CXL mapping is too small for benchmark tables")
        ctypes.memmove(cursor, raw, nbytes)
        key = f"engram:l1:h{head_idx}"
        rc = store_obj.put_from(key, cursor, nbytes)
        if rc != 0:
            raise RuntimeError(
                f"CXL put_from fallback failed for {key}, rc={rc}"
            )
        cursor += aligned


def populate_store(engram, cfg, store_obj):
    """Populate store with random embedding tables. Returns (buffers, populate_ms, mode)."""
    embed_D = cfg.n_embed_per_ngram // cfg.n_head_per_ngram

    embedding_buffers = []
    for vocab_size in engram.get_table_vocab_sizes():
        emb = np.random.randn(vocab_size, embed_D).astype(np.float32)
        embedding_buffers.append(emb)

    t0 = time.perf_counter()
    mode = "engram.populate_store_from_buffers"
    try:
        engram.populate_store_from_buffers(embedding_buffers)
    except RuntimeError:
        protocol = os.environ.get("MOONCAKE_PROTOCOL", "")
        if protocol == "cxl":
            # File-backed CXL does not currently populate via the numpy
            # register_buffer path, so stage directly through the mapped CXL
            # region and publish with put_from.
            mode = "cxl put_from fallback"
            populate_store_via_cxl_segment(store_obj, embedding_buffers)
        else:
            mode = "store.put fallback"
            for head_idx, emb in enumerate(embedding_buffers):
                key = f"engram:l1:h{head_idx}"
                rc = store_obj.put(key, emb.tobytes())
                if rc != 0:
                    raise RuntimeError(
                        f"fallback populate failed for {key}, rc={rc}"
                    )
    populate_ms = (time.perf_counter() - t0) * 1000
    return embedding_buffers, populate_ms, mode


def run_benchmark(engram, bb, batch_size, num_warmup, num_iter):
    """Run query benchmark for given batch size. Returns timing breakdown."""
    L = 1  # 1 token
    input_ids = [[i % bb.vocab_size for _ in range(L)] for i in range(batch_size)]

    # Warmup
    for _ in range(num_warmup):
        engram.query(input_ids)

    # Timed runs with query_with_timing
    total_ms_list = []
    store_read_ms_list = []
    hash_ms_list = []
    emb_detail_lists = {}
    for _ in range(num_iter):
        t0 = time.perf_counter()
        output, timing = engram.query_with_timing(input_ids)
        t1 = time.perf_counter()
        total_ms_list.append((t1 - t0) * 1000)
        store_read_ms_list.append(timing["store_read_ms"])
        hash_ms_list.append(timing["hash_ms"])
        emb = timing.get("embedding_lookup", {})
        for k, v in emb.items():
            emb_detail_lists.setdefault(k, []).append(v)

    total_ms = np.array(total_ms_list)
    store_read_ms = np.array(store_read_ms_list)
    hash_ms = np.array(hash_ms_list)
    emb_detail_means = {k: np.mean(v) for k, v in emb_detail_lists.items()}

    mean_total = np.mean(total_ms)
    mean_read = np.mean(store_read_ms)
    mean_hash = np.mean(hash_ms)
    p50_total = np.percentile(total_ms, 50)
    p99_total = np.percentile(total_ms, 99)

    tokens_per_sec = batch_size / (mean_total / 1000)
    bytes_per_request = (
        batch_size
        * L
        * engram.get_num_heads()
        * engram.get_embedding_dim()
        * 4
    )
    gbps = (bytes_per_request / 1e9) / (mean_total / 1000)

    return {
        "batch_size": batch_size,
        "mean_total_ms": mean_total,
        "mean_store_read_ms": mean_read,
        "mean_hash_ms": mean_hash,
        "emb_detail": emb_detail_means,
        "p50_ms": p50_total,
        "p99_ms": p99_total,
        "tokens_per_sec": tokens_per_sec,
        "gbps": gbps,
    }


def validate_query_correctness(engram, embedding_buffers):
    """Validate one query against the populated embedding tables."""
    input_ids = [[0]]
    hash_ids = np.asarray(engram.hash_input_ids(input_ids))
    output = np.asarray(engram.query(input_ids))

    max_abs_err = 0.0
    for head_idx in range(output.shape[2]):
        row_idx = int(hash_ids[0, 0, head_idx])
        expected = embedding_buffers[head_idx][row_idx]
        actual = output[0, 0, head_idx]
        max_abs_err = max(max_abs_err, float(np.max(np.abs(actual - expected))))
    return max_abs_err


def main():
    print("=" * 60)
    print("Engram-27B Performance Benchmark")
    print("  Config: 1 token, Mooncake-side hash + embedding query only")
    print("  Batch sizes: 1, 4, 16, 64, 128, 256")
    print(f"  Build dir: {build_dir}")
    print(f"  Protocol: {os.environ.get('MOONCAKE_PROTOCOL', '<unset>')}")
    print("=" * 60)

    # Connect store
    store_obj = store.MooncakeDistributedStore()
    config = MooncakeConfig.load_from_env()
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
        print(f"❌ Store setup failed: {rc}")
        sys.exit(1)
    print("✅ Store connected\n")

    cfg, bb = create_engram_config()
    engram = store.Engram(layer_id=1, config=cfg, backbone_cfg=bb, store=store_obj)

    store_obj.remove_all()
    embedding_buffers, populate_ms, populate_mode = populate_store(
        engram, cfg, store_obj
    )
    print(
        f"✅ Embedding tables populated (store write: {populate_ms:.1f} ms, "
        f"mode={populate_mode})\n"
    )

    max_abs_err = validate_query_correctness(engram, embedding_buffers)
    print(f"✅ Correctness check max_abs_err={max_abs_err:.6f}\n")

    print("Latency breakdown (ms):")
    print("  total = hash + store_read (embedding lookup)")
    print("-" * 60)

    results = []
    for batch_size in BATCH_SIZES:
        r = run_benchmark(engram, bb, batch_size, NUM_WARMUP, NUM_ITER)
        results.append(r)
        print(
            f"  Batch {batch_size:3d}: total={r['mean_total_ms']:.2f} "
            f"(hash={r['mean_hash_ms']:.2f} read={r['mean_store_read_ms']:.2f}) "
            f"p50={r['p50_ms']:.2f} p99={r['p99_ms']:.2f} "
            f"| {r['tokens_per_sec']:.0f} tok/s"
        )

    print("\n" + "=" * 60)
    print("Summary: hash vs store read (mean ms)")
    print("-" * 60)
    for r in results:
        read_pct = 100 * r["mean_store_read_ms"] / r["mean_total_ms"]
        print(
            f"  B={r['batch_size']:3d}: total={r['mean_total_ms']:.2f} ms  "
            f"hash={r['mean_hash_ms']:.2f}  "
            f"store_read={r['mean_store_read_ms']:.2f} ({read_pct:.0f}%)  "
            f"| {r['tokens_per_sec']:.0f} tok/s"
        )

    print("\n" + "=" * 60)
    print("Embedding lookup breakdown (store_read) - identify bottlenecks")
    print("  range path: setup, register, batch_query, get_into_range, scatter, unregister, prep")
    print("  batch path: setup, batch_query, batch_get_buffer (when range reads are too fragmented)")
    print("-" * 60)
    for r in results:
        d = r.get("emb_detail", {})
        setup = d.get("setup_ms", 0)
        reg = d.get("register_ms", 0)
        qry = d.get("batch_query_ms", 0)
        get = d.get("get_into_range_ms", 0)
        scatter = d.get("scatter_ms", 0)
        unreg = d.get("unregister_ms", 0)
        prep = d.get("prep_ms", 0)
        batch_get = d.get("batch_get_buffer_ms", 0)
        lookup = d.get("lookup_ms", 0)
        gap = d.get("gap_ms", 0)
        total_emb = setup + reg + qry + get + scatter + unreg + prep + batch_get + lookup
        internal = d.get("_total_internal_ms", 0)
        store_read = r["mean_store_read_ms"]
        print(
            f"  B={r['batch_size']:3d}: setup={setup:.2f} register={reg:.2f} batch_query={qry:.2f} "
            f"get_into_range={get:.2f} scatter={scatter:.2f} "
            f"unregister={unreg:.2f} prep={prep:.2f} "
            f"batch_get_buffer={batch_get:.2f} lookup={lookup:.2f} gap={gap:.2f}"
        )
        print(
            f"       phases_sum={total_emb:.2f} internal={internal:.2f} store_read={store_read:.2f} "
            f"(expect internal≈store_read, phases_sum+gap=internal)"
        )

    print("\n  Store write (populate, one-time): {:.1f} ms".format(populate_ms))
    print("=" * 60)

    del engram
    store_obj.close()


if __name__ == "__main__":
    main()
