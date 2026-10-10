#!/usr/bin/env python3
"""Compare legacy session/range reads, serial ReadPlan and pipelined ReadPlan.

Uses an existing Master/Store. Each run owns a UUID key namespace and registered
CPU buffers. No model execution. See read_plan_bench.md for setup and timing.
"""

import argparse
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import statistics
import time
from concurrent.futures import Future, ThreadPoolExecutor
from uuid import uuid4


def check(actual, expected, operation):
    if actual != expected:
        raise RuntimeError(f"{operation}: expected {expected!r}, got {actual!r}")


def percentile(values, q):
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    i = int(position)
    return ordered[i] + (ordered[min(i + 1, len(ordered) - 1)] - ordered[i]) * (
        position - i
    )


def make_workload(prefix, layers, num_keys, layer_bytes):
    """One packed object per key, one equally sized range per layer."""
    object_size = layers * layer_bytes
    dst = (ctypes.c_ubyte * (num_keys * object_size))()
    keys = [f"{prefix}{i:08d}" for i in range(num_keys)]
    components = [
        [
            (
                ctypes.addressof(dst) + layer * layer_bytes,
                object_size,
                layer_bytes,
                layer * layer_bytes,
            )
        ]
        for layer in range(layers)
    ]
    layouts = [(keys, list(range(num_keys)), True, components)]
    pools = [dict(keys=keys, dst=dst, object_size=object_size)]
    return layers, layouts, pools


def payload(key, size):
    # Offset-sensitive content detects reading the wrong group of an object.
    return hashlib.shake_256(key.encode()).digest(size)


def expand(layout, group):
    keys, rows, packed, components = layout
    if not packed:
        raise ValueError("Benchmark supports packed layouts only")
    if not components[group]:
        return None
    ptrs = [
        [base + row * stride for base, stride, size, offset in components[group]]
        for row in rows
    ]
    sizes = [[size for base, stride, size, offset in components[group]] for row in rows]
    offsets = [
        [offset for base, stride, size, offset in components[group]] for row in rows
    ]
    return keys, ptrs, sizes, offsets


def legacy(store, layouts, groups, signals, stages):
    started = []
    try:
        begin = time.perf_counter_ns()
        for keys, *_ in layouts:
            started.extend(keys)  # Also clean up partial starts on failure.
            check(
                list(store.batch_get_session_start(keys)),
                [0] * len(keys),
                "session start",
            )
        stages["legacy_session_ms"] = (time.perf_counter_ns() - begin) / 1e6
        for group in range(groups):
            for layout in layouts:
                ranges = expand(layout, group)
                if ranges is None:
                    continue
                keys, ptrs, sizes, offsets = ranges
                check(
                    list(
                        store.batch_get_into_multi_buffer_ranges(
                            keys, ptrs, sizes, offsets
                        )
                    ),
                    [sum(s) for s in sizes],
                    "range read",
                )
                stages["range_calls"] += 1
            if group < groups - 1:
                signals[group].set_result(None)
    finally:
        if started:
            check(store.batch_get_session_end(started), 0, "session end")
    # Match ReadPlan: final group publication includes session cleanup.
    signals[-1].set_result(None)


def trial(store, mode, layouts, pools, groups, executor, consumer_ms):
    for p in pools:
        ctypes.memset(ctypes.addressof(p["dst"]), 0xA5, len(p["dst"]))
    os.environ["MOONCAKE_READ_PLAN_PIPELINE"] = "1" if mode == "plan_pipeline" else "0"
    signals = [Future() for _ in range(groups)]
    published = Future()
    stages = {"range_calls": 0}
    start = time.perf_counter_ns()

    def produce():
        try:
            if mode == "legacy":
                legacy(store, layouts, groups, signals, stages)
            else:
                before = time.perf_counter_ns()
                plan = store.create_read_plan(layouts, groups)
                stages["create_plan_ms"] = (time.perf_counter_ns() - before) / 1e6
                published.set_result(plan)
                plan.run()
                stages.setdefault("producer_complete_ns", time.perf_counter_ns())
                stages["plan_stats"] = list(plan.stats())
                check(
                    stages["plan_stats"][2],
                    sum(len(p["dst"]) for p in pools),
                    "plan bytes",
                )
        except BaseException as error:
            for future in [published, *signals]:
                if not future.done():
                    future.set_exception(error)
            raise
        finally:
            stages.setdefault("producer_complete_ns", time.perf_counter_ns())

    running = executor.submit(produce)
    waits, ready = [], []
    try:
        for group in range(groups):
            before = time.perf_counter_ns()
            if mode == "legacy":
                signals[group].result()
            else:
                published.result().wait(group)
            now = time.perf_counter_ns()
            waits.append((now - before) / 1e6)
            ready.append((now - start) / 1e6)
            if consumer_ms:
                time.sleep(consumer_ms / 1000)
        consumer_done = time.perf_counter_ns()
    finally:
        running.result()  # Always join before reusing/unregistering destinations.
    for p in pools:
        size = p["object_size"]
        for row, key in enumerate(p["keys"]):
            actual = ctypes.string_at(ctypes.addressof(p["dst"]) + row * size, size)
            if actual != payload(key, size):
                raise RuntimeError(f"Data mismatch for {key}")
    read_ms = (stages.pop("producer_complete_ns") - start) / 1e6
    size = sum(len(p["dst"]) for p in pools)
    return dict(
        mode=mode,
        first_ready_ms=ready[0],
        layer_observed_ready_ms=ready,
        layer_wait_ms=waits,
        wait_sum_ms=sum(waits),
        read_complete_ms=read_ms,
        consumer_complete_ms=(consumer_done - start) / 1e6,
        effective_gib_s=size / (1024**3) / (read_ms / 1000),
        **stages,
    )


def self_test():
    for layers in (1, 3, 61):
        groups, layouts, pools = make_workload("test_", layers, 3, 33)
        assert groups == layers
        spans = []
        for group in range(groups):
            keys, ptrs, sizes, offsets = expand(layouts[0], group)
            assert len(keys) == 3
            for key, destinations, lengths, sources in zip(keys, ptrs, sizes, offsets):
                source = payload(key, pools[0]["object_size"])
                for ptr, size, offset in zip(destinations, lengths, sources):
                    assert size == 33 and offset == group * 33
                    spans.append((ptr, ptr + size))
                    ctypes.memmove(ptr, source[offset : offset + size], size)
        spans.sort()
        assert all(left[1] == right[0] for left, right in zip(spans, spans[1:]))
        assert sum(end - start for start, end in spans) == layers * 3 * 33
        for p in pools:
            assert bytes(p["dst"]) == b"".join(
                payload(k, p["object_size"]) for k in p["keys"]
            )
    print(
        "PASS: 1/3/61 layers, exact range count, offsets, disjoint destinations and data"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--layers", type=int, default=32, help="Number of read groups")
    parser.add_argument(
        "--keys", type=int, default=16, help="Objects read in every group"
    )
    parser.add_argument(
        "--layer-bytes", type=int, default=4096, help="Bytes per key per group"
    )
    parser.add_argument("--repeats", type=int, default=9)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument(
        "--consumer-ms",
        type=float,
        default=0,
        help="Synthetic consumer delay per group; not GPU compute",
    )
    parser.add_argument(
        "--master", default=os.getenv("MOONCAKE_MASTER", "127.0.0.1:50051")
    )
    parser.add_argument(
        "--metadata", default=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE")
    )
    parser.add_argument(
        "--hostname", default=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost")
    )
    parser.add_argument("--protocol", default=os.getenv("MOONCAKE_PROTOCOL", "tcp"))
    parser.add_argument("--devices", default=os.getenv("MOONCAKE_DEVICE", ""))
    parser.add_argument(
        "--global-segment-bytes",
        type=int,
        default=0,
        help="Local Store capacity; leave 0 when using a separate owner",
    )
    parser.add_argument("--output", type=Path, default=Path("read_plan_bench_results"))
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if (
        min(args.layers, args.keys, args.layer_bytes, args.repeats) < 1
        or args.warmup < 0
        or args.consumer_ms < 0
        or not math.isfinite(args.consumer_ms)
        or args.global_segment_bytes < 0
    ):
        parser.error("Invalid sizes or repetitions")
    from mooncake.store import MooncakeDistributedStore

    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()) + "_" + uuid4().hex
    out = args.output / run_id
    out.mkdir(parents=True)
    groups, layouts, pools = make_workload(
        "plan_bench_" + run_id + "_", args.layers, args.keys, args.layer_bytes
    )
    keys = [k for p in pools for k in p["keys"]]
    report = dict(
        config={k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        environment={
            name: os.getenv(name)
            for name in (
                "MC_STORE_MEMCPY",
                "MC_IB_PCI_RELAXED_ORDERING",
                "MC_FORCE_TCP",
                "MC_USE_TENT",
                "MC_MS_AUTO_DISC",
            )
        },
        layers=groups,
        keys=len(keys),
        read_bytes=sum(len(p["dst"]) for p in pools),
        trials=[],
        cleanup_errors=[],
    )
    (out / "fixture_keys.json").write_text(json.dumps(keys))
    store = MooncakeDistributedStore()
    registered = []
    initialized = False
    saved_pipeline = os.environ.get("MOONCAKE_READ_PLAN_PIPELINE")
    try:
        check(
            store.setup(
                args.hostname,
                args.metadata,
                args.global_segment_bytes,
                16 * 1024**2,
                args.protocol,
                args.devices,
                args.master,
            ),
            0,
            "setup",
        )
        initialized = True
        for p in pools:
            addr = ctypes.addressof(p["dst"])
            check(store.register_buffer(addr, len(p["dst"])), 0, "register")
            registered.append(addr)
        print(
            f"Fixture: {len(keys)} keys, {report['read_bytes']} bytes, {groups} layers",
            flush=True,
        )
        for p in pools:
            for key in p["keys"]:
                check(store.put(key, payload(key, p["object_size"])), 0, "put")
        modes = ["legacy", "plan_serial", "plan_pipeline"]
        with ThreadPoolExecutor(max_workers=1) as executor:
            for iteration in range(args.warmup + args.repeats):
                # Rotate order without relying on random-number implementations.
                shift = iteration % len(modes)
                for mode in modes[shift:] + modes[:shift]:
                    row = trial(
                        store, mode, layouts, pools, groups, executor, args.consumer_ms
                    )
                    row["warmup"] = iteration < args.warmup
                    report["trials"].append(row)
                    print(
                        f"{mode}: first={row['first_ready_ms']:.3f}ms total={row['read_complete_ms']:.3f}ms verified",
                        flush=True,
                    )
        report["summary"] = {}
        for mode in modes:
            rows = [
                r for r in report["trials"] if r["mode"] == mode and not r["warmup"]
            ]
            report["summary"][mode] = {
                metric: {
                    "p50": statistics.median([r[metric] for r in rows]),
                    "p95": percentile([r[metric] for r in rows], 0.95),
                }
                for metric in [
                    "first_ready_ms",
                    "read_complete_ms",
                    "wait_sum_ms",
                    "consumer_complete_ms",
                    "effective_gib_s",
                ]
            }
        baseline = report["summary"]["legacy"]["read_complete_ms"]["p50"]
        for mode in modes:
            report["summary"][mode]["read_speedup_vs_legacy"] = (
                baseline / report["summary"][mode]["read_complete_ms"]["p50"]
            )
        report["status"] = "passed"
    except BaseException as error:
        report["status"] = "failed"
        report["error"] = repr(error)
        raise
    finally:
        if saved_pipeline is None:
            os.environ.pop("MOONCAKE_READ_PLAN_PIPELINE", None)
        else:
            os.environ["MOONCAKE_READ_PLAN_PIPELINE"] = saved_pipeline
        for i in range(0, len(keys) if initialized else 0, 256):
            chunk = keys[i : i + 256]
            try:
                # All reads are joined. Only our exact UUID keys are removed.
                states = list(store.batch_is_exist(chunk))
                check(len(states), len(chunk), "cleanup query length")
                present = [k for k, state in zip(chunk, states) if state == 1]
                if any(state not in (0, 1) for state in states):
                    raise RuntimeError("cleanup query failed")
                if present:
                    check(
                        list(store.batch_remove(present, True)),
                        [0] * len(present),
                        "cleanup",
                    )
                check(
                    list(store.batch_is_exist(chunk)),
                    [0] * len(chunk),
                    "cleanup verification",
                )
            except Exception as error:
                report["cleanup_errors"].append(repr(error))
        for addr in registered:
            try:
                check(store.unregister_buffer(addr), 0, "unregister")
            except Exception as error:
                report["cleanup_errors"].append(repr(error))
        try:
            check(store.close(), 0, "close")
        except Exception as error:
            report["cleanup_errors"].append(repr(error))
        if report["cleanup_errors"]:
            report["status"] = "failed"
        (out / "summary.json").write_text(json.dumps(report, indent=2))
        print(f"Result: {out / 'summary.json'} status={report['status']}", flush=True)
    if report["status"] != "passed":
        raise RuntimeError("Cleanup failed; inspect summary.json")
    print("| Mode | Read p50 (ms) | Read p95 (ms) | First ready p50 (ms) | Speedup |")
    print("| --- | ---: | ---: | ---: | ---: |")
    for mode, metrics in report["summary"].items():
        read = metrics["read_complete_ms"]
        first = metrics["first_ready_ms"]["p50"]
        speedup = metrics["read_speedup_vs_legacy"]
        print(
            f"| {mode} | {read['p50']:.3f} | {read['p95']:.3f} | {first:.3f} | {speedup:.3f}x |"
        )


if __name__ == "__main__":
    main()
