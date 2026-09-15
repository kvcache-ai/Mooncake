#!/usr/bin/env python3
"""ReadPlan TCP e2e, including caller-owned buffers and session range reads.

Requires a running mooncake_master and a ReadPlan-enabled store Python module.
For a managed master and both execution configurations, run from the repository
root after loading the platform runtime environment:

  BUILD_DIR="$PWD/build" bash mooncake-store/tests/e2e/run_read_plan_tcp_e2e.sh

The runner loads the built extension and its dependent libraries.
"""

from __future__ import annotations

import ctypes
import gc
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
import os
import sys
import time
import weakref
from uuid import uuid4


def _require_store():
    try:
        from mooncake import store
    except Exception as exc:  # pragma: no cover
        print(f"import_fail {exc}", flush=True)
        raise SystemExit(10)
    return store


def _ptr(buf: ctypes.Array) -> int:
    return ctypes.addressof(buf)


def run() -> int:
    store = _require_store()

    master = os.getenv("MOONCAKE_MASTER", "127.0.0.1:50051")
    metadata = os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE")
    protocol = os.getenv("MOONCAKE_PROTOCOL", "tcp")
    device = os.getenv("MOONCAKE_DEVICE", "")
    hostname = os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost:17814")
    segment = int(os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", str(64 * 1024 * 1024)))
    local_buf = int(os.getenv("MOONCAKE_LOCAL_BUFFER_SIZE", str(64 * 1024 * 1024)))

    num_layers = int(os.getenv("E2E_NUM_LAYERS", "4"))
    page_size = int(os.getenv("E2E_PAGE_SIZE", "4096"))
    num_keys = int(os.getenv("E2E_NUM_KEYS", "3"))
    object_size = page_size * num_layers

    print(
        f"e2e_session_ranges protocol={protocol} master={master} "
        f"keys={num_keys} layers={num_layers} page={page_size}",
        flush=True,
    )

    mc = store.MooncakeDistributedStore()
    setup_ret = mc.setup(
        hostname, metadata, segment, local_buf, protocol, device, master
    )
    print(f"setup_ret {setup_ret}", flush=True)
    if setup_ret != 0:
        return setup_ret

    src = (ctypes.c_char * (object_size * num_keys))()
    dst = (ctypes.c_char * (object_size * num_keys))()
    for i in range(len(src)):
        src[i] = ord("a") + (i % 26)
        dst[i] = ord("B")

    assert mc.register_buffer(_ptr(src), len(src)) == 0
    assert mc.register_buffer(_ptr(dst), len(dst)) == 0

    run_id = uuid4().hex
    keys = [f"session_e2e_key_{i}_{run_id}" for i in range(num_keys)]
    sizes = [object_size] * num_keys

    put_start = mc.batch_put_session_start(keys, sizes)
    print(f"batch_put_session_start {put_start}", flush=True)
    if any(rc != 0 for rc in put_start):
        return 20

    for layer in range(num_layers):
        all_buffers = []
        all_sizes = []
        all_dst_offsets = []
        for i in range(num_keys):
            offset = i * object_size + layer * page_size
            all_buffers.append([_ptr(src) + offset])
            all_sizes.append([page_size])
            all_dst_offsets.append([layer * page_size])
        put_rcs = mc.batch_put_from_multi_buffer_ranges(
            keys, all_buffers, all_sizes, all_dst_offsets
        )
        print(f"batch_put_ranges layer={layer} rcs={put_rcs}", flush=True)
        if any(rc != page_size for rc in put_rcs):
            mc.batch_put_session_revoke(keys)
            return 21

    put_end = mc.batch_put_session_end(keys)
    print(f"batch_put_session_end {put_end}", flush=True)
    if any(rc != 0 for rc in put_end):
        return 22

    Plan = mc.create_read_plan
    layout = [
        [
            (
                _ptr(dst) + layer * page_size,
                object_size,
                page_size // 2,
                layer * page_size,
            ),
            (
                _ptr(dst) + layer * page_size + page_size // 2,
                object_size,
                page_size // 2,
                layer * page_size + page_size // 2,
            ),
        ]
        for layer in range(num_layers)
    ]
    # The caller retains registered destination memory until run() exits.
    destination_ref = weakref.ref(dst)
    plan = Plan(
        [(keys, list(range(num_keys)), True, layout)],
        num_layers,
    )
    # An unstarted plan reserves the client too; close must not invalidate it.
    try:
        mc.close()
    except RuntimeError as exc:
        assert "unfinished ReadPlans" in str(exc)
    else:
        raise AssertionError("close accepted an unstarted plan")
    plan.run()
    for layer in range(num_layers):
        plan.wait(layer)
    assert plan.stats() == [num_layers, num_layers * num_keys, object_size * num_keys]
    print("read_plan_stats", plan.stats(), flush=True)

    if bytes(src) != bytes(dst):
        print("data_mismatch", flush=True)
        return 40

    # Partial session-start failure must notify consumers and clean up.
    bad_plan = Plan(
        [(keys[:1] + ["missing_read_plan_key"], [0, 1], True, layout)],
        num_layers,
    )
    try:
        bad_plan.run()
    except RuntimeError:
        pass
    else:
        raise AssertionError("missing key accepted")
    try:
        bad_plan.wait(num_layers - 1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("failed plan wait unexpectedly succeeded")
    assert mc.batch_get_session_start(keys) == [0] * num_keys
    assert mc.batch_get_session_end(keys) == 0
    print("read_plan_failure_cleanup PASSED", flush=True)

    # Revoke path: start put then revoke before end.
    revoke_keys = [f"session_e2e_revoke_{int(time.time())}"]
    revoke_start = mc.batch_put_session_start(revoke_keys, [page_size])
    print(f"batch_put_session_start_revoke {revoke_start}", flush=True)
    if any(rc != 0 for rc in revoke_start):
        return 50
    revoke_rcs = mc.batch_put_session_revoke(revoke_keys)
    print(f"batch_put_session_revoke {revoke_rcs}", flush=True)
    if any(rc != 0 for rc in revoke_rcs):
        return 51

    # A waiting consumer releases the GIL, but does not make an unstarted plan
    # safe to close. Rejected close must leave the client usable by run().
    pending = Plan([], 1)
    entered = Event()

    def wait_pending(plan):
        entered.set()
        plan.wait(0)

    with ThreadPoolExecutor(max_workers=1) as pool:
        waiting = pool.submit(wait_pending, pending)
        assert entered.wait(5)
        try:
            mc.close()
        except RuntimeError as exc:
            assert "unfinished ReadPlans" in str(exc)
        else:
            raise AssertionError("close accepted a plan with a waiting consumer")
        finally:
            pending.run()  # Always release the waiting thread, even on failure.
        waiting.result(timeout=5)

    # Allocation managers may own plans without a reverse owner reference.
    # Releasing one must reclaim both unstarted and completed plans, so an
    # abandoned plan cannot permanently block close().
    class AllocationOwner:
        pass

    for completed in (False, True):
        owner = AllocationOwner()
        owner.plan = Plan([], 1)
        if completed:
            owner.plan.run()
        owner_ref = weakref.ref(owner)
        plan_ref = weakref.ref(owner.plan)
        del owner
        gc.collect()
        assert owner_ref() is None
        assert plan_ref() is None
    # Prepare before racing close(), which may legitimately close the client.
    retained = Plan([], 1)
    retained.run()
    client_ref = weakref.ref(mc)
    # Race run() (which releases the GIL) against close(). Close must either
    # reject the unfinished plan or succeed after cleanup; reads must succeed.
    concurrent = Plan(
        [(keys, list(range(num_keys)), True, layout)],
        num_layers,
    )
    barrier = Barrier(2)

    def run_concurrent(plan):
        barrier.wait(timeout=5)
        plan.run()

    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(run_concurrent, concurrent)
        barrier.wait(timeout=5)
        try:
            assert mc.close() == 0
        except RuntimeError as exc:
            assert "unfinished ReadPlans" in str(exc)
        running.result(timeout=30)
    assert bytes(src) == bytes(dst)
    # Successful, failed and released plans no longer prevent close.
    assert mc.close() == 0
    assert mc.close() == 0
    try:
        Plan([], 1)
    except RuntimeError:
        pass
    else:
        raise AssertionError("created plan on closed client")
    del Plan, plan, bad_plan, pending, concurrent, dst, mc
    gc.collect()
    assert destination_ref() is None
    # A completed plan still retains its Store wrapper until destruction.
    assert client_ref() is not None
    del retained
    gc.collect()
    assert client_ref() is None
    print("read_plan_caller_and_client_lifetimes PASSED", flush=True)
    print("read_plan_tcp_e2e PASSED", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(run())
