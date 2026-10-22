"""Opt-in integration with a compatible native Part backend, not the main wheel.

Set MC_KV_STORE_NATIVE=1, point PYTHONPATH at that backend and set
MC_KV_STORE_BUILD to its matching Master build. No backend is bundled here.
"""

from __future__ import annotations

import contextlib
import multiprocessing
import os
import socket
import subprocess
import time
import uuid
from pathlib import Path

import pytest
from _store_test_utils import _manifest, memory_binding
from mooncake.reshard.kv_cache import (
    KVCacheStoreFormat,
    KVCacheTransferLimits,
    MultipartKVCacheStore,
    MultipartReadContext,
    plan_kv_cache_store_upload,
)
from test_kv_cache_reshard import _placement

pytestmark = pytest.mark.skipif(
    os.environ.get("MC_KV_STORE_NATIVE") != "1",
    reason="set MC_KV_STORE_NATIVE=1 for actual Store tests",
)


def _port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait(process, port, logfile):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(Path(logfile).read_text()[-6000:])
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError(f"service on port {port} did not start")


def _stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _real(addresses, *, segment_size=0):
    import torch  # noqa: F401 -- native module initialization order
    from mooncake.store import MooncakeDistributedStore

    store = MooncakeDistributedStore()
    result = store.setup(
        f"127.0.0.1:{_port()}",
        addresses[1],
        segment_size,
        32 << 20,
        "tcp",
        "",
        addresses[0],
    )
    if result != 0:
        raise RuntimeError(f"Store setup failed: {result}")
    return store


@pytest.fixture
def cluster(tmp_path):
    build = Path(
        os.environ.get(
            "MC_KV_STORE_BUILD",
            str(Path(__file__).resolve().parents[3] / "build-kv-reshard-r2s"),
        )
    )
    master = build / "mooncake-store/src/mooncake_master"
    assert master.is_file(), f"missing native master: {master}"
    ports = set()
    while len(ports) < 3:
        ports.add(_port())
    rpc, http, metrics = sorted(ports)
    logfile = tmp_path / "master.log"
    with logfile.open("w") as log:
        process = subprocess.Popen(
            [
                str(master),
                f"--port={rpc}",
                "--rpc_address=127.0.0.1",
                "--rpc_thread_num=4",
                "--enable_metric_reporting=false",
                f"--metrics_port={metrics}",
                "--enable_http_metadata_server=true",
                "--http_metadata_server_host=127.0.0.1",
                f"--http_metadata_server_port={http}",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    provider = None
    try:
        _wait(process, rpc, logfile)
        _wait(process, http, logfile)
        addresses = (f"127.0.0.1:{rpc}", f"http://127.0.0.1:{http}/metadata")
        provider = _real(addresses, segment_size=64 << 20)
        yield addresses, build, tmp_path
    finally:
        if provider is not None:
            provider.close()
        _stop(process)


@contextlib.contextmanager
def _bound(
    store,
    placement,
    participant,
    fmt,
    pages,
    operation,
    *,
    fill=True,
    shared=False,
    salts=None,
):
    binding, buffers, expected = memory_binding(
        placement,
        participant,
        fmt,
        pages,
        operation,
        fill=fill,
        allocator=store.alloc_from_mem_pool if shared else None,
        page_salts=salts,
    )
    registered = []
    try:
        for region in binding.regions:
            assert store.register_buffer(region.address, region.nbytes) == 0
            registered.append(region)
        yield binding, buffers, expected
    finally:
        for region in registered:
            assert store.unregister_buffer(region.address) == 0


@pytest.mark.parametrize("fmt", tuple(KVCacheStoreFormat))
@pytest.mark.parametrize("target_fmt", tuple(KVCacheStoreFormat))
def test_native_tp1pp3_to_tp2pp2_restore(cluster, fmt, target_fmt):
    addresses, _, _ = cluster
    raw = _real(addresses)
    placement = _placement("source", ((0,), (1, 2), (3,)), 1)
    target = _placement("target", ((0, 1), (2, 3)), 2)
    plan = plan_kv_cache_store_upload(placement, object_format=fmt)
    writer = MultipartKVCacheStore(raw, _manifest(plan.layout))
    keys = (uuid.uuid4().hex,)
    try:
        for part in placement.parts:
            with _bound(raw, placement, part.participant_id, fmt, 1, "upload") as (
                binding,
                _,
                _,
            ):
                assert writer.upload(plan, keys, binding, operation_id="upload") == (
                    True,
                )
        reader = MultipartKVCacheStore(
            raw,
            _manifest(
                plan_kv_cache_store_upload(target, object_format=target_fmt).layout
            ),
        )
        reader.transfer_limits = KVCacheTransferLimits(max_batch_operations=2)
        context = MultipartReadContext(
            "read",
            writer.manifest.model_domain,
            keys,
            (writer.manifest.layout.layout_id,),
            (writer.manifest,),
        )
        assert context.page_keys == keys
        for part in target.parts:
            with _bound(
                raw, target, part.participant_id, target_fmt, 1, "read", fill=False
            ) as (binding, buffers, expected):
                page_reader = reader.prepare_page_reader(target, binding)
                assert (
                    page_reader.load(
                        context, [{r.region_id: 0 for r in binding.regions}]
                    )
                    == 1
                )
                assert [bytes(buffer) for buffer in buffers] == expected
    finally:
        raw.close()


def _upload_in_child(addresses, key):
    raw = _real(addresses)
    source = _placement("source", ((0,), (1, 2)), 1)
    fmt = KVCacheStoreFormat.PLHD
    plan = plan_kv_cache_store_upload(source, object_format=fmt)
    writer = MultipartKVCacheStore(raw, _manifest(plan.layout))
    try:
        for part in source.parts:
            with _bound(raw, source, part.participant_id, fmt, 1, "put") as (
                binding,
                _,
                _,
            ):
                assert writer.upload(plan, (key,), binding, operation_id="put") == (
                    True,
                )
    finally:
        raw.close()


def test_native_store_survives_uploader_process_exit(cluster):
    addresses, _, _ = cluster
    key = uuid.uuid4().hex
    worker = multiprocessing.get_context("spawn").Process(
        target=_upload_in_child, args=(addresses, key)
    )
    worker.start()
    try:
        worker.join(timeout=60)
        assert worker.exitcode == 0
    finally:
        if worker.is_alive():
            worker.terminate()
            worker.join(timeout=10)

    # Only the independent Store provider remains; no source runtime is alive.
    raw = _real(addresses)
    target = _placement("target", ((0, 1), (2,)), 2)
    fmt = KVCacheStoreFormat.PLHD
    reader = MultipartKVCacheStore(
        raw, _manifest(plan_kv_cache_store_upload(target, object_format=fmt).layout)
    )
    try:
        source = _placement("source", ((0,), (1, 2)), 1)
        manifest = _manifest(
            plan_kv_cache_store_upload(source, object_format=fmt).layout
        )
        context = MultipartReadContext(
            "get",
            manifest.model_domain,
            (key,),
            (manifest.layout.layout_id,),
            (manifest,),
        )
        assert context.page_keys == (key,)
        for part in target.parts:
            with _bound(
                raw, target, part.participant_id, fmt, 1, "get", fill=False
            ) as (binding, buffers, expected):
                page_reader = reader.prepare_page_reader(target, binding)
                assert (
                    page_reader.load(
                        context, [{r.region_id: 0 for r in binding.regions}]
                    )
                    == 1
                )
                assert [bytes(buffer) for buffer in buffers] == expected
    finally:
        raw.close()
