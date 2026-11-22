"""Installed-wheel Engram placement/RPC contracts; no GPU or RDMA required."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import replace
import importlib
from importlib import metadata
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

import pytest

import mooncake
from mooncake.engram import EngramStore, EngramStoreConfig


def test_installed_runtime_contains_control_and_jit_inputs():
    package = Path(mooncake.__file__).resolve().parent
    installed = metadata.distribution("mooncake-transfer-engine")
    assert package == Path(installed.locate_file("mooncake")).resolve(), package
    control = importlib.import_module("mooncake._engram.remote.control")
    jit = importlib.import_module("mooncake._engram.remote.deepep_rdma.jit")
    assert Path(control.__file__).resolve().is_relative_to(package)
    backend = Path(jit.__file__).parent
    assert (backend / "bridge.cpp").is_file()
    assert (backend / "DEEP_EP_LICENSE").is_file()
    assert "torch" not in sys.modules


@pytest.fixture(scope="module")
def master(tmp_path_factory):
    binary = os.environ.get("MOONCAKE_TEST_MASTER_BINARY") or shutil.which(
        "mooncake_master", path=str(Path(sys.executable).parent)
    )
    assert binary, "Install the Mooncake wheel or set MOONCAKE_TEST_MASTER_BINARY"
    # Reserve both ports together so they cannot accidentally be identical.
    with socket.socket() as rpc, socket.socket() as metrics:
        rpc.bind(("127.0.0.1", 0))
        metrics.bind(("127.0.0.1", 0))
        port, metrics_port = rpc.getsockname()[1], metrics.getsockname()[1]
    log_path = tmp_path_factory.mktemp("engram-master") / "master.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                binary,
                "--rpc_address=127.0.0.1",
                f"--rpc_port={port}",
                f"--metrics_port={metrics_port}",
                "--rpc_thread_num=2",
                "--handler_worker_thread_num=2",
                "--enable_metric_reporting=false",
                "--enable_http_metadata_server=false",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            while True:
                assert process.poll() is None, log_path.read_text()
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                        break
                except OSError:
                    assert time.monotonic() < deadline, log_path.read_text()
                    time.sleep(0.05)
            yield f"127.0.0.1:{port}"
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


@pytest.mark.parametrize("assignment", ["auto", "manual"])
def test_placement_is_shared_by_callers_and_metadata_uses_real_rpc(
    master, monkeypatch, tmp_path, assignment
):
    monkeypatch.setenv("MOONCAKE_MASTER", master)
    for name in ("MOONCAKE_CONFIG_PATH", "SGLANG_HICACHE_MOONCAKE_CONFIG_PATH"):
        monkeypatch.delenv(name, raising=False)
    tables = {
        "a": {"num_rows": 3, "row_bytes": 8},
        "b": {"num_rows": 5, "row_bytes": 8},
    }

    def create(node, caller):
        config = EngramStoreConfig(
            backend="deepep_rdma",
            assignment=assignment,
            group_size=2,
            node_id=f"node-{node}",
            model_key=str(tmp_path),
            caller_id=caller,
            num_callers=2,
            max_lookup_rows=64,
            timeout=10,
            group_id="ci-group" if assignment == "manual" else None,
            shard_id=node if assignment == "manual" else None,
        )
        return EngramStore(tables=tables, namespace="layer0", config=config)

    with ExitStack() as stack, ThreadPoolExecutor(max_workers=4) as workers:
        pending = [
            workers.submit(create, node, caller)
            for node in range(2)
            for caller in range(2)
        ]
        stores = [stack.enter_context(future.result(timeout=15)) for future in pending]
        assert len({store.group_id for store in stores}) == 1
        assert stores[0].shard_id == stores[1].shard_id
        assert stores[2].shard_id == stores[3].shard_id
        assert {store.shard_id for store in stores} == {0, 1}
        assert all(store.group_size == 2 for store in stores)
        if assignment == "manual":
            assert stores[0].group_id == "ci-group"
            assert [store.shard_id for store in stores] == [0, 0, 1, 1]
        # Same-node callers rejoin idempotently without taking another slot.
        with create(0, 0) as repeated:
            assert (repeated.group_id, repeated.shard_id) == (
                stores[0].group_id,
                stores[0].shard_id,
            )
        first = stores[0]._assignment.rendezvous("layer0", 0, 2)
        peer = stores[2]._assignment.rendezvous("layer0", 0, 2)
        first.publish("peer", {"shard": stores[0].shard_id})
        assert peer.wait("peer") == {"shard": stores[0].shard_id}
        with pytest.raises(RuntimeError, match="Conflicting immutable"):
            peer.publish("peer", {"shard": stores[2].shard_id})
        assert peer.wait("peer") == {"shard": stores[0].shard_id}
        if assignment == "manual":
            conflicting = replace(stores[0]._config, node_id="other-node")
            with pytest.raises(RuntimeError, match="already belongs"):
                EngramStore(tables=tables, namespace="layer0", config=conflicting)
        assert "torch" not in sys.modules
