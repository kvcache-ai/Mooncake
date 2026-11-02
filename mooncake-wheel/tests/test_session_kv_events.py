"""Run with MOONCAKE_TEST_MASTER_BINARY pointing to an ENABLE_KV_EVENTS build."""

import ctypes
import os
import socket
import subprocess
import time

import pytest


@pytest.fixture(params=[False, True], ids=["canonical", "legacy-fields"])
def session_store(request, tmp_path):
    binary = os.getenv("MOONCAKE_TEST_MASTER_BINARY")
    if not binary:
        pytest.skip("set MOONCAKE_TEST_MASTER_BINARY to run native KV event tests")
    zmq = pytest.importorskip("zmq")
    msgpack = pytest.importorskip("msgpack")
    from mooncake.store import MooncakeDistributedStore

    # Reserve distinct local ports while selecting the endpoints.
    reservations = [socket.socket() for _ in range(4)]
    for reservation in reservations:
        reservation.bind(("0.0.0.0", 0))
    rpc, metadata, events, admin = [s.getsockname()[1] for s in reservations]
    for reservation in reservations:
        reservation.close()
    endpoint = f"tcp://127.0.0.1:{events}"
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"")
    subscriber.setsockopt(zmq.LINGER, 0)
    subscriber.connect(endpoint)
    store = MooncakeDistributedStore()
    with (tmp_path / "master.log").open("w") as log:
        master = subprocess.Popen(
            [
                binary,
                f"--port={rpc}",
                f"--metrics_port={admin}",
                "--enable_metric_reporting=false",
                "--default_kv_lease_ttl=100ms",
                "--enable_http_metadata_server=true",
                f"--http_metadata_server_port={metadata}",
                "--enable_kv_events=true",
                f"--kv_events_bind_endpoint={endpoint}",
                "--kv_events_backend_id=session-test",
                f"--kv_events_emit_legacy_compat={str(request.param).lower()}",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 15
            while True:
                assert master.poll() is None, (tmp_path / "master.log").read_text()
                try:
                    with socket.create_connection(("127.0.0.1", rpc), timeout=0.1):
                        break
                except OSError:
                    assert time.monotonic() < deadline, "Master startup timed out"
                    time.sleep(0.05)
            assert (
                store.setup(
                    "127.0.0.1",
                    f"http://127.0.0.1:{metadata}/metadata",
                    32 * 1024 * 1024,
                    16 * 1024 * 1024,
                    "tcp",
                    "",
                    f"127.0.0.1:{rpc}",
                )
                == 0
            )
            time.sleep(0.3)  # Allow the PUB/SUB subscription to reach the publisher.

            def receive(count):
                result = []
                while len(result) < count:
                    assert subscriber.poll(5000), "Missing KV event"
                    frames = subscriber.recv_multipart()
                    assert len(frames) == 3
                    result.extend(msgpack.unpackb(frames[2], raw=False)[1])
                assert len(result) == count
                return result

            yield store, receive, subscriber
        finally:
            store.close()
            master.terminate()
            try:
                master.wait(timeout=10)
            except subprocess.TimeoutExpired:
                master.kill()
                master.wait()
            subscriber.close()
            context.term()


def tagged(*ids):
    from mooncake.store import ReplicateConfig

    config = ReplicateConfig()
    config.session_ids = list(ids)
    return config


def test_session_tags_over_rpc_and_zmq(session_store):
    from mooncake.store import ReplicateConfig

    store, receive, subscriber = session_store
    config = ReplicateConfig()
    assert config.session_ids is None
    config.session_ids = ["a"]
    assert config.session_ids == ["a"]
    config.session_ids = None
    assert config.session_ids is None

    def expect_stored(key, session):
        event = receive(1)[0]
        assert event["event_type"] == "stored"
        assert event["object_key"] == key
        assert event["session_id"] == session

    assert store.put("shared", b"old", tagged("a")) == 0
    expect_stored("shared", "a")
    # Duplicate Put and reads do not claim a new session association.
    store.put("shared", b"new", tagged("b"))
    assert store.get("shared") == b"old"
    assert not subscriber.poll(300)
    assert store.upsert("shared", b"new", tagged("b")) == 0
    changes = receive(2)
    assert [e["session_id"] for e in changes if e["event_type"] == "stored"] == ["b"]
    assert store.upsert("shared", b"same-size-and-larger", tagged("")) == 0
    changes = receive(2)
    assert {event["event_type"] for event in changes} == {"stored", "removed"}
    for event in changes:
        if event["event_type"] == "stored":
            assert event["session_id"] == "b"
        else:
            assert "session_id" not in event
    assert store.upsert("shared", b"absent") == 0
    changes = receive(2)
    assert [e["session_id"] for e in changes if e["event_type"] == "stored"] == ["b"]

    assert store.put_batch(["one", "two"], [b"1", b"2"], tagged("x", "")) == 0
    assert {e["object_key"]: e["session_id"] for e in receive(2)} == {
        "one": "x",
        "two": None,
    }
    assert store.upsert_batch(["one", "two"], [b"3", b"4"], tagged("y", "z")) == 0
    assert {
        e["object_key"]: e["session_id"]
        for e in receive(4)
        if e["event_type"] == "stored"
    } == {
        "one": "y",
        "two": "z",
    }

    config = tagged("same-node-a", "same-node-b")
    config.prefer_alloc_in_same_node = True
    buffer = ctypes.create_string_buffer(b"ab")
    pointer = ctypes.addressof(buffer)
    assert store.register_buffer(pointer, 2) == 0
    try:
        assert store.batch_put_from_multi_buffers(
            ["local-a", "local-b"], [[pointer], [pointer + 1]], [[1], [1]], config
        ) == [0, 0]
    finally:
        store.unregister_buffer(pointer)
    assert (store.get("local-a"), store.get("local-b")) == (b"a", b"b")
    assert {e["object_key"]: e["session_id"] for e in receive(2)} == {
        "local-a": "same-node-a",
        "local-b": "same-node-b",
    }

    for invalid in (tagged("x"), tagged("x", "x" * 257), tagged("x", b"\xff")):
        assert store.put_batch(["bad1", "bad2"], [b"1", b"2"], invalid) != 0
        assert store.is_exist("bad1") == 0
    for write in (store.put, store.upsert):
        assert write("invalid-utf8", b"x", tagged(b"\xff")) != 0
        assert store.is_exist("invalid-utf8") == 0
    assert (
        store.upsert_batch(["bad1", "bad2"], [b"1", b"2"], tagged("valid", b"\xff"))
        != 0
    )
    assert store.is_exist("bad1") == 0
    assert store.put("too-long", b"x", tagged("会" * 86)) != 0
    assert not subscriber.poll(300)
    assert store.put("boundary", b"x", tagged("x" * 256)) == 0
    expect_stored("boundary", "x" * 256)
    assert store.put("unicode-boundary", b"x", tagged("会" * 85 + "x")) == 0
    expect_stored("unicode-boundary", "会" * 85 + "x")
    assert store.remove("shared", force=True) == 0
    assert "session_id" not in receive(1)[0]
    assert store.put("shared", b"fresh") == 0
    expect_stored("shared", None)

    # An already-open write session is filtered before the second RPC batch.
    assert store.batch_put_session_start(["held"], [4], tagged("original")) == [0]
    results = store.batch_put_session_start(
        ["held", "kept"], [4, 4], tagged("wrong", "kept-session")
    )
    assert results[0] != 0 and results[1] == 0
    buffer = ctypes.create_string_buffer(b"data")
    pointer = ctypes.addressof(buffer)
    assert store.register_buffer(pointer, 4) == 0
    try:
        assert store.batch_put_from_multi_buffer_ranges(
            ["kept"], [[pointer]], [[4]], [[0]]
        ) == [4]
        assert store.batch_put_session_end(["kept"]) == [0]
        expect_stored("kept", "kept-session")
    finally:
        store.unregister_buffer(pointer)
        store.batch_put_session_revoke(["held"])
    assert not subscriber.poll(300)


def test_tensor_filtering_and_tp_session_and_hca_mapping(session_store):
    torch = pytest.importorskip("torch")
    store, receive, subscriber = session_store
    tensor = torch.arange(8, dtype=torch.float32).reshape(2, 4)
    for write in (store.batch_pub_tensor, store.batch_upsert_pub_tensor):
        config = tagged("wrong", "right")
        config.group_ids = ["wrong-group", "right-group"]
        config.preferred_hca_names = ["mlx5_7"]
        config.per_key_preferred_hca_names = [["mlx5_0"], ["mlx5_1"]]
        results = write(["invalid", "valid"], [None, tensor], config)
        assert results[0] != 0 and results[1] == 0
        event = next(e for e in receive(1) if e["event_type"] == "stored")
        assert (event["object_key"], event["session_id"], event["group_id"]) == (
            "valid",
            "right",
            "right-group",
        )
        assert torch.equal(store.get_tensor("valid"), tensor)
        assert config.session_ids == ["wrong", "right"]
        assert config.group_ids == ["wrong-group", "right-group"]
        assert config.preferred_hca_names == ["mlx5_7"]
        assert config.per_key_preferred_hca_names == [["mlx5_0"], ["mlx5_1"]]
        assert store.remove("valid", force=True) == 0
        assert receive(1)[0]["event_type"] == "removed"

    config = tagged("tp-session")
    config.group_ids = ["tp-left", "tp-right"]
    config.per_key_preferred_hca_names = [["mlx5_0"], ["mlx5_1"]]
    assert store.pub_tensor_with_tp("tp", tensor, config, tp_size=2) == 0
    events = receive(2)
    assert [e["session_id"] for e in events] == ["tp-session"] * 2
    assert sorted(e["group_id"] for e in events) == ["tp-left", "tp-right"]
    config = tagged("wrong", "a", "b")
    config.group_ids = ["wrong-group", "a-group", "b-group"]
    config.per_key_preferred_hca_names = [["mlx5_0"], ["mlx5_1"], []]
    results = store.batch_pub_tensor_with_tp(
        ["tp-invalid", "tp-a", "tp-b"], [None, tensor, tensor], config, tp_size=2
    )
    assert results[0] != 0 and results[1:] == [0, 0]
    assert {e["object_key"]: (e["session_id"], e["group_id"]) for e in receive(4)} == {
        f"tp-{key}_tp_{rank}": (key, f"{key}-group")
        for key in ("a", "b")
        for rank in range(2)
    }
    assert not subscriber.poll(300)


@pytest.mark.parametrize(
    "field", ["session_ids", "group_ids", "per_key_preferred_hca_names"]
)
def test_tensor_hint_counts_are_validated_before_filtering(session_store, field):
    torch = pytest.importorskip("torch")
    store, _, subscriber = session_store
    tensor = torch.arange(8, dtype=torch.float32).reshape(2, 4)
    config = tagged("wrong", "right")
    config.group_ids = ["wrong-group", "right-group"]
    config.per_key_preferred_hca_names = [["mlx5_0"], ["mlx5_1"]]
    setattr(config, field, getattr(config, field)[1:])
    for write, kwargs in (
        (store.batch_pub_tensor, {}),
        (store.batch_upsert_pub_tensor, {}),
        (store.batch_pub_tensor_with_tp, {"tp_size": 2}),
    ):
        results = write(["invalid", "valid"], [None, tensor], config, **kwargs)
        assert len(results) == 2 and all(result != 0 for result in results)
        assert store.is_exist("valid") == 0
        assert store.is_exist("valid_tp_0") == 0
    assert not subscriber.poll(300)


def test_structured_session_expansion_preserves_native_write_options():
    from mooncake.store import ReplicateConfig, SoftPinAction
    from mooncake.structured_object_store import _config_for_grouped_keys

    config = ReplicateConfig()
    config.dfs_replica_num = 1
    config.soft_pin_action = SoftPinAction.ENABLE
    config.soft_pin_ttl_ms = 60000
    config.session_ids = ["session"]
    config.preferred_hca_names = ["mlx5_0"]
    config.per_key_preferred_hca_names = [["mlx5_1"]]
    expanded = _config_for_grouped_keys(config, 2)
    assert expanded.session_ids == ["session", "session"]
    assert expanded.dfs_replica_num == 1
    assert expanded.soft_pin_action == SoftPinAction.ENABLE
    assert expanded.soft_pin_ttl_ms == 60000
    assert expanded.preferred_hca_names == ["mlx5_0"]
    assert expanded.per_key_preferred_hca_names == [["mlx5_1"], ["mlx5_1"]]
    assert config.session_ids == ["session"]
    assert config.per_key_preferred_hca_names == [["mlx5_1"]]
    assert expanded.group_ids is None
