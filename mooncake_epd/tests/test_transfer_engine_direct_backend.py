from __future__ import annotations

import torch

from mooncake_epd.core.transfer import Mode, TransferEngine, TransferPolicy


def test_remote_transfer_accepts_mooncake_engine_direct_backend(monkeypatch):
    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    engine._mooncake = object()  # noqa: SLF001

    called = {}

    def _fake_engine_direct(*, tensor, target_device, policy):
        called["backend"] = policy.extra["transport_backend"]
        return tensor.clone()

    monkeypatch.setattr(
        engine,
        "_remote_transfer_via_engine_buffer",
        _fake_engine_direct,
    )

    tensor = torch.randn(4, 8, dtype=torch.float32)
    out = engine._remote_transfer(  # noqa: SLF001
        tensor,
        torch.device("cpu"),
        TransferPolicy(
            mode=Mode.STREAM,
            extra={
                "transport_backend": "mooncake_engine_direct",
                "remote_session": "peer-0",
                "peer_buffer_addr": 4096,
            },
        ),
    )

    assert called["backend"] == "mooncake_engine_direct"
    assert out.shape == tensor.shape


def test_load_store_config_uses_mooncake_official_key_names(monkeypatch):
    monkeypatch.setenv("MOONCAKE_TE_META_DATA_SERVER", "http://127.0.0.1:8090/metadata")
    monkeypatch.setenv("MOONCAKE_MASTER", "127.0.0.1:50061")
    monkeypatch.setenv("MOONCAKE_LOCAL_HOSTNAME", "127.0.0.1")
    monkeypatch.setenv("MOONCAKE_PROTOCOL", "tcp")
    monkeypatch.setenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", "1048576")
    monkeypatch.setenv("MOONCAKE_LOCAL_BUFFER_SIZE", "524288")

    engine = TransferEngine(protocol="tcp", device_name="mlx5_0")
    cfg = engine._load_store_config()  # noqa: SLF001

    assert cfg is not None
    assert cfg["device_name"] == "mlx5_0"
    assert cfg["master_server_address"] == "127.0.0.1:50061"
    assert "master_server_addr" not in cfg
    assert "rdma_devices" not in cfg


def test_peer_buffer_write_falls_back_to_latin1_string(monkeypatch):
    class _FakeMooncake:
        def __init__(self):
            self.writes = []

        def write_bytes_to_buffer(self, ptr, payload, nbytes):
            if isinstance(payload, bytes):
                raise TypeError("bytes overload unavailable")
            self.writes.append((ptr, payload, nbytes))
            return 0

        def read_bytes_from_buffer(self, ptr, nbytes):
            return "hello"

    engine = TransferEngine(protocol="tcp")
    engine._initialized = True  # noqa: SLF001
    engine._mooncake = _FakeMooncake()  # noqa: SLF001

    peer = type("Peer", (), {"pointer": 1234, "size_bytes": 16, "registered": False})()

    engine.write_peer_buffer(peer, b"hello")
    echoed = engine.read_peer_buffer(peer, 5)

    assert engine._mooncake.writes == [(1234, "hello", 5)]  # noqa: SLF001
    assert echoed == b"hello"
