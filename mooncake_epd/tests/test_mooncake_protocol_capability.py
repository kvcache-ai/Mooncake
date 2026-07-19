from __future__ import annotations

from types import SimpleNamespace

import pytest

from mooncake_epd.core.transfer import (
    MooncakeProtocolCapabilityError,
    MooncakeProtocolError,
    TransferEngine,
    require_mooncake_protocol_support,
    validate_mooncake_protocol_pair,
)


def test_nvlink_capability_gate_accepts_explicit_native_support():
    require_mooncake_protocol_support(
        "nvlink_intra",
        engine_module=SimpleNamespace(SUPPORT_INTRA_NVLINK=True),
    )


@pytest.mark.parametrize("capability", [None, False])
def test_nvlink_capability_gate_rejects_missing_or_disabled_native_support(capability):
    with pytest.raises(MooncakeProtocolCapabilityError, match="SUPPORT_INTRA_NVLINK=true"):
        require_mooncake_protocol_support(
            "nvlink_intra",
            engine_module=SimpleNamespace(SUPPORT_INTRA_NVLINK=capability),
        )


def test_capability_gate_ignores_non_native_protocols():
    require_mooncake_protocol_support("tcp", engine_module=object())
    require_mooncake_protocol_support("shm", engine_module=object())


@pytest.mark.parametrize(
    ("kv_protocol", "direct_protocol"),
    [("nvlink_intra", "tcp"), ("tcp", "nvlink_intra")],
)
def test_nvlink_protocol_pair_rejects_process_scoped_mixed_protocols(
    kv_protocol, direct_protocol
):
    with pytest.raises(MooncakeProtocolError, match="process-scoped"):
        validate_mooncake_protocol_pair(kv_protocol, direct_protocol)


def test_nvlink_protocol_pair_accepts_matching_protocols_and_non_nvlink_splits():
    validate_mooncake_protocol_pair("nvlink_intra", "nvlink_intra")
    validate_mooncake_protocol_pair("rdma", "tcp")


def test_transfer_engine_rejects_unsupported_nvlink_before_native_constructor(
    monkeypatch,
):
    import mooncake_epd.core.transfer.engine as engine_module

    monkeypatch.setattr(
        engine_module.importlib,
        "import_module",
        lambda _name: SimpleNamespace(SUPPORT_INTRA_NVLINK=False),
    )
    engine = TransferEngine(protocol="nvlink_intra")

    with pytest.raises(RuntimeError, match="SUPPORT_INTRA_NVLINK=true"):
        engine.initialize()

    assert engine._mooncake is None  # noqa: SLF001


def test_transfer_engine_accepts_supported_nvlink_and_checks_initialize_result(
    monkeypatch,
):
    import mooncake_epd.core.transfer.engine as engine_module

    calls = []

    class _NativeEngine:
        def initialize(self, *args):
            calls.append(args)
            return 0

        def shutdown(self):
            return None

    monkeypatch.setattr(
        engine_module.importlib,
        "import_module",
        lambda _name: SimpleNamespace(
            SUPPORT_INTRA_NVLINK=True,
            TransferEngine=_NativeEngine,
        ),
    )
    monkeypatch.delenv("MC_INTRANODE_NVLINK", raising=False)
    monkeypatch.setenv("MC_FORCE_TCP", "1")
    engine = TransferEngine(
        protocol="nvlink_intra",
        local_hostname="host-a",
        metadata_server="metadata-a",
        device_name="cuda0",
    )

    engine.initialize()

    assert calls == [("host-a", "metadata-a", "nvlink_intra", "cuda0")]
    assert engine._initialized is True  # noqa: SLF001
    assert engine_module.os.environ["MC_INTRANODE_NVLINK"] == "1"
    assert "MC_FORCE_TCP" not in engine_module.os.environ
    engine.shutdown()


def test_transfer_engine_rejects_nonzero_native_initialize_result(monkeypatch):
    import mooncake_epd.core.transfer.engine as engine_module

    class _NativeEngine:
        def initialize(self, *args):
            del args
            return -1

    monkeypatch.setattr(
        engine_module.importlib,
        "import_module",
        lambda _name: SimpleNamespace(
            SUPPORT_INTRA_NVLINK=True,
            TransferEngine=_NativeEngine,
        ),
    )
    engine = TransferEngine(protocol="nvlink_intra")

    with pytest.raises(RuntimeError, match="initialize returned rc=-1"):
        engine.initialize()
