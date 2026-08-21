import socket

import pytest

from mooncake import engine


@pytest.mark.parametrize(
    ("protocol", "is_supported"),
    (
        ("nvlink", engine.SUPPORT_MNNVL),
        ("hip", engine.SUPPORT_HIP),
        ("nvlink_intra", engine.SUPPORT_INTRA_NVLINK),
    ),
)
def test_initialize_rejects_unsupported_protocol(protocol, is_supported):
    if is_supported:
        pytest.skip(f"{protocol} is available in this build")

    transfer_engine = engine.TransferEngine()

    assert (
        transfer_engine.initialize(socket.gethostname(), "P2PHANDSHAKE", protocol, "")
        == -1
    )
