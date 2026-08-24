from importlib.util import find_spec
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

if find_spec("paramiko") is None:
    raise unittest.SkipTest("paramiko is required for SPDK target tests")

import mooncake.spdk_tgt_create as spdk_tgt_create
from mooncake.spdk_tgt_create import SPDKTgtCreator, parse_arguments


def test_module_loads_from_canonical_source():
    repository_root = Path(__file__).resolve().parents[3]

    assert Path(spdk_tgt_create.__file__).resolve() == (
        repository_root / "python" / "mooncake" / "spdk_tgt_create.py"
    )


def test_parse_arguments_accepts_ssh_port():
    with patch.object(
        sys,
        "argv",
        [
            "spdk_tgt_create",
            "--spdk_target_info",
            "ip:127.0.0.1 path:/home/spdk",
            "--port",
            "2222",
        ],
    ):
        args = parse_arguments()

    assert args.port == 2222


def test_ssh_connect_uses_requested_port():
    creator = SPDKTgtCreator(["ip:127.0.0.1 path:/home/spdk"])

    with patch("mooncake.spdk_tgt_create.paramiko.SSHClient") as ssh_client:
        creator._ssh_connect("127.0.0.1", port=2222)

    ssh_client.return_value.connect.assert_called_once_with(
        "127.0.0.1",
        port=2222,
        username="root",
        password=None,
    )
