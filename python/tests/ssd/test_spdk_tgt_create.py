import sys
from unittest.mock import patch

from mooncake.spdk_tgt_create import SPDKTgtCreator, parse_arguments


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

    with patch("mooncake.spdk_tgt_create.require_paramiko") as require_paramiko:
        ssh_client = require_paramiko.return_value.SSHClient
        creator._ssh_connect("127.0.0.1", port=2222)

    ssh_client.return_value.connect.assert_called_once_with(
        "127.0.0.1",
        port=2222,
        username="root",
        password=None,
    )
