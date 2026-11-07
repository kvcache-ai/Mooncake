import sys
from unittest.mock import patch

import pytest

from mooncake.spdk_tgt_create import SPDKTgtCreator, main, parse_arguments


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


@pytest.mark.parametrize(
    "auth_args, expected_auth",
    [
        (["--password", "secret"], {"password": "secret"}),
        (
            ["--key-file", "/home/alice/.ssh/id_ed25519"],
            {"key_filename": "/home/alice/.ssh/id_ed25519"},
        ),
    ],
)
def test_main_uses_requested_ssh_credentials(auth_args, expected_auth):
    argv = [
        "spdk_tgt_create",
        "--spdk_target_info",
        "ip:127.0.0.1 path:/home/spdk",
        "--port",
        "2222",
        "--username",
        "alice",
        *auth_args,
    ]

    with (
        patch.object(sys, "argv", argv),
        patch("mooncake.spdk_tgt_create.require_paramiko") as require_paramiko,
    ):
        ssh_client = require_paramiko.return_value.SSHClient
        ssh_client.return_value.connect.side_effect = RuntimeError("stop")
        with pytest.raises(SystemExit):
            main()

    ssh_client.return_value.connect.assert_called_once_with(
        "127.0.0.1",
        port=2222,
        username="alice",
        **expected_auth,
    )
