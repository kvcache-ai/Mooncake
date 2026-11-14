"""Exercise CLI credentials through deployment to the SSH client."""

import unittest
from unittest.mock import MagicMock, patch

from mooncake import spdk_tgt_create as target


class SSHCredentialsTests(unittest.TestCase):
    def test_cli_credentials_reach_ssh(self) -> None:
        cases = [
            ([], {"port": 22, "username": "root", "password": None}),
            (
                [
                    "--username",
                    "operator",
                    "--password",
                    "test-secret",
                    "--port",
                    "2222",
                ],
                {"port": 2222, "username": "operator", "password": "test-secret"},
            ),
            (
                [
                    "--username",
                    "operator",
                    "--key-file",
                    "/keys/test key",
                    "--port",
                    "2222",
                ],
                {
                    "port": 2222,
                    "username": "operator",
                    "key_filename": "/keys/test key",
                },
            ),
        ]
        for options, expected in cases:
            with self.subTest(options=options):
                paramiko = MagicMock()
                ssh = paramiko.SSHClient.return_value
                # 在建立连接后立即停止，避免任何硬件管理操作。
                with patch(
                    "sys.argv",
                    [
                        "spdk_tgt_create",
                        "--spdk_target_info",
                        "ip:example.invalid path:/spdk",
                    ]
                    + options,
                ), patch.object(
                    target, "require_paramiko", return_value=paramiko
                ), patch.object(
                    target.SPDKTgtCreator,
                    "_discover_nvme_pci_devices",
                    side_effect=RuntimeError("stop after connect"),
                ):
                    with self.assertRaises(SystemExit) as result:
                        target.main()
                self.assertEqual(result.exception.code, 1)
                ssh.connect.assert_called_once_with("example.invalid", **expected)
                ssh.exec_command.assert_not_called()
                ssh.close.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
