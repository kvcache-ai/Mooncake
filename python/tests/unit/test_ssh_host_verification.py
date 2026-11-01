"""SSH entry points must reject untrusted servers before running commands."""

import importlib.util
import logging
from pathlib import Path
import sys
from types import ModuleType
import unittest
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[3]


class SSHHostVerificationTests(unittest.TestCase):
    def test_admin_entry_points_reject_unknown_host(self) -> None:
        cases = [
            ("spdk_tgt_create", "SPDKTgtCreator", "_ssh_connect"),
            ("mooncake_ssd_register", "MooncakeNoFRegister", "_execute_ssh_command"),
            (
                "mooncake_ssd_unregister",
                "MooncakeNoFUnregister",
                "_execute_ssh_command",
            ),
        ]
        for filename, classname, method in cases:
            with self.subTest(module=filename):
                client = MagicMock()
                paramiko = MagicMock()
                paramiko.SSHClient.return_value = client
                administration = ModuleType("mooncake._administration")
                administration.require_paramiko = lambda: paramiko
                store = ModuleType("mooncake.store")
                store.MooncakeDistributedNoFRegister = MagicMock()
                modules = {
                    "mooncake": ModuleType("mooncake"),
                    "mooncake._administration": administration,
                    "mooncake.store": store,
                }
                with patch.dict(sys.modules, modules):
                    spec = importlib.util.spec_from_file_location(
                        filename, ROOT / "python" / "mooncake" / f"{filename}.py"
                    )
                    module = importlib.util.module_from_spec(spec)
                    spec.loader.exec_module(module)
                instance = getattr(module, classname).__new__(
                    getattr(module, classname)
                )
                instance.cli_config = {"password": "test-password"}
                instance.logger = logging.getLogger(__name__)

                def reject(*args: object, **kwargs: object) -> None:
                    client.load_system_host_keys.assert_called_once_with()
                    client.set_missing_host_key_policy.assert_called_once_with(
                        paramiko.RejectPolicy.return_value
                    )
                    raise RuntimeError("untrusted server")

                client.connect.side_effect = reject
                args = (
                    ("host",)
                    if method == "_ssh_connect"
                    else ("host", "command", "/spdk")
                )
                with self.assertRaisesRegex(RuntimeError, "untrusted server"):
                    getattr(instance, method)(*args)
                client.exec_command.assert_not_called()
                client.close.assert_called_once_with()

                # 验证可信连接仍可使用，且成功返回的连接由调用方管理。
                client.reset_mock()
                client.connect.side_effect = None
                stdout, stderr = MagicMock(), MagicMock()
                stdout.channel.recv_exit_status.return_value = 0
                stdout.read.return_value = b"output"
                stderr.read.return_value = b""
                client.exec_command.return_value = (MagicMock(), stdout, stderr)
                result = getattr(instance, method)(*args)
                if method == "_ssh_connect":
                    self.assertIs(result, client)
                    client.close.assert_not_called()
                else:
                    self.assertEqual(result, "output")
                    self.assertEqual(client.exec_command.call_count, 2)
                    client.close.assert_called_once_with()

    def test_topology_rejects_unknown_host(self) -> None:
        paramiko = MagicMock()
        client = paramiko.SSHClient.return_value
        modules = {
            name: MagicMock() for name in ("numpy", "scipy", "scipy.optimize", "tqdm")
        }
        modules["paramiko"] = paramiko
        with patch.dict(sys.modules, modules):
            spec = importlib.util.spec_from_file_location(
                "topology", ROOT / "scripts" / "generate_cluster_topology.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
        client.connect.side_effect = RuntimeError("untrusted server")
        with self.assertRaisesRegex(RuntimeError, "untrusted server"):
            module.ssh_exec("remote.example", 2222, "command")
        client.load_system_host_keys.assert_called_once_with()
        client.set_missing_host_key_policy.assert_called_once_with(
            paramiko.RejectPolicy.return_value
        )
        client.exec_command.assert_not_called()
        client.close.assert_called_once_with()
        client.reset_mock()
        client.connect.side_effect = None
        stdout, stderr = MagicMock(), MagicMock()
        stdout.read.return_value = b"output"
        stderr.read.return_value = b""
        client.exec_command.return_value = (MagicMock(), stdout, stderr)
        self.assertEqual(module.ssh_exec("remote.example", 2222, "command"), "output")
        client.connect.assert_called_once_with("remote.example", port=2222)
        client.close.assert_called_once_with()
        client.reset_mock()
        client.exec_command.side_effect = RuntimeError("command failed")
        with self.assertRaisesRegex(RuntimeError, "command failed"):
            module.ssh_exec("remote.example", 2222, "command")
        client.close.assert_called_once_with()
        with patch.object(module, "local_exec", return_value="local") as local:
            self.assertEqual(module.ssh_exec("localhost", 22, "command"), "local")
            local.assert_called_once_with("command")


if __name__ == "__main__":
    unittest.main()
