"""SPDK deployment paths must remain single shell arguments."""

import shlex
import unittest
from unittest.mock import MagicMock, patch

from mooncake.spdk_tgt_create import SPDKTgtCreator


class SPDKPathQuotingTests(unittest.TestCase):
    def test_deployment_commands_preserve_paths(self) -> None:
        for path in ("/opt/spdk", "/opt/SPDK pool", "/opt/user's spdk;echo marker"):
            with self.subTest(path=path):
                creator = SPDKTgtCreator([f"ip:127.0.0.1 path:{path}"])
                self.assertEqual(creator.target_configs[0]["path"], path)
                commands = []

                def record(ssh: object, command: str, **kwargs: object) -> tuple:
                    commands.append(command)
                    return "", ""

                with patch.object(
                    creator, "_execute_command", side_effect=record
                ), patch("mooncake.spdk_tgt_create.time.sleep"):
                    ssh = MagicMock()
                    creator._setup_spdk(ssh, path, ["0000:01:00.0"])
                    creator._start_spdk_tgt(ssh, path)
                    creator._create_transport(ssh, path)
                    creator._create_bdevs(ssh, path, ["0000:01:00.0"])
                    nqn = creator._create_subsystem(ssh, path)
                    creator._add_namespaces(ssh, path, nqn, ["Nvme0n1"])
                    creator._add_listener(ssh, path, nqn, "127.0.0.1")
                tokens = [
                    list(shlex.shlex(c, posix=True, punctuation_chars=True))
                    for c in commands
                ]
                self.assertIn(f"{path}/scripts/setup.sh", tokens[0])
                start = tokens[3]
                self.assertEqual(start[1], f"{path}/build/bin/nvmf_tgt")
                self.assertEqual(start[start.index(">") + 1], f"{path}/tgt.log")
                for rpc in tokens[4:]:
                    self.assertEqual(rpc[0], f"{path}/scripts/rpc.py")
                    self.assertNotIn(";", rpc)


if __name__ == "__main__":
    unittest.main()
