from __future__ import annotations

import io
import json
import tempfile
import sys
import types
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from mooncake.weight_store.cli import build_parser, main


class FakeModelClient:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def import_model(self, **kwargs):
        self.calls.append(("import", kwargs))
        return {"checkpoint_id": kwargs["checkpoint_id"], "status": "READY"}

    def list_models(self):
        self.calls.append(("list",))
        return ["ckpt-a"]

    def inspect_model(self, checkpoint_id):
        self.calls.append(("inspect", checkpoint_id))
        return {"checkpoint_id": checkpoint_id, "status": "READY"}

    def verify_model(self, checkpoint_id):
        self.calls.append(("verify", checkpoint_id))
        return {"checkpoint_id": checkpoint_id, "status": "READY"}

    def delete_model(self, checkpoint_id):
        self.calls.append(("delete", checkpoint_id))

    def materialize_file(self, checkpoint_id, path, output_path):
        self.calls.append(("materialize", checkpoint_id, path, output_path))


class FakeStoreForSetup:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def setup(self, *args):
        self.calls.append(args)
        return 0


class TestModelCacheCli(unittest.TestCase):
    def test_parser_accepts_model_import_command(self) -> None:
        args = build_parser().parse_args(
            [
                "model",
                "import",
                "--checkpoint-id",
                "ckpt-a",
                "--model-id",
                "demo/model",
                "--revision",
                "main",
                "--source",
                "/models/demo",
            ]
        )

        self.assertEqual(args.command, "model")
        self.assertEqual(args.model_command, "import")
        self.assertEqual(args.checkpoint_id, "ckpt-a")

    def test_model_commands_call_client_and_print_json(self) -> None:
        client = FakeModelClient()
        with (
            mock.patch(
                "mooncake.weight_store.cli._create_model_client",
                return_value=client,
            ),
            redirect_stdout(io.StringIO()) as stdout,
        ):
            result = main(["model", "verify", "ckpt-a"])

        self.assertEqual(result, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"checkpoint_id": "ckpt-a", "status": "READY"},
        )
        self.assertEqual(client.calls, [("verify", "ckpt-a")])

    def test_model_materialize_command_accepts_path_and_output(self) -> None:
        client = FakeModelClient()
        with tempfile.TemporaryDirectory() as tmp:
            output = str(Path(tmp) / "config.json")
            with (
                mock.patch(
                    "mooncake.weight_store.cli._create_model_client",
                    return_value=client,
                ),
                redirect_stdout(io.StringIO()) as stdout,
            ):
                result = main(
                    [
                        "model",
                        "materialize-file",
                        "ckpt-a",
                        "--path",
                        "config.json",
                        "--output",
                        output,
                    ]
                )

        self.assertEqual(result, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {"checkpoint_id": "ckpt-a", "path": "config.json", "output": output},
        )
        self.assertEqual(
            client.calls,
            [("materialize", "ckpt-a", "config.json", output)],
        )

    def test_config_init_writes_store_defaults(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "weight-store.json"

            with redirect_stdout(io.StringIO()) as stdout:
                result = main(
                    [
                        "--config",
                        str(config_path),
                        "--metadata-server",
                        "http://meta.example/metadata",
                        "--master-server-addr",
                        "10.0.0.1:50051",
                        "config",
                        "init",
                    ]
                )

            self.assertEqual(result, 0)
            self.assertEqual(
                json.loads(stdout.getvalue()),
                {"config": str(config_path), "written": True},
            )
            saved = json.loads(config_path.read_text(encoding="utf-8"))

        self.assertEqual(saved["metadata_server"], "http://meta.example/metadata")
        self.assertEqual(saved["master_server_addr"], "10.0.0.1:50051")
        # config init only writes connection settings, not import-only options.
        self.assertNotIn("replica_num", saved)
        self.assertNotIn("file_chunk_size", saved)
        self.assertNotIn("global_segment_size", saved)
        self.assertNotIn("local_buffer_size", saved)

    def test_config_init_accepts_store_args_after_subcommand(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "weight-store.json"

            with redirect_stdout(io.StringIO()):
                result = main(
                    [
                        "config",
                        "init",
                        "--config",
                        str(config_path),
                        "--local-hostname",
                        "cli-client:12346",
                        "--protocol",
                        "rdma",
                    ]
                )

            self.assertEqual(result, 0)
            saved = json.loads(config_path.read_text(encoding="utf-8"))

        self.assertEqual(saved["local_hostname"], "cli-client:12346")
        self.assertEqual(saved["protocol"], "rdma")

    def test_config_file_supplies_store_defaults_and_cli_can_override(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "weight-store.json"
            config_path.write_text(
                json.dumps(
                    {
                        "metadata_server": "http://meta.example/metadata",
                        "master_server_addr": "10.0.0.1:50051",
                        "replica_num": 3,
                    }
                ),
                encoding="utf-8",
            )

            # Connection defaults come from the config file; the import-only
            # --replica-num overrides the value the config file pre-stored.
            args = build_parser(["--config", str(config_path)]).parse_args(
                [
                    "--config",
                    str(config_path),
                    "model",
                    "import",
                    "--checkpoint-id",
                    "ckpt-a",
                    "--model-id",
                    "demo/model",
                    "--revision",
                    "main",
                    "--source",
                    "/models/demo",
                    "--replica-num",
                    "4",
                ]
            )

        self.assertEqual(args.metadata_server, "http://meta.example/metadata")
        self.assertEqual(args.master_server_addr, "10.0.0.1:50051")
        self.assertEqual(args.replica_num, 4)

    def test_build_parser_loads_config_from_sys_argv_when_argv_is_none(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_path = Path(tmp) / "weight-store.json"
            config_path.write_text(
                json.dumps({"metadata_server": "http://meta.example/metadata"}),
                encoding="utf-8",
            )
            with mock.patch(
                "sys.argv",
                [
                    "mooncake_weight_store",
                    "--config",
                    str(config_path),
                    "model",
                    "list",
                ],
            ):
                args = build_parser().parse_args()

        self.assertEqual(args.metadata_server, "http://meta.example/metadata")

    def test_connect_store_uses_positional_setup_for_existing_bindings(self) -> None:
        from mooncake.weight_store import cli

        fake_store = FakeStoreForSetup()
        args = build_parser().parse_args(["model", "list"])

        fake_module = types.SimpleNamespace(MooncakeDistributedStore=lambda: fake_store)
        with mock.patch.dict(sys.modules, {"mooncake.store": fake_module}):
            created = cli._connect_store(args)

        self.assertIs(created, fake_store)
        # The CLI is always a pure client: segment size 0, fixed local buffer.
        self.assertEqual(fake_store.calls[0][2], cli._CLIENT_GLOBAL_SEGMENT_SIZE)
        self.assertEqual(fake_store.calls[0][3], cli._CLIENT_LOCAL_BUFFER_SIZE)


if __name__ == "__main__":
    unittest.main()
