from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr
from pathlib import Path

from _support import EXAMPLE_DIR, cloned_config_dict, proxy, valid_config_dict


def valid_cli_args() -> list[str]:
    return [
        "--prefiller-hosts",
        "prefill-a.test",
        "prefill-b.test",
        "--prefiller-ports",
        "8100",
        "8101",
        "--prefiller-instance-ids",
        "prefill-a",
        "prefill-b",
        "--prefiller-registration",
        "prefill-a",
        "0",
        "tcp://prefill-a.test:5557",
        "--prefiller-registration",
        "prefill-a",
        "1",
        "tcp://prefill-a.test:5558",
        "--prefiller-registration",
        "prefill-b",
        "0",
        "tcp://prefill-b.test:5557",
        "--decoder-hosts",
        "decode-a.test",
        "decode-b.test",
        "--decoder-ports",
        "8200",
        "8201",
        "--conductor-address",
        "http://conductor.test:13333",
        "--model",
        "test-model",
        "--block-size",
        "16",
        "--tenant-id",
        "tenant-a",
        "--lora-name",
        "adapter-a",
        "--python-hash-seed",
        "0",
        "--query-timeout-seconds",
        "0.25",
        "--registration-timeout-seconds",
        "2.0",
    ]


class ConfigTest(unittest.TestCase):
    def assert_cli_error(self, argv: list[str], message: str) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
            proxy.parse_args(argv)
        self.assertEqual(2, raised.exception.code)
        self.assertIn(message, stderr.getvalue())

    def test_sample_config_loads(self) -> None:
        sample_path = EXAMPLE_DIR / "cache_aware_proxy_config.json"
        config = proxy.load_config(sample_path)

        self.assertEqual(
            ["prefill-a", "prefill-b"],
            [p.instance_id for p in config.prefill.instances],
        )
        self.assertEqual(1, len(config.decode.instances))
        self.assertEqual("0", config.prefill.config.hash_profile.python_hash_seed)

    def test_valid_multi_rank_config_is_typed_and_normalized(self) -> None:
        raw = valid_config_dict()
        raw["prefill"]["config"]["tenant_id"] = ""
        config = proxy.parse_config(raw)

        self.assertIsInstance(config.conductor, proxy.ConductorConfig)
        self.assertIsInstance(config.prefill, proxy.PrefillPoolConfig)
        self.assertIsInstance(config.prefill.config, proxy.PrefillSharedConfig)
        self.assertIsInstance(config.decode, proxy.DecodePoolConfig)
        self.assertEqual("default", config.prefill.config.tenant_id)
        self.assertEqual(
            (0, 1),
            tuple(r.dp_rank for r in config.prefill.instances[0].registrations),
        )
        self.assertEqual(
            "http://prefill-a.test:8100",
            config.prefill.instances[0].http_endpoint,
        )

    def test_json_and_cli_configuration_are_equivalent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "proxy.json"
            config_path.write_text(json.dumps(valid_config_dict()), encoding="utf-8")
            json_args = proxy.parse_args(
                [
                    "--config",
                    str(config_path),
                    "--host",
                    "0.0.0.0",
                    "--port",
                    "9000",
                    "--log-level",
                    "DEBUG",
                ]
            )

        cli_args = proxy.parse_args(valid_cli_args())

        self.assertEqual(json_args.proxy_config, cli_args.proxy_config)
        self.assertEqual("0.0.0.0", json_args.host)
        self.assertEqual(9000, json_args.port)
        self.assertEqual("DEBUG", json_args.log_level)

    def test_cli_mode_preserves_proxy_defaults_and_aliases(self) -> None:
        config = proxy.parse_args(
            [
                "--prefiller-instance-id",
                "prefill-default",
                "--prefill-registration",
                "prefill-default",
                "0",
                "tcp://prefill.test:5557",
                "--conductor-address",
                "http://conductor.test:13333",
                "--modelname",
                "test-model",
                "--block-size",
                "16",
                "--python-hash-seed",
                "0",
            ]
        ).proxy_config

        self.assertEqual(
            "http://localhost:8100", config.prefill.instances[0].http_endpoint
        )
        self.assertEqual(
            "http://localhost:8200", config.decode.instances[0].http_endpoint
        )

        alias_args = [
            {
                "--prefiller-hosts": "--prefiller-host",
                "--prefiller-ports": "--prefiller-port",
                "--decoder-hosts": "--decoder-host",
                "--decoder-ports": "--decoder-port",
            }.get(argument, argument)
            for argument in valid_cli_args()
        ]
        self.assertEqual(
            proxy.parse_config(valid_config_dict()),
            proxy.parse_args(alias_args).proxy_config,
        )

    def test_cli_configuration_rejects_ambiguous_mappings(self) -> None:
        mismatched_ports = valid_cli_args()
        ports_index = mismatched_ports.index("--prefiller-ports")
        mismatched_ports.pop(ports_index + 2)

        mismatched_ids = valid_cli_args()
        ids_index = mismatched_ids.index("--prefiller-instance-ids")
        mismatched_ids.pop(ids_index + 2)

        unknown_registration = valid_cli_args()
        registration_index = unknown_registration.index("--prefiller-registration")
        unknown_registration[registration_index + 1] = "unknown-prefill"

        invalid_rank = valid_cli_args()
        registration_index = invalid_rank.index("--prefiller-registration")
        invalid_rank[registration_index + 2] = "rank-zero"

        cases = (
            (mismatched_ports, "prefiller hosts must match"),
            (mismatched_ids, "instance IDs must match"),
            (unknown_registration, "unknown instance ID"),
            (invalid_rank, "DP_RANK must be an integer"),
        )
        for argv, message in cases:
            with self.subTest(message=message):
                self.assert_cli_error(argv, message)

    def test_configuration_source_is_required_and_modes_cannot_mix(self) -> None:
        self.assert_cli_error([], "CLI configuration requires")

        sample_path = EXAMPLE_DIR / "cache_aware_proxy_config.json"
        for cli_option in (
            ["--prefiller-hosts", "localhost"],
            ["--conductor-address", "http://conductor.test:13333"],
            ["--model", "test-model"],
        ):
            with self.subTest(option=cli_option[0]):
                self.assert_cli_error(
                    ["--config", str(sample_path), *cli_option],
                    "--config cannot be combined",
                )

    def test_missing_sections_and_empty_nested_lists_are_rejected(self) -> None:
        for field in ("conductor", "prefill", "decode"):
            with self.subTest(field=field):
                raw = cloned_config_dict()
                raw.pop(field)
                with self.assertRaises(proxy.ConfigError):
                    proxy.parse_config(raw)

        missing_prefill_config = cloned_config_dict()
        missing_prefill_config["prefill"].pop("config")
        with self.assertRaises(proxy.ConfigError):
            proxy.parse_config(missing_prefill_config)

        for section in ("prefill", "decode"):
            for replacement in (None, []):
                with self.subTest(section=section, replacement=replacement):
                    raw = cloned_config_dict()
                    if replacement is None:
                        raw[section].pop("instances")
                    else:
                        raw[section]["instances"] = replacement
                    with self.assertRaises(proxy.ConfigError):
                        proxy.parse_config(raw)

        empty_registrations = cloned_config_dict()
        empty_registrations["prefill"]["instances"][0]["registrations"] = []
        with self.assertRaises(proxy.ConfigError):
            proxy.parse_config(empty_registrations)

    def test_configuration_domains_and_old_layout_are_rejected(self) -> None:
        misplaced_configs = []
        for field in (
            "modelname",
            "block_size",
            "tenant_id",
            "lora_name",
            "hash_profile",
        ):
            raw = cloned_config_dict()
            raw["conductor"][field] = raw["prefill"]["config"][field]
            misplaced_configs.append((f"{field}_under_conductor", raw))

        for field in (
            "address",
            "query_timeout_seconds",
            "registration_timeout_seconds",
        ):
            raw = cloned_config_dict()
            raw["prefill"]["config"][field] = raw["conductor"][field]
            misplaced_configs.append((f"{field}_under_prefill_config", raw))

        per_instance_config = cloned_config_dict()
        per_instance_config["prefill"]["instances"][0]["config"] = per_instance_config[
            "prefill"
        ]["config"]
        misplaced_configs.append(("config_under_prefill_instance", per_instance_config))

        for name, raw in misplaced_configs:
            with self.subTest(name=name), self.assertRaises(proxy.ConfigError):
                proxy.parse_config(raw)

        old_flat_layout = cloned_config_dict()
        old_prefill = old_flat_layout.pop("prefill")
        old_decode = old_flat_layout.pop("decode")
        old_flat_layout["conductor"].update(old_prefill["config"])
        old_flat_layout["prefill_instances"] = old_prefill["instances"]
        old_flat_layout["decode_instances"] = old_decode["instances"]

        with self.assertRaises(proxy.ConfigError):
            proxy.parse_config(old_flat_layout)

    def test_duplicate_identity_and_endpoint_are_rejected(self) -> None:
        mutations = []

        duplicate_id = cloned_config_dict()
        duplicate_id["prefill"]["instances"][1]["instance_id"] = "prefill-a"
        mutations.append(duplicate_id)

        duplicate_rank = cloned_config_dict()
        duplicate_rank["prefill"]["instances"][0]["registrations"][1]["dp_rank"] = 0
        mutations.append(duplicate_rank)

        duplicate_endpoint = cloned_config_dict()
        duplicate_endpoint["prefill"]["instances"][1]["registrations"][0][
            "endpoint"
        ] = "tcp://prefill-a.test:5557"
        mutations.append(duplicate_endpoint)

        for index, raw in enumerate(mutations):
            with self.subTest(index=index), self.assertRaises(proxy.ConfigError):
                proxy.parse_config(raw)

    def test_invalid_rank_and_block_size_are_rejected(self) -> None:
        cases = []
        for rank in (-1, True, "0", 1 << 31):
            raw = cloned_config_dict()
            raw["prefill"]["instances"][0]["registrations"][0]["dp_rank"] = rank
            cases.append(raw)
        for block_size in (0, -1, True, 16.0):
            raw = cloned_config_dict()
            raw["prefill"]["config"]["block_size"] = block_size
            cases.append(raw)

        for index, raw in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(proxy.ConfigError):
                proxy.parse_config(raw)

    def test_invalid_timeouts_are_rejected(self) -> None:
        for field in ("query_timeout_seconds", "registration_timeout_seconds"):
            for value in (0, -1, True, float("nan"), float("inf")):
                with self.subTest(field=field, value=value):
                    raw = cloned_config_dict()
                    raw["conductor"][field] = value
                    with self.assertRaises(proxy.ConfigError):
                        proxy.parse_config(raw)

    def test_missing_fields_and_invalid_urls_are_rejected(self) -> None:
        cases = []

        missing_event = cloned_config_dict()
        missing_event["prefill"]["instances"][0]["registrations"][0].pop("endpoint")
        cases.append(missing_event)

        path_endpoint = cloned_config_dict()
        path_endpoint["prefill"]["instances"][0]["http_endpoint"] = (
            "http://prefill-a.test:8100/v1"
        )
        cases.append(path_endpoint)

        invalid_conductor = cloned_config_dict()
        invalid_conductor["conductor"]["address"] = "tcp://conductor.test:13333"
        cases.append(invalid_conductor)

        missing_profile = cloned_config_dict()
        missing_profile["prefill"]["config"].pop("hash_profile")
        cases.append(missing_profile)

        for index, raw in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(proxy.ConfigError):
                proxy.parse_config(raw)

    def test_unsupported_hash_profiles_and_seeds_are_rejected(self) -> None:
        cases = []
        for field, value in (
            ("strategy", "other"),
            ("algorithm", "xxh64"),
            ("index_projection", "high64_be"),
            ("python_hash_seed", "-1"),
            ("python_hash_seed", " 0"),
            ("python_hash_seed", "4294967296"),
            ("python_hash_seed", "٠"),
        ):
            raw = cloned_config_dict()
            raw["prefill"]["config"]["hash_profile"][field] = value
            cases.append(raw)

        unknown = cloned_config_dict()
        unknown["prefill"]["config"]["hash_profile"]["root_digest"] = "00"
        cases.append(unknown)

        for index, raw in enumerate(cases):
            with self.subTest(index=index), self.assertRaises(proxy.ConfigError):
                proxy.parse_config(raw)

    def test_load_config_reports_invalid_json(self) -> None:
        invalid = Path(__file__).with_name("invalid-config.json")
        try:
            invalid.write_text("{", encoding="utf-8")
            with self.assertRaises(proxy.ConfigError):
                proxy.load_config(invalid)
        finally:
            invalid.unlink(missing_ok=True)

    def test_sample_config_is_plain_json(self) -> None:
        sample_path = EXAMPLE_DIR / "cache_aware_proxy_config.json"
        parsed = json.loads(sample_path.read_text(encoding="utf-8"))
        self.assertNotIn("replay_endpoint", json.dumps(parsed))


if __name__ == "__main__":
    unittest.main()
