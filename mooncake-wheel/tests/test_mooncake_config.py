import json
import os
import tempfile
import unittest

from mooncake.mooncake_config import (
    MooncakeConfig,
    DEFAULT_GLOBAL_SEGMENT_SIZE,
    DEFAULT_LOCAL_BUFFER_SIZE,
    _parse_segment_size,
)


class TestMooncakeConfig(unittest.TestCase):
    def setUp(self):
        # Create temporary directory
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_file = os.path.join(self.temp_dir.name, "config.json")

        # Valid configuration
        self.valid_config = {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "master_server_address": "localhost:8081",
            "global_segment_size": 3355443200,
            "local_buffer_size": 1073741824,
            "protocol": "tcp",
            "device_name": "eth0",
            "enable_ssd_offload": True,
            "ssd_offload_path": "/nvme/mooncake_offload",
            "tenant_id": "tenant-a"
        }

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_config(self, config_data):
        """Write configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(config_data, f)

    def test_load_valid_config(self):
        """Test loading valid configuration"""
        self.write_config(self.valid_config)
        config = MooncakeConfig.from_file(self.config_file)

        self.assertEqual(config.local_hostname, "localhost")
        self.assertEqual(config.metadata_server, "localhost:8080")
        self.assertEqual(config.master_server_address, "localhost:8081")
        self.assertEqual(config.global_segment_size, 3355443200)
        self.assertEqual(config.local_buffer_size, 1073741824)
        self.assertEqual(config.protocol, "tcp")
        self.assertEqual(config.device_name, "eth0")
        self.assertEqual(config.enable_ssd_offload, True)
        self.assertEqual(config.ssd_offload_path, "/nvme/mooncake_offload")
        self.assertEqual(config.tenant_id, "tenant-a")

    def test_load_with_default_values(self):
        """Test loading configuration with default values"""
        minimal_config = {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "master_server_address": "localhost:8081"
        }
        self.write_config(minimal_config)
        config = MooncakeConfig.from_file(self.config_file)

        self.assertEqual(config.global_segment_size, DEFAULT_GLOBAL_SEGMENT_SIZE)
        self.assertEqual(config.local_buffer_size, DEFAULT_LOCAL_BUFFER_SIZE)
        self.assertEqual(config.protocol, "tcp")
        self.assertEqual(config.device_name, "")
        self.assertEqual(config.enable_ssd_offload, False)
        self.assertEqual(config.ssd_offload_path, "")
        self.assertEqual(config.tenant_id, "default")

    def test_load_tenant_id_from_file(self):
        """Test loading tenant_id from configuration file"""
        self.write_config({**self.valid_config, "tenant_id": "tenant-from-file"})
        config = MooncakeConfig.from_file(self.config_file)

        self.assertEqual(config.tenant_id, "tenant-from-file")

    def test_tenant_id_defaults(self):
        """Test tenant_id defaults to default when omitted"""
        minimal_config = {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "master_server_address": "localhost:8081"
        }
        self.write_config(minimal_config)
        config = MooncakeConfig.from_file(self.config_file)

        self.assertEqual(config.tenant_id, "default")

    def test_tenant_id_null_defaults(self):
        """Test tenant_id defaults to default when explicitly null"""
        self.write_config({**self.valid_config, "tenant_id": None})
        config = MooncakeConfig.from_file(self.config_file)

        self.assertEqual(config.tenant_id, "default")

    def test_ssd_offload_path_null_defaults(self):
        """Test ssd_offload_path defaults to empty when explicitly null"""
        self.write_config({**self.valid_config, "ssd_offload_path": None})
        config = MooncakeConfig.from_file(self.config_file)

        self.assertEqual(config.ssd_offload_path, "")

    def test_enable_ssd_offload_string_values(self):
        """from_file must parse string booleans like load_from_env, and reject typos.

        from_file used bool(value), so any non-empty string (including "false")
        turned SSD offload on, disagreeing with load_from_env which parses the
        string. _parse_bool now understands the common textual forms and raises
        on anything unrecognized instead of silently defaulting to False.
        """
        required = {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "master_server_address": "localhost:8081",
        }
        cases = [
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("off", False),
            ("true", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            (True, True),
            (False, False),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.write_config({**required, "enable_ssd_offload": raw})
                config = MooncakeConfig.from_file(self.config_file)
                self.assertEqual(config.enable_ssd_offload, expected)

        # An unrecognized string is a config error, not a silent disable.
        self.write_config({**required, "enable_ssd_offload": "notabool"})
        with self.assertRaises(ValueError):
            MooncakeConfig.from_file(self.config_file)

    def test_missing_required_field(self):
        """Test missing required field"""
        for field in ["local_hostname", "metadata_server", "master_server_address"]:
            with self.subTest(field=field):
                invalid_config = self.valid_config.copy()
                invalid_config.pop(field)
                self.write_config(invalid_config)

                with self.assertRaises(ValueError) as cm:
                    MooncakeConfig.from_file(self.config_file)
                self.assertIn(f"Missing required config field: {field}", str(cm.exception))

    def test_load_from_config_path_env(self):
        """Test loading configuration from environment variable MOONCAKE_CONFIG_PATH"""
        self.write_config(self.valid_config)

        # Set environment variable
        os.environ['MOONCAKE_CONFIG_PATH'] = self.config_file

        try:
            config = MooncakeConfig.load_from_env()
            self.assertEqual(config.local_hostname, "localhost")
        finally:
            # Clean up environment variable
            del os.environ['MOONCAKE_CONFIG_PATH']

    def test_load_from_config_env(self):
        """Test loading configuration from environment variable MOONCAKE_MASTER"""
        # Set environment variable
        os.environ['MOONCAKE_MASTER'] = self.valid_config["master_server_address"]
        os.environ['LOCAL_HOSTNAME'] = self.valid_config["local_hostname"]
        os.environ['MOONCAKE_TE_META_DATA_SERVER'] = self.valid_config["metadata_server"]
        os.environ['MOONCAKE_GLOBAL_SEGMENT_SIZE'] = str(self.valid_config["global_segment_size"])
        os.environ['MOONCAKE_PROTOCOL'] = self.valid_config["protocol"]
        os.environ['MOONCAKE_DEVICE'] = self.valid_config["device_name"]
        os.environ['MOONCAKE_OFFLOAD_ENABLED'] = str(self.valid_config["enable_ssd_offload"])
        os.environ['MOONCAKE_OFFLOAD_FILE_STORAGE_PATH'] = self.valid_config["ssd_offload_path"]
        os.environ['MOONCAKE_TENANT_ID'] = self.valid_config["tenant_id"]

        try:
            config = MooncakeConfig.load_from_env()
            self.assertEqual(config.master_server_address, self.valid_config["master_server_address"])
            self.assertEqual(config.metadata_server, self.valid_config["metadata_server"])
            self.assertEqual(config.local_hostname, self.valid_config["local_hostname"])
            self.assertEqual(config.global_segment_size, self.valid_config["global_segment_size"])
            self.assertEqual(config.protocol, self.valid_config["protocol"])
            self.assertEqual(config.device_name, self.valid_config["device_name"])
            self.assertEqual(config.enable_ssd_offload, self.valid_config["enable_ssd_offload"])
            self.assertEqual(config.ssd_offload_path, self.valid_config["ssd_offload_path"])
            self.assertEqual(config.tenant_id, self.valid_config["tenant_id"])

        finally:
            # Clean up environment variable
            del os.environ['MOONCAKE_MASTER']
            del os.environ['LOCAL_HOSTNAME']
            del os.environ['MOONCAKE_TE_META_DATA_SERVER']
            del os.environ['MOONCAKE_GLOBAL_SEGMENT_SIZE']
            del os.environ['MOONCAKE_PROTOCOL']
            del os.environ['MOONCAKE_DEVICE']
            del os.environ['MOONCAKE_OFFLOAD_ENABLED']
            del os.environ['MOONCAKE_OFFLOAD_FILE_STORAGE_PATH']
            del os.environ['MOONCAKE_TENANT_ID']

    def test_tenant_id_from_env(self):
        """Test loading tenant_id from MOONCAKE_TENANT_ID"""
        previous_config_path = os.environ.pop("MOONCAKE_CONFIG_PATH", None)
        previous_master = os.environ.pop("MOONCAKE_MASTER", None)
        previous_tenant_id = os.environ.pop("MOONCAKE_TENANT_ID", None)

        os.environ["MOONCAKE_MASTER"] = self.valid_config["master_server_address"]
        os.environ["MOONCAKE_TENANT_ID"] = "tenant-from-env"

        try:
            config = MooncakeConfig.load_from_env()
            self.assertEqual(config.tenant_id, "tenant-from-env")
        finally:
            os.environ.pop("MOONCAKE_MASTER", None)
            os.environ.pop("MOONCAKE_TENANT_ID", None)
            if previous_config_path is not None:
                os.environ["MOONCAKE_CONFIG_PATH"] = previous_config_path
            if previous_master is not None:
                os.environ["MOONCAKE_MASTER"] = previous_master
            if previous_tenant_id is not None:
                os.environ["MOONCAKE_TENANT_ID"] = previous_tenant_id

    def test_tenant_id_env_defaults(self):
        """Test tenant_id defaults to default when MOONCAKE_TENANT_ID is omitted"""
        previous_config_path = os.environ.pop("MOONCAKE_CONFIG_PATH", None)
        previous_master = os.environ.pop("MOONCAKE_MASTER", None)
        previous_tenant_id = os.environ.pop("MOONCAKE_TENANT_ID", None)

        os.environ["MOONCAKE_MASTER"] = self.valid_config["master_server_address"]

        try:
            config = MooncakeConfig.load_from_env()
            self.assertEqual(config.tenant_id, "default")
        finally:
            os.environ.pop("MOONCAKE_MASTER", None)
            os.environ.pop("MOONCAKE_TENANT_ID", None)
            if previous_config_path is not None:
                os.environ["MOONCAKE_CONFIG_PATH"] = previous_config_path
            if previous_master is not None:
                os.environ["MOONCAKE_MASTER"] = previous_master
            if previous_tenant_id is not None:
                os.environ["MOONCAKE_TENANT_ID"] = previous_tenant_id

    def test_load_from_env_missing(self):
        """Test loading configuration from environment variable when not set"""
        with self.assertRaises(ValueError) as cm:
            MooncakeConfig.load_from_env()
        self.assertIn("Neither the environment variable 'MOONCAKE_CONFIG_PATH' nor 'MOONCAKE_MASTER' is set.", str(cm.exception))


class TestParseSegmentSize(unittest.TestCase):
    def test_integer_passthrough(self):
        self.assertEqual(_parse_segment_size(1024), 1024)
        self.assertEqual(_parse_segment_size(0), 0)

    def test_float_passthrough(self):
        self.assertEqual(_parse_segment_size(1.5), 1)

    def test_bytes_string(self):
        self.assertEqual(_parse_segment_size("1024"), 1024)
        self.assertEqual(_parse_segment_size("  2048  "), 2048)

    def test_kb_suffix(self):
        self.assertEqual(_parse_segment_size("1kb"), 1024)
        self.assertEqual(_parse_segment_size("1KB"), 1024)
        self.assertEqual(_parse_segment_size("512k"), 512 * 1024)
        self.assertEqual(_parse_segment_size("1.5kb"), int(1.5 * 1024))

    def test_mb_suffix(self):
        self.assertEqual(_parse_segment_size("1mb"), 1024 ** 2)
        self.assertEqual(_parse_segment_size("512MB"), 512 * 1024 ** 2)
        self.assertEqual(_parse_segment_size("1m"), 1024 ** 2)

    def test_gb_suffix(self):
        self.assertEqual(_parse_segment_size("1gb"), 1024 ** 3)
        self.assertEqual(_parse_segment_size("3GB"), 3 * 1024 ** 3)
        self.assertEqual(_parse_segment_size("1g"), 1024 ** 3)
        self.assertEqual(_parse_segment_size("1.5gb"), int(1.5 * 1024 ** 3))

    def test_tb_suffix(self):
        self.assertEqual(_parse_segment_size("1tb"), 1024 ** 4)
        self.assertEqual(_parse_segment_size("1TB"), 1024 ** 4)
        self.assertEqual(_parse_segment_size("1t"), 1024 ** 4)

    def test_b_suffix(self):
        self.assertEqual(_parse_segment_size("4096b"), 4096)
        self.assertEqual(_parse_segment_size("4096B"), 4096)

    def test_empty_string_raises(self):
        with self.assertRaises(ValueError):
            _parse_segment_size("")
        with self.assertRaises(ValueError):
            _parse_segment_size("   ")

    def test_missing_number_raises(self):
        with self.assertRaises(ValueError):
            _parse_segment_size("gb")
        with self.assertRaises(ValueError):
            _parse_segment_size("mb")

    def test_bare_float_string(self):
        self.assertEqual(_parse_segment_size("1.5"), 1)
        self.assertEqual(_parse_segment_size("1e9"), 1000000000)

    def test_invalid_string_raises(self):
        with self.assertRaises(ValueError):
            _parse_segment_size("abc")

    def test_whitespace_handling(self):
        self.assertEqual(_parse_segment_size("  3 gb  "), 3 * 1024 ** 3)


if __name__ == '__main__':
    unittest.main()
