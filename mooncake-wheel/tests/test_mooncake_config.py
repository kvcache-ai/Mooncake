import json
import os
import tempfile
import unittest

from mooncake.mooncake_config import MooncakeConfig, DEFAULT_GLOBAL_SEGMENT_SIZE, DEFAULT_LOCAL_BUFFER_SIZE

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
            "device_name": "eth0"
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

        try:
            config = MooncakeConfig.load_from_env()
            self.assertEqual(config.master_server_address, self.valid_config["master_server_address"])
            self.assertEqual(config.metadata_server, self.valid_config["metadata_server"])
            self.assertEqual(config.local_hostname, self.valid_config["local_hostname"])
            self.assertEqual(config.global_segment_size, self.valid_config["global_segment_size"])
            self.assertEqual(config.protocol, self.valid_config["protocol"])
            self.assertEqual(config.device_name, self.valid_config["device_name"])

        finally:
            # Clean up environment variable
            del os.environ['MOONCAKE_MASTER']
            del os.environ['LOCAL_HOSTNAME']
            del os.environ['MOONCAKE_TE_META_DATA_SERVER']
            del os.environ['MOONCAKE_GLOBAL_SEGMENT_SIZE']
            del os.environ['MOONCAKE_PROTOCOL']
            del os.environ['MOONCAKE_DEVICE']

    def test_load_from_env_missing(self):
        """Test loading configuration from environment variable when not set"""
        with self.assertRaises(ValueError) as cm:
            MooncakeConfig.load_from_env()
        self.assertIn("Neither the environment variable 'MOONCAKE_CONFIG_PATH' nor 'MOONCAKE_MASTER' is set.", str(cm.exception))

if __name__ == '__main__':
    unittest.main()
