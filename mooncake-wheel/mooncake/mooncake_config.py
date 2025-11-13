#!/usr/bin/env python3
"""
Mooncake Configuration
"""
import json
import os
from dataclasses import dataclass
from typing import Optional

DEFAULT_GLOBAL_SEGMENT_SIZE = 3355443200  # 3.125 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 1073741824  # 1.0 GiB

def _parse_segment_size(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if s.endswith("gb"):
            num = s[:-2].strip()
            if not num:
                raise ValueError(
                    "Invalid segment size: missing number before 'gb'"
                )
            return int(num) * 1024 * 1024 * 1024
        return int(s)
    return int(value)

@dataclass
class MooncakeConfig:
    """The configuration class for Mooncake.

    Attributes:
        local_hostname (str): The hostname of the local machine.
        metadata_server (str): The address of the metadata server.
        global_segment_size (int): The size of each global segment in bytes.
        local_buffer_size (int): The size of the local buffer in bytes.
        protocol (str): The communication protocol to use.
        device_name (Optional[str]): The name of the device to use.
        master_server_address (str): The address of the master server.

    Example of configuration file:
        {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "global_segment_size": 3355443200,
            "local_buffer_size": 1073741824,
            "protocol": "tcp",
            "device_name": "",
            "master_server_address": "localhost:8081"
        }
    """
    local_hostname: str
    metadata_server: str
    global_segment_size: int
    local_buffer_size: int
    protocol: str
    device_name: Optional[str]
    master_server_address: str

    @staticmethod
    def from_file(file_path: str) -> 'MooncakeConfig':
        """Load the config from a JSON file."""
        with open(file_path) as fin:
            config = json.load(fin)
        required_fields = [
            "local_hostname",
            "metadata_server",
            "master_server_address",
        ]
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required config field: {field}")
        return MooncakeConfig(
            local_hostname=config.get("local_hostname"),
            metadata_server=config.get("metadata_server"),
            global_segment_size=_parse_segment_size(
                config.get("global_segment_size", DEFAULT_GLOBAL_SEGMENT_SIZE)
            ),
            local_buffer_size=_parse_segment_size(
                config.get("local_buffer_size", DEFAULT_LOCAL_BUFFER_SIZE)
            ),
            protocol=config.get("protocol", "tcp"),
            device_name=config.get("device_name", ""),
            master_server_address=config.get("master_server_address"),
        )

    @staticmethod
    def load_from_env() -> 'MooncakeConfig':
        """Load config from a file specified in the environment variable.
        export MOONCAKE_MASTER=10.13.3.232:50051
        export MOONCAKE_PROTOCOL="rdma"
        export MOONCAKE_DEVICE=""
        export MOONCAKE_TE_META_DATA_SERVER="P2PHANDSHAKE"
        """
        config_file_path = os.getenv('MOONCAKE_CONFIG_PATH')
        if config_file_path is None:
            if not os.getenv("MOONCAKE_MASTER"):
                raise ValueError("Neither the environment variable 'MOONCAKE_CONFIG_PATH' nor 'MOONCAKE_MASTER' is set.")
            return MooncakeConfig(
                local_hostname=os.getenv("MOONCAKE_LOCAL_HOSTNAME", "localhost"),
                metadata_server=os.getenv("MOONCAKE_TE_META_DATA_SERVER", "P2PHANDSHAKE"),
                global_segment_size=_parse_segment_size(
                    os.getenv("MOONCAKE_GLOBAL_SEGMENT_SIZE", DEFAULT_GLOBAL_SEGMENT_SIZE)
                ),
                local_buffer_size=_parse_segment_size(
                    os.getenv("MOONCAKE_LOCAL_BUFFER_SIZE", DEFAULT_LOCAL_BUFFER_SIZE)
                ),
                protocol=os.getenv("MOONCAKE_PROTOCOL", "tcp"),
                device_name=os.getenv("MOONCAKE_DEVICE", ""),
                master_server_address=os.getenv("MOONCAKE_MASTER"),
            )
        return MooncakeConfig.from_file(config_file_path)