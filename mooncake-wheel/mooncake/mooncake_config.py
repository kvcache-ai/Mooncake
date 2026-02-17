#!/usr/bin/env python3
"""
Mooncake Configuration

Supported Protocols
===================

Mooncake Transfer Engine supports multiple communication protocols for data transfer.
The protocol can be configured via the 'protocol' field in the configuration file or
via the MOONCAKE_PROTOCOL environment variable.

Python API Level (Commonly Used):
---------------------------------
- tcp: TCP/IP protocol for network communication. Works in all environments,
       no special hardware required. Default protocol.
- rdma: Remote Direct Memory Access protocol for high-performance, low-latency
        data transfer. Requires RDMA-capable NICs (InfiniBand, RoCE, etc.).
        Supports GPUDirect RDMA for zero-copy GPU memory transfers.

Transfer Engine C++ Level (Advanced):
-------------------------------------
In addition to tcp and rdma, the C++ Transfer Engine also supports:
- nvmeof: NVMe over Fabric for direct NVMe storage access
- nvlink: NVIDIA NVLink for inter-GPU communication across nodes
- nvlink_intra: NVIDIA NVLink for intra-node GPU communication
- hip: ROCm/HIP for AMD GPU communication using IPC/Shareable handles
- barex: Bare-metal RDMA extension protocol
- cxl: Compute Express Link for memory pooling and sharing
- ascend: Huawei Ascend NPU communication (HCCL and direct transport)

For most use cases, 'tcp' or 'rdma' is recommended. The default is 'tcp'.
For RDMA, you also need to specify the device_name (e.g., 'mlx5_0', 'erdma_0')
or use auto-discovery.

Examples:
---------
# Using TCP (default, no device_name needed)
export MOONCAKE_PROTOCOL="tcp"

# Using RDMA with specific device
export MOONCAKE_PROTOCOL="rdma"
export MOONCAKE_DEVICE="mlx5_0"

# Using RDMA with auto-discovery
export MOONCAKE_PROTOCOL="rdma"
export MOONCAKE_DEVICE="auto-discovery"
"""
import json
import os
from dataclasses import dataclass
from typing import Optional
import mooncake

DEFAULT_GLOBAL_SEGMENT_SIZE = 3355443200  # 3.125 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 1073741824  # 1.0 GiB

def _validate_mooncake_version():
    expected_version = os.getenv("MOONCAKE_EXPECTED_VERSION", mooncake.__version__)
    
    if not mooncake.check_version_compatibility(mooncake.__version__, expected_version):
        raise mooncake.VersionMismatchError(
            f"Mooncake version mismatch: "
            f"client version={mooncake.__version__}, expected version={expected_version}. "
            f"This may cause invalid RPC arguments. Please ensure client and server versions match."
        )

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
        protocol (str): The communication protocol to use. Supported values:
            - "tcp" (default): Standard TCP/IP protocol
            - "rdma": RDMA protocol (requires RDMA-capable NICs and device_name)
            See module docstring for full list of supported protocols.
        device_name (Optional[str]): The name of the RDMA device to use
            (e.g., "mlx5_0", "erdma_0", or "auto-discovery").
            Required when protocol is "rdma", optional for other protocols.
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
        
        For RDMA:
        {
            "local_hostname": "node1",
            "metadata_server": "master:8080",
            "global_segment_size": 3355443200,
            "local_buffer_size": 1073741824,
            "protocol": "rdma",
            "device_name": "mlx5_0",
            "master_server_address": "master:8081"
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
        _validate_mooncake_version()
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
        _validate_mooncake_version()
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