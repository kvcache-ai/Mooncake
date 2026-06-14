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

DEFAULT_GLOBAL_SEGMENT_SIZE = 3355443200  # 3.125 GiB
DEFAULT_LOCAL_BUFFER_SIZE = 1073741824  # 1.0 GiB

_SIZE_SUFFIXES = [
    ("kb", 1024),
    ("mb", 1024 ** 2),
    ("gb", 1024 ** 3),
    ("tb", 1024 ** 4),
    ("k", 1024),
    ("m", 1024 ** 2),
    ("g", 1024 ** 3),
    ("t", 1024 ** 4),
    ("b", 1),
]

# All protocols accepted by the Transfer Engine. The Python API commonly uses
# "tcp" and "rdma"; the remaining values are handled at the C++ level. Keep this
# in sync with the protocol list documented in the module docstring above.
_VALID_PROTOCOLS = frozenset({
    "tcp",
    "rdma",
    "nvmeof",
    "nvlink",
    "nvlink_intra",
    "hip",
    "barex",
    "cxl",
    "ascend",
})

# Required fields that must be present AND non-empty.
_REQUIRED_NON_EMPTY_FIELDS = (
    "local_hostname",
    "metadata_server",
    "master_server_address",
)


def _parse_segment_size(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            raise ValueError("Invalid segment size: empty string")
        for suffix, multiplier in _SIZE_SUFFIXES:
            if s.endswith(suffix):
                num = s[: -len(suffix)].strip()
                if not num:
                    raise ValueError(
                        f"Invalid segment size: missing number before '{suffix}'"
                    )
                return int(float(num) * multiplier)
        return int(float(s))
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
        enable_ssd_offload (bool): Enable SSD offload. Default is False.
        ssd_offload_path (str): The path to the SSD directory for offloading.

    Example of configuration file:
        {
            "local_hostname": "localhost",
            "metadata_server": "localhost:8080",
            "global_segment_size": 3355443200,
            "local_buffer_size": 1073741824,
            "protocol": "tcp",
            "device_name": "",
            "master_server_address": "localhost:8081",
            "enable_ssd_offload": true,
            "ssd_offload_path": "/nvme/mooncake_offload"
        }
        
        For RDMA:
        {
            "local_hostname": "node1",
            "metadata_server": "master:8080",
            "global_segment_size": 3355443200,
            "local_buffer_size": 1073741824,
            "protocol": "rdma",
            "device_name": "mlx5_0",
            "master_server_address": "master:8081",
            "enable_ssd_offload": true,
            "ssd_offload_path": "/nvme/mooncake_offload"
        }
    """
    local_hostname: str
    metadata_server: str
    global_segment_size: int
    local_buffer_size: int
    protocol: str
    device_name: Optional[str]
    master_server_address: str
    enable_ssd_offload: bool = False
    ssd_offload_path: str = ""

    def __post_init__(self):
        """Validate configuration invariants, failing fast with clear errors.

        This runs for every ``MooncakeConfig`` instance regardless of how it is
        constructed (``from_file``, ``load_from_env`` or direct instantiation),
        so a misconfiguration is reported here with an actionable message
        instead of surfacing as a cryptic failure deep inside the C++ engine.
        """
        if (
            not isinstance(self.protocol, str)
            or self.protocol.lower() not in _VALID_PROTOCOLS
        ):
            raise ValueError(
                f"Invalid protocol: {self.protocol!r}. Supported protocols are: "
                f"{', '.join(sorted(_VALID_PROTOCOLS))}."
            )

        for field_name in ("global_segment_size", "local_buffer_size"):
            value = getattr(self, field_name)
            if value < 0:
                raise ValueError(
                    f"Invalid {field_name}: {value}. Size must be non-negative."
                )

        for field_name in _REQUIRED_NON_EMPTY_FIELDS:
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"Config field {field_name!r} must be a non-empty string, "
                    f"got {value!r}."
                )

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
            enable_ssd_offload=bool(config.get("enable_ssd_offload", False)),
            ssd_offload_path=str(config.get("ssd_offload_path", "")),
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
                enable_ssd_offload=os.getenv("MOONCAKE_OFFLOAD_ENABLED", "false").lower() in ("true", "1"),
                ssd_offload_path=os.getenv("MOONCAKE_OFFLOAD_FILE_STORAGE_PATH", ""),
            )
        return MooncakeConfig.from_file(config_file_path)