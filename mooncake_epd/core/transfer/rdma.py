"""RDMA capability discovery and protocol resolution.

Mooncake's legacy verbs transport drives RC QPs directly and is suitable for
InfiniBand/RoCE HCAs.  Intel ``irdma`` devices expose iWARP and require the
RDMA connection manager.  Treating both devices as a single boolean caused the
old auto-detection to select a transport that could never establish a QP.

This module keeps those capabilities separate:

* ``mooncake_compatible``: IB/RoCE device usable by Mooncake's verbs backend.
* ``rdmacm_compatible``: active IP-routed RDMA device usable by the staged
  ``rdmacm`` backend.
* ``has_gpudirect``: conservative, opt-in evidence for direct GPU memory
  registration.  Merely loading ``nvidia_peermem`` is not sufficient.
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class RdmaDeviceInfo:
    name: str
    transport: str
    active: bool
    netdev: str = ""
    addresses: tuple[str, ...] = ()
    link_layer: str = ""

    @property
    def is_iwarp(self) -> bool:
        value = self.transport.lower()
        return value == "iwarp" or self.name.lower().startswith("irdma")

    @property
    def is_roce(self) -> bool:
        return self.transport.lower() == "roce"

    @property
    def is_infiniband(self) -> bool:
        return self.transport.lower() == "infiniband"


@dataclass(frozen=True)
class RdmaCapabilities:
    devices: tuple[RdmaDeviceInfo, ...] = ()
    rdmacm_library: str = ""
    nvidia_peermem_loaded: bool = False
    gpudirect_devices: tuple[str, ...] = ()
    issues: tuple[str, ...] = ()

    @property
    def active_devices(self) -> tuple[RdmaDeviceInfo, ...]:
        return tuple(device for device in self.devices if device.active)

    @property
    def has_rdma(self) -> bool:
        return bool(self.active_devices)

    @property
    def mooncake_compatible(self) -> bool:
        return any(
            device.active and (device.is_infiniband or device.is_roce)
            for device in self.devices
        )

    @property
    def rdmacm_compatible(self) -> bool:
        return bool(self.rdmacm_library) and any(
            device.active and bool(device.netdev) and bool(device.addresses)
            for device in self.devices
        )

    @property
    def has_gpudirect(self) -> bool:
        return self.nvidia_peermem_loaded and bool(self.gpudirect_devices)

    @property
    def preferred_device(self) -> RdmaDeviceInfo | None:
        candidates = sorted(
            self.active_devices,
            key=lambda item: (
                not bool(item.addresses),
                item.is_iwarp,
                item.name,
            ),
        )
        return candidates[0] if candidates else None

    def as_dict(self) -> dict:
        return {
            "has_rdma": self.has_rdma,
            "mooncake_compatible": self.mooncake_compatible,
            "rdmacm_compatible": self.rdmacm_compatible,
            "has_gpudirect": self.has_gpudirect,
            "rdmacm_library": self.rdmacm_library,
            "nvidia_peermem_loaded": self.nvidia_peermem_loaded,
            "gpudirect_devices": list(self.gpudirect_devices),
            "devices": [
                {
                    "name": device.name,
                    "transport": device.transport,
                    "active": device.active,
                    "netdev": device.netdev,
                    "addresses": list(device.addresses),
                    "link_layer": device.link_layer,
                }
                for device in self.devices
            ],
            "issues": list(self.issues),
        }


def _read(path: Path) -> str:
    try:
        return path.read_text().strip()
    except OSError:
        return ""


def _run(args: list[str]) -> str:
    try:
        result = subprocess.run(
            args,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout if result.returncode == 0 else ""


def _transport_by_device() -> dict[str, str]:
    output = _run(["ibv_devinfo"])
    mapping: dict[str, str] = {}
    current = ""
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line.startswith("hca_id:"):
            current = line.split(":", 1)[1].strip()
            continue
        if current and line.startswith("transport:"):
            value = line.split(":", 1)[1].strip().split(" ", 1)[0].lower()
            mapping[current] = {
                "iwARP".lower(): "iwarp",
                "InfiniBand".lower(): "infiniband",
            }.get(value, value)
    return mapping


def _netdev_addresses(netdev: str) -> tuple[str, ...]:
    if not netdev:
        return ()
    output = _run(["ip", "-o", "addr", "show", "dev", netdev])
    addresses: list[str] = []
    for line in output.splitlines():
        match = re.search(r"\s(?:inet|inet6)\s+([^\s/]+)", line)
        if not match:
            continue
        address = match.group(1)
        if address.startswith("127.") or address == "::1":
            continue
        addresses.append(address)
    return tuple(addresses)


def _find_rdmacm_library() -> str:
    candidates = [
        "/lib/x86_64-linux-gnu/librdmacm.so.1",
        "/usr/lib/x86_64-linux-gnu/librdmacm.so.1",
    ]
    for candidate in candidates:
        if Path(candidate).exists():
            return candidate
    output = _run(["ldconfig", "-p"])
    for line in output.splitlines():
        if "librdmacm.so.1" in line and "=>" in line:
            path = line.split("=>", 1)[1].strip()
            if Path(path).exists():
                return path
    return ""


def _normalize_transport(name: str, detected: str, link_layer: str) -> str:
    if detected:
        return detected
    lowered = name.lower()
    if lowered.startswith("irdma"):
        return "iwarp"
    if link_layer.lower() == "infiniband":
        return "infiniband"
    # Ethernet verbs devices are normally RoCE unless the driver explicitly
    # identifies itself as iWARP above.
    if link_layer.lower() == "ethernet":
        return "roce"
    return "unknown"


def detect_rdma_capabilities(
    *,
    gpudirect_devices: Iterable[str] | None = None,
) -> RdmaCapabilities:
    """Inspect sysfs and userspace libraries without opening a QP.

    ``MOONCAKE_EPD_GPUDIRECT_DEVICES`` is deliberately explicit.  The project
    no longer infers GPUDirect from "an HCA exists + CUDA exists", which gave a
    false positive on Intel X722 iWARP.
    """

    root = Path("/sys/class/infiniband")
    transports = _transport_by_device()
    devices: list[RdmaDeviceInfo] = []
    issues: list[str] = []
    for device_path in sorted(root.glob("*")) if root.exists() else []:
        port_path = device_path / "ports" / "1"
        state = _read(port_path / "state").upper()
        link_layer = _read(port_path / "link_layer")
        netdevs = sorted((device_path / "device" / "net").glob("*"))
        netdev = netdevs[0].name if netdevs else ""
        transport = _normalize_transport(
            device_path.name,
            transports.get(device_path.name, ""),
            link_layer,
        )
        devices.append(
            RdmaDeviceInfo(
                name=device_path.name,
                transport=transport,
                active="ACTIVE" in state,
                netdev=netdev,
                addresses=_netdev_addresses(netdev),
                link_layer=link_layer,
            )
        )

    configured_gpudirect = tuple(
        item.strip()
        for item in (
            gpudirect_devices
            if gpudirect_devices is not None
            else os.getenv("MOONCAKE_EPD_GPUDIRECT_DEVICES", "").split(",")
        )
        if item.strip()
    )
    active_names = {device.name for device in devices if device.active}
    valid_gpudirect = tuple(
        name for name in configured_gpudirect if name in active_names
    )
    invalid_gpudirect = sorted(set(configured_gpudirect) - active_names)
    if invalid_gpudirect:
        issues.append(
            "Configured GPUDirect devices are not active RDMA devices: "
            + ", ".join(invalid_gpudirect)
        )
    if not devices:
        issues.append("No RDMA devices found under /sys/class/infiniband.")
    elif not any(device.active for device in devices):
        issues.append("No ACTIVE RDMA device found.")
    for device in devices:
        if device.active and not device.addresses:
            issues.append(
                f"Active RDMA device {device.name} has no routable address on "
                f"{device.netdev or '<unknown netdev>'}."
            )

    return RdmaCapabilities(
        devices=tuple(devices),
        rdmacm_library=_find_rdmacm_library(),
        nvidia_peermem_loaded=(
            Path("/sys/module/nvidia_peermem").exists()
            or Path("/sys/module/nv_peer_mem").exists()
        ),
        gpudirect_devices=valid_gpudirect,
        issues=tuple(issues),
    )


def resolve_rdma_protocol(
    requested: str,
    capabilities: RdmaCapabilities,
    *,
    same_host: bool = False,
) -> str:
    """Resolve ``local|tcp|rdma|rdmacm|auto`` to an executable data path."""

    protocol = str(requested or "auto").strip().lower()
    if protocol not in {"local", "tcp", "rdma", "rdmacm", "auto"}:
        raise ValueError(f"unsupported transfer protocol: {requested!r}")
    if protocol == "local" or same_host:
        return "local"
    if protocol == "tcp":
        return "tcp"
    if protocol == "rdmacm":
        if not capabilities.rdmacm_compatible:
            raise RuntimeError("rdmacm requested but no active IP-routed RDMA device is usable")
        return "rdmacm"
    if protocol == "rdma":
        if capabilities.mooncake_compatible:
            return "rdma"
        if capabilities.rdmacm_compatible:
            return "rdmacm"
        raise RuntimeError("RDMA requested but neither Mooncake verbs nor rdmacm is usable")
    if capabilities.mooncake_compatible:
        return "rdma"
    if capabilities.rdmacm_compatible:
        return "rdmacm"
    return "tcp"


def default_rdma_bind_address(capabilities: RdmaCapabilities) -> str:
    preferred = capabilities.preferred_device
    if preferred is None or not preferred.addresses:
        return ""
    ipv4 = [address for address in preferred.addresses if ":" not in address]
    return ipv4[0] if ipv4 else preferred.addresses[0]
