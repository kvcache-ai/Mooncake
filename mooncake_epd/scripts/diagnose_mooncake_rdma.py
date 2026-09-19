#!/usr/bin/env python3
"""Diagnose Mooncake RDMA/GPUDirect readiness without requiring root.

The Omni stage-transfer E2E can only use true RDMA peer-buffer writes when
three layers are simultaneously healthy:

1. an active RDMA HCA and routable RDMA netdev/GID,
2. a Mooncake RDMA QP/data-plane smoke transfer,
3. CUDA memory registration for GPUDirect RDMA.

This script records all three as JSON so failures are actionable instead of
surfacing later as opaque ``register_memory rc=-202`` or QP ``EINVAL`` errors.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import traceback
from pathlib import Path
from typing import Any, Dict, List


def _run(cmd: List[str]) -> Dict[str, Any]:
    try:
        proc = subprocess.run(cmd, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        return {
            "cmd": cmd,
            "returncode": proc.returncode,
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
        }
    except Exception as exc:  # pragma: no cover - diagnostic best effort
        return {"cmd": cmd, "error": f"{type(exc).__name__}: {exc}"}


def _read(path: str) -> str:
    try:
        return Path(path).read_text().strip()
    except Exception:
        return ""


def _module_present(name: str) -> bool:
    return Path(f"/sys/module/{name}").exists()


def _ib_devices() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    root = Path("/sys/class/infiniband")
    for dev in sorted(root.glob("*")) if root.exists() else []:
        device_root = dev / "device"
        counter_dir = dev / "ports" / "1" / "hw_counters"
        counter_names = sorted(p.name for p in counter_dir.glob("*")) if counter_dir.exists() else []
        ports: List[Dict[str, Any]] = []
        for port in sorted((dev / "ports").glob("*")):
            gids = []
            gid_dir = port / "gids"
            for gid_path in sorted(gid_dir.glob("*")) if gid_dir.exists() else []:
                idx = gid_path.name
                gids.append(
                    {
                        "index": idx,
                        "gid": _read(str(gid_path)),
                        "type": _read(str(port / "gid_attrs" / "types" / idx)),
                        "ndev": _read(str(port / "gid_attrs" / "ndevs" / idx)),
                    }
                )
            ports.append(
                {
                    "port": port.name,
                    "state": _read(str(port / "state")),
                    "link_layer": _read(str(port / "link_layer")),
                    "gids": gids,
                }
            )
        out.append(
            {
                "name": dev.name,
                "node_type": _read(str(dev / "node_type")),
                "fw_ver": _read(str(dev / "fw_ver")),
                "pci_vendor": _read(str(device_root / "vendor")),
                "pci_device": _read(str(device_root / "device")),
                "driver": Path(os.path.realpath(str(device_root / "driver"))).name
                if (device_root / "driver").exists()
                else "",
                "has_iwarp_counters": any(name.lower().startswith("iw") for name in counter_names),
                "ports": ports,
            }
        )
    return out


def _default_ip() -> str:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        return socket.gethostbyname(socket.gethostname())


def _default_rdma_ip() -> str:
    try:
        from mooncake_epd.core.transfer import (
            default_rdma_bind_address,
            detect_rdma_capabilities,
        )

        return default_rdma_bind_address(detect_rdma_capabilities()) or _default_ip()
    except Exception:
        return _default_ip()


def _smoke_mooncake(args: argparse.Namespace) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "enabled": not args.no_smoke,
        "managed_buffer_rdma": None,
        "cuda_register": None,
    }
    if args.no_smoke:
        return result

    try:
        from mooncake_epd.core.transfer import (
            TransferEngine,
            detect_rdma_capabilities,
        )

        capabilities = detect_rdma_capabilities()
        if not capabilities.mooncake_compatible:
            result["managed_buffer_rdma"] = {
                "ok": False,
                "skipped": True,
                "reason": (
                    "No active IB/RoCE HCA for Mooncake legacy verbs; "
                    "use the rdmacm staged smoke for iWARP."
                ),
            }
            result["cuda_register"] = {
                "ok": False,
                "skipped": True,
                "reason": "GPUDirect is not validated for the selected RDMA device.",
            }
            return result

        engine = TransferEngine(
            protocol="rdma",
            local_hostname=args.local_hostname,
            metadata_server=args.metadata_server,
            device_name=args.device_name,
        )
        engine.initialize()
        rpc_port = int(engine._mooncake.get_rpc_port())  # noqa: SLF001 - diagnostic
        remote_session = f"{args.local_hostname}:{rpc_port}"
        a = engine.allocate_peer_buffer(args.bytes)
        b = engine.allocate_peer_buffer(args.bytes)
        try:
            engine.write_peer_buffer(a, b"x" * args.bytes)
            plan = engine.build_pointer_transfer_plan(
                remote_session=remote_session,
                local_pointers=[a.pointer],
                remote_pointers=[b.pointer],
                lengths=[args.bytes],
                registered=True,
            )
            transfer_result = engine.transfer_peer_buffer_plan(plan)
            raw = engine.read_peer_buffer(b, min(16, args.bytes))
            result["managed_buffer_rdma"] = {
                "ok": raw == b"x" * min(16, args.bytes),
                "remote_session": remote_session,
                "nbytes": transfer_result.nbytes,
                "descriptor_count": transfer_result.descriptor_count,
            }
        finally:
            try:
                engine.free_peer_buffer(a)
                engine.free_peer_buffer(b)
            except Exception:
                pass

        try:
            import torch

            if torch.cuda.is_available():
                tensor = torch.empty(args.bytes, dtype=torch.uint8, device=args.cuda_device)
                handle = engine.register_tensor_memory(tensor)
                engine.unregister_tensor_memory(handle)
                result["cuda_register"] = {
                    "ok": True,
                    "device": str(tensor.device),
                    "nbytes": args.bytes,
                }
            else:
                result["cuda_register"] = {"ok": False, "reason": "torch.cuda.is_available() is false"}
        except Exception as exc:
            result["cuda_register"] = {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(limit=6),
            }
        finally:
            engine.shutdown()
    except Exception as exc:
        result["managed_buffer_rdma"] = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(limit=8),
        }
    return result


def _smoke_rdmacm(args: argparse.Namespace) -> Dict[str, Any]:
    if args.no_smoke:
        return {"enabled": False}
    try:
        from mooncake_epd.core.transfer import (
            detect_rdma_capabilities,
            rdmacm_cuda_staging_smoke,
            rdmacm_listener_smoke,
        )

        capabilities = detect_rdma_capabilities()
        result = rdmacm_listener_smoke(
            bind_address=args.local_hostname,
            port=args.rdmacm_port,
            capabilities=capabilities,
        )
        result["enabled"] = True
        result["cuda_staging"] = rdmacm_cuda_staging_smoke(
            cuda_device=args.cuda_device,
            size_bytes=args.bytes,
        )
        result["note"] = (
            "Listener creation exercises rdma_cm on the local RNIC. "
            "A byte-transfer smoke additionally requires a second RDMA host; "
            "iWARP cannot route a connection back into the same local RNIC."
        )
        return result
    except Exception as exc:
        return {
            "enabled": True,
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(limit=8),
        }


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--device-name", default="irdma0")
    ap.add_argument(
        "--local-hostname",
        default=os.getenv("MOONCAKE_RDMA_HOSTNAME") or _default_rdma_ip(),
    )
    ap.add_argument("--metadata-server", default="P2PHANDSHAKE")
    ap.add_argument("--cuda-device", default="cuda:0")
    ap.add_argument("--bytes", type=int, default=4096)
    ap.add_argument("--rdmacm-port", type=int, default=47991)
    ap.add_argument("--no-smoke", action="store_true")
    args = ap.parse_args()

    from mooncake_epd.core.transfer import detect_rdma_capabilities

    capability_report = detect_rdma_capabilities()

    diagnostics: Dict[str, Any] = {
        "capabilities": capability_report.as_dict(),
        "env": {
            key: os.getenv(key)
            for key in [
                "WITH_NVIDIA_PEERMEM",
                "MC_GID_INDEX",
                "MC_RDMA_BIND_ADDRESS",
                "MC_TE_FILTERS",
                "MC_NUM_QP_PER_EP",
                "MC_MTU",
            ]
        },
        "modules": {
            "nvidia_peermem": _module_present("nvidia_peermem"),
            "nv_peer_mem": _module_present("nv_peer_mem"),
            "irdma": _module_present("irdma"),
            "ib_uverbs": _module_present("ib_uverbs"),
        },
        "nvidia_peermem_parameters": {
            "peerdirect_support": _read("/sys/module/nvidia_peermem/parameters/peerdirect_support"),
            "persistent_api_support": _read("/sys/module/nvidia_peermem/parameters/persistent_api_support"),
        },
        "infiniband": _ib_devices(),
        "ip_br_addr": _run(["ip", "-br", "addr"]),
        "rdma_link": _run(["rdma", "link"]),
        "ibv_devinfo": _run(["ibv_devinfo"]),
        "smoke": {
            **_smoke_mooncake(args),
            "rdmacm_listener": _smoke_rdmacm(args),
        },
    }

    issues: List[str] = []
    if not diagnostics["modules"]["nvidia_peermem"]:
        issues.append("nvidia_peermem is not loaded; CUDA memory registration for GPUDirect RDMA will fail.")
    active = [
        (dev["name"], port, gid)
        for dev in diagnostics["infiniband"]
        for port in dev["ports"]
        if "ACTIVE" in port.get("state", "")
        for gid in port.get("gids", [])
    ]
    if not active:
        issues.append("No ACTIVE RDMA port found.")
    if (
        active
        and not any(gid.get("ndev") for _, _, gid in active)
        and not any(
            device.active and device.netdev and device.addresses
            for device in capability_report.devices
        )
    ):
        issues.append(
            "Active RDMA GIDs have no associated netdev; configure an IP address on the RDMA netdev "
            "or set a valid MC_RDMA_BIND_ADDRESS/GID index."
        )
    intel_irdma = any(
        dev.get("name", "").startswith("irdma")
        or dev.get("pci_vendor") == "0x8086"
        or dev.get("has_iwarp_counters")
        for dev in diagnostics["infiniband"]
    )
    managed = diagnostics["smoke"].get("managed_buffer_rdma")
    if (
        capability_report.mooncake_compatible
        and isinstance(managed, dict)
        and not managed.get("ok")
    ):
        issues.append("Mooncake RDMA managed-buffer smoke failed before CUDA registration; fix RDMA QP/routing first.")
        if intel_irdma:
            issues.append(
                "Intel irdma/iWARP-style RNIC detected. Mooncake's legacy RDMA transport uses IB/RoCE RC QPs; "
                "if QP creation or INIT fails, use a Mellanox/compatible RoCEv2 or IB HCA, or enable a true "
                "RoCEv2 mode/GID for this NIC if the platform supports it."
            )
    rdmacm = diagnostics["smoke"].get("rdmacm_listener")
    if (
        capability_report.rdmacm_compatible
        and isinstance(rdmacm, dict)
        and not rdmacm.get("ok")
    ):
        issues.append(
            "rdma_cm listener smoke failed; verify the RDMA netdev address and rdma-core provider."
        )
    cuda = diagnostics["smoke"].get("cuda_register")
    if (
        capability_report.mooncake_compatible
        and isinstance(cuda, dict)
        and not cuda.get("ok")
    ):
        issues.append("CUDA tensor memory registration failed; check nvidia_peermem/DMABUF/driver/NIC GPUDirect support.")
    limitations: List[str] = []
    if capability_report.rdmacm_compatible and not capability_report.has_gpudirect:
        limitations.append(
            "RDMA is available through rdma_cm with host staging; GPUDirect is not validated."
        )
    if intel_irdma:
        limitations.append(
            "Intel iWARP requires a remote RDMA peer. Same-host EPD workers must use CUDA P2P/SHM."
        )
    diagnostics["issues"] = issues
    diagnostics["limitations"] = limitations
    diagnostics["ready"] = not issues
    diagnostics["ready_scope"] = (
        "local capability discovery and RDMA-CM endpoint creation"
    )
    diagnostics["data_plane_validated"] = bool(
        isinstance(managed, dict)
        and managed.get("ok")
        and not managed.get("skipped")
    )
    diagnostics["gpudirect_ready"] = (
        diagnostics["ready"] and capability_report.has_gpudirect
    )
    print(json.dumps(diagnostics, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
