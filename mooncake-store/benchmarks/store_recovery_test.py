#!/usr/bin/env python3
"""Mooncake Store master restart and client reconnect recovery test."""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time
from pathlib import Path
from typing import Any, Callable

from store_api_benchmark import (
    BYTES_PER_MIB,
    MasterProcess,
    collect_metrics_snapshots,
    import_store_class,
    make_payload,
    parse_size,
    setup_store,
    start_master,
    DEFAULT_MASTER_METRICS_PORT,
    DEFAULT_MASTER_METRICS_URL,
)


HC_HEALTHY = 0
HC_MASTER_UNREACHABLE = 2


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Mooncake Store client recovery after master restart."
    )
    parser.add_argument("--build-dir", required=True, help="CMake build directory")
    parser.add_argument("--master-bin", help="path to mooncake_master")
    parser.add_argument("--master-log", help="path for mooncake_master stdout/stderr")
    parser.add_argument("--metadata-url", default="http://127.0.0.1:8080/metadata")
    parser.add_argument("--master-addr", default="127.0.0.1:50051")
    parser.add_argument(
        "--master-metrics-port", type=int, default=DEFAULT_MASTER_METRICS_PORT
    )
    parser.add_argument("--master-metrics-url", default=DEFAULT_MASTER_METRICS_URL)
    parser.add_argument("--client-metrics-url", default="")
    parser.add_argument("--worker-metrics-url", default="")
    parser.add_argument("--metrics-timeout", type=float, default=2.0)
    parser.add_argument("--skip-metrics", action="store_true")
    parser.add_argument("--startup-timeout", type=float, default=20.0)
    parser.add_argument("--disconnect-timeout", type=float, default=20.0)
    parser.add_argument("--reconnect-timeout", type=float, default=30.0)
    parser.add_argument("--value-size", type=parse_size, default=parse_size("1K"))
    parser.add_argument("--key-prefix", default="store_recovery_test")
    parser.add_argument("--protocol", default="tcp", choices=["tcp", "rdma"])
    parser.add_argument("--rdma-devices", default="")
    parser.add_argument("--local-hostname", default="127.0.0.1")
    parser.add_argument("--global-segment-size-mb", type=int, default=512)
    parser.add_argument("--local-buffer-size-mb", type=int, default=128)
    parser.add_argument(
        "--master-extra-flag",
        action="append",
        default=[],
        help="extra flag passed to mooncake_master, repeatable",
    )
    parser.add_argument("--output-json", help="write JSON report to this path")
    args = parser.parse_args()
    args.start_master = True
    return args


def wait_for(
    description: str,
    timeout_s: float,
    probe: Callable[[], tuple[bool, Any]],
    interval_s: float = 0.25,
) -> dict[str, Any]:
    started_at = time.monotonic()
    last_value: Any = None
    last_error: str | None = None
    while time.monotonic() - started_at < timeout_s:
        try:
            matched, value = probe()
            last_value = value
            if matched:
                return {
                    "passed": True,
                    "description": description,
                    "elapsed_s": time.monotonic() - started_at,
                    "value": value,
                }
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(interval_s)
    return {
        "passed": False,
        "description": description,
        "elapsed_s": time.monotonic() - started_at,
        "last_value": last_value,
        "last_error": last_error,
    }


def roundtrip(store: Any, key: str, payload: bytes) -> dict[str, Any]:
    put_code = store.put(key, payload)
    if put_code != 0:
        return {"passed": False, "put_code": put_code}
    value = store.get(key)
    if value != payload:
        return {
            "passed": False,
            "put_code": put_code,
            "get_matches": False,
            "get_type": type(value).__name__,
            "get_size": len(value) if isinstance(value, (bytes, bytearray)) else None,
        }
    remove_code = store.remove(key, True)
    return {
        "passed": remove_code == 0,
        "put_code": put_code,
        "get_matches": True,
        "remove_code": remove_code,
    }


def wait_for_roundtrip(
    store: Any, key_prefix: str, payload: bytes, timeout_s: float
) -> dict[str, Any]:
    attempts = 0

    def probe() -> tuple[bool, Any]:
        nonlocal attempts
        attempts += 1
        result = roundtrip(store, f"{key_prefix}:{attempts}", payload)
        result["attempt"] = attempts
        return bool(result["passed"]), result

    result = wait_for(
        "post-restart put/get/remove roundtrip", timeout_s, probe, interval_s=0.5
    )
    result["attempts"] = attempts
    return result


def record_step(
    steps: list[dict[str, Any]], name: str, passed: bool, details: Any = None
) -> None:
    step = {"name": name, "passed": passed}
    if details is not None:
        step["details"] = details
    steps.append(step)


def main() -> int:
    args = parse_args()
    args.build_dir = str(Path(args.build_dir).resolve())
    if args.value_size <= 0:
        raise ValueError("--value-size must be positive")
    if args.metrics_timeout <= 0:
        raise ValueError("--metrics-timeout must be positive")
    if (
        args.master_metrics_url == DEFAULT_MASTER_METRICS_URL
        and args.master_metrics_port != DEFAULT_MASTER_METRICS_PORT
    ):
        args.master_metrics_url = f"http://127.0.0.1:{args.master_metrics_port}/metrics"

    StoreClass, import_mode = import_store_class(Path(args.build_dir))
    payload = make_payload(args.value_size, "recovery")
    steps: list[dict[str, Any]] = []
    master: MasterProcess | None = None
    store = None
    report: dict[str, Any] = {
        "metadata": {
            "timestamp_unix": time.time(),
            "hostname": socket.gethostname(),
            "store_import": import_mode,
            "note": (
                "This non-HA recovery test validates client disconnect detection, "
                "reconnect, and post-restart Store operations. It does not require "
                "pre-restart object metadata to survive a standalone master restart."
            ),
        },
        "config": {
            "build_dir": args.build_dir,
            "metadata_url": args.metadata_url,
            "master_addr": args.master_addr,
            "master_metrics_url": args.master_metrics_url,
            "value_size": args.value_size,
            "protocol": args.protocol,
            "local_hostname": args.local_hostname,
            "global_segment_size_bytes": args.global_segment_size_mb * BYTES_PER_MIB,
            "local_buffer_size_bytes": args.local_buffer_size_mb * BYTES_PER_MIB,
            "disconnect_timeout_s": args.disconnect_timeout,
            "reconnect_timeout_s": args.reconnect_timeout,
        },
        "steps": steps,
    }

    try:
        master = start_master(args)
        record_step(
            steps, "start_initial_master", True, {"master_log": str(master.log_path)}
        )

        store = setup_store(args, StoreClass)
        record_step(steps, "setup_store", True)

        initial_roundtrip = roundtrip(store, f"{args.key_prefix}:initial", payload)
        record_step(
            steps,
            "initial_roundtrip",
            bool(initial_roundtrip["passed"]),
            initial_roundtrip,
        )

        initial_health = store.health_check()
        record_step(
            steps,
            "initial_health_healthy",
            initial_health == HC_HEALTHY,
            {"health_code": initial_health},
        )

        report["metrics_before_restart"] = collect_metrics_snapshots(args)

        master.stop()
        master = None
        record_step(steps, "stop_master", True)

        disconnect = wait_for(
            "health_check reports HC_MASTER_UNREACHABLE after master stop",
            args.disconnect_timeout,
            lambda: (
                (code := store.health_check()) == HC_MASTER_UNREACHABLE,
                {"health_code": code},
            ),
        )
        record_step(
            steps, "client_detects_master_disconnect", disconnect["passed"], disconnect
        )

        master = start_master(args)
        record_step(steps, "restart_master", True, {"master_log": str(master.log_path)})

        reconnect = wait_for(
            "health_check returns HC_HEALTHY after master restart",
            args.reconnect_timeout,
            lambda: (
                (code := store.health_check()) == HC_HEALTHY,
                {"health_code": code},
            ),
        )
        record_step(
            steps, "client_reconnects_to_master", reconnect["passed"], reconnect
        )

        post_restart = wait_for_roundtrip(
            store,
            f"{args.key_prefix}:post_restart",
            payload,
            args.reconnect_timeout,
        )
        record_step(
            steps, "post_restart_roundtrip", post_restart["passed"], post_restart
        )

        report["metrics_after_restart"] = collect_metrics_snapshots(args)
    except Exception as exc:
        record_step(steps, "unexpected_error", False, f"{type(exc).__name__}: {exc}")
    finally:
        if store is not None:
            try:
                store.close()
            except Exception:
                pass
        if master is not None:
            master.stop()

    report["passed"] = all(step["passed"] for step in steps)
    output = json.dumps(report, indent=2, sort_keys=True)
    print(output)
    if args.output_json:
        Path(args.output_json).write_text(output + "\n")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
