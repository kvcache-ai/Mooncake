#!/usr/bin/env python3
from __future__ import annotations

"""Manual/nightly drain HTTP verification script.

This script intentionally targets non-ASan builds only. The ASan CI gate uses
TaskExecutorIntegrationTest.DrainJobCompleteFlow instead, because running the
pybind store client inside a Python host process is not stable under ASan.
"""

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path
from urllib.parse import urlparse, urlunparse

REPO_ROOT = Path(__file__).resolve().parents[1]
BUILD_PYTHON_DIR = REPO_ROOT / "build" / "mooncake-integration"


def _find_store_extension() -> Path:
    candidates = sorted(BUILD_PYTHON_DIR.glob("store*.so"))
    if not candidates:
        return BUILD_PYTHON_DIR / "store.so"
    return candidates[0]


STORE_EXTENSION = _find_store_extension()


def _artifact_needs_asan_runtime(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        result = subprocess.run(
            ["ldd", str(path)],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return "libasan" in result.stdout or "libasan" in result.stderr


def ensure_non_asan(argv: list[str]) -> None:
    del argv
    asan_artifacts = (
        [str(STORE_EXTENSION)] if _artifact_needs_asan_runtime(STORE_EXTENSION) else []
    )
    if not asan_artifacts:
        return
    artifact_list = ", ".join(asan_artifacts)
    raise RuntimeError(
        "scripts/test_drain_http_api.py is manual/nightly only and does not "
        "support ASan builds. Use "
        "TaskExecutorIntegrationTest.DrainJobCompleteFlow for sanitizer CI. "
        f"ASan-linked artifacts: {artifact_list}"
    )


ensure_non_asan(sys.argv)

try:
    from mooncake.store import MooncakeDistributedStore, ReplicateConfig  # noqa: E402
    from mooncake.mooncake_config import MooncakeConfig  # noqa: E402
except ModuleNotFoundError:
    wheel_dir = REPO_ROOT / "mooncake-wheel"
    if str(BUILD_PYTHON_DIR) not in sys.path:
        sys.path.insert(0, str(BUILD_PYTHON_DIR))
    if str(wheel_dir) not in sys.path:
        sys.path.insert(0, str(wheel_dir))
    try:
        from mooncake.store import MooncakeDistributedStore, ReplicateConfig  # noqa: E402
        from mooncake.mooncake_config import MooncakeConfig  # noqa: E402
    except ModuleNotFoundError:
        import store as store_module  # noqa: E402
        from mooncake.mooncake_config import MooncakeConfig  # noqa: E402

        MooncakeDistributedStore = store_module.MooncakeDistributedStore
        ReplicateConfig = store_module.ReplicateConfig


class TestFailure(RuntimeError):
    pass


def wait_http_ok(url: str, timeout_sec: float) -> None:
    deadline = time.time() + timeout_sec
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                if resp.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(0.2)
    raise TestFailure(f"timeout waiting for {url}: {last_error}")


def http_post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5.0) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def http_get_json(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5.0) as resp:
        body = resp.read().decode("utf-8")
        return json.loads(body)


def wait_until(description: str, timeout_sec: float, callback):
    deadline = time.time() + timeout_sec
    last_error = None
    while time.time() < deadline:
        try:
            return callback()
        except TestFailure as exc:
            last_error = exc
        time.sleep(0.2)
    raise TestFailure(f"timeout waiting for {description}: {last_error}")


def endpoint_list(
    store_client: MooncakeDistributedStore,
    key: str,
) -> list[str]:
    endpoints = []
    for replica_desc in store_client.get_replica_desc(key):
        if replica_desc.is_memory_replica:
            mem_desc = replica_desc.get_memory_descriptor()
            endpoints.append(mem_desc.buffer_descriptor.transport_endpoint)
    return endpoints


def create_store(
    local_hostname: str,
    metadata_url: str,
    master_addr: str,
    protocol: str,
) -> MooncakeDistributedStore:
    client = MooncakeDistributedStore()
    rc = client.setup(
        local_hostname,
        metadata_url,
        64 * 1024 * 1024,
        16 * 1024 * 1024,
        protocol,
        "",
        master_addr,
    )
    if rc != 0:
        raise TestFailure(f"setup failed for {local_hostname}, rc={rc}")
    return client


def put_key(
    store_client: MooncakeDistributedStore,
    key: str,
    value: bytes,
    preferred_segment: str,
) -> None:
    replicate_config = ReplicateConfig()
    replicate_config.replica_num = 1
    replicate_config.preferred_segment = preferred_segment
    rc = store_client.put(key, value, replicate_config)
    if rc != 0:
        raise TestFailure(f"put failed for key={key}, rc={rc}")


def assert_key_data(
    store_client: MooncakeDistributedStore,
    key: str,
    expected_value: bytes,
) -> None:
    data = store_client.get(key)
    if data != expected_value:
        raise TestFailure(f"data mismatch for key={key}: got={data!r}")


def assert_key_segments(
    store_client: MooncakeDistributedStore,
    key: str,
    must_include: set[str] | None = None,
    must_exclude: set[str] | None = None,
) -> list[str]:
    endpoints = sorted(endpoint_list(store_client, key))
    must_include = must_include or set()
    must_exclude = must_exclude or set()
    for segment in must_include:
        if segment not in endpoints:
            raise TestFailure(
                f"key={key} missing expected segment={segment}, endpoints={endpoints}"
            )
    for segment in must_exclude:
        if segment in endpoints:
            raise TestFailure(
                f"key={key} still contains forbidden segment={segment}, endpoints={endpoints}"
            )
    return endpoints


def wait_key_segments(
    store_client: MooncakeDistributedStore,
    key: str,
    timeout_sec: float,
    must_include: set[str] | None = None,
    must_exclude: set[str] | None = None,
) -> list[str]:
    return wait_until(
        f"key {key} replica placement",
        timeout_sec,
        lambda: assert_key_segments(
            store_client,
            key,
            must_include=must_include,
            must_exclude=must_exclude,
        ),
    )


def wait_segment_status(
    control_base: str,
    segment: str,
    expected_status: str,
    timeout_sec: float,
) -> dict:
    status_url = f"{control_base}/api/v1/segments/status?segment={segment}"

    def check():
        status = http_get_json(status_url)
        if not status.get("success", False):
            raise TestFailure(f"query segment status failed: {status}")
        if status.get("status_name") != expected_status:
            raise TestFailure(f"segment={segment} status={status}")
        return status

    return wait_until(
        f"segment {segment} to reach {expected_status}",
        timeout_sec,
        check,
    )


def wait_job_succeeded(control_base: str, job_id: str, timeout_sec: float) -> dict:
    deadline = time.time() + timeout_sec
    last = None
    query_url = f"{control_base}/api/v1/drain_jobs/query?job_id={job_id}"
    while time.time() < deadline:
        last = http_get_json(query_url)
        if not last.get("success", False):
            raise TestFailure(f"job query failed: {last}")
        status_name = last.get("status_name")
        if status_name in {"SUCCEEDED", "FAILED", "CANCELED"}:
            if status_name != "SUCCEEDED":
                raise TestFailure(f"job finished in unexpected status: {last}")
            return last
        time.sleep(0.2)
    raise TestFailure(f"job did not finish in time, last={last}")


def resolve_runtime_config(
    protocol_override: str | None,
    control_base_override: str | None,
) -> tuple[str, str, str, str, str | None]:
    config = MooncakeConfig.load_from_env()
    protocol = protocol_override or config.protocol
    control_base = control_base_override or os.getenv("MOONCAKE_MASTER_CONTROL_BASE")
    if not control_base:
        host = config.master_server_address
        if host.startswith("[") and "]" in host:
            host = host.split("]", 1)[0].lstrip("[")
        elif ":" in host:
            host = host.rsplit(":", 1)[0]
        control_base = f"http://{host}:9003"
    parsed_metadata = urlparse(config.metadata_server)
    metadata_health_url = None
    if parsed_metadata.scheme in {"http", "https"} and parsed_metadata.netloc:
        metadata_health_url = urlunparse(
            parsed_metadata._replace(path="/health", params="", query="", fragment="")
        )
    return (
        config.master_server_address,
        config.metadata_server,
        protocol,
        control_base.rstrip("/"),
        metadata_health_url,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify master drain HTTP control plane (manual/nightly, non-ASan only)"
    )
    parser.add_argument("--protocol", default=None)
    parser.add_argument("--control-base", default=None)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    args = parser.parse_args()

    master_addr, metadata_url, protocol, control_base, metadata_health_url = (
        resolve_runtime_config(args.protocol, args.control_base)
    )

    os.environ.setdefault("MC_RPC_PROTOCOL", protocol)

    stores = []
    try:
        wait_http_ok(f"{control_base}/health", timeout_sec=10.0)
        if metadata_health_url:
            wait_http_ok(metadata_health_url, timeout_sec=10.0)

        source_segment = "127.0.0.1:19001"
        target_segment = "127.0.0.1:19002"
        source_store = create_store(source_segment, metadata_url, master_addr, protocol)
        target_store = create_store(target_segment, metadata_url, master_addr, protocol)
        stores.extend([source_store, target_store])

        time.sleep(1.0)

        prefix = f"drain-http-{int(time.time() * 1000)}"
        preload_items = [
            (
                f"{prefix}-preload-{index:02d}",
                f"preload-value-{index:02d}-".encode("utf-8") + os.urandom(256 * 1024),
            )
            for index in range(24)
        ]
        redirected_items = [
            (
                f"{prefix}-redirect-{index:02d}",
                f"redirect-value-{index:02d}-".encode("utf-8") + os.urandom(64 * 1024),
            )
            for index in range(8)
        ]

        for key, value in preload_items:
            put_key(source_store, key, value, source_segment)
            wait_key_segments(
                target_store,
                key,
                timeout_sec=5.0,
                must_include={source_segment},
            )

        drain_resp = http_post_json(
            f"{control_base}/api/v1/drain_jobs",
            {
                "segments": [source_segment],
                "target_segments": [target_segment],
                "max_concurrency": 1,
            },
        )
        if not drain_resp.get("success", False):
            raise TestFailure(f"create drain job failed: {drain_resp}")
        job_id = drain_resp["job_id"]

        wait_segment_status(control_base, source_segment, "DRAINING", timeout_sec=5.0)

        redirected_endpoints: dict[str, list[str]] = {}
        for key, value in redirected_items:
            put_key(source_store, key, value, source_segment)
            redirected_endpoints[key] = wait_key_segments(
                target_store,
                key,
                timeout_sec=5.0,
                must_include={target_segment},
                must_exclude={source_segment},
            )

        final_job = wait_job_succeeded(control_base, job_id, args.timeout_sec)
        if final_job.get("active_units") != 0:
            raise TestFailure(f"job still has active units: {final_job}")
        if final_job.get("succeeded_units", 0) < len(preload_items):
            raise TestFailure(f"unexpected succeeded_units after drain: {final_job}")

        wait_segment_status(control_base, source_segment, "DRAINED", timeout_sec=5.0)

        final_endpoints: dict[str, list[str]] = {}
        for key, value in preload_items + redirected_items:
            assert_key_data(target_store, key, value)
            final_endpoints[key] = wait_key_segments(
                target_store,
                key,
                timeout_sec=5.0,
                must_include={target_segment},
                must_exclude={source_segment},
            )

        source_store.close()
        stores.remove(source_store)
        time.sleep(1.0)

        post_close_endpoints: dict[str, list[str]] = {}
        for key, value in preload_items + redirected_items:
            assert_key_data(target_store, key, value)
            post_close_endpoints[key] = wait_key_segments(
                target_store,
                key,
                timeout_sec=5.0,
                must_include={target_segment},
                must_exclude={source_segment},
            )

        print(
            json.dumps(
                {
                    "success": True,
                    "job_id": job_id,
                    "status": final_job.get("status_name"),
                    "preload_keys": len(preload_items),
                    "redirected_keys": len(redirected_items),
                    "redirected_segments": redirected_endpoints,
                    "final_segments": final_endpoints,
                    "post_close_segments": post_close_endpoints,
                }
            )
        )
        return 0
    finally:
        for client in stores:
            try:
                client.close()
            except Exception:
                pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except TestFailure as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
