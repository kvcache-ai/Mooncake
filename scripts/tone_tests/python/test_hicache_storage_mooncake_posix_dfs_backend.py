"""
E2E test for SGLang HiCache with Mooncake POSIX DFS replicas.
"""

import json
import os
import shutil
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

import requests
from test_hicache_storage_file_backend import HiCacheStorageBaseMixin

from sglang.test.test_utils import CustomTestCase, find_available_port


class HiCacheStorageMooncakePosixDfsBackendMixin(HiCacheStorageBaseMixin):
    """Launch Mooncake services and configure SGLang to request DFS replicas."""

    mooncake_master_port_base = 50051
    mooncake_metadata_port_base = 8080
    mooncake_metrics_port_base = 9003
    dfs_shard_count = 2
    dfs_shard_capacity = 1073741824
    dfs_scan_limit = 64 * 1024 * 1024

    @classmethod
    def setUpClass(cls):
        cls.mooncake_master_port = find_available_port(cls.mooncake_master_port_base)
        cls.mooncake_metadata_port = find_available_port(
            cls.mooncake_metadata_port_base
        )
        cls.mooncake_metrics_port = find_available_port(cls.mooncake_metrics_port_base)
        cls.dfs_root = tempfile.mkdtemp(prefix="mooncake_posix_dfs_", dir="/tmp")
        cls.dfs_env = cls._get_dfs_env()

        try:
            cls._start_mooncake_services()
            super().setUpClass()
        except Exception:
            cls._stop_mooncake_services()
            cls._cleanup_dfs_root()
            raise

    @classmethod
    def tearDownClass(cls):
        try:
            super().tearDownClass()
        finally:
            cls._stop_mooncake_services()
            cls._cleanup_dfs_root()

    @classmethod
    def _get_dfs_env(cls):
        return {
            "MOONCAKE_ENABLE_DFS": "1",
            "MOONCAKE_DFS_ENABLED": "1",
            "MOONCAKE_DFS_FS_ADAPTER": "posix",
            "MOONCAKE_DFS_EVICTION_ENABLED": "0",
            "MOONCAKE_DFS_DEFERRED_FREE_SECONDS": "0",
            "MOONCAKE_DFS_ROOT_DIR": cls.dfs_root,
            "MOONCAKE_DFS_SHARD_COUNT": str(cls.dfs_shard_count),
            "MOONCAKE_DFS_SHARD_CAPACITY": str(cls.dfs_shard_capacity),
            "MOONCAKE_DFS_ALIGNMENT": "4096",
            "MOONCAKE_DFS_SINGLE_TENANT": "true",
            "MOONCAKE_ENABLE_SSD_OFFLOAD": "1",
            "MOONCAKE_OFFLOAD_FILE_STORAGE_PATH": cls.dfs_root,
            "MOONCAKE_OFFLOAD_HEARTBEAT_INTERVAL_SECONDS": "1",
            "MOONCAKE_OFFLOAD_STORAGE_BACKEND_DESCRIPTOR": (
                "distributed_storage_backend"
            ),
        }

    @classmethod
    def _start_mooncake_services(cls):
        print("Starting Mooncake POSIX DFS services...")
        print(
            f"Using master port: {cls.mooncake_master_port}, "
            f"metadata port: {cls.mooncake_metadata_port}, "
            f"metrics port: {cls.mooncake_metrics_port}, "
            f"dfs root: {cls.dfs_root}"
        )

        service_env = {**os.environ, **cls.dfs_env}

        cls.metadata_service_process = subprocess.Popen(
            [
                "python3",
                "-m",
                "mooncake.http_metadata_server",
                "--port",
                str(cls.mooncake_metadata_port),
            ],
            env=service_env,
            preexec_fn=os.setsid,
        )
        print(f"Mooncake metadata service started on port {cls.mooncake_metadata_port}")

        cls.master_service_process = subprocess.Popen(
            [
                "mooncake_master",
                "--port",
                str(cls.mooncake_master_port),
                f"--metrics_port={cls.mooncake_metrics_port}",
                "--enable_offload=true",
            ],
            env=service_env,
            preexec_fn=os.setsid,
        )
        print(f"Mooncake master service started on port {cls.mooncake_master_port}")

        cls._wait_for_mooncake_services_ready()

    @classmethod
    def _wait_for_mooncake_services_ready(cls, timeout: int = 30) -> bool:
        print("Waiting for Mooncake POSIX DFS services to be ready...")

        start_time = time.time()
        while time.time() - start_time < timeout:
            metadata_ready = False
            if (
                cls.metadata_service_process
                and cls.metadata_service_process.poll() is None
            ):
                try:
                    metadata_url = (
                        f"http://127.0.0.1:{cls.mooncake_metadata_port}/metadata"
                    )
                    params = {"key": "__mooncake_posix_dfs_health__"}
                    put_response = requests.put(
                        metadata_url,
                        params=params,
                        data=b"ok",
                        timeout=2,
                    )
                    get_response = requests.get(
                        metadata_url,
                        params=params,
                        timeout=2,
                    )
                    metadata_ready = (
                        put_response.status_code == 200
                        and get_response.status_code == 200
                    )
                except requests.RequestException:
                    pass

            master_ready = (
                cls.master_service_process
                and cls.master_service_process.poll() is None
                and time.time() - start_time > 5
            )

            if metadata_ready and master_ready:
                print("Mooncake POSIX DFS services are ready")
                return True

            if (
                cls.metadata_service_process
                and cls.metadata_service_process.poll() is not None
            ):
                raise RuntimeError("Mooncake metadata service exited early")
            if (
                cls.master_service_process
                and cls.master_service_process.poll() is not None
            ):
                raise RuntimeError("Mooncake master service exited early")

            time.sleep(2)

        print("Warning: Mooncake POSIX DFS services may not be fully ready")
        return False

    @classmethod
    def _stop_mooncake_services(cls):
        print("Stopping Mooncake POSIX DFS services...")
        for process_name in ("metadata_service_process", "master_service_process"):
            process = getattr(cls, process_name, None)
            if not process:
                continue
            try:
                os.killpg(os.getpgid(process.pid), 9)
                process.wait(timeout=5)
                print(f"Stopped {process_name}")
            except (ProcessLookupError, subprocess.TimeoutExpired, OSError) as exc:
                print(f"Warning: could not stop {process_name}: {exc}")

    @classmethod
    def _cleanup_dfs_root(cls):
        if hasattr(cls, "dfs_root"):
            shutil.rmtree(cls.dfs_root, ignore_errors=True)

    @classmethod
    def _get_model_name(cls):
        return os.environ.get("SGLANG_TEST_MODEL_PATH", super()._get_model_name())

    @classmethod
    def _get_additional_server_args_and_env(cls):
        extra_config = {
            "hicache_storage_pass_prefix_keys": True,
            "backend_name": "mooncake_posix_dfs",
            "module_path": "sglang_mooncake_posix_dfs_backend",
            "class_name": "MooncakePosixDfsStore",
            "master_server_address": f"127.0.0.1:{cls.mooncake_master_port}",
            "metadata_server": (
                f"http://127.0.0.1:{cls.mooncake_metadata_port}/metadata"
            ),
            "protocol": "tcp",
            "device_name": "",
            "global_segment_size": "4294967296",
            "check_server": False,
            "enable_ssd_offload": True,
            "ssd_offload_path": cls.dfs_root,
            "interface_v1": 1,
            "replica_num": 1,
            "dfs_replica_num": 1,
        }
        server_args = {
            "--tp-size": 2,
            "--hicache-ratio": 1.2,
            "--hicache-storage-backend": "dynamic",
            "--hicache-storage-backend-extra-config": json.dumps(extra_config),
            "--hicache-mem-layout": "page_first",
            "--mem-fraction-static": 0.6,
        }
        env_vars = {
            "MOONCAKE_MASTER": f"127.0.0.1:{cls.mooncake_master_port}",
            "MOONCAKE_PROTOCOL": "tcp",
            "MC_MS_AUTO_DISC": "0",
            "MOONCAKE_DEVICE": "",
            "MOONCAKE_TE_META_DATA_SERVER": (
                f"http://127.0.0.1:{cls.mooncake_metadata_port}/metadata"
            ),
            "MOONCAKE_GLOBAL_SEGMENT_SIZE": "4294967296",
            **cls.dfs_env,
        }
        return server_args, env_vars

    @classmethod
    def _dfs_shards_with_payload(cls):
        shard_paths = sorted(Path(cls.dfs_root).glob("dfs_shard_*.data"))
        payload_shards = []
        for path in shard_paths:
            remaining = min(path.stat().st_size, cls.dfs_scan_limit)
            with path.open("rb") as shard:
                while remaining > 0:
                    chunk = shard.read(min(1024 * 1024, remaining))
                    if not chunk:
                        break
                    if any(chunk):
                        payload_shards.append(path)
                        break
                    remaining -= len(chunk)
        return shard_paths, payload_shards

    @classmethod
    def _wait_for_dfs_payload(cls, timeout: int = 45):
        deadline = time.time() + timeout
        shard_paths = []
        payload_shards = []
        while time.time() < deadline:
            shard_paths, payload_shards = cls._dfs_shards_with_payload()
            if payload_shards:
                return shard_paths, payload_shards
            time.sleep(1)
        return shard_paths, payload_shards


class TestMooncakePosixDfsBackend(
    HiCacheStorageMooncakePosixDfsBackendMixin, CustomTestCase
):
    """Page-first HiCache test that requires a POSIX DFS replica write."""

    def test_basic_backup_and_prefetch(self):
        super().test_basic_backup_and_prefetch()

        shard_paths, payload_shards = self._wait_for_dfs_payload()
        self.assertGreater(
            len(shard_paths), 0, f"Expected DFS shard files under {self.dfs_root}"
        )
        self.assertGreater(
            len(payload_shards),
            0,
            f"Expected at least one DFS shard to contain payload: {shard_paths}",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
