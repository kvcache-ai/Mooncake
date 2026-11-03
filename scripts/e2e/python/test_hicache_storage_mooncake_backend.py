"""
This test case is from the sglang community: https://github.com/sgl-project/sglang/blob/main/test/srt/hicache/test_hicache_storage_mooncake_backend.py
However, it overrides the mem-fraction-static parameter setting of HiCacheStorageBaseMixin, changing it from 0.6 to 0.8

Benchmark tests for HiCache Storage with Mooncake backend.
Usage:
    python3 -m pytest test/srt/hicache/test_hicache_storage_mooncake_backend.py -v
"""

import os
import socket
import subprocess
import sys
import time
import unittest

import requests
from test_hicache_storage_file_backend import HiCacheStorageBaseMixin

from sglang.test.test_utils import (
    DEFAULT_MLA_MODEL_NAME_FOR_TEST,
    CustomTestCase,
    find_available_port,
    is_in_ci,
)

ROCM_MHA_MODEL_NAME_FOR_TEST = "Qwen/Qwen3-8B"


class HiCacheStorageMooncakeBackendBaseMixin(HiCacheStorageBaseMixin):
    """Base mixin class with common setup and utilities"""

    # Default port ranges for Mooncake services - can be overridden in subclasses
    mooncake_master_port_base = 50051
    mooncake_metadata_port_base = 8080

    @classmethod
    def _get_model_name(cls):
        if os.getenv("CI_ACCELERATOR") == "rocm":
            return ROCM_MHA_MODEL_NAME_FOR_TEST
        return super()._get_model_name()

    @classmethod
    def setUpClass(cls):
        """Set up test environment and launch Mooncake services before server setup"""
        # Find available ports for Mooncake services to avoid conflicts
        cls.mooncake_master_port = find_available_port(
            HiCacheStorageMooncakeBackendBaseMixin.mooncake_master_port_base
        )
        cls.mooncake_metadata_port = find_available_port(
            HiCacheStorageMooncakeBackendBaseMixin.mooncake_metadata_port_base
        )

        # Start Mooncake services first
        cls._start_mooncake_services()

        # Call parent setup
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        """Clean up Mooncake services after server teardown"""
        # Call parent teardown first
        super().tearDownClass()

        # Stop Mooncake services
        cls._stop_mooncake_services()

    @classmethod
    def _start_mooncake_services(cls):
        """Start Mooncake metadata and master services with configurable ports and readiness detection"""
        print("Starting Mooncake services...")
        print(
            f"Using master port: {cls.mooncake_master_port}, metadata port: {cls.mooncake_metadata_port}"
        )

        # The metadata service is bound to loopback on purpose: it refuses an
        # unauthenticated non-loopback bind (mooncake #4472), and every client
        # used by this test runs on the same host. Child output is inherited so
        # a failed launch shows up in the CI log instead of being swallowed by
        # an unread pipe.
        cls.metadata_service_process = cls._spawn_service(
            "metadata",
            [
                sys.executable,
                "-m",
                "mooncake.http_metadata_server",
                "--host",
                "127.0.0.1",
                "--port",
                str(cls.mooncake_metadata_port),
            ],
        )

        # Start master service with configurable port
        cls.master_service_process = cls._spawn_service(
            "master",
            ["mooncake_master", "--port", str(cls.mooncake_master_port)],
        )

        # Wait for services to be ready instead of fixed sleep
        cls._wait_for_mooncake_services_ready()

    @classmethod
    def _spawn_service(cls, name, command):
        """Start a Mooncake service process, returning None if it cannot be spawned"""
        try:
            process = subprocess.Popen(
                command,
                preexec_fn=os.setsid,  # Create new process group
            )
        except (FileNotFoundError, subprocess.SubprocessError) as e:
            print(f"Warning: Could not start Mooncake {name} service: {e}")
            return None
        print(f"Mooncake {name} service started (pid={process.pid})")
        return process

    @classmethod
    def _wait_for_mooncake_services_ready(cls, timeout: int = 60) -> bool:
        """Wait for Mooncake services to be ready by checking their endpoints

        Fails fast when a service exits, so a broken launch is reported here
        instead of surfacing later as "Failed to setup Mooncake store".
        """
        print("Waiting for Mooncake services to be ready...")

        deadline = time.time() + timeout
        metadata_ready = False
        master_ready = False

        while time.time() < deadline:
            cls._ensure_service_running("metadata", cls.metadata_service_process)
            cls._ensure_service_running("master", cls.master_service_process)

            if not metadata_ready and cls._probe_metadata_endpoint():
                metadata_ready = True
                print("Mooncake metadata service is ready")

            if not master_ready and cls._probe_tcp_port(cls.mooncake_master_port):
                master_ready = True
                print("Mooncake master service is ready")

            # Both services should be ready
            if metadata_ready and master_ready:
                print("All Mooncake services are ready")
                return True

            time.sleep(1)

        raise RuntimeError(
            f"Mooncake services were not ready within {timeout}s "
            f"(metadata port {cls.mooncake_metadata_port}: "
            f"{'ready' if metadata_ready else 'not ready'}, "
            f"master port {cls.mooncake_master_port}: "
            f"{'ready' if master_ready else 'not ready'})"
        )

    @staticmethod
    def _ensure_service_running(name, process):
        """Raise if a Mooncake service process has already exited"""
        if process is None:
            raise RuntimeError(f"Mooncake {name} service was never started")
        return_code = process.poll()
        if return_code is not None:
            raise RuntimeError(
                f"Mooncake {name} service exited with code {return_code} before "
                "it became ready; check the service output above"
            )

    @classmethod
    def _probe_metadata_endpoint(cls) -> bool:
        """Return True once the metadata service answers requests"""
        # Any HTTP status means the listener is up: a missing key is answered
        # with 400/404 and a token-protected server with 401.
        url = (
            f"http://127.0.0.1:{cls.mooncake_metadata_port}"
            "/metadata?key=__hicache_probe__"
        )
        try:
            requests.get(url, timeout=2)
        except (requests.RequestException, ConnectionError):
            # Service might not be fully started yet
            return False
        return True

    @classmethod
    def _probe_tcp_port(cls, port: int, host: str = "127.0.0.1") -> bool:
        """Return True when something accepts connections on host:port"""
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            return False

    @classmethod
    def _stop_mooncake_services(cls):
        """Stop Mooncake services"""
        print("Stopping Mooncake services...")

        for name in ("metadata", "master"):
            process = getattr(cls, f"{name}_service_process", None)
            if process is None:
                continue
            if process.poll() is not None:
                # Already gone, e.g. it exited during readiness detection.
                print(f"Mooncake {name} service already exited")
                continue
            try:
                os.killpg(os.getpgid(process.pid), 9)
                process.wait(timeout=5)
                print(f"Mooncake {name} service stopped")
            except (ProcessLookupError, subprocess.TimeoutExpired, OSError) as e:
                print(f"Warning: Could not stop Mooncake {name} service: {e}")

    @classmethod
    def _get_additional_server_args_and_env(cls):
        """Get additional server arguments specific to configuration - override in subclasses"""

        server_args = {
            "--tp-size": 2,
            "--hicache-ratio": 2,
            "--hicache-storage-backend": "mooncake",
            "--mem-fraction-static": 0.8,
        }

        # Set the environment variables for Mooncake using dynamic ports
        env_vars = {
            "MOONCAKE_MASTER": f"127.0.0.1:{cls.mooncake_master_port}",
            "MOONCAKE_PROTOCOL": "tcp",
            "MC_MS_AUTO_DISC": "0",
            "MOONCAKE_DEVICE": "",
            "MOONCAKE_TE_META_DATA_SERVER": f"http://127.0.0.1:{cls.mooncake_metadata_port}/metadata",
            "MOONCAKE_GLOBAL_SEGMENT_SIZE": "4294967296",  # 4 GiB
        }

        return server_args, env_vars


'''
# Same as #10131, layer first layout test TODO(mateng): will make it work
class TestMooncakeBackendLayerFirstLayout(
    HiCacheStorageMooncakeBackendBaseMixin, CustomTestCase
):
    """Layer first layout tests for HiCache-Mooncake backend"""

    @classmethod
    def _get_additional_server_args_and_env(cls):
        """Get additional server arguments specific to configuration - override in subclasses"""
        server_args, env_vars = super()._get_additional_server_args_and_env()
        server_args["--hicache-mem-layout"] = "layer_first"
        server_args["--hicache-io-backend"] = "direct"
        return server_args, env_vars
'''


@unittest.skipIf(is_in_ci(), "To reduce the CI execution time.")
class TestMooncakeBackendPageFirstLayout(
    HiCacheStorageMooncakeBackendBaseMixin, CustomTestCase
):
    """Page first layout tests for HiCache-Mooncake backend"""

    @classmethod
    def _get_additional_server_args_and_env(cls):
        """Get additional server arguments specific to configuration - override in subclasses"""
        server_args, env_vars = super()._get_additional_server_args_and_env()
        server_args["--hicache-mem-layout"] = "page_first"
        return server_args, env_vars


class TestMooncakeBackendMLAModel(
    HiCacheStorageMooncakeBackendBaseMixin, CustomTestCase
):
    """MLA Model tests for HiCache-Mooncake backend"""

    @classmethod
    def _get_model_name(cls):
        """Use MLA model for testing"""
        return DEFAULT_MLA_MODEL_NAME_FOR_TEST

    @classmethod
    def _get_additional_server_args_and_env(cls):
        """Get additional server arguments specific to configuration - override in subclasses"""
        server_args, env_vars = super()._get_additional_server_args_and_env()
        server_args["--hicache-mem-layout"] = "page_first"
        server_args["--tp-size"] = 2
        # Keep arena coverage explicit in the self-hosted integration suite.
        env_vars.pop("MC_DISABLE_MMAP_ARENA", None)
        env_vars["MC_MMAP_ARENA_POOL_SIZE"] = "8gb"
        return server_args, env_vars


class TestMooncakeBackendAccuracy(
    HiCacheStorageMooncakeBackendBaseMixin, CustomTestCase
):
    """Accuracy tests for HiCache-Mooncake backend"""

    @classmethod
    def _get_additional_server_args_and_env(cls):
        """Get additional server arguments specific to configuration - override in subclasses"""
        server_args, env_vars = super()._get_additional_server_args_and_env()
        server_args["--hicache-ratio"] = 1.5
        server_args["--tp-size"] = 2
        server_args["--hicache-mem-layout"] = "page_first_direct"
        server_args["--hicache-io-backend"] = "direct"
        # Explicitly cover the allocator-disabled fallback path as part of the
        # self-hosted HiCache integration suite.
        env_vars.pop("MC_MMAP_ARENA_POOL_SIZE", None)
        env_vars["MC_DISABLE_MMAP_ARENA"] = "1"
        return server_args, env_vars

    def test_eval_accuracy(self):
        """Test eval accuracy with cache persistence across cache flushes"""
        if os.getenv("CI_ACCELERATOR") != "rocm":
            from test_hicache_storage_file_backend import run_eval_accuracy_test

            run_eval_accuracy_test(self)
            return

        # Qwen3 can spend the upstream 512-token budget entirely in its
        # thinking trace, leaving no final GSM8K answer. Give ROCm CI enough
        # room to finish the answer so this remains a cache-accuracy test rather
        # than a reasoning-length lottery.
        from types import SimpleNamespace

        from sglang.test.run_eval import run_eval

        args = SimpleNamespace(
            base_url=f"http://{self.base_host}:{self.base_port}",
            eval_name="gsm8k",
            api="completion",
            max_tokens=2048,
            num_examples=200,
            num_threads=64,
        )
        metrics_initial = run_eval(args)
        self.flush_cache()
        metrics_cached = run_eval(args)

        accuracy_diff = abs(metrics_initial["score"] - metrics_cached["score"])
        print(f"Accuracy difference: {accuracy_diff:.4f}")
        self.assertGreater(
            metrics_initial["score"], 0.6, "Initial accuracy should be reasonable"
        )
        self.assertGreater(
            metrics_cached["score"], 0.6, "Cached accuracy should be reasonable"
        )
        self.assertLess(
            accuracy_diff,
            0.03,
            "Accuracy should be consistent between cache states",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
