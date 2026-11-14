#!/usr/bin/env python3
"""
Test script to verify that the mooncake_master entry point works correctly.
"""

import sys
import subprocess
import time


def test_entry_point_installed():
    """Test that the entry point is installed and can be executed."""
    try:
        # Check if mooncake_master is in PATH
        result = subprocess.run(
            ["which", "mooncake_master"], capture_output=True, text=True
        )

        if result.returncode != 0:
            print("❌ mooncake_master entry point not found in PATH")
            return False

        print(f"✅ mooncake_master entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", "mooncake_client"], capture_output=True, text=True
        )

        if result.returncode != 0:
            print("❌ mooncake_client entry point not found in PATH")
            return False

        print(f"✅ mooncake_client entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", "transfer_engine_bench"], capture_output=True, text=True
        )

        if result.returncode != 0:
            print("❌ transfer_engine_bench entry point not found in PATH")
            return False

        print(f"✅ transfer_engine_bench entry point found at: {result.stdout.strip()}")
        return True
    except Exception as e:
        print(f"❌ Error checking for entry point: {e}")
        return False


def test_run_master_and_client():
    """Test running the master service through the entry point."""
    try:
        # Use non-default ports to avoid collisions with processes started
        # earlier in the CI test-wheel-ubuntu job:
        #  * --port: gRPC port. The test client connects via
        #    --master_server_address (direct gRPC), so it does not need the
        #    embedded HTTP metadata server on the default 8080 — the job runs a
        #    shared Python mooncake_http_metadata_server on 8080 for the whole
        #    run_tests.sh suite, so enabling it here would collide and
        #    LOG(FATAL) (master.cpp:1449).
        #  * --metrics_port: the master admin server always binds this port
        #    (master.cpp:1485, created unconditionally and cannot be disabled),
        #    and the default 9003 is still held by a lingering master from the
        #    prior CXL test step when this smoke test runs.
        process = subprocess.Popen(
            [
                "mooncake_master",
                "--port=61351",
                "--max_threads=2",
                "--metrics_port=19003",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        # Give it a moment to start
        time.sleep(2)

        # Check if process is running
        if process.poll() is None:
            print("✅ mooncake_master process started successfully")
            client_process = subprocess.Popen(
                [
                    "mooncake_client",
                    "--master_server_address=127.0.0.1:61351",
                    "--port=61352",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # Give the client some time to connect
            time.sleep(2)
            if client_process.poll() is None:
                print("✅ mooncake_client connected to mooncake_master successfully")
                # Terminate the process
                client_process.terminate()
                client_process.wait(timeout=5)
                print("✅ mooncake_client process terminated successfully")
            else:
                stdout, stderr = client_process.communicate()
                print("❌ mooncake_client failed to start")
                print(f"stdout: {stdout.decode()}")
                print(f"stderr: {stderr.decode()}")
            # Terminate the process
            process.terminate()
            process.wait(timeout=5)
            print("✅ mooncake_master process terminated successfully")
            return True
        else:
            stdout, stderr = process.communicate()
            print("❌ mooncake_master process failed to start")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
    except Exception as e:
        print(f"❌ Error running mooncake_master: {e}")
        return False


def test_client_rejects_invalid_size_flags():
    """Test that mooncake_client refuses to start on an invalid size flag.

    Such values used to become 0, and a global segment size of 0 starts a
    pure client that contributes no memory (#5429). IEC suffixes like
    "MiB" are not accepted by the size parser, so they must be rejected.
    """
    for flag in ("global_segment_size", "local_buffer_size"):
        try:
            result = subprocess.run(
                ["mooncake_client", f"--{flag}=256MiB"],
                capture_output=True,
                text=True,
                timeout=20,
            )
        except subprocess.TimeoutExpired:
            print(f"❌ mooncake_client kept running with --{flag}=256MiB")
            return False

        if result.returncode == 0 or f"Invalid value for --{flag}" not in result.stderr:
            print(f"❌ mooncake_client did not reject --{flag}=256MiB")
            print(f"returncode: {result.returncode}")
            print(f"stderr: {result.stderr}")
            return False
        print(f"✅ mooncake_client rejected --{flag}=256MiB")
    return True


if __name__ == "__main__":
    print("Testing mooncake_master entry point...")

    # Run tests
    entry_point_installed = test_entry_point_installed()

    if entry_point_installed:
        run_master_and_client_success = test_run_master_and_client()
        invalid_size_rejected = test_client_rejects_invalid_size_flags()
    else:
        run_master_and_client_success = False
        invalid_size_rejected = False

    # Print summary
    print("\nTest Summary:")
    print(f"Entry point installed: {'✅' if entry_point_installed else '❌'}")
    print(f"Run master successful: {'✅' if run_master_and_client_success else '❌'}")
    print(f"Invalid size rejected: {'✅' if invalid_size_rejected else '❌'}")

    # Exit with appropriate status code
    if (
        entry_point_installed
        and run_master_and_client_success
        and invalid_size_rejected
    ):
        print("\nAll tests passed! 🎉")
        sys.exit(0)
    else:
        print("\nSome tests failed. 😢")
        sys.exit(1)
