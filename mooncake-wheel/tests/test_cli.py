#!/usr/bin/env python3
"""
Test script to verify that the mooncake_master entry point works correctly.
"""

import os
import sys
import subprocess
import time


def test_entry_point_installed():
    """Test that the entry point is installed and can be executed."""
    try:
        # Check if mooncake_master is in PATH
        result = subprocess.run(
            ["which", "mooncake_master"],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print("❌ mooncake_master entry point not found in PATH")
            return False

        print(f"✅ mooncake_master entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", "mooncake_client"],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print("❌ mooncake_client entry point not found in PATH")
            return False

        print(f"✅ mooncake_client entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", "transfer_engine_bench"],
            capture_output=True,
            text=True
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
        # Run mooncake_master on a non-default gRPC port. Do NOT enable the
        # embedded HTTP metadata server: the CI test-wheel-ubuntu job runs a
        # shared Python mooncake_http_metadata_server on the default port 8080
        # for the whole run_tests.sh suite, so enabling it here would collide
        # on 8080 and LOG(FATAL). This smoke test connects the client via
        # direct gRPC (--master_server_address), so it does not need the HTTP
        # metadata server.
        process = subprocess.Popen(
            ["mooncake_master", "--port=61351", "--max_threads=2"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Give it a moment to start
        time.sleep(2)

        # Check if process is running
        if process.poll() is None:
            print("✅ mooncake_master process started successfully")
            client_process = subprocess.Popen(
                ["mooncake_client", "--master_server_address=127.0.0.1:61351", "--port=61352"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
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
                print(f"❌ mooncake_client failed to start")
                print(f"stdout: {stdout.decode()}")
                print(f"stderr: {stderr.decode()}")
            # Terminate the process
            process.terminate()
            process.wait(timeout=5)
            print("✅ mooncake_master process terminated successfully")
            return True
        else:
            stdout, stderr = process.communicate()
            print(f"❌ mooncake_master process failed to start")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
    except Exception as e:
        print(f"❌ Error running mooncake_master: {e}")
        return False


if __name__ == "__main__":
    print("Testing mooncake_master entry point...")

    # Run tests
    entry_point_installed = test_entry_point_installed()

    if entry_point_installed:
        run_master_and_client_success = test_run_master_and_client()
    else:
        run_master_and_client_success = False

    # Print summary
    print("\nTest Summary:")
    print(f"Entry point installed: {'✅' if entry_point_installed else '❌'}")
    print(f"Run master successful: {'✅' if run_master_and_client_success else '❌'}")

    # Exit with appropriate status code
    if entry_point_installed and run_master_and_client_success:
        print("\nAll tests passed! 🎉")
        sys.exit(0)
    else:
        print("\nSome tests failed. 😢")
        sys.exit(1)
