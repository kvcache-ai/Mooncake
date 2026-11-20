#!/usr/bin/env python3
"""
Test script to verify that the mooncake_master entry point works correctly.
"""

import os
import sys
import subprocess
import time

BACKEND = os.environ.get("MOONCAKE_STORE_BACKEND", "centralized")
COMMANDS = {
    "centralized": ("mooncake_master", "mooncake_client"),
    "p2p": ("mooncake_p2p_master", "mooncake_p2p_client"),
}


def test_entry_point_installed():
    """Test that the entry point is installed and can be executed."""
    try:
        master, client = COMMANDS[BACKEND]
        # Check that the selected backend's commands are installed.
        result = subprocess.run(
            ["which", master],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"❌ {master} entry point not found in PATH")
            return False

        print(f"✅ {master} entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", client],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print(f"❌ {client} entry point not found in PATH")
            return False

        print(f"✅ {client} entry point found at: {result.stdout.strip()}")
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


def test_run_p2p_master_and_client():
    """Test the P2P master and client entry points."""
    try:
        # Reuse the metadata server on 8080; isolate this Master's RPC and metrics.
        process = subprocess.Popen(
            ["mooncake_p2p_master", "--rpc_port=61351", "--rpc_thread_num=2",
             "--metrics_port=19003"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

        # Give it a moment to start
        time.sleep(2)

        # Check if process is running
        if process.poll() is None:
            print("✅ mooncake_master process started successfully")
            tiered_backend_config = (
                '{"tiers":[{"type":"DRAM","capacity":"128MB",'
                '"priority":10,"allocator_type":"OFFSET"}]}'
            )
            client_process = subprocess.Popen(
                ["mooncake_p2p_client", "--master_server_address=127.0.0.1:61351",
                 "--port=61352", f"--tiered_backend_config={tiered_backend_config}",
                 "--client_rpc_port=0", "--enable_http_server=false"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Give the client some time to connect
            time.sleep(2)
            client_started = client_process.poll() is None
            if client_started:
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
            return client_started
        else:
            stdout, stderr = process.communicate()
            print(f"❌ mooncake_master process failed to start")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
    except Exception as e:
        print(f"❌ Error running mooncake_master: {e}")
        return False

def test_run_centralized_master_and_client():
    """Test the centralized master and client entry points."""
    try:
        # Reuse the metadata server on 8080; isolate this Master's RPC and metrics.
        process = subprocess.Popen(
            ["mooncake_master", "--port=61351", "--max_threads=2",
             "--metrics_port=19003"],
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
            client_started = client_process.poll() is None
            if client_started:
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
            return client_started
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

    run_master_and_client_success = entry_point_installed and {
        "centralized": test_run_centralized_master_and_client,
        "p2p": test_run_p2p_master_and_client,
    }[BACKEND]()

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
