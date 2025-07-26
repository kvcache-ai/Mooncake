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


def test_run_master():
    """Test running the master service through the entry point."""
    try:
        # Run mooncake_master with a non-default port to avoid conflicts
        process = subprocess.Popen(
            ["mooncake_master", "--port=61351", "--conf_path=/home/runner/work/Mooncake/Mooncake/mooncake-store/conf/master.yaml","--enable_gc=false"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Give it a moment to start
        time.sleep(2)
        
        # Check if process is running
        if process.poll() is None:
            print("✅ mooncake_master process started successfully")
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
        run_master_success = test_run_master()
    else:
        run_master_success = False
    
    # Print summary
    print("\nTest Summary:")
    print(f"Entry point installed: {'✅' if entry_point_installed else '❌'}")
    print(f"Run master successful: {'✅' if run_master_success else '❌'}")
    
    # Exit with appropriate status code
    if entry_point_installed and run_master_success:
        print("\nAll tests passed! 🎉")
        sys.exit(0)
    else:
        print("\nSome tests failed. 😢")
        sys.exit(1)
