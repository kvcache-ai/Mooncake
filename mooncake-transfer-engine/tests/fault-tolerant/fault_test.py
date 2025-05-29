import subprocess
import time
import signal
import sys
import os
from typing import Optional

def start_process(cmd: list[str]) -> subprocess.Popen:
    """Start a process and return the Popen object."""
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

def monitor_output(process: subprocess.Popen, name: str, timeout: int = 5) -> bool:
    """Monitor process output for a given timeout period."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if process.poll() is not None:
            print(f"{name} process exited with code {process.returncode}")
            return False
        
        # Read output without blocking
        if process.stdout is not None:
            output = process.stdout.readline()
            if output:
                print(f"{name}: {output.strip()}")
        
        time.sleep(0.1)
    return True

def main():
    # Start server
    print("Starting server...")
    server_process = start_process([sys.executable, "server_test.py"])
    
    # Wait for server to initialize
    time.sleep(2)
    
    # Start client
    print("Starting client...")
    client_process = start_process([sys.executable, "client_test.py"])
    
    # Monitor both processes for 5 seconds
    print("Monitoring initial behavior...")
    if not monitor_output(server_process, "Server") or not monitor_output(client_process, "Client"):
        print("Initial monitoring failed - one of the processes exited unexpectedly")
        cleanup(server_process, client_process)
        return
    
    # Kill server
    print("\nKilling server process...")
    server_process.terminate()
    server_process.wait(timeout=5)
    
    # Monitor client behavior after server death
    print("\nMonitoring client behavior after server death...")
    client_alive = monitor_output(client_process, "Client", timeout=10)
    
    if client_alive:
        print("\nClient is still running after server death - it may be blocked")
    else:
        print("\nClient exited after server death - normal behavior")
    
    # Cleanup
    cleanup(server_process, client_process)

def cleanup(server_process: Optional[subprocess.Popen], client_process: Optional[subprocess.Popen]):
    """Clean up processes."""
    for proc, name in [(server_process, "Server"), (client_process, "Client")]:
        if proc and proc.poll() is None:
            print(f"Terminating {name} process...")
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                print(f"Force killing {name} process...")
                proc.kill()

if __name__ == "__main__":
    main() 