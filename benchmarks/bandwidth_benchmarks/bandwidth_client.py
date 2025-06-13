import numpy as np
import zmq
import time
import json
import argparse
from transfer_engine import MooncakeTransferEngine

def format_bytes(bytes_value):
    """Convert bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_value < 1024.0:
            return f"{bytes_value:.2f} {unit}"
        bytes_value /= 1024.0
    return f"{bytes_value:.2f} TB"

def format_bandwidth(bps):
    """Convert bits per second to human readable format"""
    for unit in ['bps', 'Kbps', 'Mbps', 'Gbps']:
        if bps < 1000.0:
            return f"{bps:.2f} {unit}"
        bps /= 1000.0
    return f"{bps:.2f} Tbps"

def run_bandwidth_test(client_engine, server_session_id, client_ptr, server_ptr, 
                      transfer_size, num_iterations, warmup_iterations=5):
    """Run bandwidth test with specified parameters"""
    
    print(f"\n=== Bandwidth Test Configuration ===")
    print(f"Transfer size: {format_bytes(transfer_size)}")
    print(f"Warmup iterations: {warmup_iterations}")
    print(f"Test iterations: {num_iterations}")
    print(f"Total data to transfer: {format_bytes(transfer_size * num_iterations)}")
    
    # Warmup phase
    print(f"\n--- Warmup Phase ---")
    for i in range(warmup_iterations):
        ret = client_engine.transfer_sync(
            server_session_id,
            client_ptr,
            server_ptr,
            transfer_size
        )
        if ret < 0:
            raise RuntimeError(f"Warmup transfer {i+1} failed")
        print(f"Warmup {i+1}/{warmup_iterations} completed")
    
    # Actual benchmark phase
    print(f"\n--- Benchmark Phase ---")
    transfer_times = []
    
    for i in range(num_iterations):
        start_time = time.perf_counter()
        
        ret = client_engine.transfer_sync(
            server_session_id,
            client_ptr,
            server_ptr,
            transfer_size
        )
        
        end_time = time.perf_counter()
        
        if ret < 0:
            raise RuntimeError(f"Transfer {i+1} failed")
        
        transfer_time = end_time - start_time
        transfer_times.append(transfer_time)
        
        # Calculate current bandwidth
        current_bandwidth = (transfer_size * 8) / transfer_time  # bits per second
        # print(f"Transfer {i+1}/{num_iterations}: {transfer_time*1000:.3f} ms, "
            #   f"Bandwidth: {format_bandwidth(current_bandwidth)}")
    
    return transfer_times

def calculate_statistics(transfer_times, transfer_size):
    """Calculate bandwidth statistics"""
    total_time = sum(transfer_times)
    total_data = transfer_size * len(transfer_times)
    
    # Calculate bandwidth in bits per second
    total_bandwidth_bps = (total_data * 8) / total_time
    
    # Calculate individual bandwidths
    individual_bandwidths = [(transfer_size * 8) / t for t in transfer_times]
    
    # Statistics
    avg_bandwidth = np.mean(individual_bandwidths)
    min_bandwidth = np.min(individual_bandwidths)
    max_bandwidth = np.max(individual_bandwidths)
    std_bandwidth = np.std(individual_bandwidths)
    
    # Time statistics
    avg_time = np.mean(transfer_times)
    min_time = np.min(transfer_times)
    max_time = np.max(transfer_times)
    
    return {
        'total_time': total_time,
        'total_data': total_data,
        'total_bandwidth_bps': total_bandwidth_bps,
        'avg_bandwidth': avg_bandwidth,
        'min_bandwidth': min_bandwidth,
        'max_bandwidth': max_bandwidth,
        'std_bandwidth': std_bandwidth,
        'avg_time': avg_time,
        'min_time': min_time,
        'max_time': max_time,
        'transfer_times': transfer_times
    }

def main():
    parser = argparse.ArgumentParser(description='Mooncake Transfer Engine Bandwidth Benchmark')
    parser.add_argument('--size', type=int, default=1024*1024*32, 
                       help='Transfer size in bytes (default: 1MB)')
    parser.add_argument('--iterations', type=int, default=1000,
                       help='Number of test iterations (default: 100)')
    parser.add_argument('--warmup', type=int, default=5,
                       help='Number of warmup iterations (default: 5)')
    parser.add_argument('--server-host', type=str, default='localhost',
                       help='Server hostname (default: localhost)')
    
    args = parser.parse_args()
    
    # Initialize ZMQ context and socket
    context = zmq.Context()
    socket = context.socket(zmq.PULL)
    socket.connect(f"tcp://{args.server_host}:5555")
    
    # Wait for buffer info from server
    print("Waiting for server buffer information...")
    buffer_info = socket.recv_json()
    server_session_id = str(buffer_info["session_id"])
    server_ptr = int(buffer_info["ptr"])
    server_len = int(buffer_info["len"])
    print(f"Received server info - Session ID: {server_session_id}")
    print(f"Server buffer address: {server_ptr}, length: {format_bytes(server_len)}")
    
    # Initialize client engine
    client_engine = MooncakeTransferEngine(
        hostname="localhost:10011",
        gpu_id=0,  # Using GPU 0
        ib_device=None  # No specific IB device
    )
    
    # Allocate and initialize client buffer
    client_buffer = np.ones(args.size, dtype=np.uint8)  # Fill with ones
    client_ptr = client_buffer.ctypes.data
    client_len = client_buffer.nbytes
    
    # Register memory with Mooncake
    client_engine.register(client_ptr, client_len)
    print(f"Client initialized with session ID: {client_engine.get_session_id()}")
    print(f"Client buffer size: {format_bytes(client_len)}")
    
    # Verify buffer sizes
    transfer_size = min(client_len, server_len, args.size)
    if transfer_size != args.size:
        print(f"Warning: Requested size {format_bytes(args.size)} not available, "
              f"using {format_bytes(transfer_size)}")
    
    try:
        # Run bandwidth test
        transfer_times = run_bandwidth_test(
            client_engine, server_session_id, client_ptr, server_ptr,
            transfer_size, args.iterations, args.warmup
        )
        
        # Calculate and display results
        stats = calculate_statistics(transfer_times, transfer_size)
        
        print(f"\n=== Bandwidth Test Results ===")
        print(f"Total transfers: {len(transfer_times)}")
        print(f"Total data transferred: {format_bytes(stats['total_data'])}")
        print(f"Total time: {stats['total_time']:.3f} seconds")
        print(f"Average transfer time: {stats['avg_time']*1000:.3f} ms")
        print(f"Min transfer time: {stats['min_time']*1000:.3f} ms")
        print(f"Max transfer time: {stats['max_time']*1000:.3f} ms")
        print(f"\nBandwidth Statistics:")
        print(f"  Average: {format_bandwidth(stats['avg_bandwidth'])}")
        print(f"  Minimum: {format_bandwidth(stats['min_bandwidth'])}")
        print(f"  Maximum: {format_bandwidth(stats['max_bandwidth'])}")
        print(f"  Std Dev: {format_bandwidth(stats['std_bandwidth'])}")
        print(f"  Overall: {format_bandwidth(stats['total_bandwidth_bps'])}")
        
    except Exception as e:
        print(f"Benchmark failed: {e}")
        raise
    finally:
        # Cleanup
        client_engine.deregister(client_ptr)
        socket.close()
        context.term()

if __name__ == "__main__":
    main() 