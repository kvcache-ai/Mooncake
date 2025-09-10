#!/usr/bin/env python3
"""
Enhanced Network Bandwidth Monitor
Real-time monitoring of network interface throughput with 0.5s interval
"""

import time
import subprocess
import sys
from datetime import datetime


def get_network_stats_sar():
    """Get network stats using sar command (if available)"""
    try:
        result = subprocess.run(['sar', '-n', 'DEV', '1', '1'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            for line in lines:
                if 'eth0' in line or 'enp' in line:  # Common interface patterns
                    parts = line.split()
                    if len(parts) >= 6:
                        rx_mb = float(parts[4]) * 8 / 1000  # Convert KB/s to Mbps
                        tx_mb = float(parts[5]) * 8 / 1000
                        return rx_mb, tx_mb
    except (subprocess.TimeoutExpired, subprocess.CalledProcessError, FileNotFoundError):
        pass
    return 0, 0


def get_network_stats_proc():
    """Get network stats from /proc/net/dev"""
    try:
        with open('/proc/net/dev', 'r') as f:
            lines = f.readlines()
        
        interfaces = {}
        for line in lines[2:]:  # Skip header
            parts = line.split()
            if len(parts) >= 10:
                interface = parts[0].rstrip(':')
                if interface != 'lo':  # Skip loopback
                    rx_bytes = int(parts[1])
                    tx_bytes = int(parts[9])
                    interfaces[interface] = {'rx': rx_bytes, 'tx': tx_bytes}
        
        return interfaces
    except Exception:
        return {}


def monitor_bandwidth_realtime(interval=0.5, show_interfaces=False):
    """
    Real-time network bandwidth monitoring with customizable interval
    
    Args:
        interval: Monitoring interval in seconds (default: 0.5)
        show_interfaces: Show individual interface stats (default: False)
    """
    print("Enhanced Network Bandwidth Monitor")
    print(f"Monitoring interval: {interval}s")
    print("Press Ctrl+C to stop")
    print("-" * 80)
    
    if show_interfaces:
        print(f"{'Time':<12} {'Interface':<12} {'RX(MB/s)':<10} {'TX(MB/s)':<10} {'Total(MB/s)':<12}")
    else:
        print(f"{'Time':<12} {'RX(MB/s)':<10} {'TX(MB/s)':<10} {'Total(MB/s)':<12} {'Peak RX':<10} {'Peak TX':<10}")
    print("-" * 80)
    
    prev_stats = get_network_stats_proc()
    prev_time = time.time()
    iteration = 0
    max_rx = 0
    max_tx = 0
    
    try:
        while True:
            time.sleep(interval)
            
            current_stats = get_network_stats_proc()
            current_time = time.time()
            time_diff = current_time - prev_time
            
            if time_diff <= 0:
                continue
                
            # Get current timestamp
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            if show_interfaces:
                # Show individual interface statistics
                for interface in current_stats:
                    if interface in prev_stats:
                        rx_diff = current_stats[interface]['rx'] - prev_stats[interface]['rx']
                        tx_diff = current_stats[interface]['tx'] - prev_stats[interface]['tx']
                        
                        rx_mbps = (rx_diff / time_diff) / (1024 * 1024)
                        tx_mbps = (tx_diff / time_diff) / (1024 * 1024)
                        total_mbps = rx_mbps + tx_mbps
                        
                        if rx_mbps > 0.01 or tx_mbps > 0.01:  # Only show active interfaces
                            print(f"{timestamp:<12} {interface:<12} {rx_mbps:<10.2f} {tx_mbps:<10.2f} {total_mbps:<12.2f}")
            else:
                # Show aggregated statistics
                total_rx_diff = 0
                total_tx_diff = 0
                
                for interface in current_stats:
                    if interface in prev_stats:
                        rx_diff = current_stats[interface]['rx'] - prev_stats[interface]['rx']
                        tx_diff = current_stats[interface]['tx'] - prev_stats[interface]['tx']
                        total_rx_diff += rx_diff
                        total_tx_diff += tx_diff
                
                rx_mbps = (total_rx_diff / time_diff) / (1024 * 1024)
                tx_mbps = (total_tx_diff / time_diff) / (1024 * 1024)
                total_mbps = rx_mbps + tx_mbps
                
                # Track peak values
                max_rx = max(max_rx, rx_mbps)
                max_tx = max(max_tx, tx_mbps)
                
                print(f"{timestamp:<12} {rx_mbps:<10.2f} {tx_mbps:<10.2f} {total_mbps:<12.2f} {max_rx:<10.2f} {max_tx:<10.2f}")
            
            prev_stats = current_stats
            prev_time = current_time
            iteration += 1
            
    except KeyboardInterrupt:
        print(f"\nMonitoring stopped after {iteration} iterations")
        if not show_interfaces:
            print(f"Peak RX: {max_rx:.2f} MB/s, Peak TX: {max_tx:.2f} MB/s")


def monitor_bandwidth_duration(duration=10, interval=0.5):
    """
    Monitor network bandwidth for a specific duration
    
    Args:
        duration: Total monitoring duration in seconds
        interval: Monitoring interval in seconds
    """
    print(f"Network Bandwidth Monitor - Running for {duration} seconds (interval: {interval}s)")
    print(f"{'Time':<12} {'RX(MB/s)':<10} {'TX(MB/s)':<10} {'Total(MB/s)':<12}")
    print("-" * 50)
    
    prev_stats = get_network_stats_proc()
    prev_time = time.time()
    elapsed = 0
    
    while elapsed < duration:
        time.sleep(interval)
        elapsed += interval
        
        current_stats = get_network_stats_proc()
        current_time = time.time()
        time_diff = current_time - prev_time
        
        total_rx_diff = 0
        total_tx_diff = 0
        
        for interface in current_stats:
            if interface in prev_stats:
                rx_diff = current_stats[interface]['rx'] - prev_stats[interface]['rx']
                tx_diff = current_stats[interface]['tx'] - prev_stats[interface]['tx']
                total_rx_diff += rx_diff
                total_tx_diff += tx_diff
        
        rx_mbps = (total_rx_diff / time_diff) / (1024 * 1024)
        tx_mbps = (total_tx_diff / time_diff) / (1024 * 1024)
        total_mbps = rx_mbps + tx_mbps
        
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"{timestamp:<12} {rx_mbps:<10.2f} {tx_mbps:<10.2f} {total_mbps:<12.2f}")
        
        prev_stats = current_stats
        prev_time = current_time


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Network Bandwidth Monitor")
    parser.add_argument('-d', '--duration', type=int, default=0, 
                       help='Monitoring duration in seconds (0 for infinite)')
    parser.add_argument('-i', '--interval', type=float, default=0.5, 
                       help='Monitoring interval in seconds (default: 0.5)')
    parser.add_argument('--interfaces', action='store_true', 
                       help='Show individual interface statistics')
    
    args = parser.parse_args()
    
    try:
        if args.duration > 0:
            monitor_bandwidth_duration(args.duration, args.interval)
        else:
            monitor_bandwidth_realtime(args.interval, args.interfaces)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")
