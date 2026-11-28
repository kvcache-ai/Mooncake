"""
Entry point for running the benchmark as a module

Usage:
    python -m benchmarks.storage_benchmark_v1 --scenario toolagent --max-requests 100
"""

from .benchmark import main

if __name__ == "__main__":
    main()
