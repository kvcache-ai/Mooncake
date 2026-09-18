#!/usr/bin/env python3
"""
Minimal CLI module for transfer_engine_bench.
"""

import subprocess
import sys

from mooncake._launcher import locate


def main():
    """
    Main entry point for the transfer_engine_bench command.
    Simply runs the transfer_engine_bench binary with all arguments passed through.
    """
    bin_path = locate("transfer_engine_bench")

    # Run the binary with all arguments passed through
    return subprocess.call([bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
