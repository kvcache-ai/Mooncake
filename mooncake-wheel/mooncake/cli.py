#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_master.
"""

import os
import sys


def main():
    """
    Main entry point for the mooncake_master command.
    Simply runs the mooncake_master binary with all arguments passed through.
    """
    # Get the path to the mooncake_master binary
    package_dir = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(package_dir, "mooncake_master")
    
    # Make sure the binary is executable
    os.chmod(bin_path, 0o755)
    
    # Preserve the service PID so signals and wait() track native shutdown.
    os.execv(bin_path, [bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
