#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_client.
"""

import os
import sys
import subprocess


def main():
    """
    Main entry point for the mooncake_client command.
    Simply runs the mooncake_client binary with all arguments passed through.
    """
    # Get the path to the mooncake_client binary
    package_dir = os.path.dirname(os.path.abspath(__file__))
    bin_path = os.path.join(package_dir, "mooncake_client")

    # Make sure the binary is executable
    os.chmod(bin_path, 0o755)

    # Run the binary with all arguments passed through
    return subprocess.call([bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
