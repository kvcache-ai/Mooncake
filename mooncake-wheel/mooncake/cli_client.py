#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_client.
"""

import os
import stat
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
    if not os.access(bin_path, os.X_OK):
        st = os.stat(bin_path)
        os.chmod(bin_path, st.st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    # Run the binary with all arguments passed through
    return subprocess.call([bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
