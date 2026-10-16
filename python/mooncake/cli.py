#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_master.
"""

import os
import sys

from mooncake._launcher import locate


def main():
    """
    Main entry point for the mooncake_master command.
    Simply runs the mooncake_master binary with all arguments passed through.
    """
    bin_path = locate("mooncake_master")

    # Preserve the CLI process ID so callers can reliably stop the server.
    os.execv(bin_path, [bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
