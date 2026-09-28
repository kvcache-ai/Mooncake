#!/usr/bin/env python3
"""
Minimal CLI module for mooncake_client.
"""

import os
import sys

from mooncake._launcher import locate


def main():
    """
    Main entry point for the mooncake_client command.
    Simply runs the mooncake_client binary with all arguments passed through.
    """
    bin_path = locate("mooncake_client")

    # Preserve the CLI process ID so callers can reliably stop the client.
    os.execv(bin_path, [bin_path] + sys.argv[1:])


if __name__ == "__main__":
    sys.exit(main())
