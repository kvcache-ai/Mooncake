from __future__ import annotations

import sys

from ._runtime import execute_packaged_binary, invoked_binary_name


def main() -> int:
    return execute_packaged_binary(invoked_binary_name(), sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
