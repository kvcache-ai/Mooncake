from __future__ import annotations

import sys

from . import __edition__, __version__
from ._runtime import execute_packaged_binary, invoked_binary_name


def main() -> int:
    if sys.argv[1:] in (["--version"], ["-V"]):
        print(f"mooncake {__version__} ({__edition__})")
        return 0
    return execute_packaged_binary(invoked_binary_name(), sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
