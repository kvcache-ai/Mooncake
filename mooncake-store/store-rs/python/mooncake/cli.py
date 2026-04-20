from __future__ import annotations

import sys

from . import __edition__, __version__
from ._build_info import format_version_output
from ._runtime import execute_packaged_binary, invoked_binary_name


def main() -> int:
    argv = sys.argv[1:]
    program_name = invoked_binary_name()
    if argv in (["--version"], ["-V"]):
        print(
            format_version_output(
                "mooncake",
                __version__,
                __edition__,
                verbose=False,
            )
        )
        return 0
    if argv == ["-v"]:
        print(
            format_version_output(
                "mooncake",
                __version__,
                __edition__,
                verbose=True,
            )
        )
        return 0
    return execute_packaged_binary(program_name, argv)


if __name__ == "__main__":
    raise SystemExit(main())
