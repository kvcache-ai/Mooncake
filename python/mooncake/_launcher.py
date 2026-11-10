"""Locate and launch fixed-backend native binaries bundled with the wheel.

The ``mooncake.cli`` / ``cli_client`` / ``cli_bench`` console entry points
launch C++ binaries. The ``mooncake-store-rs-*`` entry points below launch the
Store-RS binaries directly; they do not follow ``MOONCAKE_STORE_BACKEND``,
which selects only the Python Store facade. Package resources keep both command
families independent of their installation directory.
"""

from __future__ import annotations

import os
import stat
from importlib.resources import files
import sys


def locate(name: str) -> str:
    """
    Return the absolute path of a native binary bundled in this package.

    The named binary must live next to the package (``mooncake/<name>``). It is
    chmodded to be executable if needed. A clear :class:`FileNotFoundError` is
    raised when the installed package does not contain that build component.
    """
    path = files("mooncake") / name
    if not path.is_file():
        raise FileNotFoundError(
            f"{name!r} not found in the mooncake package; "
            "install a wheel built with the requested native component"
        )
    file_path = str(path)
    if not os.access(file_path, os.X_OK):
        os.chmod(
            file_path,
            os.stat(file_path).st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,
        )
    return file_path


def _exec_store_rs_binary(name: str) -> None:
    path = locate(name)
    os.execv(path, [path, *sys.argv[1:]])


def store_rs_client() -> None:
    _exec_store_rs_binary("mooncake-store-rs-client")


def store_rs_admin() -> None:
    _exec_store_rs_binary("mooncake-store-rs-admin")


def store_rs_bench() -> None:
    _exec_store_rs_binary("mooncake-store-rs-bench")
