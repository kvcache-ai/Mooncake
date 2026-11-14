#!/usr/bin/env python3
"""Reject cross-module inline YLT exports on ELF Python extensions."""

from __future__ import annotations

import itertools
from pathlib import Path
import subprocess
import sys


INIT_SYMBOLS = {
    "engine": "PyInit_engine",
    "_store": "PyInit__store",
    "_conductor": "PyInit__conductor",
}
INLINE_NAMESPACES = ("asio", "coro_io", "coro_rpc")


def defined_symbols(path: Path) -> dict[str, str]:
    output = subprocess.check_output(
        ["nm", "-D", "--defined-only", str(path)], text=True
    )
    symbols = {}
    for line in output.splitlines():
        fields = line.split()
        if len(fields) >= 3:
            symbols[fields[2]] = fields[1]
    return symbols


def main(paths: list[Path]) -> int:
    if len(paths) < 2:
        raise SystemExit("expected at least two Python extension paths")

    modules = {path.name.split(".", 1)[0]: defined_symbols(path) for path in paths}
    failures = []
    for name, path in zip(modules, paths, strict=True):
        init = INIT_SYMBOLS[name]
        if init not in modules[name]:
            failures.append(f"{path}: missing required Python entry point {init}")

    for (left_name, left), (right_name, right) in itertools.combinations(
        modules.items(), 2
    ):
        left_inline = {
            name
            for name, kind in left.items()
            if kind in {"T", "W"} and any(ns in name for ns in INLINE_NAMESPACES)
        }
        right_inline = {
            name
            for name, kind in right.items()
            if kind in {"T", "W"} and any(ns in name for ns in INLINE_NAMESPACES)
        }
        shared = sorted(left_inline & right_inline)
        print(f"{left_name} <-> {right_name}: {len(shared)} shared inline YLT exports")
        if shared:
            failures.append(
                f"{left_name} and {right_name} interpose inline YLT symbols: "
                + ", ".join(shared[:5])
            )

    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main([Path(arg) for arg in sys.argv[1:]]))
