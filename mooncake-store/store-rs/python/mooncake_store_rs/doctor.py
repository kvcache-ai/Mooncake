"""Report which store backend ``import mooncake.store`` resolves to.

The redirect in `mooncake_store_rs._shim` is deliberately invisible at the call
site, so this exists to make it observable:

    python -m mooncake_store_rs.doctor
"""

from __future__ import annotations

import importlib.util
import os
import sys
from importlib.machinery import PathFinder

from . import _shim


def _upstream_origin() -> str:
    """Locate the upstream package without going through the redirect."""
    spec = PathFinder.find_spec("mooncake")
    if spec is None:
        return "not installed"
    return spec.origin or "namespace package"


def _resolved_store_origin() -> str:
    try:
        spec = importlib.util.find_spec("mooncake.store")
    except ImportError as exc:
        return f"unresolvable ({exc})"
    if spec is None:
        return "not found"
    return spec.origin or "unknown"


def main() -> int:
    requested = _shim.backend_selected()
    installed = _shim.active_backend() == "store-rs"

    print(f"{_shim.BACKEND_ENV}={os.environ.get(_shim.BACKEND_ENV, '')!r}")
    print(f"backend requested:    {'store-rs' if requested else 'upstream (default)'}")
    print(f"redirect installed:   {installed}")
    print(f"upstream mooncake:    {_upstream_origin()}")
    print(f"mooncake.store from:  {_resolved_store_origin()}")

    if requested and not installed:
        print(
            f"\nwarning: store-rs is requested but the redirect is not installed.\n"
            f"The .pth hook runs at interpreter startup, so setting "
            f"{_shim.BACKEND_ENV} from inside Python is too late -- export it "
            f"before launching the process.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
