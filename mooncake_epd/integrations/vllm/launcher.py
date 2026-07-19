"""Run vLLM's CLI after explicitly installing Mooncake EPD hooks.

Usage from generated EPD workers::

    python -m mooncake_epd.integrations.vllm.launcher -- vllm serve MODEL ...
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Sequence

from .adapter import install_vllm_epd_adapter


def _command(argv: Sequence[str]) -> list[str]:
    values = list(argv)
    if values and values[0] == "--":
        values = values[1:]
    if values and values[0] == "vllm":
        values = values[1:]
    if not values:
        raise ValueError("expected a vLLM command, e.g. -- vllm serve MODEL")
    return values


def _enable_child_adapter_bootstrap() -> None:
    """Propagate the explicit adapter to vLLM's spawned EngineCore process.

    vLLM starts its EngineCore in a fresh Python interpreter.  The hooks
    installed in this launcher process are therefore not inherited, while the
    prompt-only Prefill path still needs the ``max_tokens=0`` compatibility
    hook in that child.  ``sitecustomize`` treats this opt-in marker as the
    explicit request to install the same adapter during child startup.
    """

    os.environ["MOONCAKE_EPD_ENABLE_VLLM_PATCHES"] = "1"


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--strict", action="store_true", help="require every EPD hook")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    command = _command(args.command)
    install_vllm_epd_adapter(strict=True if args.strict else None)
    _enable_child_adapter_bootstrap()
    from vllm.entrypoints.cli.main import main as vllm_main

    sys.argv = ["vllm", *command]
    vllm_main()


if __name__ == "__main__":
    main()
