"""Strict/no-fallback runtime switches for Mooncake EPD.

The RFC evaluation path must be able to prove that no compatibility fallback
hid a broken EPD data plane.  Keep the switch tiny and dependency-free so it can
be imported from sitecustomize, vLLM workers, the proxy, and unit tests.
"""

from __future__ import annotations

import os
from typing import Any


TRUE_VALUES = {"1", "true", "yes", "on", "strict"}


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() in TRUE_VALUES


def strict_no_fallback_enabled(extra: Any | None = None) -> bool:
    """Return whether strict no-fallback mode is active.

    `extra` may be a TransferPolicy.extra / request metadata dict.  Explicit
    per-call strict flags win over the process environment.
    """

    if isinstance(extra, dict):
        for key in (
            "strict_no_fallback",
            "no_fallback",
            "performance_strict",
            "epd_strict",
        ):
            if key in extra:
                return str(extra.get(key)).strip().lower() in TRUE_VALUES
    return env_flag("MOONCAKE_EPD_STRICT", False) or env_flag(
        "MOONCAKE_EPD_STRICT_NO_FALLBACK", False
    )


def require_not_fallback(message: str, *, extra: Any | None = None) -> None:
    if strict_no_fallback_enabled(extra):
        raise RuntimeError(message)
