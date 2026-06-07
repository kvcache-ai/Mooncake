"""Align Python logging with Mooncake C++ glog via MC_LOG_LEVEL."""

from __future__ import annotations

import logging
import os

_LEVEL_MAP = {
    "TRACE": logging.DEBUG,
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.CRITICAL,
}


def configure_logging_from_env() -> None:
    """Apply MC_LOG_LEVEL to the root Python logger (idempotent)."""
    raw = os.environ.get("MC_LOG_LEVEL")
    if not raw:
        return
    level_name = raw.strip().upper()
    level = _LEVEL_MAP.get(level_name)
    if level is None:
        logging.getLogger(__name__).warning(
            "Unknown MC_LOG_LEVEL=%r; expected TRACE, INFO, WARNING, or ERROR",
            raw,
        )
        return
    root = logging.getLogger()
    if root.level == level and root.handlers:
        return
    logging.basicConfig(
        level=level,
        format="%(levelname)s %(asctime)s %(name)s: %(message)s",
        force=True,
    )
    root.setLevel(level)
