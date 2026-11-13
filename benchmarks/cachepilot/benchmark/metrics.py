"""Common utility helpers for CachePilot benchmarks."""

from __future__ import annotations

import csv
import logging
import math
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union


def ensure_dir(path: Union[str, Path]) -> Path:
    """Create directory (and parents) if missing; return Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def percentile(values: Sequence[float], p: float) -> Optional[float]:
    """Return the p-th percentile (0-100). Empty input -> None."""
    if not values:
        return None
    if p < 0 or p > 100:
        raise ValueError(f"percentile p must be in [0, 100], got {p}")
    data = sorted(float(v) for v in values)
    if len(data) == 1:
        return data[0]
    rank = (p / 100.0) * (len(data) - 1)
    lo = int(math.floor(rank))
    hi = int(math.ceil(rank))
    if lo == hi:
        return data[lo]
    weight = rank - lo
    return data[lo] * (1.0 - weight) + data[hi] * weight


def safe_mean(values: Sequence[float]) -> Optional[float]:
    """Arithmetic mean; empty input -> None."""
    if not values:
        return None
    return sum(float(v) for v in values) / len(values)


def parse_int_list(text: str) -> List[int]:
    """Parse comma-separated integers, e.g. '1,4,8'."""
    text = (text or "").strip()
    if not text:
        return []
    return [int(x.strip()) for x in text.split(",") if x.strip()]


def parse_float_list(text: str) -> List[float]:
    """Parse comma-separated floats, e.g. '0,0.25,0.5'."""
    text = (text or "").strip()
    if not text:
        return []
    return [float(x.strip()) for x in text.split(",") if x.strip()]


def write_csv(
    path: Union[str, Path],
    rows: Iterable[dict],
    fieldnames: Sequence[str],
    append: bool = False,
) -> Path:
    """Write rows to CSV. Creates parent dirs. Returns path."""
    out = Path(path)
    ensure_dir(out.parent)
    mode = "a" if append else "w"
    file_exists = out.exists() and out.stat().st_size > 0
    write_header = not (append and file_exists)
    with out.open(mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames), extrasaction="ignore")
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})
    return out


def setup_logger(
    log_file: Optional[Union[str, Path]] = None,
    level: str = "INFO",
    name: str = "cachepilot",
) -> logging.Logger:
    """Configure and return a logger with console (+ optional file) handlers."""
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.propagate = False

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    if log_file is not None:
        lf = Path(log_file)
        ensure_dir(lf.parent)
        fh = logging.FileHandler(lf, encoding="utf-8")
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger
