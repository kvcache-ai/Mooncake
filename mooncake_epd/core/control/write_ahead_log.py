"""Small JSON-lines write-ahead log helper.

The repo's control/data-plane modules need a lightweight durable journal for:

- A2A handoff prepare/commit/rollback recovery
- Offload / restore recovery
- Optional MM-store event tracing

This helper keeps the implementation dependency-free and fsync-backed so it
can be used in unit tests and local real-service runs without introducing an
external database.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional


class JsonLineWAL:
    """Append-only JSON-lines WAL with best-effort atomic durability."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    def append(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        entry = dict(record)
        entry.setdefault("ts_unix", time.time())
        entry.setdefault("ts_mono", time.monotonic())
        payload = json.dumps(entry, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with self.path.open("a", encoding="utf-8") as f:
                f.write(payload)
                f.write("\n")
                f.flush()
                os.fsync(f.fileno())
        return entry

    def read_all(self) -> List[Dict[str, Any]]:
        if not self.path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        with self._lock:
            for line in self.path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line:
                    continue
                rows.append(json.loads(line))
        return rows

    def latest_by(self, key: str) -> Dict[str, Dict[str, Any]]:
        latest: Dict[str, Dict[str, Any]] = {}
        for row in self.read_all():
            value = row.get(key)
            if value is None:
                continue
            latest[str(value)] = row
        return latest

    def rewrite(self, records: Iterable[Mapping[str, Any]]) -> None:
        serialized = [
            json.dumps(dict(record), ensure_ascii=False, sort_keys=True)
            for record in records
        ]
        with self._lock:
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text("\n".join(serialized) + ("\n" if serialized else ""), encoding="utf-8")
            os.replace(tmp, self.path)

    def clear(self) -> None:
        with self._lock:
            if self.path.exists():
                self.path.unlink()


def merge_wal_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    group_key: str,
) -> Dict[str, Dict[str, Any]]:
    """Merge rows by `group_key`, later keys overriding earlier ones."""

    merged: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        value = row.get(group_key)
        if value is None:
            continue
        key = str(value)
        current = merged.get(key, {}).copy()
        current.update(dict(row))
        merged[key] = current
    return merged
