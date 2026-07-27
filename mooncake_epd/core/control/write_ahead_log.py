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

    def __init__(
        self,
        path: str | os.PathLike[str],
        *,
        fsync_interval_s: float = 0.0,
        max_pending_records: int = 1,
    ):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._fsync_interval_s = max(0.0, float(fsync_interval_s))
        self._max_pending_records = max(1, int(max_pending_records))
        self._records = 0
        self._fsyncs = 0
        self._deferred_records = 0
        self._pending_records = 0
        self._last_fsync_mono = time.monotonic()
        self._closed = False
        self._stop = threading.Event()
        self._flusher: Optional[threading.Thread] = None
        if self._fsync_interval_s > 0.0:
            self._flusher = threading.Thread(
                target=self._flush_loop,
                daemon=True,
                name=f"jsonl-wal-{self.path.name}",
            )
            self._flusher.start()

    def append(self, record: Mapping[str, Any]) -> Dict[str, Any]:
        entry = dict(record)
        entry.setdefault("ts_unix", time.time())
        entry.setdefault("ts_mono", time.monotonic())
        payload = json.dumps(entry, ensure_ascii=False, sort_keys=True)
        with self._lock:
            if self._closed:
                raise RuntimeError(f"WAL is closed: {self.path}")
            with self.path.open("a", encoding="utf-8") as f:
                f.write(payload)
                f.write("\n")
                f.flush()
                self._records += 1
                self._pending_records += 1
                should_fsync = (
                    self._fsync_interval_s <= 0.0
                    or self._max_pending_records <= 1
                    or self._pending_records >= self._max_pending_records
                    or (time.monotonic() - self._last_fsync_mono)
                    >= self._fsync_interval_s
                )
                if should_fsync:
                    self._fsync_file_locked(f)
                else:
                    self._deferred_records += 1
        return entry

    def flush(self) -> None:
        """Force pending records to stable storage."""

        with self._lock:
            if self._pending_records <= 0 or not self.path.exists():
                return
            with self.path.open("a", encoding="utf-8") as f:
                f.flush()
                self._fsync_file_locked(f)

    def close(self) -> None:
        """Stop interval flushing and durably commit all pending records."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
        self._stop.set()
        flusher = self._flusher
        if flusher is not None and flusher is not threading.current_thread():
            flusher.join(timeout=max(1.0, self._fsync_interval_s * 2.0))
        self.flush()

    def stats(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "records": self._records,
                "fsyncs": self._fsyncs,
                "deferred_records": self._deferred_records,
                "pending_records": self._pending_records,
                "fsync_interval_s": self._fsync_interval_s,
                "max_pending_records": self._max_pending_records,
            }

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
            with tmp.open("w", encoding="utf-8") as f:
                f.write("\n".join(serialized) + ("\n" if serialized else ""))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
            self._fsyncs += 1
            self._pending_records = 0
            self._last_fsync_mono = time.monotonic()

    def clear(self) -> None:
        with self._lock:
            if self.path.exists():
                self.path.unlink()
            self._pending_records = 0

    def _flush_loop(self) -> None:
        while not self._stop.wait(self._fsync_interval_s):
            self.flush()

    def _fsync_file_locked(self, file_obj) -> None:
        os.fsync(file_obj.fileno())
        self._fsyncs += 1
        self._pending_records = 0
        self._last_fsync_mono = time.monotonic()


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
