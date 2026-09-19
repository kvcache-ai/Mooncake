"""Shared connector worker metrics sink and reader.

The vLLM KV connector runs inside separate worker processes, while the
OpenAI-compatible proxy exposes `/metrics` from a different process. This
module provides a small, real process-bridging metrics plane:

- connector workers accumulate layered/direct-transfer counters locally;
- each worker writes an atomic JSON snapshot to a shared directory;
- the proxy aggregates all worker snapshots on demand.

This keeps observability tied to the real connector path rather than to
control-plane configuration alone.
"""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from .vllm_transfer_primitives import LayeredTransferWorkerMeta


def _sanitize_component(text: Any) -> str:
    raw = "unknown" if text is None else str(text).strip()
    raw = raw or "unknown"
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)


@dataclass(frozen=True)
class ConnectorMetricsAggregate:
    workers: int
    updated_at_max: float
    totals: LayeredTransferWorkerMeta
    path_totals: Dict[str, LayeredTransferWorkerMeta] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "workers": int(self.workers),
            "updated_at_max": float(self.updated_at_max),
            "totals": self.totals.to_dict(),
            "path_totals": {
                str(path): meta.to_dict()
                for path, meta in sorted(self.path_totals.items())
            },
        }


class ConnectorMetricsSink:
    """Per-worker cumulative metrics writer backed by an atomic JSON file."""

    def __init__(
        self,
        metrics_dir: str | os.PathLike[str] | None,
        *,
        engine_id: str,
        role: str,
        hostname: str = "",
        rpc_port: int | None = None,
        tp_rank: int | None = None,
        pid: int | None = None,
        flush_interval_s: float = 0.0,
        max_pending_records: int = 1,
    ):
        self._lock = threading.RLock()
        self._totals = LayeredTransferWorkerMeta()
        self._path_totals: Dict[str, LayeredTransferWorkerMeta] = {}
        self._enabled = bool(metrics_dir)
        self._dir = Path(metrics_dir).expanduser() if metrics_dir else None
        self._path: Optional[Path] = None
        self._flush_interval_s = max(0.0, float(flush_interval_s))
        self._max_pending_records = max(1, int(max_pending_records))
        self._pending_records = 0
        self._last_flush_mono = time.monotonic()
        self._io_stats = {
            "records": 0,
            "deferred_records": 0,
            "flushes": 0,
        }
        self._identity = {
            "engine_id": str(engine_id),
            "role": str(role),
            "hostname": str(hostname or ""),
            "rpc_port": None if rpc_port is None else int(rpc_port),
            "tp_rank": None if tp_rank is None else int(tp_rank),
            "pid": int(pid or os.getpid()),
        }
        if self._enabled and self._dir is not None:
            self._dir.mkdir(parents=True, exist_ok=True)
            rank_component = _sanitize_component(tp_rank)
            self._worker_glob_prefix = (
                f"{_sanitize_component(engine_id)}-"
                f"{_sanitize_component(role)}-"
                f"rank{rank_component}-"
            )
            filename = (
                f"{self._worker_glob_prefix}"
                f"pid{_sanitize_component(self._identity['pid'])}.json"
            )
            self._path = self._dir / filename
            self._cleanup_stale_worker_files()

    @property
    def enabled(self) -> bool:
        return self._enabled and self._path is not None

    @property
    def path(self) -> Optional[Path]:
        return self._path

    @property
    def io_stats(self) -> Dict[str, int]:
        with self._lock:
            return {str(key): int(value) for key, value in self._io_stats.items()}

    def record(
        self,
        meta: LayeredTransferWorkerMeta | None,
        *,
        path_totals: Optional[Dict[str, LayeredTransferWorkerMeta]] = None,
        force: bool = False,
    ) -> None:
        has_total_delta = meta is not None and not meta.is_empty()
        has_path_delta = any(
            delta is not None and not delta.is_empty()
            for delta in dict(path_totals or {}).values()
        )
        if not self.enabled or (not has_total_delta and not has_path_delta):
            return
        with self._lock:
            if has_total_delta and meta is not None:
                self._totals = self._totals.aggregate(meta)
            for path, delta in dict(path_totals or {}).items():
                if delta is None or delta.is_empty():
                    continue
                bucket = _sanitize_component(path).upper()
                existing = self._path_totals.get(bucket, LayeredTransferWorkerMeta())
                self._path_totals[bucket] = existing.aggregate(delta)
            self._pending_records += 1
            self._io_stats["records"] += 1
            elapsed = time.monotonic() - self._last_flush_mono
            should_flush = (
                force
                or self._flush_interval_s <= 0.0
                or self._pending_records >= self._max_pending_records
                or elapsed >= self._flush_interval_s
            )
            if should_flush:
                self._flush_locked()
            else:
                self._io_stats["deferred_records"] += 1

    def flush(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if self._pending_records <= 0:
                if self._path is not None and self._path.exists():
                    return
                if self._totals.is_empty() and not self._path_totals:
                    return
            self._flush_locked()

    def close(self) -> None:
        self.flush()

    def snapshot(self) -> ConnectorMetricsAggregate:
        with self._lock:
            return ConnectorMetricsAggregate(
                workers=1 if (not self._totals.is_empty() or self._path_totals) else 0,
                updated_at_max=time.time(),
                totals=LayeredTransferWorkerMeta.from_dict(self._totals.to_dict()),
                path_totals={
                    str(path): LayeredTransferWorkerMeta.from_dict(meta.to_dict())
                    for path, meta in self._path_totals.items()
                },
            )

    def _flush_locked(self) -> None:
        assert self._path is not None
        payload = {
            "version": 1,
            "identity": dict(self._identity),
            "updated_at": time.time(),
            "totals": self._totals.to_dict(),
            "path_totals": {
                str(path): meta.to_dict()
                for path, meta in sorted(self._path_totals.items())
            },
            "io": {
                **self.io_stats,
                "flushes": int(self._io_stats["flushes"]) + 1,
            },
        }
        tmp_path = self._path.with_suffix(f"{self._path.suffix}.tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, sort_keys=True),
            encoding="utf-8",
        )
        os.replace(tmp_path, self._path)
        self._pending_records = 0
        self._last_flush_mono = time.monotonic()
        self._io_stats["flushes"] += 1

    def _cleanup_stale_worker_files(self) -> None:
        assert self._dir is not None
        assert self._path is not None
        prefix = getattr(self, "_worker_glob_prefix", "")
        if not prefix:
            return
        for path in self._dir.glob(f"{prefix}*.json"):
            if path == self._path:
                continue
            try:
                path.unlink()
            except FileNotFoundError:
                continue


class ConnectorMetricsReader:
    """Aggregate connector worker snapshots from a shared directory."""

    def __init__(self, metrics_dir: str | os.PathLike[str] | None):
        self._dir = Path(metrics_dir).expanduser() if metrics_dir else None

    @property
    def metrics_dir(self) -> Optional[str]:
        if self._dir is None:
            return None
        return str(self._dir)

    def aggregate(self) -> ConnectorMetricsAggregate:
        if self._dir is None or not self._dir.exists():
            return ConnectorMetricsAggregate(
                workers=0,
                updated_at_max=0.0,
                totals=LayeredTransferWorkerMeta(),
                path_totals={},
            )
        totals = LayeredTransferWorkerMeta()
        path_totals: Dict[str, LayeredTransferWorkerMeta] = {}
        workers = 0
        updated_at_max = 0.0
        for payload in self._iter_payloads():
            if payload.get("kind"):
                continue
            totals = totals.aggregate(
                LayeredTransferWorkerMeta.from_dict(payload.get("totals"))
            )
            for path, meta_payload in dict(payload.get("path_totals") or {}).items():
                bucket = _sanitize_component(path).upper()
                existing = path_totals.get(bucket, LayeredTransferWorkerMeta())
                path_totals[bucket] = existing.aggregate(
                    LayeredTransferWorkerMeta.from_dict(meta_payload)
                )
            workers += 1
            updated_at_max = max(updated_at_max, float(payload.get("updated_at", 0.0) or 0.0))
        return ConnectorMetricsAggregate(
            workers=workers,
            updated_at_max=updated_at_max,
            totals=totals,
            path_totals=path_totals,
        )

    def aggregate_mm_hidden_cache(self) -> Dict[str, Any]:
        """Aggregate vLLM multimodal hidden-state cache snapshots.

        The hidden cache runs inside vLLM worker processes, not the proxy.  It
        writes ``kind=mm_hidden_cache`` JSON snapshots into the same metrics dir
        so the proxy can expose real encoder-skip evidence without RPCs.
        """
        if self._dir is None or not self._dir.exists():
            return {"workers": 0, "updated_at_max": 0.0}

        totals: Dict[str, Any] = {
            "workers": 0,
            "updated_at_max": 0.0,
            "enabled_workers": 0,
            "lookups": 0,
            "hits": 0,
            "misses": 0,
            "stores": 0,
            "evictions": 0,
            "bytes": 0,
            "entries": 0,
            "stable_key_lookups": 0,
            "tensor_key_lookups": 0,
            "skipped_unkeyed_calls": 0,
            "native_encoder_cache_hits": 0,
            "precomputed_image_embeds_hits": 0,
            "partial_hit_batches": 0,
            "full_hit_batches": 0,
            "full_miss_batches": 0,
            "vision_compute_ms_total": 0.0,
            "cache_load_ms_total": 0.0,
            "hash_ms_total": 0.0,
            "errors": 0,
        }
        identities = []
        last_errors = []
        for payload in self._iter_payloads(include_auxiliary=True):
            if payload.get("kind") != "mm_hidden_cache":
                continue
            metrics = dict(payload.get("metrics") or {})
            totals["workers"] += 1
            totals["updated_at_max"] = max(
                float(totals["updated_at_max"]),
                float(payload.get("updated_at", 0.0) or 0.0),
            )
            if bool(metrics.get("enabled")):
                totals["enabled_workers"] += 1
            for key in (
                "lookups",
                "hits",
                "misses",
                "stores",
                "evictions",
                "bytes",
                "entries",
                "errors",
                "stable_key_lookups",
                "tensor_key_lookups",
                "skipped_unkeyed_calls",
                "native_encoder_cache_hits",
                "precomputed_image_embeds_hits",
                "partial_hit_batches",
                "full_hit_batches",
                "full_miss_batches",
            ):
                totals[key] += int(metrics.get(key, 0) or 0)
            for key in (
                "vision_compute_ms_total",
                "cache_load_ms_total",
                "hash_ms_total",
            ):
                totals[key] += float(metrics.get(key, 0.0) or 0.0)
            identity = payload.get("identity")
            if isinstance(identity, dict):
                identities.append(identity)
            last_error = str(metrics.get("last_error") or "")
            if last_error:
                last_errors.append(last_error)

        lookups = int(totals["lookups"])
        hits = int(totals["hits"])
        misses = int(totals["misses"])
        totals["hit_rate"] = (hits / lookups) if lookups else 0.0
        totals["miss_rate"] = (misses / lookups) if lookups else 0.0
        totals["vision_compute_ms_avg"] = (
            float(totals["vision_compute_ms_total"]) / misses if misses else 0.0
        )
        totals["cache_load_ms_avg"] = (
            float(totals["cache_load_ms_total"]) / hits if hits else 0.0
        )
        totals["identities"] = identities
        totals["last_errors"] = last_errors[-5:]
        return totals

    def _iter_payloads(self, *, include_auxiliary: bool = False) -> Iterable[Dict[str, Any]]:
        assert self._dir is not None
        for path in sorted(self._dir.glob("*.json")):
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            if not include_auxiliary and payload.get("kind"):
                continue
            yield payload
