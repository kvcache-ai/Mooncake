"""HTTP service + client wrapper for KV directory / A2A handoff control-plane.

This keeps the directory semantics transport-agnostic while making the
owner-shard metadata plane runnable out-of-process for tests and deployment.
"""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, Iterable, Mapping, Optional, Protocol

import requests
from fastapi import FastAPI

from .distributed_kv_directory import DistributedKVDirectory
from .kv_directory import HandoffRecord, KVBlockRecord, LocalKVDirectory, PromoteResult


class KVDirecoryLike(Protocol):
    def register_block(self, local_pid: int, workflow_id: str = "", node_id: Optional[str] = None) -> str: ...
    def ensure_block_record(self, global_block_id: str, **kwargs: Any) -> KVBlockRecord: ...
    def gid_for_physical_id(self, local_pid: int) -> Optional[str]: ...
    def resolve_physical_id(self, global_block_id: str) -> Optional[int]: ...
    def get_record(self, global_block_id: str) -> Optional[KVBlockRecord]: ...
    def drop_block_record(self, global_block_id: str, *, only_placeholder: bool = False) -> bool: ...
    def incref(self, global_block_id: str) -> int: ...
    def decref(self, global_block_id: str) -> int: ...
    def refcount(self, global_block_id: str) -> int: ...
    def lease(self, global_block_id: str) -> int: ...
    def release_lease(self, global_block_id: str) -> int: ...
    def release_physical(self, global_block_id: str) -> bool: ...
    def promote_block(self, global_block_id: str, node_id: str) -> PromoteResult: ...
    def begin_handoff(self, **kwargs: Any) -> HandoffRecord: ...
    def get_handoff(self, handoff_id: str) -> Optional[HandoffRecord]: ...
    def commit_handoff(self, handoff_id: str) -> Optional[HandoffRecord]: ...
    def rollback_handoff(self, handoff_id: str) -> Optional[HandoffRecord]: ...
    def clear_handoff(self, handoff_id: str) -> Optional[HandoffRecord]: ...
    def handoff_states(self) -> Dict[str, HandoffRecord]: ...
    def commit_epoch(self, workflow_id: str, version_id: str, snapshot_epoch: Optional[int] = None) -> int: ...
    def workflow_epoch(self, workflow_id: str) -> int: ...
    def workflow_versions(self, workflow_id: str) -> list[tuple[int, str]]: ...
    def orphan_block_ids(self) -> list[str]: ...
    def sweep_orphans(self) -> int: ...
    def stats(self) -> Dict[str, int]: ...


def _serialize(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, (KVBlockRecord, HandoffRecord, PromoteResult)):
        return asdict(obj)
    if isinstance(obj, dict):
        return {str(k): _serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_serialize(v) for v in obj]
    return obj


def _kv_block_from_json(payload: Optional[Mapping[str, Any]]) -> Optional[KVBlockRecord]:
    if payload is None:
        return None
    return KVBlockRecord(**dict(payload))


def _handoff_from_json(payload: Optional[Mapping[str, Any]]) -> Optional[HandoffRecord]:
    if payload is None:
        return None
    return HandoffRecord(**dict(payload))


def _promote_from_json(payload: Mapping[str, Any]) -> PromoteResult:
    return PromoteResult(**dict(payload))


def create_kv_directory_app(directory: KVDirecoryLike) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return {
            "status": "ok",
            "directory_type": type(directory).__name__,
            "stats": directory.stats(),
        }

    @app.get("/stats")
    async def stats() -> Dict[str, Any]:
        return directory.stats()

    @app.post("/register_block")
    async def register_block(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "gid": directory.register_block(
                local_pid=int(payload["local_pid"]),
                workflow_id=str(payload.get("workflow_id", "")),
                node_id=payload.get("node_id"),
            )
        }

    @app.post("/ensure_block_record")
    async def ensure_block_record(payload: Dict[str, Any]) -> Dict[str, Any]:
        record = directory.ensure_block_record(
            str(payload["global_block_id"]),
            workflow_id=str(payload.get("workflow_id", "")),
            owner_shard=payload.get("owner_shard"),
            physical_node_id=payload.get("physical_node_id"),
            local_pid=payload.get("local_pid"),
            refcount=int(payload.get("refcount", 1) or 1),
            external_placeholder=bool(payload.get("external_placeholder", False)),
        )
        return {"record": _serialize(record)}

    @app.get("/gid_for_physical_id/{local_pid}")
    async def gid_for_physical_id(local_pid: int) -> Dict[str, Any]:
        return {"gid": directory.gid_for_physical_id(local_pid)}

    @app.get("/resolve_physical_id/{global_block_id:path}")
    async def resolve_physical_id(global_block_id: str) -> Dict[str, Any]:
        return {"local_pid": directory.resolve_physical_id(global_block_id)}

    @app.get("/record/{global_block_id:path}")
    async def get_record(global_block_id: str) -> Dict[str, Any]:
        return {"record": _serialize(directory.get_record(global_block_id))}

    @app.post("/drop_block_record")
    async def drop_block_record(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "ok": directory.drop_block_record(
                str(payload["global_block_id"]),
                only_placeholder=bool(payload.get("only_placeholder", False)),
            )
        }

    @app.post("/incref/{global_block_id:path}")
    async def incref(global_block_id: str) -> Dict[str, Any]:
        return {"refcount": directory.incref(global_block_id)}

    @app.post("/decref/{global_block_id:path}")
    async def decref(global_block_id: str) -> Dict[str, Any]:
        return {"refcount": directory.decref(global_block_id)}

    @app.get("/refcount/{global_block_id:path}")
    async def refcount(global_block_id: str) -> Dict[str, Any]:
        return {"refcount": directory.refcount(global_block_id)}

    @app.post("/lease/{global_block_id:path}")
    async def lease(global_block_id: str) -> Dict[str, Any]:
        return {"lease_count": directory.lease(global_block_id)}

    @app.post("/release_lease/{global_block_id:path}")
    async def release_lease(global_block_id: str) -> Dict[str, Any]:
        return {"lease_count": directory.release_lease(global_block_id)}

    @app.post("/release_physical/{global_block_id:path}")
    async def release_physical(global_block_id: str) -> Dict[str, Any]:
        return {"ok": directory.release_physical(global_block_id)}

    @app.post("/promote_block")
    async def promote_block(payload: Dict[str, Any]) -> Dict[str, Any]:
        result = directory.promote_block(str(payload["global_block_id"]), str(payload["node_id"]))
        return {"result": _serialize(result)}

    @app.post("/begin_handoff")
    async def begin_handoff(payload: Dict[str, Any]) -> Dict[str, Any]:
        record = directory.begin_handoff(
            handoff_id=str(payload["handoff_id"]),
            state_id=str(payload["state_id"]),
            workflow_id=str(payload["workflow_id"]),
            source_agent_id=str(payload["source_agent_id"]),
            target_agent_id=str(payload["target_agent_id"]),
            source_node_id=str(payload["source_node_id"]),
            target_node_id=str(payload["target_node_id"]),
            block_ids=[str(item) for item in list(payload.get("block_ids") or [])],
            feature_hashes=[str(item) for item in list(payload.get("feature_hashes") or [])],
        )
        return {"record": _serialize(record)}

    @app.get("/handoff/{handoff_id}")
    async def get_handoff(handoff_id: str) -> Dict[str, Any]:
        return {"record": _serialize(directory.get_handoff(handoff_id))}

    @app.post("/commit_handoff/{handoff_id}")
    async def commit_handoff(handoff_id: str) -> Dict[str, Any]:
        return {"record": _serialize(directory.commit_handoff(handoff_id))}

    @app.post("/rollback_handoff/{handoff_id}")
    async def rollback_handoff(handoff_id: str) -> Dict[str, Any]:
        return {"record": _serialize(directory.rollback_handoff(handoff_id))}

    @app.post("/clear_handoff/{handoff_id}")
    async def clear_handoff(handoff_id: str) -> Dict[str, Any]:
        return {"record": _serialize(directory.clear_handoff(handoff_id))}

    @app.get("/handoff_states")
    async def handoff_states() -> Dict[str, Any]:
        return {"records": _serialize(directory.handoff_states())}

    @app.post("/commit_epoch")
    async def commit_epoch(payload: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "epoch": directory.commit_epoch(
                workflow_id=str(payload["workflow_id"]),
                version_id=str(payload["version_id"]),
                snapshot_epoch=payload.get("snapshot_epoch"),
            )
        }

    @app.get("/workflow_epoch/{workflow_id:path}")
    async def workflow_epoch(workflow_id: str) -> Dict[str, Any]:
        return {"epoch": directory.workflow_epoch(workflow_id)}

    @app.get("/workflow_versions/{workflow_id:path}")
    async def workflow_versions(workflow_id: str) -> Dict[str, Any]:
        return {"versions": directory.workflow_versions(workflow_id)}

    @app.get("/orphan_block_ids")
    async def orphan_block_ids() -> Dict[str, Any]:
        return {"block_ids": directory.orphan_block_ids()}

    @app.post("/sweep_orphans")
    async def sweep_orphans() -> Dict[str, Any]:
        return {"count": directory.sweep_orphans()}

    return app


class RemoteKVDirectory:
    """HTTP client façade matching the Local/Distributed KV directory API."""

    def __init__(
        self,
        base_url: str,
        *,
        session: Optional[Any] = None,
        timeout_s: float = 30.0,
    ):
        self.base_url = str(base_url).rstrip("/")
        self.session = session or requests.Session()
        self.timeout_s = max(1.0, float(timeout_s))
        if hasattr(self.session, "trust_env"):
            self.session.trust_env = False

    def _get(self, path: str) -> Any:
        resp = self.session.get(f"{self.base_url}{path}", timeout=self.timeout_s)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, payload: Optional[Mapping[str, Any]] = None) -> Any:
        resp = self.session.post(
            f"{self.base_url}{path}",
            json=None if payload is None else dict(payload),
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        return resp.json()

    def close(self) -> None:
        close = getattr(self.session, "close", None)
        if callable(close):
            close()

    def register_block(self, local_pid: int, workflow_id: str = "", node_id: Optional[str] = None) -> str:
        payload = self._post(
            "/register_block",
            {
                "local_pid": int(local_pid),
                "workflow_id": workflow_id,
                "node_id": node_id,
            },
        )
        return str(payload["gid"])

    def ensure_block_record(self, global_block_id: str, **kwargs: Any) -> KVBlockRecord:
        payload = self._post(
            "/ensure_block_record",
            {"global_block_id": global_block_id, **kwargs},
        )
        return _kv_block_from_json(payload["record"])  # type: ignore[arg-type]

    def gid_for_physical_id(self, local_pid: int) -> Optional[str]:
        return self._get(f"/gid_for_physical_id/{int(local_pid)}").get("gid")

    def resolve_physical_id(self, global_block_id: str) -> Optional[int]:
        return self._get(f"/resolve_physical_id/{global_block_id}").get("local_pid")

    def get_record(self, global_block_id: str) -> Optional[KVBlockRecord]:
        return _kv_block_from_json(self._get(f"/record/{global_block_id}").get("record"))

    def drop_block_record(self, global_block_id: str, *, only_placeholder: bool = False) -> bool:
        return bool(
            self._post(
                "/drop_block_record",
                {
                    "global_block_id": global_block_id,
                    "only_placeholder": bool(only_placeholder),
                },
            ).get("ok", False)
        )

    def incref(self, global_block_id: str) -> int:
        return int(self._post(f"/incref/{global_block_id}").get("refcount", 0) or 0)

    def decref(self, global_block_id: str) -> int:
        return int(self._post(f"/decref/{global_block_id}").get("refcount", 0) or 0)

    def refcount(self, global_block_id: str) -> int:
        return int(self._get(f"/refcount/{global_block_id}").get("refcount", 0) or 0)

    def lease(self, global_block_id: str) -> int:
        return int(self._post(f"/lease/{global_block_id}").get("lease_count", 0) or 0)

    def release_lease(self, global_block_id: str) -> int:
        return int(self._post(f"/release_lease/{global_block_id}").get("lease_count", 0) or 0)

    def release_physical(self, global_block_id: str) -> bool:
        return bool(self._post(f"/release_physical/{global_block_id}").get("ok", False))

    def promote_block(self, global_block_id: str, node_id: str) -> PromoteResult:
        payload = self._post(
            "/promote_block",
            {"global_block_id": global_block_id, "node_id": node_id},
        )
        return _promote_from_json(payload["result"])

    def begin_handoff(self, **kwargs: Any) -> HandoffRecord:
        payload = self._post("/begin_handoff", kwargs)
        return _handoff_from_json(payload["record"])  # type: ignore[arg-type]

    def get_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        return _handoff_from_json(self._get(f"/handoff/{handoff_id}").get("record"))

    def commit_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        return _handoff_from_json(self._post(f"/commit_handoff/{handoff_id}").get("record"))

    def rollback_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        return _handoff_from_json(self._post(f"/rollback_handoff/{handoff_id}").get("record"))

    def clear_handoff(self, handoff_id: str) -> Optional[HandoffRecord]:
        return _handoff_from_json(self._post(f"/clear_handoff/{handoff_id}").get("record"))

    def handoff_states(self) -> Dict[str, HandoffRecord]:
        payload = self._get("/handoff_states").get("records") or {}
        return {
            str(key): _handoff_from_json(value)  # type: ignore[arg-type]
            for key, value in dict(payload).items()
            if value is not None
        }

    def commit_epoch(self, workflow_id: str, version_id: str, snapshot_epoch: Optional[int] = None) -> int:
        return int(
            self._post(
                "/commit_epoch",
                {
                    "workflow_id": workflow_id,
                    "version_id": version_id,
                    "snapshot_epoch": snapshot_epoch,
                },
            ).get("epoch", -1)
            or -1
        )

    def workflow_epoch(self, workflow_id: str) -> int:
        return int(self._get(f"/workflow_epoch/{workflow_id}").get("epoch", -1) or -1)

    def workflow_versions(self, workflow_id: str) -> list[tuple[int, str]]:
        raw = list(self._get(f"/workflow_versions/{workflow_id}").get("versions") or [])
        return [(int(epoch), str(version_id)) for epoch, version_id in raw]

    def orphan_block_ids(self) -> list[str]:
        return [str(item) for item in list(self._get("/orphan_block_ids").get("block_ids") or [])]

    def sweep_orphans(self) -> int:
        return int(self._post("/sweep_orphans").get("count", 0) or 0)

    def stats(self) -> Dict[str, int]:
        payload = self._get("/stats")
        return {str(key): int(value) for key, value in dict(payload).items()}


def build_default_directory(
    *,
    owner_shards: int = 1,
    node_id: str = "local",
) -> LocalKVDirectory | DistributedKVDirectory:
    if int(owner_shards) > 1:
        return DistributedKVDirectory(shards=int(owner_shards), default_node_id=node_id)
    return LocalKVDirectory(node_id=node_id)
