from __future__ import annotations

"""
Agent State Cloning - KVCache 零拷贝克隆

实现 Agent 工作流中的 KVCache 状态克隆，支持多并行"思考"分支。
基于引用计数的共享内存管理。
"""

import logging
import time
from typing import TYPE_CHECKING
from typing import Dict, Any, Iterable, List, Optional, Sequence, Tuple
from dataclasses import dataclass, field
from enum import Enum

import torch

if TYPE_CHECKING:
    from mooncake_epd.core.state import BlockRef, KVStateDescriptor, MooncakeKVStateStore

logger = logging.getLogger(__name__)


class CloneStatus(Enum):
    ACTIVE = "active"
    COMPLETED = "completed"
    FAILED = "failed"
    PRUNED = "pruned"


@dataclass
class BranchState:
    """一个思考分支的状态"""
    branch_id: str
    parent_id: Optional[str]
    source_cache_id: str
    kv_state_id: Optional[str] = None
    target_node_id: Optional[str] = None
    status: CloneStatus = CloneStatus.ACTIVE
    created_at: float = field(default_factory=time.time)
    tokens_generated: int = 0
    score: float = 0.0
    clone_semantics: str = "unknown"


@dataclass
class _CacheEntry:
    """内部缓存条目，跟踪物理张量和引用计数"""
    kv_cache: Tuple[torch.Tensor, torch.Tensor]
    ref_count: int = 0
    size_bytes: int = 0


class AgentStateCloner:
    """
    Agent State Cloner

    当 Agent 需要 fork 出多个并行"思考"分支时，
    通过引用计数实现 KVCache 状态的零拷贝克隆与共享。

    核心机制：
    1. 引用计数：多个分支共享同一份 KV Cache 物理内存
    2. 写时复制（CoW）：当某个分支需要修改 KV Cache 时才分配新内存
    3. 生命周期管理：引用计数为 0 时自动回收
    """

    def __init__(
        self,
        mooncake_store=None,
        *,
        kv_state_store: Optional["MooncakeKVStateStore"] = None,
    ):
        self.mooncake_store = mooncake_store
        self.kv_state_store = kv_state_store or (
            mooncake_store
            if all(hasattr(mooncake_store, name) for name in ("register_state", "clone_state", "release_state"))
            else None
        )
        self._caches: Dict[str, _CacheEntry] = {}
        self._branch_to_cache: Dict[str, str] = {}
        self._branch_to_kv_state: Dict[str, str] = {}
        self._branches: Dict[str, BranchState] = {}
        self._clone_count = 0
        self._total_clone_time_ms = 0.0
        self._fork_counter = 0

    def register_kv_cache(
        self,
        cache_id: str,
        kv_cache: Tuple[torch.Tensor, torch.Tensor],
    ):
        """注册一份 KV Cache 到 Store"""
        k, v = kv_cache
        size_bytes = k.nelement() * k.element_size() + v.nelement() * v.element_size()
        self._caches[cache_id] = _CacheEntry(
            kv_cache=kv_cache, ref_count=1, size_bytes=size_bytes,
        )
        logger.info(f"Registered KV Cache '{cache_id}', size={size_bytes / 1024 / 1024:.1f}MB")

    def register_kv_refs(
        self,
        state_id: str,
        refs: Sequence["BlockRef"],
        *,
        workflow_id: str,
        version_id: Optional[str] = None,
        parent_version_id: Optional[str] = None,
        snapshot_epoch: int = 0,
        feature_hashes: Optional[Iterable[str]] = None,
        ttl_deadline: float = float("inf"),
        metadata: Optional[Dict[str, Any]] = None,
    ) -> "KVStateDescriptor":
        """Register serving KV page refs as an Agent-clonable state.

        This is the production path for vLLM/Mooncake integration: the state is
        represented by BlockRef descriptors and refcounts in MooncakeKVStateStore,
        not by duplicating raw K/V tensors inside this helper.
        """

        if self.kv_state_store is None:
            raise RuntimeError("register_kv_refs requires a MooncakeKVStateStore")
        descriptor = self.kv_state_store.register_state(
            refs,
            workflow_id=workflow_id,
            version_id=version_id,
            parent_version_id=parent_version_id,
            snapshot_epoch=snapshot_epoch,
            state_id=state_id,
            feature_hashes=feature_hashes,
            ttl_deadline=ttl_deadline,
            metadata=metadata,
        )
        self._branch_to_kv_state[state_id] = descriptor.state_id
        self._branches[state_id] = BranchState(
            branch_id=state_id,
            parent_id=None,
            source_cache_id=descriptor.state_id,
            kv_state_id=descriptor.state_id,
            target_node_id=descriptor.owner_node_id,
            clone_semantics="registered_owner_state",
        )
        return descriptor

    def clone_state(
        self,
        source_id: str,
        branch_id: str,
        parent_branch_id: Optional[str] = None,
    ) -> BranchState:
        """
        克隆 KVCache 状态（零拷贝）。

        多个分支共享同一份物理张量，仅增加引用计数。
        """
        start_time = time.perf_counter()

        entry = self._caches.get(source_id)
        if entry is None:
            raise KeyError(f"KV Cache '{source_id}' not found")

        entry.ref_count += 1
        self._branch_to_cache[branch_id] = source_id

        branch = BranchState(
            branch_id=branch_id,
            parent_id=parent_branch_id,
            source_cache_id=source_id,
            clone_semantics="same_node_tensor_zero_copy",
        )
        self._branches[branch_id] = branch

        clone_time_ms = (time.perf_counter() - start_time) * 1000
        self._clone_count += 1
        self._total_clone_time_ms += clone_time_ms

        logger.info(
            f"Cloned '{source_id}' -> '{branch_id}' "
            f"(zero-copy, ref={entry.ref_count}, time={clone_time_ms:.4f}ms)"
        )

        return branch

    def clone_same_node_zero_copy(
        self,
        source_id: str,
        branch_id: str,
        parent_branch_id: Optional[str] = None,
    ) -> BranchState:
        """Explicit same-node tensor KVCache zero-copy clone.

        This method is a semantic alias for the legacy ``clone_state`` tensor path:
        it only increments the local in-process refcount and never claims
        cross-node physical sharing.
        """

        return self.clone_state(
            source_id,
            branch_id,
            parent_branch_id=parent_branch_id,
        )

    def fork_same_node_zero_copy_branches(
        self,
        source_id: str,
        num_branches: int,
        source_branch_id: Optional[str] = None,
    ) -> List[BranchState]:
        """Fork local tensor KVCache branches with same-node zero-copy semantics."""

        return self.fork_branches(
            source_id,
            num_branches,
            source_branch_id=source_branch_id,
        )

    def fork_branches(
        self,
        source_id: str,
        num_branches: int,
        source_branch_id: Optional[str] = None,
    ) -> List[BranchState]:
        """从一个状态 fork 出多个并行思考分支"""
        branches = []
        self._fork_counter += 1
        fork_id = self._fork_counter

        for i in range(num_branches):
            bid = f"{source_id}_fork{fork_id}_b{i}"
            branch = self.clone_state(
                source_id=source_id,
                branch_id=bid,
                parent_branch_id=source_branch_id,
            )
            branches.append(branch)

        logger.info(
            f"Forked {num_branches} branches from '{source_id}', "
            f"avg clone time={self._total_clone_time_ms / max(self._clone_count, 1):.4f}ms"
        )

        return branches

    def clone_kv_state(
        self,
        source_state_id: str,
        branch_id: str,
        *,
        parent_branch_id: Optional[str] = None,
        target_node_id: Optional[str] = None,
        share_remote_descriptor: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BranchState:
        """Clone a MooncakeKVStateStore descriptor for an Agent branch.

        Same-node clones are refcount-only. Cross-node clones either descriptor
        share owner blocks or use the configured remote materializer, matching
        MooncakeKVStateStore semantics and failing fast when the required real
        materializer is absent.
        """

        if self.kv_state_store is None:
            raise RuntimeError("clone_kv_state requires a MooncakeKVStateStore")
        start_time = time.perf_counter()
        child = self.kv_state_store.clone_state(
            source_state_id,
            child_state_id=branch_id,
            target_node_id=target_node_id,
            share_remote_descriptor=share_remote_descriptor,
            metadata=metadata,
        )
        if bool(child.metadata.get("remote_descriptor_shared")):
            clone_semantics = "cross_node_descriptor_share"
        elif child.owner_node_id == self.kv_state_store.node_id:
            clone_semantics = "same_node_block_ref_zero_copy"
        else:
            clone_semantics = "cross_node_materialized_copy"
        branch = BranchState(
            branch_id=branch_id,
            parent_id=parent_branch_id,
            source_cache_id=source_state_id,
            kv_state_id=child.state_id,
            target_node_id=target_node_id or child.owner_node_id,
            clone_semantics=clone_semantics,
        )
        self._branches[branch_id] = branch
        self._branch_to_kv_state[branch_id] = child.state_id
        clone_time_ms = (time.perf_counter() - start_time) * 1000
        self._clone_count += 1
        self._total_clone_time_ms += clone_time_ms
        logger.info(
            "Cloned KVState '%s' -> branch '%s' "
            "(owner=%s, target=%s, semantics=%s, time=%.4fms)",
            source_state_id,
            branch_id,
            child.owner_node_id,
            branch.target_node_id,
            clone_semantics,
            clone_time_ms,
        )
        return branch

    def clone_kv_same_node_zero_copy(
        self,
        source_state_id: str,
        branch_id: str,
        *,
        parent_branch_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BranchState:
        """Explicit same-node BlockRef zero-copy clone.

        The child descriptor owns no new physical pages; it increments the
        authoritative local KV directory refcounts via ``PagedKVManager.fork_refs``.
        """

        if self.kv_state_store is None:
            raise RuntimeError("clone_kv_same_node_zero_copy requires a MooncakeKVStateStore")
        merged_metadata = {"clone_semantics": "same_node_block_ref_zero_copy"}
        merged_metadata.update(metadata or {})
        return self.clone_kv_state(
            source_state_id,
            branch_id,
            parent_branch_id=parent_branch_id,
            target_node_id=self.kv_state_store.node_id,
            share_remote_descriptor=False,
            metadata=merged_metadata,
        )

    def share_kv_state_descriptor_cross_node(
        self,
        source_state_id: str,
        branch_id: str,
        *,
        target_node_id: str,
        parent_branch_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> BranchState:
        """Create a cross-node descriptor-share branch, not a physical zero-copy clone.

        The branch points at owner-node blocks and must be materialized before any
        target-node write.  This is the explicit API to use when Agent planning
        wants cheap cross-node fan-out without pretending local physical IDs are
        valid on the target node.
        """

        if not target_node_id:
            raise ValueError("target_node_id is required for cross-node descriptor sharing")
        if self.kv_state_store is None:
            raise RuntimeError("share_kv_state_descriptor_cross_node requires a MooncakeKVStateStore")
        if str(target_node_id) == str(self.kv_state_store.node_id):
            raise ValueError("target_node_id must differ from the owner node for cross-node descriptor sharing")
        merged_metadata = {"clone_semantics": "cross_node_descriptor_share"}
        merged_metadata.update(metadata or {})
        return self.clone_kv_state(
            source_state_id,
            branch_id,
            parent_branch_id=parent_branch_id,
            target_node_id=target_node_id,
            share_remote_descriptor=True,
            metadata=merged_metadata,
        )

    def materialize_cross_node_descriptor_for_write(
        self,
        branch_id: str,
        *,
        target_node_id: Optional[str] = None,
    ) -> "KVStateDescriptor":
        """Materialize a descriptor-shared branch before target-node writes."""

        return self.materialize_branch_for_write(
            branch_id,
            target_node_id=target_node_id,
        )

    def fork_kv_branches(
        self,
        source_state_id: str,
        num_branches: int,
        *,
        source_branch_id: Optional[str] = None,
        target_node_id: Optional[str] = None,
        share_remote_descriptor: Optional[bool] = None,
    ) -> List[BranchState]:
        """Fork multiple MooncakeKVStateStore-backed Agent thinking branches."""

        if num_branches < 0:
            raise ValueError("num_branches must be non-negative")
        self._fork_counter += 1
        fork_id = self._fork_counter
        out: List[BranchState] = []
        for i in range(num_branches):
            branch_id = f"{source_state_id}_fork{fork_id}_b{i}"
            out.append(
                self.clone_kv_state(
                    source_state_id,
                    branch_id,
                    parent_branch_id=source_branch_id,
                    target_node_id=target_node_id,
                    share_remote_descriptor=share_remote_descriptor,
                )
            )
        return out

    def get_branch_kv_refs(
        self,
        branch_id: str,
        *,
        target_node_id: Optional[str] = None,
        for_write: bool = False,
    ) -> List["BlockRef"]:
        """Resolve BlockRefs for a MooncakeKVStateStore-backed branch."""

        if self.kv_state_store is None:
            raise RuntimeError("get_branch_kv_refs requires a MooncakeKVStateStore")
        state_id = self._branch_to_kv_state.get(branch_id, branch_id)
        return self.kv_state_store.resolve_remote_refs(
            state_id,
            target_node_id=target_node_id,
            for_write=for_write,
        )

    def materialize_branch_for_write(
        self,
        branch_id: str,
        *,
        target_node_id: Optional[str] = None,
    ) -> "KVStateDescriptor":
        """Materialize a descriptor-shared branch on its write target node."""

        if self.kv_state_store is None:
            raise RuntimeError("materialize_branch_for_write requires a MooncakeKVStateStore")
        state_id = self._branch_to_kv_state.get(branch_id, branch_id)
        descriptor = self.kv_state_store.materialize_for_write(
            state_id,
            target_node_id=target_node_id,
        )
        self._branch_to_kv_state[branch_id] = descriptor.state_id
        branch = self._branches.get(branch_id)
        if branch is not None:
            branch.kv_state_id = descriptor.state_id
            branch.target_node_id = descriptor.owner_node_id
            branch.clone_semantics = "cross_node_materialized_for_write"
        return descriptor

    def get_branch_kv_cache(
        self, branch_id: str,
    ) -> Optional[Tuple[torch.Tensor, torch.Tensor]]:
        """获取分支的 KV Cache"""
        cache_id = self._branch_to_cache.get(branch_id)
        if cache_id is None:
            return None
        entry = self._caches.get(cache_id)
        if entry is None:
            return None
        return entry.kv_cache

    def write_copy_on_write(
        self,
        branch_id: str,
        new_kv_cache: Tuple[torch.Tensor, torch.Tensor],
    ):
        """写时复制：当分支需要修改 KV Cache 时，分配新的物理内存"""
        old_cache_id = self._branch_to_cache.get(branch_id)
        if old_cache_id is None:
            return

        new_id = f"cow_{branch_id}"
        k, v = new_kv_cache
        size_bytes = k.nelement() * k.element_size() + v.nelement() * v.element_size()
        self._caches[new_id] = _CacheEntry(
            kv_cache=new_kv_cache, ref_count=1, size_bytes=size_bytes,
        )
        self._branch_to_cache[branch_id] = new_id

        self._decrement_ref(old_cache_id)

        logger.info(f"CoW for branch '{branch_id}': new cache '{new_id}'")

    def release_branch(self, branch_id: str, status: CloneStatus = CloneStatus.COMPLETED):
        """释放一个分支，减少引用计数"""
        branch = self._branches.pop(branch_id, None)
        if branch is None:
            return

        branch.status = status

        kv_state_id = self._branch_to_kv_state.pop(branch_id, None)
        if kv_state_id is not None and self.kv_state_store is not None:
            self.kv_state_store.release_state(kv_state_id)
            return

        cache_id = self._branch_to_cache.pop(branch_id, None)
        if cache_id is not None:
            self._decrement_ref(cache_id)

    def _decrement_ref(self, cache_id: str):
        """减少缓存引用计数，为 0 时释放"""
        entry = self._caches.get(cache_id)
        if entry is None:
            return
        entry.ref_count = max(0, entry.ref_count - 1)
        if entry.ref_count == 0:
            del self._caches[cache_id]
            logger.debug(f"Released KV Cache '{cache_id}' (ref_count=0)")

    def prune_branches(self, keep_top_k: int):
        """剪枝：保留 top-k 得分最高的分支，释放其余分支"""
        active = [
            b for b in self._branches.values()
            if b.status == CloneStatus.ACTIVE
        ]
        active.sort(key=lambda b: b.score, reverse=True)

        pruned = 0
        for branch in active[keep_top_k:]:
            self.release_branch(branch.branch_id, status=CloneStatus.PRUNED)
            pruned += 1

        logger.info(f"Pruned {pruned} branches, kept top {keep_top_k}")

    def get_stats(self) -> Dict[str, Any]:
        """获取克隆统计"""
        return {
            "total_clones": self._clone_count,
            "avg_clone_time_ms": self._total_clone_time_ms / max(self._clone_count, 1),
            "active_branches": sum(
                1 for b in self._branches.values()
                if b.status == CloneStatus.ACTIVE
            ),
            "total_kv_caches": len(self._caches),
            "total_ref_counts": sum(e.ref_count for e in self._caches.values()),
            "kv_state_store_enabled": self.kv_state_store is not None,
            "kv_state_branches": len(self._branch_to_kv_state),
            "kv_state_store_stats": (
                self.kv_state_store.stats() if self.kv_state_store is not None else {}
            ),
        }

    def get_memory_usage(self) -> Dict[str, Any]:
        """估算内存使用量（物理内存，不重复计算共享）"""
        total_bytes = sum(e.size_bytes for e in self._caches.values())
        unique_bytes = sum(
            e.size_bytes for e in self._caches.values() if e.ref_count >= 1
        )
        return {
            "total_bytes": total_bytes,
            "unique_physical_bytes": unique_bytes,
            "total_mb": total_bytes / (1024 * 1024),
            "unique_physical_mb": unique_bytes / (1024 * 1024),
        }
