"""SGLang dynamic Mooncake backend for POSIX DFS E2E tests."""

from typing import Any, List, Optional

from sglang.srt.mem_cache.hicache_storage import HiCacheStorageConfig
from sglang.srt.mem_cache.storage.mooncake_store.mooncake_store import MooncakeStore


def _read_int(extra_config: dict, key: str, default: int) -> int:
    value = extra_config.get(key, default)
    if isinstance(value, bool):
        raise ValueError(f"{key} must be an integer, got bool")
    result = int(value)
    if result < 0:
        raise ValueError(f"{key} must be non-negative, got {result}")
    return result


class MooncakePosixDfsStore(MooncakeStore):
    """Mooncake backend variant that requests a DFS replica on every put."""

    def __init__(
        self,
        storage_config: Optional[HiCacheStorageConfig] = None,
        mem_pool: Any = None,
    ):
        extra_config = (
            getattr(storage_config, "extra_config", None) if storage_config else None
        ) or {}

        # Dynamic backends in current SGLang pass a kwargs dict as the second
        # positional argument. MooncakeStore only needs mem_pool for standalone
        # storage, which this test does not use.
        mooncake_mem_pool = None if isinstance(mem_pool, dict) else mem_pool
        super().__init__(storage_config, mooncake_mem_pool)

        from mooncake.store import ReplicateConfig

        probe_config = ReplicateConfig()
        if not hasattr(probe_config, "dfs_replica_num"):
            raise RuntimeError(
                "The installed Mooncake package does not expose "
                "ReplicateConfig.dfs_replica_num"
            )

        self._dfs_replicate_config_cls = ReplicateConfig
        self._dfs_replica_num = _read_int(extra_config, "dfs_replica_num", 1)
        self._replica_num = _read_int(extra_config, "replica_num", 1)
        self._nof_replica_num = _read_int(extra_config, "nof_replica_num", 0)
        if self._dfs_replica_num <= 0:
            raise ValueError("dfs_replica_num must be positive for POSIX DFS test")
        if self._replica_num <= 0:
            raise ValueError(
                "replica_num must be positive when DFS replicas are used"
            )

    def _make_dfs_replicate_config(
        self, group_ids: Optional[List[str]] = None
    ) -> Any:
        config = self._dfs_replicate_config_cls()
        config.replica_num = self._replica_num
        config.nof_replica_num = self._nof_replica_num
        config.dfs_replica_num = self._dfs_replica_num
        if group_ids is not None:
            if not hasattr(config, "group_ids"):
                raise RuntimeError(
                    "The installed Mooncake package does not support "
                    "ReplicateConfig.group_ids"
                )
            config.group_ids = group_ids
        return config

    def _put_batch_zero_copy_impl(
        self,
        key_strs: List[str],
        buffer_ptrs: List[Any],
        buffer_sizes: List[Any],
        group_ids: Optional[List[str]] = None,
    ) -> List[int]:
        if group_ids is not None and len(group_ids) != len(key_strs):
            raise ValueError(
                "Mooncake group_ids length must match key_strs length: "
                f"{len(group_ids)} != {len(key_strs)}"
            )

        config = self._make_dfs_replicate_config(group_ids)
        if self._uses_multi_buffer(buffer_ptrs):
            return self.store.batch_put_from_multi_buffers(
                key_strs, buffer_ptrs, buffer_sizes, config
            )
        return self.store.batch_put_from(
            key_strs, buffer_ptrs, buffer_sizes, config
        )
