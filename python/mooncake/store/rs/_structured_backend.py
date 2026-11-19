"""Store-RS operations for the shared structured-object transport."""

from __future__ import annotations

from mooncake._structured_backend import StructuredStoreBackend

from .store import BufferPool, TENSOR_METADATA_WIRE_SIZE, _tensor_from_raw_bytes


class _StructuredPoolStore:
    """Keep result leases independent of the runtime's bounded transfer scratch."""

    def __init__(self, store):
        self.store = store

    def local_buffer_pool_try_acquire(self, size):
        # Materialized arrays can outlive a transfer; reserve external registered
        # memory instead of holding scratch required by subsequent store I/O.
        return None

    def __getattr__(self, name):
        return getattr(self.store, name)


class StoreRSStructuredBackend(StructuredStoreBackend):
    prefer_registered_put = True
    supports_storage_groups = False

    def supports_native_tensor(self, tensor) -> bool:
        # The native header has eight dimensions; empty native writes may use a null pointer.
        return tensor.dim() <= 8 and tensor.numel() > 0

    def validate_write_config(self, config) -> None:
        groups = getattr(config, "group_ids", None)
        if groups is not None and (groups if isinstance(groups, str) else any(groups)):
            raise NotImplementedError(
                "this store backend does not support storage groups"
            )

    def pointer_put(self, config=None):
        # RS put_tensor_from accepts a tensor, not an object-buffer pointer.
        return self.store.put_from

    def tensor_put(self, config=None):
        # Raw writes preserve placement options that the native tensor API omits.
        return self._put_tensor if config is None else None

    def _put_tensor(self, key, tensor, config=None):
        # The RS native tensor API reads host memory and requires contiguous data.
        tensor = tensor.detach().cpu().contiguous()
        return self.store.put_tensor(key, tensor, config=config)

    def get_tensor_into(self, key: str, ptr: int, size: int):
        self.store.get_tensor_into(key, ptr, size)
        # Return the caller's destination holder, which retains its buffer owner.
        return None

    def existence_status(self, key: str) -> int:
        # is_exist converts backend failures to False; retain the per-key status.
        statuses = self.store.batch_is_exist([key])
        if len(statuses) != 1 or statuses[0] not in (0, 1):
            raise RuntimeError(f"batch_is_exist failed for {key}: {statuses}")
        return int(statuses[0])

    def create_buffer_pool(self):
        if self.store.local_buffer_pool_capacity() == 0:
            return None
        return BufferPool(_StructuredPoolStore(self.store))

    @staticmethod
    def tensor_metadata_size() -> int:
        return TENSOR_METADATA_WIRE_SIZE

    deserialize_tensor = staticmethod(_tensor_from_raw_bytes)
