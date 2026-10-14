"""Opt-in, synchronous FP8 storage for contiguous CPU FP16/BF16 tensors.

The caller's key must already identify compatible model, tokens, layout and
shard. This wrapper adds only the encoding namespace. It changes stored bytes,
not the dtype used by attention. Calls retain their buffers until completion;
callers must not concurrently modify source or destination tensors.
"""

from mooncake._kv_codec import Dtype, InvalidRecord, ScaledFp8Codec

__all__ = ["CodecStoreClient", "InvalidRecord", "StoreReadError"]


class StoreReadError(RuntimeError):
    """The byte Store returned no record (missing object or failed read).

    The underlying get API does not distinguish those two cases. This wrapper
    deliberately does not label every empty read as a cache miss.
    """


class CodecStoreClient:
    """A thin wrapper around an initialized MooncakeDistributedStore.

    Groups are consecutive flattened elements, with a short final group
    allowed. Choose group_size to match the desired component/head boundaries.
    Store the complete K/V chunk as one tensor, or use separate logical keys
    when the application already manages component completeness.

    No fallback or re-quantization of packed/FP8 inputs is performed. Disabled
    quantization uses the original Store directly. Reserve ``__mc_codec__/``
    for encoded keys; use storage_key() for direct remove/is_exist operations.
    """

    def __init__(self, store, group_size: int = 128):
        if type(group_size) is not int or not 1 <= group_size <= 0xFFFFFFFF:
            raise ValueError("group_size must be a positive uint32")
        self._store = store
        self._codec = ScaledFp8Codec(group_size)

    def storage_key(self, key: str) -> str:
        """Stable representation key; independent of Python hash randomization."""
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a nonempty string")
        return f"__mc_codec__/{self._codec.format_id}/{key}"

    @staticmethod
    def _view(tensor):
        import torch

        if not isinstance(tensor, torch.Tensor):
            raise TypeError("Expected a torch.Tensor")
        if tensor.device.type != "cpu" or tensor.layout != torch.strided:
            raise ValueError("Only strided CPU tensors are supported")
        if not tensor.is_contiguous():
            raise ValueError("Tensor must be contiguous")
        if tensor.dtype not in (torch.float16, torch.bfloat16):
            raise ValueError("Only FP16/BF16 tensors are supported")
        if tensor.requires_grad or tensor.is_conj() or tensor.is_neg():
            raise ValueError("Tensor must have no gradient or lazy view bits")
        if not 1 <= tensor.ndim <= 8 or tensor.numel() == 0:
            raise ValueError("Expected a nonempty tensor with rank between 1 and 8")
        dtype = Dtype.FLOAT16 if tensor.dtype == torch.float16 else Dtype.BFLOAT16
        # Byte view avoids numpy's lack of bfloat16 support and retains storage.
        data = memoryview(tensor.detach().view(torch.uint8).numpy()).cast("B")
        return data, dtype, list(tensor.shape)

    def put(self, key: str, tensor, config=None) -> int:
        """Encode one complete record and return the Store put status (0=OK).

        Codec errors raise before any put. Existing-key semantics and replica
        configuration remain those of the underlying Store.
        """
        encoded_key = self.storage_key(key)
        data, dtype, shape = self._view(tensor)
        record = self._codec.encode(data, dtype, shape)
        if config is None:
            return self._store.put(encoded_key, record)
        return self._store.put(encoded_key, record, config)

    def get_into(self, key: str, destination):
        """Read one complete record, validate and decode into destination.

        Returns destination only after decode completes. Invalid records and
        layout mismatches leave destination unchanged. This method never does
        separate header and payload reads.
        """
        encoded_key = self.storage_key(key)
        data, dtype, shape = self._view(destination)
        record = self._store.get(encoded_key)
        if not record:
            raise StoreReadError(f"No record returned for {encoded_key!r}")
        self._codec.decode_into(record, data, dtype, shape)
        return destination
