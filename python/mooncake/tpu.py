# Copyright 2026 KVCache.AI
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Register TPU framework buffers (e.g. JAX arrays) with Mooncake TENT.

TENT moves TPU data by staging it through host DRAM. It runs the host<->host
hop itself (RDMA/TCP) and performs the HBM<->host hop through its TPU adapter,
``libmooncake_tpu_pjrt.so``. The adapter delegates the actual device copies to
a *TPU buffer provider*: a library built with the ML framework that can reach
its device buffers, such as tpu-sync for JAX. See
``tent/include/tent/platform/tpu_buffer_provider.h`` for the C contract.

A Python provider object exposes two things:

* ``library_path() -> str``: path of the shared library exporting
  ``mc_tpu_buffer_provider_get``.
* ``export_shards(array) -> list[int]``: one provider handle per addressable
  shard of ``array``. Ownership of the handles passes to Mooncake.

Typical use, in the process that owns the arrays (requires Mooncake built with
``-DUSE_TENT=ON -DUSE_TPU=ON``)::

    from mooncake import tpu

    # 1. Before creating the engine: TENT reads the TPU device topology from
    #    the adapter once, at construction.
    shards = tpu.prepare(kv_caches, provider=my_provider)

    os.environ["MC_USE_TENT"] = "1"
    from mooncake.engine import TransferEngine
    engine = TransferEngine()
    engine.initialize(...)

    # 2. Register the shards' tokens; TENT records each as "tpu:<device>".
    tpu.register_with_engine(engine, shards)

Tokens are addresses for Mooncake's bookkeeping only; they point at reserved,
inaccessible memory and must never be dereferenced.
"""

import ctypes
import dataclasses
import os
from typing import Any, List, Optional, Sequence

# Environment variable TENT's TpuPjrtShim reads to locate the adapter.
ADAPTER_LIB_ENV = "MC_TPU_PJRT_LIB"
ADAPTER_LIB_NAME = "libmooncake_tpu_pjrt.so"


@dataclasses.dataclass(frozen=True)
class TpuShard:
    """One registered device buffer, e.g. one addressable shard of an array."""

    token: int  # Address to register with the TransferEngine. Never deref.
    size: int  # Bytes of the buffer's on-device representation.
    device: int  # Local device ordinal; TENT records it as "tpu:<device>".


def default_adapter_path() -> str:
    """The adapter shipped in the mooncake package (USE_TPU builds)."""
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), ADAPTER_LIB_NAME)


def _load_adapter(path: str) -> ctypes.CDLL:
    lib = ctypes.CDLL(path)
    lib.mc_tpu_adapter_load_provider.argtypes = [ctypes.c_char_p]
    lib.mc_tpu_adapter_load_provider.restype = ctypes.c_int
    lib.mc_tpu_adapter_register.argtypes = [
        ctypes.c_uint64,
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_uint64),
        ctypes.POINTER(ctypes.c_int),
    ]
    lib.mc_tpu_adapter_register.restype = ctypes.c_int
    lib.mc_tpu_adapter_unregister.argtypes = [ctypes.c_uint64]
    lib.mc_tpu_adapter_unregister.restype = ctypes.c_int
    lib.mc_tpu_adapter_release_buffer.argtypes = [ctypes.c_uint64]
    lib.mc_tpu_adapter_release_buffer.restype = ctypes.c_int
    return lib


class TpuAdapter:
    """Thin wrapper over the adapter's control API (tpu_pjrt_adapter.h)."""

    def __init__(self, path: Optional[str] = None, lib: Any = None):
        self.path = os.path.abspath(path or default_adapter_path())
        self._lib = lib if lib is not None else _load_adapter(self.path)

    def load_provider(self, provider_path: str) -> None:
        if self._lib.mc_tpu_adapter_load_provider(provider_path.encode()) != 0:
            raise RuntimeError(
                f"Mooncake TPU adapter could not bind buffer provider "
                f"{provider_path!r}; see stderr for details"
            )

    def register(self, handle: int) -> TpuShard:
        """Registers a provider handle. The adapter owns it from now on."""
        token = ctypes.c_uint64()
        size = ctypes.c_uint64()
        device = ctypes.c_int()
        rc = self._lib.mc_tpu_adapter_register(
            handle, ctypes.byref(token), ctypes.byref(size), ctypes.byref(device)
        )
        if rc != 0:
            raise RuntimeError(
                f"Mooncake TPU adapter could not register buffer handle {handle}; "
                "see stderr for details"
            )
        return TpuShard(token.value, size.value, device.value)

    def unregister(self, token: int) -> None:
        if self._lib.mc_tpu_adapter_unregister(token) != 0:
            raise ValueError(f"unknown TPU buffer token {token:#x}")

    def release_buffer(self, handle: int) -> None:
        self._lib.mc_tpu_adapter_release_buffer(handle)


_adapter: Optional[TpuAdapter] = None


def get_adapter(path: Optional[str] = None) -> TpuAdapter:
    """Loads the adapter once per process and points TENT at the same file.

    TENT and this module must use the same adapter library, or TENT will not
    see the buffers registered here. Raises if MC_TPU_PJRT_LIB already names a
    different file.
    """
    global _adapter
    if _adapter is None:
        candidate = os.path.abspath(
            path or os.environ.get(ADAPTER_LIB_ENV) or default_adapter_path()
        )
        configured = os.environ.get(ADAPTER_LIB_ENV)
        if configured and os.path.realpath(configured) != os.path.realpath(candidate):
            raise RuntimeError(
                f"{ADAPTER_LIB_ENV} is {configured!r} but the requested adapter "
                f"is {candidate!r}; TENT must load the same library."
            )
        _adapter = TpuAdapter(candidate)
        os.environ[ADAPTER_LIB_ENV] = _adapter.path
    elif path and os.path.realpath(path) != os.path.realpath(_adapter.path):
        raise RuntimeError(f"the TPU adapter is already loaded from {_adapter.path!r}")
    return _adapter


def _default_provider() -> Any:
    try:
        from tpu_sync.api.jax import mooncake_provider  # type: ignore
    except ImportError as exc:
        raise ImportError(
            "No TPU buffer provider was given and tpu-sync's Mooncake "
            "provider (tpu_sync.api.jax.mooncake_provider) is not installed. "
            "Pass provider=... or install tpu-sync."
        ) from exc
    return mooncake_provider


def prepare(
    arrays: Sequence[Any],
    *,
    provider: Any = None,
    adapter: Optional[TpuAdapter] = None,
) -> List[List[TpuShard]]:
    """Registers framework arrays with the TPU adapter.

    Call this before creating the TransferEngine. All-or-nothing: if any shard
    fails, everything registered by this call is unregistered and every
    exported handle is released before the error is raised.

    Args:
      arrays: Device arrays, e.g. the per-layer KV cache arrays.
      provider: TPU buffer provider (see module docstring). Defaults to
        tpu-sync's.
      adapter: Adapter to use. Defaults to the process-wide one.

    Returns:
      For each array, one TpuShard per handle the provider exported.
    """
    provider = provider if provider is not None else _default_provider()
    adapter = adapter if adapter is not None else get_adapter()
    adapter.load_provider(provider.library_path())

    out: List[List[TpuShard]] = []
    pending: List[int] = []
    try:
        for array in arrays:
            pending = [int(h) for h in provider.export_shards(array)]
            shards: List[TpuShard] = []
            out.append(shards)
            while pending:
                # The adapter owns the handle as soon as it is passed in.
                shards.append(adapter.register(pending.pop(0)))
    except Exception:
        for handle in pending:
            adapter.release_buffer(handle)
        unregister([shard for per_array in out for shard in per_array], adapter=adapter)
        raise
    return out


def unregister(
    shards: Sequence[TpuShard], *, adapter: Optional[TpuAdapter] = None
) -> None:
    """Unregisters shards from the adapter.

    Unregister them from the TransferEngine first.
    """
    adapter = adapter if adapter is not None else get_adapter()
    for shard in shards:
        adapter.unregister(shard.token)


def register_with_engine(engine: Any, shards: Sequence[Sequence[TpuShard]]) -> None:
    """Registers every shard's token with a TransferEngine.

    Uses ``engine.batch_register_memory(addresses, capacities)``. The location
    is left to TENT, which asks the adapter and records "tpu:<device>".
    """
    flat = [shard for per_array in shards for shard in per_array]
    if not flat:
        return
    rc = engine.batch_register_memory(
        [shard.token for shard in flat], [shard.size for shard in flat]
    )
    if rc != 0:
        raise RuntimeError(
            f"batch_register_memory failed with {rc} for {len(flat)} TPU shard(s)"
        )
