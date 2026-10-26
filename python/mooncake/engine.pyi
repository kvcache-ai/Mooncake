"""Type stubs for the ``mooncake.engine`` pybind11 extension.

Keep in sync with the bindings in
``mooncake-integration/transfer_engine/transfer_engine_py.cpp``,
``mooncake-integration/transfer_engine/shared_segment_py.cpp`` and
``mooncake-transfer-engine/src/transport/rpc_communicator/rpc_interface.cpp``.
"""

import asyncio
from collections.abc import Callable, Sequence
from typing import Any, ClassVar, overload

from typing_extensions import Buffer

SUPPORT_CUDA: bool
SUPPORT_EFA: bool
SUPPORT_HIP: bool
SUPPORT_INTRA_NVLINK: bool
SUPPORT_MNNVL: bool
SUPPORT_MULTI_PROTOCOL: bool
SUPPORT_MUSA: bool

class TransferOpcode:
    Read: ClassVar[TransferOpcode]
    Write: ClassVar[TransferOpcode]
    __members__: ClassVar[dict[str, TransferOpcode]]
    def __init__(self, value: int) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __lt__(self, other: object) -> bool: ...
    def __le__(self, other: object) -> bool: ...
    def __gt__(self, other: object) -> bool: ...
    def __ge__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __int__(self) -> int: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...

Read: TransferOpcode
Write: TransferOpcode

_TransferOpcode = TransferOpcode

class ScatterTransferCompletionStatus:
    COMPLETED: ClassVar[ScatterTransferCompletionStatus]
    FAILED_DRAINED: ClassVar[ScatterTransferCompletionStatus]
    COMPLETION_UNKNOWN: ClassVar[ScatterTransferCompletionStatus]
    __members__: ClassVar[dict[str, ScatterTransferCompletionStatus]]
    def __init__(self, value: int) -> None: ...
    def __eq__(self, other: object) -> bool: ...
    def __ne__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __index__(self) -> int: ...
    def __int__(self) -> int: ...
    @property
    def name(self) -> str: ...
    @property
    def value(self) -> int: ...

class ScatterTransferTicket:
    """Keeps a scatter operation alive until native DMA is drained."""

    @property
    def status(self) -> ScatterTransferCompletionStatus: ...
    @property
    def drained(self) -> bool: ...
    def drain(self, timeout_ms: int, /) -> ScatterTransferCompletionStatus: ...

class TransferNotify:
    name: str
    msg: str
    @overload
    def __init__(self) -> None: ...
    @overload
    def __init__(self, name: str, msg: str) -> None: ...

class InnerTransferEngine: ...

class TransferEngine:
    TransferOpcode = _TransferOpcode

    def __init__(self) -> None: ...
    def initialize(
        self,
        local_hostname: str,
        metadata_server: str,
        protocol: str,
        device_name: str,
        /,
    ) -> int: ...
    def initialize_ext(
        self,
        local_hostname: str,
        metadata_server: str,
        protocol: str,
        device_name: str,
        metadata_type: str,
        /,
    ) -> int: ...
    def get_rpc_port(self) -> int: ...
    def allocate_managed_buffer(self, length: int, /) -> int: ...
    def free_managed_buffer(self, user_tensor: int, length: int, /) -> int: ...
    def transfer_sync_write(
        self,
        target_hostname: str,
        buffer: int,
        peer_buffer_address: int,
        length: int,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def transfer_sync_read(
        self,
        target_hostname: str,
        buffer: int,
        peer_buffer_address: int,
        length: int,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_sync_write(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_sync_read(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def submit_scatter_write(
        self,
        endpoint: str,
        local_base_addresses: Sequence[int],
        local_capacities: Sequence[int],
        remote_base_addresses: Sequence[int],
        remote_capacities: Sequence[int],
        local_offsets: Sequence[Sequence[int]],
        remote_offsets: Sequence[Sequence[int]],
        lengths: Sequence[Sequence[int]],
    ) -> ScatterTransferTicket: ...
    def submit_scatter_read(
        self,
        endpoint: str,
        local_base_addresses: Sequence[int],
        local_capacities: Sequence[int],
        remote_base_addresses: Sequence[int],
        remote_capacities: Sequence[int],
        local_offsets: Sequence[Sequence[int]],
        remote_offsets: Sequence[Sequence[int]],
        lengths: Sequence[Sequence[int]],
    ) -> ScatterTransferTicket: ...
    def scatter_transfer_sync_write_with_ticket(
        self,
        endpoint: str,
        local_base_addresses: Sequence[int],
        local_capacities: Sequence[int],
        remote_base_addresses: Sequence[int],
        remote_capacities: Sequence[int],
        local_offsets: Sequence[Sequence[int]],
        remote_offsets: Sequence[Sequence[int]],
        lengths: Sequence[Sequence[int]],
    ) -> ScatterTransferTicket: ...
    def scatter_transfer_sync_read_with_ticket(
        self,
        endpoint: str,
        local_base_addresses: Sequence[int],
        local_capacities: Sequence[int],
        remote_base_addresses: Sequence[int],
        remote_capacities: Sequence[int],
        local_offsets: Sequence[Sequence[int]],
        remote_offsets: Sequence[Sequence[int]],
        lengths: Sequence[Sequence[int]],
    ) -> ScatterTransferTicket: ...
    def batch_transfer_async_write(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_async_read(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    # The *_arrays and *_pages methods take contiguous 8-byte integer arrays
    # (e.g. numpy uint64/int64); `fields` is a C-contiguous [F, 5] array.
    def batch_transfer_sync_write_arrays(
        self,
        target_hostname: str,
        buffers: Buffer,
        peer_buffer_addresses: Buffer,
        lengths: Buffer,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_sync_read_arrays(
        self,
        target_hostname: str,
        buffers: Buffer,
        peer_buffer_addresses: Buffer,
        lengths: Buffer,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_async_write_arrays(
        self,
        target_hostname: str,
        buffers: Buffer,
        peer_buffer_addresses: Buffer,
        lengths: Buffer,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_async_read_arrays(
        self,
        target_hostname: str,
        buffers: Buffer,
        peer_buffer_addresses: Buffer,
        lengths: Buffer,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_sync_write_pages(
        self,
        target_hostname: str,
        src_pages: Buffer,
        dst_pages: Buffer,
        fields: Buffer,
        max_batch_size: int = 0,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_sync_read_pages(
        self,
        target_hostname: str,
        src_pages: Buffer,
        dst_pages: Buffer,
        fields: Buffer,
        max_batch_size: int = 0,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def transfer_sync(
        self,
        target_hostname: str,
        buffer: int,
        peer_buffer_address: int,
        length: int,
        opcode: _TransferOpcode,
        notify: TransferNotify | None = None,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_sync(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        opcode: _TransferOpcode,
        notify: TransferNotify | None = None,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def batch_transfer_async(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        opcode: _TransferOpcode,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    # The *_on_cuda methods are only bound when SUPPORT_CUDA is true.
    def transfer_write_on_cuda(
        self,
        target_hostname: str,
        buffer: int,
        peer_buffer_address: int,
        length: int,
        stream_ptr: int = 0,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> None: ...
    def transfer_read_on_cuda(
        self,
        target_hostname: str,
        buffer: int,
        peer_buffer_address: int,
        length: int,
        stream_ptr: int = 0,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> None: ...
    def batch_transfer_write_on_cuda(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        stream_ptr: int = 0,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> None: ...
    def batch_transfer_read_on_cuda(
        self,
        target_hostname: str,
        buffers: Sequence[int],
        peer_buffer_addresses: Sequence[int],
        lengths: Sequence[int],
        stream_ptr: int = 0,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> None: ...
    def get_batch_transfer_status(self, batch_ids: Sequence[int], /) -> int: ...
    def transfer_submit_write(
        self,
        target_hostname: str,
        buffer: int,
        peer_buffer_address: int,
        length: int,
        transport_hint: str = "",
        nic_hint: str = "",
    ) -> int: ...
    def transfer_check_status(self, batch_id: int, /) -> int: ...
    def write_bytes_to_buffer(
        self, dest_address: int, src_ptr: bytes | str, length: int, /
    ) -> int: ...
    def read_bytes_from_buffer(self, source_address: int, length: int, /) -> bytes: ...
    def register_memory(
        self,
        buffer_addr: int,
        capacity: int,
        location: str = "*",
        remote_accessible: bool = True,
    ) -> int: ...
    def unregister_memory(self, buffer_addr: int, /) -> int: ...
    def batch_register_memory(
        self,
        buffer_addresses: Sequence[int],
        capacities: Sequence[int],
        location: str = "*",
    ) -> int: ...
    def batch_unregister_memory(self, buffer_addresses: Sequence[int], /) -> int: ...
    def get_local_topology(self, device_name: str | None = None) -> str: ...
    def get_first_buffer_address(self, segment_name: str, /) -> int: ...
    def warmup_efa_segment(self, segment_name: str) -> int: ...
    def get_notifies(self) -> list[TransferNotify]: ...
    def send_probe(self, peer_server_name: str) -> int:
        """Send a JSON-RPC probe to peer to verify reachability.

        Returns 0 on success, non-zero on failure.
        """
    def get_engine(self) -> InnerTransferEngine: ...
    def get_engine_ptr(self) -> int: ...

class SharedSegment:
    @staticmethod
    def create(
        name: str,
        size: int,
        world_size: int,
        rank_id: int,
        owner_rank: int = 0,
        device_id: int = 0,
        mmap: bool = True,
        host_register: bool = False,
    ) -> tuple[SharedSegment, bytes]:
        """Phase one; returns (segment, blob)."""
    @staticmethod
    def supported(mmap: bool = True, host_register: bool = False) -> bool:
        """Whether this build can share memory across processes."""
    def complete(self, blobs: Sequence[bytes]) -> None:
        """Phase two; blobs are indexed by rank."""
    def ready(self) -> bool: ...
    def base_addr(self) -> int: ...
    def device_addr(self) -> int: ...
    def size(self) -> int: ...

class ReceivedData:
    @property
    def source_address(self) -> str: ...
    @property
    def data_size(self) -> int: ...
    def get_bytes(self) -> bytes: ...
    def get_memory_view(self) -> memoryview: ...

class ReceivedTensor:
    @property
    def source_address(self) -> str: ...
    @property
    def shape(self) -> list[int]: ...
    @property
    def dtype(self) -> str: ...
    @property
    def total_bytes(self) -> int: ...
    def get_data_size(self) -> int: ...
    def get_data_as_bytes(self) -> bytes: ...
    def get_memory_view(self) -> memoryview: ...

class RpcInterface:
    def __init__(self) -> None: ...
    def initialize(
        self,
        listen_address: str = "",
        thread_count: int = 0,
        timeout_seconds: int = 30,
        pool_size: int = 100,
    ) -> bool: ...
    def initialize_client(
        self, pool_size: int = 100, timeout_seconds: int = 30
    ) -> bool: ...
    def initialize_server(
        self, listen_address: str, thread_count: int = 8, timeout_seconds: int = 30
    ) -> bool: ...
    def start_server(self) -> bool: ...
    def start_server_async(self) -> bool: ...
    def stop_server(self) -> None: ...
    def send_data(self, target_address: str, data: bytes) -> int: ...
    def send_data_async(
        self, target_address: str, data: bytes, loop: asyncio.AbstractEventLoop
    ) -> asyncio.Future[int]: ...
    def send_tensor(self, target_address: str, tensor: Any) -> int: ...
    def send_tensor_async(
        self, target_address: str, tensor: Any, loop: asyncio.AbstractEventLoop
    ) -> asyncio.Future[int]: ...
    def set_data_receive_callback(
        self, callback: Callable[[dict[str, Any]], object], /
    ) -> None: ...
    def set_tensor_receive_callback(
        self, callback: Callable[[ReceivedTensor], object], /
    ) -> None: ...

CoroRPCInterface = RpcInterface

def create_rpc_client(local_rank: int = 0, world_size: int = 1) -> RpcInterface: ...
def create_rpc_server(local_rank: int = 0, world_size: int = 1) -> RpcInterface: ...
