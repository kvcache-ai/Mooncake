import functools
import os
import pickle
import socket
from contextlib import contextmanager
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Tuple, Union

import torch
import torch.distributed as dist

if TYPE_CHECKING:
    from mooncake import ep


def _env_enabled(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.upper() in {"1", "ON", "TRUE", "YES"}


_USE_MACA = _env_enabled("MOONCAKE_EP_USE_MACA") or bool(
    getattr(torch.version, "maca", None)
)
_USE_SPLIT_SEND_RECV = _USE_MACA

_ZERO_COPY_COMBINE_UNSUPPORTED = (
    "Mooncake EP zero-copy combine is not supported; pass the expert output "
    "tensor to combine() with zero_copy=False"
)


@functools.lru_cache(maxsize=None)
def _device_type() -> str:
    """The device the native module was built for: "cuda" or "xpu"."""
    from mooncake import ep

    return getattr(ep, "DEVICE_TYPE", "cuda")


def _native_current_stream_ptr() -> int:
    if _device_type() == "xpu":
        return int(torch.xpu.current_stream().sycl_queue)
    return int(torch.cuda.current_stream().cuda_stream)


def _synchronize_device() -> None:
    if _device_type() == "xpu":
        torch.xpu.synchronize()
    else:
        torch.cuda.synchronize()


def _wait_native_event_on_current_stream(event: "ep.EventHandle") -> None:
    event.current_stream_wait(_native_current_stream_ptr())


class EventOverlap:
    """
    A wrapper class to manage CUDA events, also for better overlapping convenience.

    Attributes:
        event: the CUDA event captured.
        extra_tensors: an easier way to simulate PyTorch tensor `record_stream`, may be useful with CUDA graph.
    """

    def __init__(
        self,
        event: Optional["ep.EventHandle"] = None,
        extra_tensors: Optional[Tuple[torch.Tensor, ...]] = None,
    ) -> None:
        """
        Initialize the class.

        Arguments:
            event: the CUDA event captured.
            extra_tensors: an easier way to simulate PyTorch tensor `record_stream`, may be useful with CUDA graph.
        """
        self.event = event

        # NOTES: we use extra tensors to achieve stream recording, otherwise,
        # stream recording will be incompatible with CUDA graph.
        self.extra_tensors = extra_tensors

    def current_stream_wait(self) -> None:
        """
        The current stream `torch.cuda.current_stream()` waits for the event to be finished.
        """
        assert self.event is not None
        _wait_native_event_on_current_stream(self.event)

    def __enter__(self) -> Any:
        """
        Utility for overlapping and Python `with` syntax.

        You can overlap the kernels on the current stream with the following example:
        ```python
        event_overlap = event_after_all_to_all_kernels()
        with event_overlap():
            do_something_on_current_stream()
        # After exiting the `with` scope, the current stream with wait the event to be finished.
        ```
        """
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Utility for overlapping and Python `with` syntax.

        Please follow the example in the `__enter__` function.
        """
        if self.event is not None:
            _wait_native_event_on_current_stream(self.event)


class Buffer:
    def __init__(
        self,
        group: dist.ProcessGroup,
        num_ep_buffer_bytes: int,
        disable_p2p: bool = False,
    ):
        self._is_elastic = False
        from mooncake import ep

        # Initialize the CPP runtime
        self.rank = group.rank()
        self.group_size = group.size()
        self.group = group
        self.num_ep_buffer_bytes = num_ep_buffer_bytes
        self.backend = self.group
        self.device_type = _device_type()
        runtime_kwargs = {}
        if self.device_type == "xpu":
            # The XPU runtime allocates on, and launches into, torch's queue.
            runtime_kwargs["stream_ptr"] = _native_current_stream_ptr()
        # NIC auto-detection happens inside ep.Buffer via Topology::discover().
        self.runtime = ep.Buffer(
            self.rank,
            self.group_size,
            num_ep_buffer_bytes,
            disable_p2p,
            **runtime_kwargs,
        )
        # Fallback flag and buffers.
        # Note: `sync_nvlink_ipc_handles()` can mutate C++ `ibgda_disabled_` (True->False when
        # P2P+IPC succeeds for all ranks). We re-evaluate after IPC sync below.
        self._use_fallback = bool(self.runtime.ibgda_disabled())
        self._fallback_next_combine_buffer: Optional[torch.Tensor] = None
        self._maca_phase_token: Optional[torch.Tensor] = None
        self._maca_phase_recv_tokens: Optional[List[torch.Tensor]] = None
        self._warned_active_ranks_without_mooncake_backend = False
        self.connect()

    @classmethod
    def create_elastic(
        cls,
        disable_ll_nvlink: bool = False,
        explicitly_destroy: bool = False,
        rank: int = 0,
        tcp_store_group: Optional[dist.TCPStore] = None,
        timeout_ms: int = 30000,
        metadata_timeout_ms: int = 300000,
    ) -> "Buffer":
        """Create an opt-in rank buffer without joining a ProcessGroup.

        Native allocation and rank connection are deferred until
        ``update_memory_buffers`` and ``connect_ranks``. ``timeout_ms`` sets
        the native GPU communication timeout in milliseconds. The separate
        ``metadata_timeout_ms`` sets the TCPStore peer-metadata wait timeout
        in milliseconds and defaults to 300000 (300 seconds). The legacy
        constructor keeps its eager ProcessGroup setup path.
        """
        if timeout_ms < 0:
            raise ValueError("timeout_ms must be non-negative")
        if metadata_timeout_ms <= 0:
            raise ValueError("metadata_timeout_ms must be positive")
        instance = cls.__new__(cls)
        instance._initialize_elastic_fields(
            disable_ll_nvlink=disable_ll_nvlink,
            explicitly_destroy=explicitly_destroy,
            rank=rank,
            tcp_store_group=tcp_store_group,
            timeout_ms=timeout_ms,
            metadata_timeout_ms=metadata_timeout_ms,
        )
        return instance

    def _initialize_elastic_fields(
        self,
        disable_ll_nvlink: bool,
        explicitly_destroy: bool,
        rank: int,
        tcp_store_group: Optional[dist.TCPStore],
        timeout_ms: int,
        metadata_timeout_ms: int,
    ) -> None:
        """Initialize wrapper state for the deferred, no-ProcessGroup path."""
        self.device_type = _device_type()
        self.rank = rank
        self.group_size = 0
        self.group = None
        self.num_ep_buffer_bytes = None
        self.backend = None
        self.runtime = None
        self._use_fallback = False
        self._fallback_next_combine_buffer = None
        self._maca_phase_token = None
        self._maca_phase_recv_tokens = None
        self._warned_active_ranks_without_mooncake_backend = False
        self._is_elastic = True
        self.disable_ll_nvlink = disable_ll_nvlink
        self.explicitly_destroy = explicitly_destroy
        self.tcp_store_group = tcp_store_group
        self.timeout_ms = timeout_ms
        self.metadata_timeout_ms = metadata_timeout_ms
        self.num_rdma_bytes = None
        self._num_experts_per_rank = None
        self._destroyed = False

    @staticmethod
    def _wrap_hook_with_resources(
        hook: Optional[Callable], resources: Tuple[Any, ...]
    ) -> Optional[Callable]:
        if hook is None:
            return None
        keepalive = [resources]

        def wrapped_hook() -> None:
            try:
                hook()
            finally:
                keepalive.clear()

        return wrapped_hook

    def _require_elastic_runtime(self) -> None:
        if not self._is_elastic:
            raise RuntimeError("This operation requires Buffer.create_elastic().")
        if self.runtime is None:
            raise RuntimeError(
                "Call update_memory_buffers() before using the elastic buffer."
            )

    def update_memory_buffers(
        self,
        num_ranks: int,
        num_experts_per_rank: int,
        num_rdma_bytes: int,
    ) -> None:
        """Allocate the fixed-capacity native buffer for an elastic instance."""
        if not self._is_elastic:
            raise RuntimeError("update_memory_buffers() requires an elastic buffer.")
        if self._destroyed:
            raise RuntimeError("Destroyed elastic buffers cannot be reinitialized.")
        if self.runtime is not None:
            raise RuntimeError(
                "Elastic buffer capacity is fixed after update_memory_buffers()."
            )
        if num_ranks <= 0 or not 0 <= self.rank < num_ranks:
            raise ValueError("rank must be within the positive rank capacity.")
        if num_experts_per_rank <= 0 or num_rdma_bytes <= 0:
            raise ValueError("expert and RDMA capacities must be positive.")
        if (
            _USE_MACA
            or getattr(torch.version, "maca", None)
            or getattr(torch.version, "musa", None)
            or getattr(torch.version, "hip", None)
            or torch.version.cuda is None
            or not torch.cuda.is_available()
        ):
            raise RuntimeError("The elastic EP buffer requires a CUDA native build.")

        from mooncake import ep

        self.group_size = num_ranks
        self.num_ep_buffer_bytes = num_rdma_bytes
        self.num_rdma_bytes = num_rdma_bytes
        self._num_experts_per_rank = num_experts_per_rank
        self._active_ranks = torch.zeros(
            (num_ranks,),
            dtype=torch.int32,
            device=torch.device("cuda", torch.cuda.current_device()),
        )
        self._active_ranks[self.rank] = 1
        self.runtime = ep.Buffer(
            self.rank,
            num_ranks,
            num_rdma_bytes,
            self.disable_ll_nvlink,
        )
        self.runtime._set_explicit_destroy(self.explicitly_destroy)
        self._use_fallback = False
        self.runtime._configure_elastic(
            num_experts_per_rank, self._active_ranks.data_ptr()
        )

    def set_tcp_store_group(self, tcp_store_group: Optional[dist.TCPStore]) -> None:
        if not self._is_elastic:
            raise RuntimeError("set_tcp_store_group() requires an elastic buffer.")
        self.tcp_store_group = tcp_store_group

    def _get_local_peer_metadata(self) -> Tuple[Any, ...]:
        self._require_elastic_runtime()
        raddr, rkey = self.runtime.get_mr_info()
        subnet_prefix, interface_id = self.runtime.get_gid()
        return (
            int(raddr),
            int(rkey),
            int(subnet_prefix),
            int(interface_id),
            tuple(int(value) for value in self.runtime.get_local_qpns()),
            tuple(int(value) for value in self.runtime.get_local_lids()),
            tuple(int(value) for value in self.runtime.get_ipc_handle()),
        )

    @contextmanager
    def _fetch_remote_metadata_from_tcp_store(self, remote_ranks: List[int]):
        if self.tcp_store_group is None:
            raise RuntimeError("Mooncake peer metadata exchange requires a TCPStore.")
        store = self.tcp_store_group
        outgoing_keys = [f"MOONCAKE_EP/{self.rank}/{rank}" for rank in remote_ranks]
        incoming_keys = [f"MOONCAKE_EP/{rank}/{self.rank}" for rank in remote_ranks]
        envelope = pickle.dumps((socket.gethostname(), self._get_local_peer_metadata()))
        consumed_incoming_keys = []
        try:
            for key in outgoing_keys:
                store.set(key, envelope)
            if incoming_keys:
                store.wait(
                    incoming_keys,
                    timedelta(milliseconds=self.metadata_timeout_ms),
                )
                remote_metadata = store.multi_get(incoming_keys)
                consumed_incoming_keys.extend(incoming_keys)
            else:
                remote_metadata = []
            yield [pickle.loads(metadata) for metadata in remote_metadata]
        finally:
            for key in consumed_incoming_keys:
                store.delete_key(key)

    def connect_ranks(self, remote_ranks: List[int], activate: bool = True) -> None:
        """Exchange peer metadata and optionally activate the connections.

        A peer can be connected while inactive. ``activate=False`` still
        stages and connects its metadata, so that peer slot is occupied.
        Masking a rank with ``update_mask_buffer`` changes activity; it does
        not disconnect the peer. To replace a locally connected peer, first
        call ``disconnect_ranks`` for that slot. Both peers must then
        coordinate a fresh metadata exchange with ``connect_ranks``; a newly
        created replacement has no old local connection to disconnect. A
        connection prepared with ``activate=False`` can be activated later by
        calling ``update_mask_buffer(rank, False)``.

        Calls to ``connect_ranks`` and ``disconnect_ranks`` on the same Buffer
        must be serialized by the caller.
        """
        self._require_elastic_runtime()
        ranks = [int(rank) for rank in remote_ranks if int(rank) != self.rank]
        if not ranks:
            return
        if len(set(ranks)) != len(ranks):
            raise ValueError("remote_ranks must not contain duplicates.")
        if any(rank < 0 or rank >= self.group_size for rank in ranks):
            raise ValueError("remote rank is outside the configured capacity.")
        self.runtime._validate_connect_ranks(ranks)
        local_hostname = socket.gethostname()
        with self._fetch_remote_metadata_from_tcp_store(ranks) as metadata:
            for rank, (peer_hostname, peer_metadata) in zip(ranks, metadata):
                peer_metadata = list(peer_metadata)
                qpns, lids = tuple(peer_metadata[4]), tuple(peer_metadata[5])
                if qpns or lids:
                    if (
                        not qpns
                        or len(qpns) != len(lids)
                        or len(qpns) % self.group_size
                    ):
                        raise RuntimeError(
                            "Peer QP/LID metadata has an invalid capacity."
                        )
                    qps_per_rank = len(qpns) // self.group_size
                    start = self.rank * qps_per_rank
                    stop = start + qps_per_rank
                    peer_metadata[4] = qpns[start:stop]
                    peer_metadata[5] = lids[start:stop]
                if peer_hostname != local_hostname:
                    peer_metadata[6] = ()
                self.runtime._stage_peer(rank, *peer_metadata)
        if activate:
            for rank in ranks:
                self.runtime._update_mask_buffer(
                    rank, False, _native_current_stream_ptr()
                )

    def disconnect_ranks(self, remote_ranks: List[int]) -> None:
        self._require_elastic_runtime()
        ranks = [int(rank) for rank in remote_ranks if int(rank) != self.rank]
        if not ranks:
            return
        if len(set(ranks)) != len(ranks):
            raise ValueError("remote_ranks must not contain duplicates.")
        if any(rank < 0 or rank >= self.group_size for rank in ranks):
            raise ValueError("remote rank is outside the configured capacity.")
        self.runtime._disconnect_ranks(ranks)

    def update_mask_buffer(self, rank_to_mask: int, mask: bool = False) -> None:
        self._require_elastic_runtime()
        if rank_to_mask == self.rank and mask:
            raise ValueError("The local rank cannot be masked.")
        if not 0 <= rank_to_mask < self.group_size:
            raise ValueError("rank_to_mask is outside the configured capacity.")
        self.runtime._update_mask_buffer(
            rank_to_mask, mask, _native_current_stream_ptr()
        )

    def query_mask_buffer(self, mask_status: torch.Tensor) -> None:
        self._require_elastic_runtime()
        if (
            mask_status.dtype != torch.int32
            or not mask_status.is_contiguous()
            or mask_status.numel() != self.group_size
            or mask_status.device != self._active_ranks.device
        ):
            raise ValueError("mask_status must be a capacity-sized CUDA int32 tensor.")
        self.runtime._query_mask_buffer(
            mask_status.data_ptr(), _native_current_stream_ptr()
        )

    def clean_mask_buffer(self) -> None:
        self._require_elastic_runtime()
        self.runtime._clean_mask_buffer(_native_current_stream_ptr())

    def barrier(self) -> None:
        self._require_elastic_runtime()
        self.runtime._barrier(self.timeout_ms * 1000, _native_current_stream_ptr())

    def destroy(self) -> None:
        self._require_elastic_runtime()
        assert self.explicitly_destroy, "explicitly_destroy flag must be set"
        self.runtime.destroy()
        self.runtime = None
        self._destroyed = True

    @staticmethod
    def capture() -> EventOverlap:
        from mooncake import ep

        return EventOverlap(ep.EventHandle(_native_current_stream_ptr()))

    def get_comm_stream(self) -> torch.Stream:
        self._require_elastic_runtime()
        stream_ptr = self.runtime._get_comm_stream()
        return torch.cuda.ExternalStream(stream_ptr, device=self._active_ranks.device)

    @staticmethod
    def _tensor_from_native_storage(
        data_ptr: int,
        logical_bytes: int,
        device: torch.device,
        dtype: torch.dtype,
        size: Tuple[int, ...],
        stride: Tuple[int, ...],
        owner: "Buffer",
    ) -> torch.Tensor:
        if data_ptr <= 0 or logical_bytes <= 0:
            raise RuntimeError("Native buffer returned an invalid memory descriptor.")
        construct_storage = getattr(
            torch._C, "_construct_storage_from_data_pointer", None
        )
        if construct_storage is None:
            raise RuntimeError(
                "This PyTorch build cannot wrap a native buffer without a copy."
            )
        storage = construct_storage(data_ptr, device, logical_bytes)
        tensor = torch.empty(0, dtype=dtype, device=device).set_(
            storage, 0, size, stride
        )
        tensor._mooncake_buffer_owner = owner
        return tensor

    def get_local_buffer_tensor(
        self,
        dtype: torch.dtype,
        size: Optional[torch.Size] = None,
        offset: int = 0,
        use_rdma_buffer: bool = True,
    ) -> torch.Tensor:
        """Return a zero-copy RDMA-buffer view.

        ``offset`` is measured in bytes. The view remains valid while this
        Buffer is alive and has not been destroyed.
        """
        self._require_elastic_runtime()
        if not use_rdma_buffer:
            raise ValueError("Only the RDMA buffer is supported.")
        if offset < 0:
            raise ValueError("offset must be nonnegative.")
        descriptor = self.runtime._get_local_buffer_descriptor()
        if len(descriptor) != 2:
            raise RuntimeError("Native local-buffer descriptor must be (ptr, nbytes).")
        data_ptr, logical_bytes = (int(value) for value in descriptor)
        if offset > logical_bytes:
            raise ValueError("offset exceeds the native RDMA payload.")
        element_size = torch.empty(0, dtype=dtype).element_size()
        if offset % element_size:
            raise ValueError("offset must be aligned to the requested dtype.")
        data_ptr += offset
        logical_bytes -= offset
        if logical_bytes % element_size:
            raise RuntimeError("Native RDMA payload is not dtype aligned.")
        if size is None:
            shape = (logical_bytes // element_size,)
            strides = (1,)
        else:
            shape = tuple(int(dim) for dim in size)
            if size.numel() > logical_bytes // element_size:
                raise ValueError("Requested RDMA buffer view exceeds the allocation.")
            strides_list = [1] * len(shape)
            for index in range(len(shape) - 2, -1, -1):
                strides_list[index] = strides_list[index + 1] * shape[index + 1]
            strides = tuple(strides_list)
        return self._tensor_from_native_storage(
            data_ptr,
            logical_bytes,
            self._active_ranks.device,
            dtype,
            shape,
            strides,
            self,
        )

    @staticmethod
    def get_low_latency_buffer_size_hint(
        num_max_dispatch_tokens_per_rank: int,
        hidden: int,
        num_ranks: int,
        num_experts: int,
    ) -> int:
        if num_ranks <= 0 or num_experts % num_ranks:
            raise ValueError("num_experts must divide evenly across rank capacity.")
        from mooncake import ep

        return ep.get_ep_elastic_buffer_size_hint(
            num_max_dispatch_tokens_per_rank,
            hidden,
            num_ranks,
            num_experts // num_ranks,
        )

    @staticmethod
    def get_rdma_size_hint(
        num_max_dispatch_tokens_per_rank: int,
        hidden: int,
        num_ranks: int,
        num_experts: int,
    ) -> int:
        return Buffer.get_low_latency_buffer_size_hint(
            num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts
        )

    def _maca_phase_fence(self, send_event: Optional[Any] = None) -> None:
        if not _USE_MACA:
            return

        backend = dist.get_backend(self.group)
        fence_device = torch.device("cpu" if backend == "gloo" else "cuda")

        def wait_send_done() -> None:
            if send_event is not None:
                send_event.synchronize()
            else:
                torch.cuda.synchronize()

        # Compatibility fence between SEND and RECV.  The EP payload still
        # uses the P2P fast path; this only keeps rank phases aligned on MACA.
        wait_send_done()
        if (
            self._maca_phase_token is None
            or self._maca_phase_token.device != fence_device
        ):
            self._maca_phase_token = torch.empty(
                1, dtype=torch.int32, device=fence_device
            )
        if (
            self._maca_phase_recv_tokens is None
            or self._maca_phase_recv_tokens[0].device != fence_device
        ):
            self._maca_phase_recv_tokens = [
                torch.empty(1, dtype=torch.int32, device=fence_device)
                for _ in range(self.group_size)
            ]
        self._maca_phase_token.fill_(1)
        ops = []
        for peer in range(self.group_size):
            if peer == self.rank:
                continue
            ops.append(dist.P2POp(dist.isend, self._maca_phase_token, peer, self.group))
            ops.append(
                dist.P2POp(
                    dist.irecv,
                    self._maca_phase_recv_tokens[peer],
                    peer,
                    self.group,
                )
            )
        if not ops:
            return
        for work in dist.batch_isend_irecv(ops):
            work.wait()

    def _wrap_maca_recv_hook(
        self, hook: Optional[Callable], send_event: Optional[Any]
    ) -> Callable:
        def wrapped_hook() -> None:
            self._maca_phase_fence(send_event)
            if hook is not None:
                hook()

        return wrapped_hook

    def _bootstrap_device(self) -> torch.device:
        """Device for the tensors that bootstrap exchanges over the group."""
        if self.device_type != "xpu":
            return torch.device("cuda")
        try:
            backend = dist.get_backend(self.group)
        except Exception:
            backend = None
        return torch.device("cpu" if backend == "gloo" else "xpu")

    def connect(self, is_update: bool = False):
        device = self._bootstrap_device()
        # XPU has no RDMA path: peers are reached over Level Zero IPC only.
        if self.device_type != "xpu" and not self._use_fallback:
            (raddr, rkey) = self.runtime.get_mr_info()
            # torchada maps the CUDA device namespace to MUSA when enabled.

            raddr = torch.tensor([raddr], dtype=torch.int64, device=device)
            raddrs = [
                torch.empty(1, dtype=torch.int64, device=device)
                for _ in range(self.group_size)
            ]
            dist.all_gather(raddrs, raddr, self.group)
            raddrs = torch.cat(raddrs).tolist()

            rkey = torch.tensor([rkey], dtype=torch.int32, device=device)
            rkeys = [
                torch.empty(1, dtype=torch.int32, device=device)
                for _ in range(self.group_size)
            ]
            dist.all_gather(rkeys, rkey, self.group)
            rkeys = torch.cat(rkeys).tolist()

            if is_update:
                self.runtime.update_local_qpns()

            local_qpns = self.runtime.get_local_qpns()
            all_to_all_size = len(local_qpns) // self.group_size
            local_qpns = list(
                torch.unbind(
                    torch.tensor(local_qpns, dtype=torch.int32, device=device).view(
                        -1, all_to_all_size
                    )
                )
            )
            remote_qpns = [
                torch.empty(all_to_all_size, dtype=torch.int32, device=device)
                for _ in range(self.group_size)
            ]
            dist.all_to_all(remote_qpns, local_qpns, self.group)
            peer_qpns = [remote_qpns[r].tolist() for r in range(self.group_size)]

            local_lids = self.runtime.get_local_lids()
            local_lids = list(
                torch.unbind(
                    torch.tensor(local_lids, dtype=torch.int32, device=device).view(
                        -1, all_to_all_size
                    )
                )
            )
            remote_lids = [
                torch.empty(all_to_all_size, dtype=torch.int32, device=device)
                for _ in range(self.group_size)
            ]
            dist.all_to_all(remote_lids, local_lids, self.group)
            peer_lids = [remote_lids[r].tolist() for r in range(self.group_size)]

            (subnet_prefix, interface_id) = self.runtime.get_gid()
            subnet_prefix_t = torch.tensor(
                [subnet_prefix], dtype=torch.int64, device=device
            )
            subnet_prefixes_list = [
                torch.empty(1, dtype=torch.int64, device=device)
                for _ in range(self.group_size)
            ]
            dist.all_gather(subnet_prefixes_list, subnet_prefix_t, self.group)
            subnet_prefixes = torch.cat(subnet_prefixes_list).tolist()

            interface_id_t = torch.tensor(
                [interface_id], dtype=torch.int64, device=device
            )
            interface_ids_list = [
                torch.empty(1, dtype=torch.int64, device=device)
                for _ in range(self.group_size)
            ]
            dist.all_gather(interface_ids_list, interface_id_t, self.group)
            interface_ids = torch.cat(interface_ids_list).tolist()

            active_ranks_mask = self._active_ranks_list(device)
            self.runtime.sync_ibgda_peers(
                raddrs,
                rkeys,
                peer_qpns,
                peer_lids,
                subnet_prefixes,
                interface_ids,
                active_ranks_mask,
            )

        if self.group_size == 1:
            # No peer can import this IPC handle in single-rank EP.  Skipping
            # export also avoids unnecessary driver IPC calls on MACA.
            self._use_fallback = False
            return
        if self.runtime.p2p_enabled():
            try:
                local_handle_ints = self.runtime.get_ipc_handle()
                # pybind11 converts std::vector<int32_t> to a list of integers
                local_handle_tensor = torch.tensor(
                    local_handle_ints, dtype=torch.int32, device=device
                )
                handles = [
                    torch.empty(
                        len(local_handle_ints), dtype=torch.int32, device=device
                    )
                    for _ in range(self.group_size)
                ]
                dist.all_gather(handles, local_handle_tensor, self.group)
                remote_handles = [h.tolist() for h in handles]
                active_ranks_mask = self._active_ranks_list(device)
                self.runtime.sync_nvlink_ipc_handles(remote_handles, active_ranks_mask)
            except Exception as e:
                import warnings

                warnings.warn(
                    f"[Rank {self.rank}] Failed to exchange IPC handles: {e}. Falling back.",
                    RuntimeWarning,
                    stacklevel=2,
                )
        else:
            return

        use_fast_path = False
        try:
            use_fast_path = bool(self.runtime.use_fast_path())
        except Exception:
            ibgda_disabled = bool(self.runtime.ibgda_disabled())
            use_fast_path = not ibgda_disabled

        if self.device_type == "xpu":
            # Each rank maps its peers on its own, so one may reach them all
            # while another cannot. Take the fast path only if every rank can;
            # a rank in native kernels would wait forever on one that runs the
            # fallback's collectives, and the other way round.
            agreed = torch.tensor(
                [int(use_fast_path)], dtype=torch.int32, device=device
            )
            dist.all_reduce(agreed, op=dist.ReduceOp.MIN, group=self.group)
            use_fast_path = bool(agreed.item())
        self._use_fallback = not use_fast_path

    def update_ep_member(self):
        self.connect(True)

    def _is_mooncake_backend(self) -> bool:
        try:
            return dist.get_backend(self.group) == "mooncake"
        except Exception:
            return False

    def _active_ranks_tensor(
        self, device: torch.device, dtype: torch.dtype = torch.int32
    ) -> torch.Tensor:
        if self._is_elastic:
            if device != self._active_ranks.device or dtype != torch.int32:
                raise ValueError(
                    "Elastic active mask must use its fixed CUDA int32 tensor."
                )
            return self._active_ranks
        if not self._is_mooncake_backend():
            # XPU builds have no Mooncake PG, so there is nothing to switch to.
            if (
                self.device_type != "xpu"
                and not self._warned_active_ranks_without_mooncake_backend
            ):
                import warnings

                try:
                    backend = dist.get_backend(self.group)
                except Exception:
                    backend = "unknown"
                warnings.warn(
                    "Mooncake EP active_ranks is only available with the "
                    f"mooncake process group; got backend={backend}. "
                    "Treating all ranks as active.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                self._warned_active_ranks_without_mooncake_backend = True
            return torch.ones((self.group_size,), dtype=dtype, device=device)

        try:
            from mooncake.pg import get_active_ranks

            return get_active_ranks(self.backend).to(device=device, dtype=dtype)
        except Exception:
            return torch.ones((self.group_size,), dtype=dtype, device=device)

    def _active_ranks_list(self, device: torch.device) -> List[int]:
        return self._active_ranks_tensor(device=device, dtype=torch.int32).tolist()

    @staticmethod
    def get_ep_buffer_size_hint(
        num_max_dispatch_tokens_per_rank: int,
        hidden: int,
        num_ranks: int,
        num_experts: int,
    ) -> int:
        from mooncake.ep import get_ep_buffer_size_hint

        return get_ep_buffer_size_hint(
            num_max_dispatch_tokens_per_rank, hidden, num_ranks, num_experts
        )

    def dispatch(self, *args, **kwargs):
        if self._is_elastic:
            return self._dispatch_elastic(*args, **kwargs)
        return self._dispatch_legacy(*args, **kwargs)

    def _dispatch_elastic(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        num_max_dispatch_tokens_per_rank: int,
        num_experts: Optional[int] = None,
        cumulative_local_expert_recv_stats: Optional[torch.Tensor] = None,
        dispatch_wait_recv_cost_stats: Optional[torch.Tensor] = None,
        use_fp8: bool = True,
        round_scale: bool = False,
        async_finish: bool = False,
        return_recv_hook: bool = False,
    ):
        self._require_elastic_runtime()
        if round_scale:
            raise NotImplementedError(
                "Mooncake EP does not implement NIXL round_scale."
            )
        if dispatch_wait_recv_cost_stats is not None:
            raise NotImplementedError(
                "Mooncake EP does not expose dispatch wait-cost statistics."
            )
        active_bound = int(self.runtime._get_active_rank_bound())
        experts_per_rank = self._num_experts_per_rank
        if num_experts is None:
            num_experts = active_bound * experts_per_rank
        elif num_experts % experts_per_rank:
            raise ValueError("num_experts must be a multiple of experts per rank.")
        rank_bound = num_experts // experts_per_rank
        if not active_bound <= rank_bound <= self.group_size:
            raise ValueError("num_experts is outside the active rank capacity.")
        if self._active_ranks.device != x.device:
            raise ValueError("Dispatch input must use the configured rank-mask device.")
        if cumulative_local_expert_recv_stats is not None:
            stats = cumulative_local_expert_recv_stats
            if (
                stats.dtype != torch.int32
                or stats.dim() != 1
                or not stats.is_contiguous()
                or stats.numel() != experts_per_rank
                or stats.device != x.device
            ):
                raise ValueError(
                    "cumulative_local_expert_recv_stats must be a contiguous "
                    "CUDA int32 vector with one value per local expert."
                )
        result = self._dispatch_legacy(
            x,
            topk_idx,
            self._active_ranks,
            num_max_dispatch_tokens_per_rank,
            num_experts,
            self.timeout_ms * 1000,
            use_fp8,
            async_finish,
            return_recv_hook,
            _cumulative_local_expert_recv_stats=(cumulative_local_expert_recv_stats),
            _rank_bound=rank_bound,
        )
        recv_x, recv_count, handle, event, hook = result
        hook = self._wrap_hook_with_resources(hook, (self, result))
        return recv_x, recv_count, handle[:4], event, hook

    # noinspection PyTypeChecker
    def _dispatch_legacy(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        active_ranks: torch.Tensor,
        num_max_dispatch_tokens_per_rank: int,
        num_experts: int,
        timeout_us: int,
        use_fp8: Optional[bool] = None,
        async_finish: bool = False,
        return_recv_hook: bool = False,
        topk_weights: Optional[torch.Tensor] = None,
        *,
        _cumulative_local_expert_recv_stats: Optional[torch.Tensor] = None,
        _rank_bound: Optional[int] = None,
    ) -> Tuple[
        Union[Tuple[torch.Tensor, torch.Tensor], torch.Tensor],
        torch.Tensor,
        Tuple,
        EventOverlap,
        Callable,
    ]:
        # topk_weights, if given, must be the weights the matching combine is
        # called with, and every rank must pass it or none: on XPU the combine
        # then reduces each token's rows on the expert side with them and does
        # not read its own topk_weights. Other backends ignore it.
        assert x.dim() == 2 and x.is_contiguous()
        assert x.dtype == torch.bfloat16
        assert topk_idx.dim() == 2 and topk_idx.is_contiguous()
        assert topk_idx.dtype == torch.int64
        assert x.size(0) == topk_idx.size(0)
        assert x.size(0) <= num_max_dispatch_tokens_per_rank
        assert x.size(1) % 16 == 0 and x.size(1) % 128 == 0
        elastic = self._is_elastic
        if elastic:
            self._require_elastic_runtime()
            if self._use_fallback:
                raise RuntimeError("Elastic EP does not use the Python fallback.")
            if _rank_bound is None or _rank_bound <= 0:
                raise ValueError("Elastic dispatch requires a positive phase bound.")
            assert num_experts == _rank_bound * self._num_experts_per_rank
            num_local_experts = self._num_experts_per_rank
            rank_bound = _rank_bound
        else:
            assert num_experts % self.group_size == 0
            num_local_experts = num_experts // self.group_size
            rank_bound = self.group_size
        if topk_weights is not None:
            assert topk_weights.shape == topk_idx.shape
            assert topk_weights.dtype == torch.float32
            assert topk_weights.is_contiguous()
        caller_active_ranks = active_ranks
        if active_ranks is None:
            active_ranks = self._active_ranks_tensor(device=x.device, dtype=torch.int32)
        else:
            assert active_ranks.dim() == 1 and active_ranks.is_contiguous()
            assert active_ranks.dtype == torch.int32
            if active_ranks.numel() != self.group_size:
                if self._is_elastic:
                    raise ValueError(
                        "active_ranks length must equal the fixed rank capacity."
                    )
                active_ranks = torch.ones(
                    (self.group_size,), dtype=torch.int32, device=x.device
                )

        if use_fp8 is None:
            use_fp8 = not _USE_MACA

        if _USE_MACA:
            assert not (
                async_finish and return_recv_hook
            ), "MACA does not support async_finish and return_recv_hook together"

        # MUSA and MACA use split SEND/RECV launches because they do not expose
        # CUDA cooperative-grid synchronization. Only MACA adds a phase fence.
        if _USE_SPLIT_SEND_RECV and async_finish:
            import warnings

            warnings.warn(
                "async_finish uses split SEND/RECV kernels plus a stream "
                "event, not CUDA cooperative single-kernel async semantics.",
                RuntimeWarning,
                stacklevel=2,
            )

        runtime_return_recv_hook = return_recv_hook or (
            _USE_SPLIT_SEND_RECV and not self._use_fallback
        )

        if self._use_fallback:
            (
                packed_recv_x,
                packed_recv_x_scales,
                packed_recv_count,
                packed_recv_src_info,
                packed_recv_layout_range,
                event,
                hook,
            ) = self._fallback_dispatch(
                x,
                topk_idx,
                num_max_dispatch_tokens_per_rank,
                num_experts,
                use_fp8,
                return_recv_hook,
            )
            backend_active_ranks = self._active_ranks_tensor(
                device=active_ranks.device, dtype=active_ranks.dtype
            )
            if (
                caller_active_ranks is not None
                and active_ranks.numel() == backend_active_ranks.numel()
            ):
                active_ranks.copy_(backend_active_ranks)
        else:
            packed_recv_x = torch.empty(
                (
                    num_local_experts,
                    rank_bound * num_max_dispatch_tokens_per_rank,
                    x.size(1),
                ),
                dtype=torch.float8_e4m3fn if use_fp8 else torch.bfloat16,
                device=x.device,
            )
            # The kernel clears these too; keep the historical zero-initialization.
            packed_recv_count = torch.zeros(
                (num_local_experts,), dtype=torch.int32, device=x.device
            )
            packed_recv_src_info = torch.empty(
                (
                    num_local_experts,
                    rank_bound * num_max_dispatch_tokens_per_rank,
                ),
                dtype=torch.int32,
                device=x.device,
            )
            packed_recv_layout_range = torch.empty(
                (num_local_experts, rank_bound),
                dtype=torch.int64,
                device=x.device,
            )
            packed_recv_x_scales = None
            if use_fp8:
                assert (rank_bound * num_max_dispatch_tokens_per_rank) % 4 == 0
                packed_recv_x_scales = torch.empty(
                    (
                        num_local_experts,
                        x.size(1) // 128,
                        rank_bound * num_max_dispatch_tokens_per_rank,
                    ),
                    dtype=torch.float32,
                    device=x.device,
                ).transpose(1, 2)
            # The MACA split path requests the native receive hook even when
            # the caller did not.  Native EP forbids async+hook together;
            # the wrapper owns completion through the phase-fence hook.
            native_async_finish = async_finish and not (
                _USE_SPLIT_SEND_RECV and runtime_return_recv_hook
            )
            native_weights = ()
            if self.device_type == "xpu":
                # The mode is passed apart from the pointer: an empty tensor's
                # data_ptr() is 0, and a rank without tokens still reduces.
                native_weights = (
                    0 if topk_weights is None else topk_weights.data_ptr(),
                    topk_weights is not None,
                )
            native_dispatch_args = (
                x.data_ptr(),
                topk_idx.data_ptr(),
                active_ranks.data_ptr(),
                x.size(0),
                x.size(1),
                topk_idx.size(1),
                num_max_dispatch_tokens_per_rank,
                num_experts,
                timeout_us,
                use_fp8,
                packed_recv_x.data_ptr(),
                0 if packed_recv_x_scales is None else packed_recv_x_scales.data_ptr(),
                packed_recv_count.data_ptr(),
                packed_recv_src_info.data_ptr(),
                packed_recv_layout_range.data_ptr(),
                native_async_finish,
                runtime_return_recv_hook,
                _native_current_stream_ptr(),
            )
            native_dispatch_args += native_weights
            if (
                self.device_type != "xpu"
                and _cumulative_local_expert_recv_stats is not None
            ):
                native_dispatch_args += (
                    _cumulative_local_expert_recv_stats.data_ptr(),
                )
            event, hook = self.runtime.dispatch(*native_dispatch_args)
            if _USE_SPLIT_SEND_RECV:
                hook = self._wrap_maca_recv_hook(hook, event)
                if not return_recv_hook:
                    hook()
                    hook = None
        handle = (
            packed_recv_src_info,
            packed_recv_layout_range,
            num_max_dispatch_tokens_per_rank,
            x.size(1),
            num_experts,
        )
        tensors_to_record = (
            x,
            topk_idx,
            topk_weights,
            packed_recv_x,
            packed_recv_x_scales,
            packed_recv_count,
            packed_recv_src_info,
            packed_recv_layout_range,
            _cumulative_local_expert_recv_stats,
        )
        return (
            (packed_recv_x, packed_recv_x_scales) if use_fp8 else packed_recv_x,
            packed_recv_count,
            handle,
            EventOverlap(
                event, tensors_to_record if async_finish or return_recv_hook else None
            ),
            hook,
        )

    def combine(self, *args, **kwargs):
        if self._is_elastic:
            return self._combine_elastic(*args, **kwargs)
        return self._combine_legacy(*args, **kwargs)

    def _combine_elastic(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        topk_weights: torch.Tensor,
        handle: tuple,
        use_logfmt: bool = False,
        zero_copy: bool = False,
        async_finish: bool = False,
        return_recv_hook: bool = False,
        out: Optional[torch.Tensor] = None,
        combine_wait_recv_cost_stats: Optional[torch.Tensor] = None,
    ) -> Tuple[torch.Tensor, EventOverlap, Callable]:
        if zero_copy:
            raise NotImplementedError(_ZERO_COPY_COMBINE_UNSUPPORTED)
        self._require_elastic_runtime()
        if use_logfmt:
            raise NotImplementedError(
                "Mooncake EP does not implement the NIXL LogFMT combine format."
            )
        if combine_wait_recv_cost_stats is not None:
            raise NotImplementedError(
                "Mooncake EP does not expose combine wait-cost statistics."
            )
        if len(handle) != 4:
            raise ValueError("Elastic dispatch handle must contain four items.")
        src_info, layout_range, max_tokens, hidden = handle
        if layout_range.dim() != 2 or not layout_range.is_contiguous():
            raise ValueError("layout_range must be a contiguous rank-by-expert matrix.")
        rank_bound = layout_range.size(1)
        active_bound = int(self.runtime._get_active_rank_bound())
        experts_per_rank = self._num_experts_per_rank
        if not active_bound <= rank_bound <= self.group_size:
            raise ValueError("Dispatch handle rank bound is outside active capacity.")
        if self._active_ranks.device != x.device:
            raise ValueError("Combine input must use the configured rank-mask device.")
        legacy_handle = (
            src_info,
            layout_range,
            max_tokens,
            hidden,
            rank_bound * experts_per_rank,
        )
        result = self._combine_legacy(
            x,
            topk_idx,
            topk_weights,
            self._active_ranks,
            self.timeout_ms * 1000,
            legacy_handle,
            zero_copy=zero_copy,
            async_finish=async_finish,
            return_recv_hook=return_recv_hook,
            out=out,
        )
        combined_x, event, hook = result
        hook = self._wrap_hook_with_resources(hook, (self, result))
        return combined_x, event, hook

    # noinspection PyTypeChecker
    def _combine_legacy(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        topk_weights: torch.Tensor,
        active_ranks: torch.Tensor,
        timeout_us: int,
        handle: tuple,
        zero_copy: bool = False,
        async_finish: bool = False,
        return_recv_hook: bool = False,
        out: Optional[torch.Tensor] = None,
    ) -> Tuple[torch.Tensor, EventOverlap, Callable]:
        if zero_copy:
            raise NotImplementedError(_ZERO_COPY_COMBINE_UNSUPPORTED)
        elastic = self._is_elastic

        if _USE_MACA:
            assert not (
                async_finish and return_recv_hook
            ), "MACA does not support async_finish and return_recv_hook together"

        if elastic:
            self._require_elastic_runtime()
            if self._use_fallback:
                raise RuntimeError("Elastic EP does not use the Python fallback.")
        assert x.dim() == 3 and x.is_contiguous()
        assert x.dtype == torch.bfloat16
        assert topk_idx.dim() == 2 and topk_idx.is_contiguous()
        assert topk_idx.dtype == torch.int64
        assert topk_weights.dim() == 2 and topk_weights.is_contiguous()
        assert topk_weights.dtype == torch.float32
        caller_active_ranks = active_ranks
        if active_ranks is None:
            active_ranks = self._active_ranks_tensor(device=x.device, dtype=torch.int32)
        else:
            assert active_ranks.dim() == 1 and active_ranks.is_contiguous()
            assert active_ranks.dtype == torch.int32
            if active_ranks.numel() != self.group_size:
                if self._is_elastic:
                    raise ValueError(
                        "active_ranks length must equal the fixed rank capacity."
                    )
                active_ranks = torch.ones(
                    (self.group_size,), dtype=torch.int32, device=x.device
                )
        # Same split-kernel behavior as dispatch().
        if _USE_SPLIT_SEND_RECV and async_finish:
            import warnings

            warnings.warn(
                "async_finish uses split SEND/RECV kernels plus a stream "
                "event, not CUDA cooperative single-kernel async semantics.",
                RuntimeWarning,
                stacklevel=2,
            )

        (
            src_info,
            layout_range,
            num_max_dispatch_tokens_per_rank,
            hidden,
            num_experts,
        ) = handle
        rank_bound = layout_range.size(1) if elastic else self.group_size
        num_local_experts = (
            self._num_experts_per_rank if elastic else num_experts // self.group_size
        )
        runtime_return_recv_hook = return_recv_hook or (
            _USE_SPLIT_SEND_RECV and not self._use_fallback
        )

        if self._use_fallback:
            combined_x, event, hook = self._fallback_combine(
                x,
                topk_idx,
                topk_weights,
                src_info,
                layout_range,
                num_max_dispatch_tokens_per_rank,
                num_experts,
                zero_copy,
                return_recv_hook,
                out,
            )
            backend_active_ranks = self._active_ranks_tensor(
                device=active_ranks.device, dtype=active_ranks.dtype
            )
            if (
                caller_active_ranks is not None
                and active_ranks.numel() == backend_active_ranks.numel()
            ):
                active_ranks.copy_(backend_active_ranks)
        else:
            assert x.size(0) == num_local_experts
            assert x.size(1) == rank_bound * num_max_dispatch_tokens_per_rank
            assert x.size(2) == hidden
            assert x.size(2) % 16 == 0 and x.size(2) % 128 == 0
            assert topk_idx.size() == topk_weights.size()
            assert topk_weights.size(0) <= num_max_dispatch_tokens_per_rank
            assert src_info.dim() == 2 and src_info.is_contiguous()
            assert src_info.dtype == torch.int32
            assert src_info.size(0) == x.size(0)
            assert layout_range.dim() == 2 and layout_range.is_contiguous()
            assert layout_range.dtype == torch.int64
            assert layout_range.size(0) == num_local_experts
            assert layout_range.size(1) == rank_bound
            combined_x = (
                out
                if out is not None
                else torch.empty(
                    (topk_weights.size(0), hidden), dtype=x.dtype, device=x.device
                )
            )
            if out is not None:
                assert out.dim() == 2 and out.is_contiguous()
                assert out.size(0) == topk_weights.size(0)
                assert out.size(1) == hidden
                assert out.dtype == x.dtype
            native_async_finish = async_finish and not (
                _USE_SPLIT_SEND_RECV and runtime_return_recv_hook
            )
            event, hook = self.runtime.combine(
                x.data_ptr(),
                topk_idx.data_ptr(),
                topk_weights.data_ptr(),
                src_info.data_ptr(),
                layout_range.data_ptr(),
                active_ranks.data_ptr(),
                x.size(0),
                topk_weights.size(0),
                hidden,
                topk_weights.size(1),
                num_max_dispatch_tokens_per_rank,
                num_experts,
                timeout_us,
                zero_copy,
                combined_x.data_ptr(),
                native_async_finish,
                runtime_return_recv_hook,
                _native_current_stream_ptr(),
            )
            if _USE_SPLIT_SEND_RECV:
                hook = self._wrap_maca_recv_hook(hook, event)
                if not return_recv_hook:
                    hook()
                    hook = None
        tensors_to_record = (
            x,
            topk_idx,
            topk_weights,
            src_info,
            layout_range,
            combined_x,
        )
        return (
            combined_x,
            EventOverlap(
                event, tensors_to_record if async_finish or return_recv_hook else None
            ),
            hook,
        )

    def get_next_combine_buffer(self, handle: object):
        raise NotImplementedError(_ZERO_COPY_COMBINE_UNSUPPORTED)

    # -----------------
    # Fallback helpers
    # -----------------
    class _DummyEvent:
        def current_stream_wait(self, stream_ptr: Optional[int] = None):
            _synchronize_device()

        def synchronize(self):
            _synchronize_device()

    @staticmethod
    def _fp8_cast(x: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        assert x.dim() == 2 and x.size(1) % 128 == 0
        m, n = x.shape
        x_view = x.view(m, -1, 128)
        x_amax = x_view.abs().float().amax(dim=2).view(m, -1).clamp(1e-4)
        x_fp8 = (
            (x_view * (448.0 / x_amax.unsqueeze(2))).to(torch.float8_e4m3fn).view(m, n)
        )
        x_scales = (x_amax / 448.0).view(m, -1)
        return x_fp8, x_scales

    def _fallback_dispatch(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        num_max_dispatch_tokens_per_rank: int,
        num_experts: int,
        use_fp8: bool,
        return_recv_hook: bool,
    ):
        with torch.profiler.record_function("dispatch"):
            num_tokens, hidden = x.shape
            k = topk_idx.size(1)
            num_ranks = self.group_size
            num_local_experts = num_experts // num_ranks

            # Gather sizes first to handle variable num_tokens per rank
            num_tokens_tensor = torch.tensor(
                [num_tokens], dtype=torch.int64, device=x.device
            )
            num_tokens_list = [
                torch.empty(1, dtype=torch.int64, device=x.device)
                for _ in range(num_ranks)
            ]
            dist.all_gather(num_tokens_list, num_tokens_tensor, group=self.group)
            num_tokens_per_rank = [t.item() for t in num_tokens_list]
            backend_active_ranks = self._active_ranks_list(x.device)
            for i in range(num_ranks):
                if backend_active_ranks[i] == 0:
                    num_tokens_per_rank[i] = 0
            max_num_tokens = max(num_tokens_per_rank)

            # Pad inputs to max_num_tokens for all_gather (all ranks must have same shape)
            if num_tokens < max_num_tokens:
                pad_size = max_num_tokens - num_tokens
                x_padded = torch.cat(
                    [
                        x,
                        torch.zeros((pad_size, hidden), dtype=x.dtype, device=x.device),
                    ],
                    dim=0,
                )
                topk_padded = torch.cat(
                    [
                        topk_idx,
                        torch.full(
                            (pad_size, k), -1, dtype=topk_idx.dtype, device=x.device
                        ),
                    ],
                    dim=0,
                )
            else:
                x_padded = x
                topk_padded = topk_idx

            num_max_dispatch_tokens = num_ranks * num_max_dispatch_tokens_per_rank

            # Gather inputs from all ranks (all have same shape after padding)
            all_x = torch.empty(
                (num_ranks, max_num_tokens, hidden), dtype=x.dtype, device=x.device
            )
            dist.all_gather_into_tensor(all_x, x_padded, group=self.group)
            all_topk = torch.empty(
                (num_ranks, max_num_tokens, k), dtype=topk_idx.dtype, device=x.device
            )
            dist.all_gather_into_tensor(all_topk, topk_padded, group=self.group)

            # Prepare outputs per local expert
            recv_x_list: List[torch.Tensor] = []
            recv_x_scales_list: List[torch.Tensor] = []
            recv_count = torch.zeros(
                (num_local_experts,), dtype=torch.int32, device=x.device
            )
            recv_src_info = torch.full(
                (num_local_experts, num_max_dispatch_tokens),
                -1,
                dtype=torch.int32,
                device=x.device,
            )
            layout_range = torch.zeros(
                (num_local_experts, num_ranks), dtype=torch.int64, device=x.device
            )

            for le in range(num_local_experts):
                expert_id = self.rank * num_local_experts + le
                # Collect tokens from all ranks that route to this expert
                tokens_per_rank_tensors: List[torch.Tensor] = []
                for src_rank in range(num_ranks):
                    src_num_tokens = num_tokens_per_rank[src_rank]
                    src_topk = all_topk[
                        src_rank, :src_num_tokens
                    ]  # Only consider valid tokens
                    # Find tokens that route to this expert
                    pos = (
                        (src_topk == expert_id)
                        .any(dim=1)
                        .nonzero(as_tuple=False)
                        .view(-1)
                    )
                    tokens_per_rank_tensors.append(pos)

                # Build ordered list grouped by src_rank (matching CUDA kernel behavior)
                begin = 0
                ordered_src_ranks_list: List[torch.Tensor] = []
                ordered_token_indices_list: List[torch.Tensor] = []
                for src_rank, tokens in enumerate(tokens_per_rank_tensors):
                    count = tokens.numel()
                    if count > 0:
                        layout_range[le, src_rank] = (begin << 32) | count
                        ordered_src_ranks_list.append(torch.full_like(tokens, src_rank))
                        ordered_token_indices_list.append(tokens)
                        begin += count
                    else:
                        layout_range[le, src_rank] = 0

                if ordered_src_ranks_list:
                    ordered_src_ranks = torch.cat(ordered_src_ranks_list)
                    ordered_token_indices = torch.cat(ordered_token_indices_list)
                else:
                    ordered_src_ranks = torch.empty(
                        0, dtype=topk_idx.dtype, device=x.device
                    )
                    ordered_token_indices = torch.empty(
                        0, dtype=topk_idx.dtype, device=x.device
                    )

                num_valid = min(ordered_src_ranks.numel(), num_max_dispatch_tokens)
                recv_count[le] = num_valid

                # Materialize data
                if num_valid > 0:
                    gathered = all_x[
                        ordered_src_ranks[:num_valid], ordered_token_indices[:num_valid]
                    ]
                    src_meta = ordered_token_indices[:num_valid].to(dtype=torch.int32)
                else:
                    gathered = torch.empty(
                        (num_valid, hidden), dtype=torch.bfloat16, device=x.device
                    )
                    src_meta = torch.empty(
                        (num_valid,), dtype=torch.int32, device=x.device
                    )

                # Pad to full size
                if use_fp8:
                    pad = num_max_dispatch_tokens - num_valid
                    if pad > 0:
                        pad_tensor = torch.zeros(
                            (pad, hidden), dtype=torch.bfloat16, device=x.device
                        )
                        gathered = torch.cat([gathered, pad_tensor], dim=0)
                    fp8, scales = self._fp8_cast(gathered)
                    recv_x_list.append(fp8)
                    recv_x_scales_list.append(scales)
                else:
                    pad = num_max_dispatch_tokens - num_valid
                    if pad > 0:
                        pad_tensor = torch.zeros(
                            (pad, hidden), dtype=torch.bfloat16, device=x.device
                        )
                        gathered = torch.cat([gathered, pad_tensor], dim=0)
                    recv_x_list.append(gathered)

                # src info padded
                if num_valid > 0:
                    recv_src_info[le, :num_valid] = src_meta

            if use_fp8:
                packed_recv_x = (
                    torch.stack(recv_x_list, dim=0)
                    if len(recv_x_list) > 0
                    else torch.empty(
                        (0, num_max_dispatch_tokens, hidden),
                        dtype=torch.float8_e4m3fn,
                        device=x.device,
                    )
                )
                # Calculate scales shape correctly
                num_scales_per_token = hidden // 128
                packed_recv_x_scales = (
                    torch.stack(recv_x_scales_list, dim=0)
                    if len(recv_x_scales_list) > 0
                    else torch.empty(
                        (0, num_max_dispatch_tokens, num_scales_per_token),
                        dtype=torch.float32,
                        device=x.device,
                    )
                )
            else:
                packed_recv_x = (
                    torch.stack(recv_x_list, dim=0)
                    if len(recv_x_list) > 0
                    else torch.empty(
                        (0, num_max_dispatch_tokens, hidden),
                        dtype=torch.bfloat16,
                        device=x.device,
                    )
                )
                packed_recv_x_scales = None

            # Allocate zero-copy buffer for next combine
            self._fallback_next_combine_buffer = torch.empty(
                (num_local_experts, num_max_dispatch_tokens, hidden),
                dtype=torch.bfloat16,
                device=x.device,
            )

            hook = (lambda: None) if return_recv_hook else (lambda: None)
            event = Buffer._DummyEvent()
            return (
                packed_recv_x,
                packed_recv_x_scales,
                recv_count,
                recv_src_info,
                layout_range,
                event,
                hook,
            )

    def _fallback_combine(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        topk_weights: torch.Tensor,
        src_info: torch.Tensor,
        layout_range: torch.Tensor,
        num_max_dispatch_tokens_per_rank: int,
        num_experts: int,
        zero_copy: bool,
        return_recv_hook: bool,
        out: Optional[torch.Tensor],
    ):
        with torch.profiler.record_function("combine"):
            num_tokens = topk_idx.size(0)
            hidden = (x if not zero_copy else self._fallback_next_combine_buffer).size(
                -1
            )
            num_ranks = self.group_size
            num_local_experts = num_experts // num_ranks

            # Gather sizes first to handle variable num_tokens per rank
            num_tokens_tensor = torch.tensor(
                [num_tokens], dtype=torch.int64, device=topk_idx.device
            )
            num_tokens_list = [
                torch.empty(1, dtype=torch.int64, device=topk_idx.device)
                for _ in range(num_ranks)
            ]
            dist.all_gather(num_tokens_list, num_tokens_tensor, group=self.group)
            num_tokens_per_rank = [t.item() for t in num_tokens_list]
            backend_active_ranks = self._active_ranks_list(topk_idx.device)
            for i in range(num_ranks):
                if backend_active_ranks[i] == 0:
                    num_tokens_per_rank[i] = 0
            max_num_tokens = max(num_tokens_per_rank)

            # Gather routing info across ranks to fetch per-token weights
            k = topk_idx.size(1)
            # Pad to max_num_tokens for all_gather
            if num_tokens < max_num_tokens:
                pad_size = max_num_tokens - num_tokens
                topk_padded = torch.cat(
                    [
                        topk_idx,
                        torch.full(
                            (pad_size, k),
                            -1,
                            dtype=topk_idx.dtype,
                            device=topk_idx.device,
                        ),
                    ],
                    dim=0,
                )
                topk_w_padded = torch.cat(
                    [
                        topk_weights,
                        torch.zeros(
                            (pad_size, k),
                            dtype=topk_weights.dtype,
                            device=topk_weights.device,
                        ),
                    ],
                    dim=0,
                )
            else:
                topk_padded = topk_idx
                topk_w_padded = topk_weights

            all_topk_idx = torch.empty(
                (num_ranks, max_num_tokens, k),
                dtype=topk_idx.dtype,
                device=topk_idx.device,
            )
            dist.all_gather_into_tensor(all_topk_idx, topk_padded, group=self.group)
            all_topk_w = torch.empty(
                (num_ranks, max_num_tokens, k),
                dtype=topk_weights.dtype,
                device=topk_weights.device,
            )
            dist.all_gather_into_tensor(all_topk_w, topk_w_padded, group=self.group)

            expert_buffers = self._fallback_next_combine_buffer if zero_copy else x
            # Ensure bf16 input for accumulation
            if expert_buffers.dtype != torch.bfloat16:
                # FP8 path should already have been cast back by caller before combine in tests
                expert_buffers = expert_buffers.to(torch.bfloat16)

            # Build send buffer [num_ranks, max_num_tokens, hidden]
            send_buf = torch.zeros(
                (num_ranks, max_num_tokens, hidden),
                dtype=torch.bfloat16,
                device=expert_buffers.device,
            )

            for le in range(num_local_experts):
                expert_id = self.rank * num_local_experts + le
                # layout_range[le, j]: upper 32 begin, lower 32 count
                for src_rank in range(num_ranks):
                    entry = layout_range[le, src_rank]
                    begin = (entry >> 32).item() & 0xFFFFFFFF
                    count = (entry & ((1 << 32) - 1)).item()
                    if count == 0:
                        continue
                    tokens = src_info[le, begin : begin + count].to(torch.long)
                    contrib = expert_buffers[le, begin : begin + count]

                    # Get source rank's actual token count and validate tokens
                    src_num_tokens = num_tokens_per_rank[src_rank]
                    valid_mask = tokens < src_num_tokens

                    if valid_mask.any():
                        tokens_valid = tokens[valid_mask]
                        contrib_valid = contrib[valid_mask]

                        # Find the per-token weight for this expert on src_rank
                        idx_rows = all_topk_idx[
                            src_rank, tokens_valid
                        ]  # [count_valid, k]
                        w_rows = all_topk_w[src_rank, tokens_valid]  # [count_valid, k]
                        mask = idx_rows == expert_id
                        weights = (w_rows * mask).sum(dim=1).view(-1, 1)
                        send_buf[src_rank, tokens_valid] += contrib_valid * weights

            # All-reduce then take local slice (only valid tokens)
            dist.all_reduce(send_buf, group=self.group)
            combined_x = send_buf[self.rank, :num_tokens]

            # Write to out if provided
            if out is not None:
                out.copy_(combined_x)
                combined = out
            else:
                combined = combined_x

            hook = (lambda: None) if return_recv_hook else (lambda: None)
            event = Buffer._DummyEvent()
            return combined, event, hook
