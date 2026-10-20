# SPDX-License-Identifier: Apache-2.0
"""Thin adapter for the fixed NIXL reference implementation."""

import os
from typing import Any, Optional

import torch


def _report_nixl_runtime_maps(phase: str) -> None:
    print(
        f"P1_NIXL_RUNTIME phase={phase} "
        f"UCX_MEMTYPE_CACHE={os.environ.get('UCX_MEMTYPE_CACHE', '<unset>')!r} "
        f"UCX_LOG_LEVEL={os.environ.get('UCX_LOG_LEVEL', '<unset>')!r}",
        flush=True,
    )
    markers = (
        "libcuda",
        "libcudart",
        "libucm_cuda",
        "libuct_cuda",
        "libplugin_ucx",
        "libucp",
        "nixl_ep_cpp",
    )
    mapped_paths = set()
    try:
        with open("/proc/self/maps", encoding="utf-8", errors="replace") as maps_file:
            for line in maps_file:
                fields = line.rstrip().split(None, 5)
                if (
                    len(fields) == 6
                    and fields[5].startswith("/")
                    and any(marker in fields[5].lower() for marker in markers)
                ):
                    mapped_paths.add(fields[5])
    except OSError as exc:
        print(f"P1_NIXL_MAPS_ERROR error={type(exc).__name__}", flush=True)
        return
    for path in sorted(mapped_paths) or ("<none>",):
        print(f"P1_NIXL_MAP path={path}", flush=True)


class NixlAdapter:
    @staticmethod
    def size_hint(
        max_tokens_per_rank: int,
        hidden: int,
        max_num_ranks: int,
        num_experts: int,
    ) -> int:
        import nixl_ep

        return nixl_ep.Buffer.get_rdma_size_hint(
            max_tokens_per_rank, hidden, max_num_ranks, num_experts
        )

    def __init__(
        self,
        rank: int,
        max_num_ranks: int,
        num_experts_per_rank: int,
        num_rdma_bytes: int,
        store: Any,
        timeout_ms: int,
        disable_ll_nvlink: bool,
    ) -> None:
        import nixl_ep

        self._nixl_ep = nixl_ep
        self.topk_idx_dtype = nixl_ep.topk_idx_t
        self._buffer = nixl_ep.Buffer(
            rank=rank,
            disable_ll_nvlink=disable_ll_nvlink,
            explicitly_destroy=True,
            tcp_store_group=store,
            timeout_ms=timeout_ms,
        )
        try:
            self._buffer.update_memory_buffers(
                num_ranks=max_num_ranks,
                num_experts_per_rank=num_experts_per_rank,
                num_rdma_bytes=num_rdma_bytes,
            )
        except Exception:
            _report_nixl_runtime_maps("registration-failed")
            raise
        else:
            _report_nixl_runtime_maps("registration-succeeded")
        self._route_reported = False

    @staticmethod
    def dispatch_format_cases(use_fp8: bool, format_set: str):
        if format_set == "common" or not use_fp8:
            return ((False, False),)
        if format_set != "nixl-full":
            raise ValueError(f"unsupported format set: {format_set}")
        # Preserve the original NIXL loops: round_scale applies to FP8,
        # and UE8M0 is tested only with round_scale enabled.
        return ((False, False), (True, False), (True, True))

    @staticmethod
    def not_applicable_dispatch_modes(format_set: str):
        return ()

    def _assert_native_route(self) -> None:
        if not self._route_reported:
            print("P1_NATIVE_ROUTE backend=nixl route=native", flush=True)
            self._route_reported = True

    def dispatch(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        max_tokens_per_rank: int,
        num_experts: int,
        *,
        use_fp8: bool,
        round_scale: bool = False,
        use_ue8m0: bool = False,
        cumulative_local_expert_recv_stats: Optional[torch.Tensor] = None,
        async_finish: bool = False,
        return_recv_hook: bool = False,
    ):
        self._assert_native_route()
        return self._buffer.dispatch(
            x,
            topk_idx,
            max_tokens_per_rank,
            num_experts,
            use_fp8=use_fp8,
            round_scale=round_scale,
            use_ue8m0=use_ue8m0,
            cumulative_local_expert_recv_stats=cumulative_local_expert_recv_stats,
            async_finish=async_finish,
            return_recv_hook=return_recv_hook,
        )

    def combine(
        self,
        x: torch.Tensor,
        topk_idx: torch.Tensor,
        topk_weights: torch.Tensor,
        handle: Any,
        *,
        use_logfmt: bool = False,
        async_finish: bool = False,
        zero_copy: bool = False,
        return_recv_hook: bool = False,
        out: Optional[torch.Tensor] = None,
    ):
        self._assert_native_route()
        return self._buffer.combine(
            x,
            topk_idx,
            topk_weights,
            handle,
            use_logfmt=use_logfmt,
            async_finish=async_finish,
            zero_copy=zero_copy,
            return_recv_hook=return_recv_hook,
            out=out,
        )

    def __getattr__(self, name: str):
        if name in {
            "connect_ranks",
            "disconnect_ranks",
            "query_mask_buffer",
            "get_next_combine_buffer",
            "barrier",
        }:
            return getattr(self._buffer, name)
        raise AttributeError(name)

    def destroy(self) -> None:
        self._buffer.destroy()
