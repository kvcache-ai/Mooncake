# SPDX-License-Identifier: Apache-2.0
"""Thin adapter for Mooncake's public elastic Buffer API."""

import os
from typing import Any, Optional

import torch


class MooncakeAdapter:
    @staticmethod
    def size_hint(
        max_tokens_per_rank: int,
        hidden: int,
        max_num_ranks: int,
        num_experts: int,
    ) -> int:
        from mooncake.mooncake_ep_buffer import Buffer

        return Buffer.get_low_latency_buffer_size_hint(
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
        from mooncake.mooncake_ep_buffer import Buffer

        self.topk_idx_dtype = torch.int64
        self._buffer = Buffer.create_elastic(
            disable_ll_nvlink=disable_ll_nvlink,
            explicitly_destroy=True,
            rank=rank,
            tcp_store_group=store,
            timeout_ms=timeout_ms,
        )
        self._buffer.update_memory_buffers(
            num_ranks=max_num_ranks,
            num_experts_per_rank=num_experts_per_rank,
            num_rdma_bytes=num_rdma_bytes,
        )
        self._route_reported = False

    @staticmethod
    def dispatch_format_cases(use_fp8: bool, format_set: str):
        if format_set != "common":
            raise ValueError("Mooncake supports only the common parity format set")
        return ((False, False),)

    @staticmethod
    def not_applicable_dispatch_modes(format_set: str):
        if format_set == "common":
            return (
                "round_scale=True (NIXL-only FP8 representation)",
                "use_ue8m0=True (NIXL-only UE8M0 scale format)",
            )
        return ()

    def _assert_native_route(self) -> None:
        runtime = self._buffer.runtime
        if runtime is None:
            raise RuntimeError("Mooncake elastic native runtime is not initialized")
        use_fallback = bool(self._buffer._use_fallback)
        use_fast_path = bool(runtime.use_fast_path())
        if use_fallback or not use_fast_path:
            raise RuntimeError(
                "P1 requires Mooncake native fast path; "
                f"use_fallback={use_fallback}, use_fast_path={use_fast_path}"
            )
        if not self._route_reported:
            print(
                "P1_NATIVE_ROUTE backend=mooncake "
                f"use_fallback={use_fallback} use_fast_path={use_fast_path}",
                flush=True,
            )
            mapped = set()
            try:
                with open("/proc/self/maps", encoding="utf-8") as maps_file:
                    for line in maps_file:
                        fields = line.rstrip().split(None, 5)
                        if len(fields) != 6 or not fields[5].startswith("/"):
                            continue
                        path = fields[5]
                        name = os.path.basename(path)
                        if (
                            (name.startswith("_ep") and name.endswith(".so"))
                            or name == "libmooncake_ep_device.so"
                            or "libcudart.so" in name
                            or "libcuda.so" in name
                        ):
                            mapped.add(os.path.realpath(path))
            except OSError as exc:
                mapped.add(f"<maps-read-error:{type(exc).__name__}>")
            print(
                f"P1_NATIVE_MAPS pid={os.getpid()} paths={';'.join(sorted(mapped)) or '<none>'}",
                flush=True,
            )
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
        if round_scale:
            raise NotImplementedError(
                "NIXL-only round_scale is explicitly N/A for Mooncake parity"
            )
        if use_ue8m0:
            raise NotImplementedError(
                "NIXL-only UE8M0 is explicitly N/A for Mooncake parity"
            )
        return self._buffer.dispatch(
            x,
            topk_idx,
            max_tokens_per_rank,
            num_experts,
            use_fp8=use_fp8,
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
        if use_logfmt:
            raise NotImplementedError(
                "LogFMT is disabled in the fixed NIXL elastic test configuration"
            )
        return self._buffer.combine(
            x,
            topk_idx,
            topk_weights,
            handle,
            use_logfmt=use_logfmt,
            zero_copy=zero_copy,
            async_finish=async_finish,
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
