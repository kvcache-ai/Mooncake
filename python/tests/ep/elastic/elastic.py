# SPDX-FileCopyrightText: Copyright (c) 2025 DeepSeek
# SPDX-FileCopyrightText: Copyright (c) 2025-2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#
# This file incorporates material from the DeepSeek project, licensed under the MIT License.
# The modifications made by NVIDIA are licensed under the Apache License, Version 2.0.
#
# SPDX-License-Identifier: MIT AND Apache-2.0
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

import argparse
import json
import os
import random
import signal
import socket
import threading
import time
from functools import partial
from typing import cast

import rank_server
import store_group
import torch
from backend import create_backend, size_hint
from plan import Plan
from test_utils import (
    bench,
    bench_kineto,
    calc_diff,
    hash_tensor,
    kineto_cuda_available,
    per_token_cast_back,
)

TCP_STORE_PORT = 9999
RANK_SERVER_PORT = 10000
CONTROL_ENV = "MC_EP_ELASTIC_CONTROL_ADDR"
CONTROL_TIMEOUT_ENV = "MC_EP_ELASTIC_CONTROL_TIMEOUT_S"

TMA_TOKEN_ALIGNMENT = 4


def _control_rpc(op: str, **fields):
    address = os.environ.get(CONTROL_ENV)
    if not address:
        return None
    host, port_text = address.rsplit(":", 1)
    timeout_s = float(os.environ.get(CONTROL_TIMEOUT_ENV, "3600"))
    request = {"op": op, **fields}
    with socket.create_connection((host, int(port_text)), timeout=timeout_s) as sock:
        sock.settimeout(timeout_s)
        sock.sendall(json.dumps(request).encode("utf-8") + b"\n")
        with sock.makefile("rb") as stream:
            response = json.loads(stream.readline())
    if not response.get("ok", False):
        raise RuntimeError(response.get("error", f"launcher rejected {op}"))
    return response


def _phase_barrier(barrier: str, rank: int, phase: int) -> None:
    _control_rpc("barrier", barrier=barrier, rank=rank, phase=phase)


def _mark_expected_fault(rank: int, phase: int) -> None:
    _control_rpc("expected_fault", rank=rank, phase=phase)


def tma_aligned_max_tokens(num_tokens: int) -> int:
    return (
        (num_tokens + TMA_TOKEN_ALIGNMENT - 1) // TMA_TOKEN_ALIGNMENT
    ) * TMA_TOKEN_ALIGNMENT


def non_negative_int(value: str) -> int:
    try:
        int_value = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a non-negative integer") from exc
    if int_value < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return int_value


def handle_sigterm(
    signum,
    frame,
    buffer,
    plan: Plan,
    rank_client: rank_server.RankClient,
    global_rank: int,
    expected_fault_signal: threading.Event,
):
    expected = expected_fault_signal.is_set() and global_rank in plan.get_killed_ranks()
    print(
        f"SIGTERM ({signum}) received for process {os.getpid()}! releasing rank and exiting...",
        flush=True,
    )
    rank_client.release_rank(user_context=plan.get_phase())
    if buffer is not None:
        buffer.destroy()  # to invalidate local MD
        del buffer
    if expected:
        _control_rpc("fault_done", rank=global_rank, phase=plan.get_phase())

    # Continue with the original SIGTERM status; unexpected signals are not
    # reclassified as planned fault exits.
    signal.signal(signum, signal.SIG_DFL)
    signal.raise_signal(signum)


def self_kill():
    os.kill(os.getpid(), signal.SIGTERM)


def test_main(
    num_tokens: int,
    max_tokens_per_rank: int,
    hidden: int,
    num_experts: int,
    num_topk: int,
    rank: int,
    num_ranks: int,
    max_num_ranks: int,
    buffer,
    use_logfmt: bool = False,
    seed: int = 0,
    kineto: bool = False,
    fault_tolerance_test: bool = False,
    phase: int = 0,
    mark_expected_fault=None,
    format_set: str = "common",
):
    torch.manual_seed(seed + rank)
    torch.cuda.manual_seed(seed + rank)
    random.seed(seed + rank)

    assert num_experts % num_ranks == 0
    num_local_experts = num_experts // num_ranks

    # NOTES: the integers greater than 256 exceed the BF16 precision limit
    rank_offset = 128
    assert (
        num_ranks - rank_offset < 257
    ), "Too many ranks (exceeding test precision limit)"

    # Track masked ranks (like shrink_test in elastic.py)
    mask_status = torch.zeros((max_num_ranks,), dtype=torch.int32, device="cuda")

    x = torch.ones((num_tokens, hidden), dtype=torch.bfloat16, device="cuda") * (
        rank - rank_offset
    )
    x[:, -128:] = torch.arange(num_tokens, device="cuda").to(torch.bfloat16).view(-1, 1)
    x_list = [x]
    for i in range(4 if use_logfmt else 0):
        # NOTES: make more LogFMT casts and also with some BF16
        x_list.append(
            torch.randn((num_tokens, hidden), dtype=torch.bfloat16, device="cuda")
            * 0.5
            * random.random()
        )
    # NOTES: the last one is for performance testing
    # Most of the values in the perf case is lower than the threshold, casting most channels
    x_list.append(
        torch.randn((num_tokens, hidden), dtype=torch.bfloat16, device="cuda") * 0.1
    )

    torch.manual_seed(seed + rank + 1000)
    torch.cuda.manual_seed(seed + rank + 1000)
    random.seed(seed + rank)

    scores = (
        torch.randn((num_tokens, num_experts), dtype=torch.float32, device="cuda").abs()
        + 1
    )
    topk_idx = torch.topk(scores, num_topk, dim=-1, largest=True, sorted=True)[1]
    topk_idx = topk_idx.to(buffer.topk_idx_dtype)
    topk_weights = torch.randn(
        (num_tokens, num_topk), dtype=torch.float32, device="cuda"
    ).abs()

    # Randomly mask some positions
    for i in range(10):
        topk_idx[
            random.randint(0, num_tokens - 1), random.randint(0, num_topk - 1)
        ] = -1

    all_topk_idx = torch.full(
        (max_num_ranks, num_tokens, num_topk), -1, dtype=topk_idx.dtype, device="cuda"
    )
    for r in range(num_ranks):
        # Use same deterministic reset as above (seed + r + 1000)
        torch.manual_seed(seed + r + 1000)
        torch.cuda.manual_seed(seed + r + 1000)
        r_random = random.Random(seed + r)
        r_scores = (
            torch.randn(
                (num_tokens, num_experts), dtype=torch.float32, device="cuda"
            ).abs()
            + 1
        )
        r_topk_idx = torch.topk(r_scores, num_topk, dim=-1, largest=True, sorted=True)[
            1
        ]
        r_topk_idx = r_topk_idx.to(buffer.topk_idx_dtype)
        # Apply same random masking
        for i in range(10):
            r_topk_idx[
                r_random.randint(0, num_tokens - 1), r_random.randint(0, num_topk - 1)
            ] = -1
        all_topk_idx[r] = r_topk_idx

    # Check dispatch correctness
    do_check = True
    hash_value, num_times = 0, 0
    timer = None
    for current_x in x_list:
        for return_recv_hook in (False, True):
            for dispatch_use_fp8 in (False, True):
                for round_scale, use_ue8m0 in buffer.dispatch_format_cases(
                    dispatch_use_fp8, format_set
                ):
                    num_times += 1
                    for i in range((num_times % 2) + 1):
                        # Kill this rank at the beginning of the first dispatch if marked to be killed
                        if fault_tolerance_test and timer is None:
                            if mark_expected_fault is not None:
                                mark_expected_fault(rank, phase)
                            print(
                                f"[rank {rank}] Killing rank during dispatch/combine",
                                flush=True,
                            )
                            timer = threading.Timer(0.0001, self_kill)
                            timer.start()

                        cumulative_local_expert_recv_stats = torch.zeros(
                            (num_local_experts,), dtype=torch.int, device="cuda"
                        )
                        packed_recv_x, packed_recv_count, handle, event, hook = (
                            buffer.dispatch(
                                current_x,
                                topk_idx,
                                max_tokens_per_rank,
                                num_experts,
                                use_fp8=dispatch_use_fp8,
                                round_scale=round_scale,
                                use_ue8m0=use_ue8m0,
                                cumulative_local_expert_recv_stats=cumulative_local_expert_recv_stats,
                                async_finish=not return_recv_hook,
                                return_recv_hook=return_recv_hook,
                            )
                        )
                        hook() if return_recv_hook else event.current_stream_wait()
                    # Query mask buffer to get current failure status
                    buffer.query_mask_buffer(mask_status)
                    packed_recv_x = (
                        (packed_recv_x[0], packed_recv_x[1].contiguous())
                        if dispatch_use_fp8
                        else packed_recv_x
                    )
                    simulated_gemm_x = (
                        per_token_cast_back(
                            packed_recv_x[0].view(-1, hidden),
                            packed_recv_x[1].view(-1, hidden // 128),
                        ).view(packed_recv_x[0].shape)
                        if dispatch_use_fp8
                        else cast(torch.Tensor, packed_recv_x).clone()
                    )

                    for i in range(num_local_experts if do_check else 0):
                        expert_id = rank * num_local_experts + i
                        recv_x = (
                            per_token_cast_back(
                                packed_recv_x[0][i], packed_recv_x[1][i]
                            )
                            if dispatch_use_fp8
                            else packed_recv_x[i]
                        )
                        recv_count, recv_src_info, recv_layout_range = (
                            packed_recv_count[i],
                            handle[0][i],
                            handle[1][i],
                        )

                        # Check expert indices
                        int_mask = (2**32) - 1
                        num_valid_tokens = recv_count.item()
                        assert (
                            cumulative_local_expert_recv_stats[i].item()
                            == num_valid_tokens
                        ), f"{cumulative_local_expert_recv_stats[i].item()} != {num_valid_tokens}"
                        assert (
                            num_valid_tokens
                            == (recv_layout_range & int_mask).sum().item()
                        ), f"{num_valid_tokens} != {recv_layout_range & int_mask}.sum().item()"
                        assert (
                            num_valid_tokens
                            == (all_topk_idx == expert_id)
                            .sum(dim=[1, 2])[mask_status == 0]
                            .sum()
                            .item()
                        ), f"{num_valid_tokens} != {(all_topk_idx == expert_id).sum(dim=[1, 2])[mask_status == 0].sum().item()}"

                        if num_valid_tokens == 0:
                            continue
                        # Check received data
                        if current_x is x:
                            recv_x = recv_x[:num_valid_tokens]
                            recv_x_amin = recv_x[:, :-128].amin(dim=-1)
                            recv_src_info = recv_src_info[:num_valid_tokens]
                            assert torch.equal(
                                recv_x_amin, recv_x[:, :-128].amax(dim=-1)
                            )
                            if round_scale:
                                assert (
                                    calc_diff(recv_x[:, -1], recv_src_info.view(-1))
                                    < 0.007
                                )
                            else:
                                assert (
                                    recv_x[:, -128:]
                                    - recv_src_info.view(-1, 1) % num_tokens
                                ).sum().item() == 0
                            for j in range(num_ranks):
                                if mask_status[j]:
                                    continue
                                begin_idx, count = (
                                    (recv_layout_range[j] >> 32).item(),
                                    (recv_layout_range[j] & int_mask).item(),
                                )
                                if not round_scale:
                                    assert (
                                        recv_x_amin == j - rank_offset
                                    ).sum().item() == (
                                        all_topk_idx[j] == expert_id
                                    ).sum().item()
                                    assert (
                                        recv_x[begin_idx : begin_idx + count, :-128]
                                        - j
                                        + rank_offset
                                    ).sum().item() == 0
                        if dispatch_use_fp8:
                            hash_value ^= hash_tensor(
                                packed_recv_x[0][i, :num_valid_tokens]
                            )
                            hash_value ^= hash_tensor(
                                packed_recv_x[1][i, :num_valid_tokens]
                            )
                        else:
                            hash_value ^= hash_tensor(
                                cast(torch.Tensor, packed_recv_x)[i, :num_valid_tokens]
                            )

                    # Check combine correctness
                    for zero_copy in (False,) if use_logfmt else (False, True):
                        if zero_copy:
                            buffer.get_next_combine_buffer(handle)[:, :, :] = (
                                simulated_gemm_x
                            )
                        out = torch.empty(
                            (num_tokens, hidden),
                            dtype=torch.bfloat16,
                            device="cuda",
                        )
                        combined_x, event, hook = buffer.combine(
                            simulated_gemm_x,
                            topk_idx,
                            topk_weights,
                            handle,
                            use_logfmt=use_logfmt,
                            async_finish=not return_recv_hook,
                            zero_copy=zero_copy,
                            return_recv_hook=return_recv_hook,
                            out=out,
                        )
                        hook() if return_recv_hook else event.current_stream_wait()
                        # Query mask buffer again after combine
                        buffer.query_mask_buffer(mask_status)
                        if do_check:
                            # Adjust topk_idx for validation: mark selections from masked ranks as -1
                            owner_by_expert = (
                                torch.arange(num_experts, device="cuda")
                                // num_local_experts
                            )
                            fail_owner_mask = (mask_status != 0).index_select(
                                0, owner_by_expert
                            )
                            valid_topk_idx = topk_idx >= 0
                            failed_topk_idx = torch.zeros_like(
                                topk_idx, device="cuda", dtype=torch.bool
                            )
                            failed_topk_idx[valid_topk_idx] = (
                                fail_owner_mask.index_select(
                                    0, topk_idx[valid_topk_idx].to(torch.int64)
                                )
                            )
                            topk_idx[failed_topk_idx] = -1
                            diff = calc_diff(
                                current_x
                                * topk_weights.masked_fill(topk_idx == -1, 0)
                                .sum(dim=1)
                                .view(-1, 1),
                                combined_x,
                            )
                            assert torch.isnan(combined_x).sum().item() == 0
                            assert diff < (
                                9e-4 if dispatch_use_fp8 else 1e-5
                            ), f"Error: {diff=}, {dispatch_use_fp8=}, {zero_copy=}"
                            hash_value ^= hash_tensor(combined_x)

    # noinspection PyShadowingNames
    def large_gemm_with_hook(hook):
        mat_0 = torch.randn((8192, 8192), dtype=torch.float)
        mat_1 = torch.randn((8192, 8192), dtype=torch.float)
        mat_0 @ mat_1
        hook()

    # noinspection PyShadowingNames
    def test_func(return_recv_hook: bool):
        recv_x, recv_count, handle, event, hook = buffer.dispatch(
            current_x,
            topk_idx,
            max_tokens_per_rank,
            num_experts,
            cumulative_local_expert_recv_stats=cumulative_local_expert_recv_stats,
            use_fp8=True,
            async_finish=False,
            return_recv_hook=return_recv_hook,
        )
        return_recv_hook and large_gemm_with_hook(hook)
        combined_x, event, hook = buffer.combine(
            simulated_gemm_x,
            topk_idx,
            topk_weights,
            handle,
            use_logfmt=use_logfmt,
            return_recv_hook=return_recv_hook,
        )
        return_recv_hook and large_gemm_with_hook(hook)

    def test_barrier():
        buffer.barrier()

    # Calculate bandwidth
    num_fp8_bytes, num_bf16_bytes = (hidden + hidden / 128 * 4 + 16), hidden * 2
    num_logfmt10_bytes = hidden * 10 / 8 + hidden / 128 * 4
    num_dispatch_comm_bytes, num_combine_comm_bytes = 0, 0
    for i in range(num_tokens):
        num_selections = (topk_idx[i] != -1).sum().item()
        num_dispatch_comm_bytes += num_fp8_bytes * num_selections
        num_combine_comm_bytes += (
            num_logfmt10_bytes if use_logfmt else num_bf16_bytes
        ) * num_selections

    # Dispatch + combine testing
    avg_t, min_t, max_t = bench(partial(test_func, return_recv_hook=False))
    print(
        f"[rank {rank}] Dispatch + combine bandwidth: {(num_dispatch_comm_bytes + num_combine_comm_bytes) / 1e9 / avg_t:.2f} GB/s, "
        f"avg_t={avg_t * 1e6:.2f} us, min_t={min_t * 1e6:.2f} us, max_t={max_t * 1e6:.2f} us",
        flush=True,
    )

    # Separate profiling
    if not kineto:
        return

    for return_recv_hook in (False, True):
        buffer.barrier()
        dispatch_t, combine_t = bench_kineto(
            partial(test_func, return_recv_hook=return_recv_hook),
            kernel_names=("dispatch", "combine"),
            barrier_comm_profiling=True,
            suppress_kineto_output=False,
            num_kernels_per_period=2 if return_recv_hook else 1,
            barrier_fn=test_barrier,
        )
        if not return_recv_hook:
            print(
                f"[rank {rank}] Dispatch bandwidth: {num_dispatch_comm_bytes / 1e9 / dispatch_t:.2f} GB/s, avg_t={dispatch_t * 1e6:.2f} us | "
                f"Combine bandwidth: {num_combine_comm_bytes / 1e9 / combine_t:.2f} GB/s, avg_t={combine_t * 1e6:.2f} us",
                flush=True,
            )
        else:
            print(
                f"[rank {rank}] Dispatch send/recv time: {dispatch_t[0] * 1e6:.2f} + {dispatch_t[1] * 1e6:.2f} us | "
                f"Combine send/recv time: {combine_t[0] * 1e6:.2f} + {combine_t[1] * 1e6:.2f} us",
                flush=True,
            )


def worker(torch_rank: int, args: argparse.Namespace):
    server_addr = args.tcp_server if args.tcp_server else "127.0.0.1"
    rank_client = rank_server.RankClient(server_addr, RANK_SERVER_PORT)
    local_rank, global_rank, last_active_phase = rank_client.get_rank()
    plan = Plan(
        args.plan,
        global_rank,
        start_phase=last_active_phase if last_active_phase is not None else 0,
    )
    if len(plan.phases) > 1 and not os.environ.get(CONTROL_ENV):
        raise RuntimeError(
            "multi-phase elastic plans must run through launch_elastic.py"
        )
    if plan.current_phase == -1:
        print(
            f"Process {torch_rank} -> no plan phases were found for rank {global_rank} after phase {last_active_phase}, exiting",
            flush=True,
        )
        return

    max_num_ranks = plan.get_max_rank() + 1
    visible_text = os.environ.get("CUDA_VISIBLE_DEVICES", "")
    visible_devices = [item.strip() for item in visible_text.split(",") if item.strip()]
    if visible_devices and local_rank >= len(visible_devices):
        raise RuntimeError(
            f"local_rank={local_rank} exceeds CUDA_VISIBLE_DEVICES={visible_devices}"
        )
    selected_gpu = visible_devices[local_rank] if visible_devices else str(local_rank)

    # Initialize torch on the rank-server local ordinal. CUDA_VISIBLE_DEVICES
    # was assigned by the launcher and is not rewritten inside the worker.
    torch.set_default_dtype(torch.bfloat16)
    torch.set_default_device("cuda")
    torch.cuda.set_device(local_rank)
    print(
        "P1_WORKER_READY "
        + json.dumps(
            {
                "mp_ordinal": torch_rank,
                "global_rank": global_rank,
                "local_rank": local_rank,
                "os_pid": os.getpid(),
                "process_group_id": os.getpgid(0),
                "hostname": socket.gethostname(),
                "visible_devices": visible_devices,
                "selected_gpu": selected_gpu,
                "cuda_ordinal": local_rank,
            },
            sort_keys=True,
        ),
        flush=True,
    )

    tcp_store = store_group.create_client_store(
        master_addr=server_addr,
        port=TCP_STORE_PORT,
    )

    # Create the selected backend through the public test adapter.
    max_tokens_per_rank = tma_aligned_max_tokens(args.num_tokens)
    num_rdma_bytes = size_hint(
        args.backend,
        max_tokens_per_rank=max_tokens_per_rank,
        hidden=args.hidden_dim,
        max_num_ranks=max_num_ranks,
        num_experts=args.num_experts_per_rank * max_num_ranks,
    )
    if local_rank == 0:
        print(f"Allocating buffer size: {num_rdma_bytes / 1e6} MB ...", flush=True)

    buffer = create_backend(
        args.backend,
        rank=global_rank,
        max_num_ranks=max_num_ranks,
        num_experts_per_rank=args.num_experts_per_rank,
        num_rdma_bytes=num_rdma_bytes,
        store=tcp_store,
        timeout_ms=args.timeout_ms,
        disable_ll_nvlink=args.disable_ll_nvlink,
    )
    for feature in buffer.not_applicable_dispatch_modes(args.format_set):
        print(
            f"P1_FORMAT_N/A backend={args.backend} feature={feature}",
            flush=True,
        )

    expected_fault_signal = threading.Event()
    signal.signal(
        signal.SIGTERM,
        partial(
            handle_sigterm,
            buffer=buffer,
            plan=plan,
            rank_client=rank_client,
            global_rank=global_rank,
            expected_fault_signal=expected_fault_signal,
        ),
    )
    remote_ranks = set()
    mask_status = torch.zeros((max_num_ranks,), dtype=torch.int32, device="cuda")

    while True:
        print(
            f"global_rank={global_rank}, local_rank={local_rank} -> start phase {plan.get_phase()}",
            flush=True,
        )

        added_ranks = plan.get_new_ranks()
        cleanly_removed = plan.get_removed_ranks()
        ranks_to_kill = plan.get_killed_ranks()

        # If this rank is being removed in this phase, exit gracefully
        if global_rank in cleanly_removed:
            print(
                f"global_rank={global_rank}, local_rank={local_rank} -> this rank is being removed in this phase, exiting",
                flush=True,
            )
            rank_client.release_rank(user_context=plan.get_phase())
            break

        if len(added_ranks) > 0:
            print(
                f"global_rank={global_rank}, local_rank={local_rank} -> adding connections to {added_ranks}",
                flush=True,
            )
            buffer.connect_ranks(added_ranks)
            remote_ranks.update(added_ranks)

        # Check if this rank should be killed
        kill_rank = global_rank in ranks_to_kill

        if len(cleanly_removed) > 0:
            print(
                f"global_rank={global_rank}, local_rank={local_rank} -> removing connections to {cleanly_removed}",
                flush=True,
            )
            buffer.disconnect_ranks(cleanly_removed)
            remote_ranks.difference_update(cleanly_removed)
            time.sleep(
                5
            )  # required to avoid race between MD invalidation and readdition of same ranks, if this is part of the test

        # Use sparse num_ranks = max(active_ranks) + 1 for proper indexing
        active_ranks_list = plan.get_active_ranks()
        current_num_ranks = max(active_ranks_list) + 1  # Sparse indexing
        current_num_experts = args.num_experts_per_rank * current_num_ranks

        _phase_barrier("ready", global_rank, plan.get_phase())

        def mark_fault(rank: int, phase: int) -> None:
            _mark_expected_fault(rank, phase)
            expected_fault_signal.set()

        test_main(
            args.num_tokens,
            max_tokens_per_rank,
            args.hidden_dim,
            current_num_experts,
            args.num_topk,
            global_rank,
            current_num_ranks,
            max_num_ranks,
            buffer,
            kineto=args.kineto,
            fault_tolerance_test=kill_rank,
            phase=plan.get_phase(),
            mark_expected_fault=mark_fault,
            format_set=args.format_set,
        )
        # Query mask buffer to detect rank failures and clean them up
        buffer.query_mask_buffer(mask_status)
        newly_failed_ranks = set()
        for r in range(current_num_ranks):
            if mask_status[r].item() != 0 and r in remote_ranks:
                newly_failed_ranks.add(r)

        if args.validate_phase_failures:
            expected_failed_ranks = set(ranks_to_kill) & remote_ranks
            unexpected_failures = newly_failed_ranks - expected_failed_ranks
            assert not unexpected_failures, f"rank {global_rank}, local_rank={local_rank} phase {plan.get_phase()}: unexpected failures {unexpected_failures}"
            missing_failures = expected_failed_ranks - newly_failed_ranks
            assert not missing_failures, f"rank {global_rank}, local_rank={local_rank} phase {plan.get_phase()}: missing expected failures {missing_failures}"

        if len(newly_failed_ranks) > 0:
            print(
                f"global_rank={global_rank}, local_rank={local_rank} -> detected rank failures: {newly_failed_ranks}, cleaning up...",
                flush=True,
            )
            remote_ranks.difference_update(newly_failed_ranks)
            buffer.disconnect_ranks(list(newly_failed_ranks))
            time.sleep(5)

        print(
            f"global_rank={global_rank}, local_rank={local_rank} -> end phase {plan.get_phase()}",
            flush=True,
        )

        _phase_barrier("done", global_rank, plan.get_phase())
        if not plan.next():
            break

    buffer.destroy()

    print(f"global_rank={global_rank}, local_rank={local_rank} -> done", flush=True)


def run_server():
    _store = store_group.create_master_store(port=TCP_STORE_PORT)  # noqa: F841
    rank_server.start_server(port=RANK_SERVER_PORT)


def main():
    parser = argparse.ArgumentParser(description="Elastic EP Test")
    parser.add_argument(
        "--plan", type=str, default="single_rank.json", help="Path to plan file"
    )
    parser.add_argument(
        "--backend",
        choices=("mooncake", "nixl"),
        default="mooncake",
        help="Backend selected by the lazy test adapter",
    )
    parser.add_argument(
        "--num-processes",
        type=int,
        default=8,
        help="Number of worker processes to launch",
    )
    parser.add_argument("--num-tokens", type=int, default=128, help="Number of tokens")
    parser.add_argument(
        "--num-experts-per-rank", type=int, default=8, help="Number of experts per rank"
    )
    parser.add_argument("--hidden-dim", type=int, default=7168, help="Hidden dimension")
    parser.add_argument("--num-topk", type=int, default=8, help="Number of topk")
    parser.add_argument(
        "--tcp-server",
        type=str,
        help="TCP server address (for both TCPStore and rank server). If not set, both will be started locally.",
    )
    parser.add_argument("--kineto", action="store_true", help="Enable kineto profiling")
    parser.add_argument(
        "--format-set",
        choices=("common", "nixl-full"),
        default="common",
        help="Run common BF16/standard-FP8 cases, or NIXL-only FP8 special formats",
    )
    parser.add_argument(
        "--disable-ll-nvlink",
        action="store_true",
        help="Disable NVLink communication for low-latency kernels",
    )
    parser.add_argument(
        "--timeout-ms",
        type=non_negative_int,
        default=30000,
        help="GPU timeout in milliseconds (non-negative integer)",
    )
    parser.add_argument(
        "--validate-phase-failures",
        action="store_true",
        default=True,
        help="Enable strict phase-local validation of observed rank failures against the plan (required)",
    )

    args = parser.parse_args()
    if args.backend == "mooncake" and args.format_set == "nixl-full":
        parser.error("nixl-full formats are NIXL-only; use --backend nixl")

    if not args.tcp_server:
        print("Starting TCPStore and rank server locally", flush=True)
        server_process = torch.multiprocessing.Process(target=run_server, daemon=True)
        server_process.start()
        time.sleep(0.5)

    if args.kineto and not kineto_cuda_available(0):
        raise SystemExit("Kineto profiling was requested but is not supported")

    # One process is the task process itself. This preserves its real PID and
    # lets the launcher distinguish planned SIGTERM from every other exit.
    if args.num_processes == 1:
        worker(0, args)
        return

    ctx = torch.multiprocessing.spawn(
        worker,
        args=(args,),
        nprocs=args.num_processes,
        join=False,
        daemon=False,
        start_method="spawn",
    )
    failed = []
    for i, p in enumerate(ctx.processes):
        p.join()
        if p.exitcode != 0:
            failed.append((i, p.exitcode))
    if failed:
        raise RuntimeError(
            f"Worker processes failed: {', '.join(f'worker {i} (exit code {code})' for i, code in failed)}"
        )


if __name__ == "__main__":
    main()
