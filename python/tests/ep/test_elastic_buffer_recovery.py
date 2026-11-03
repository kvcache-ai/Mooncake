# Copyright 2026 KVCache.AI

"""NCCL EP recovery integration tests using the Mooncake PG worker harness."""

import os
import sys
import unittest
from pathlib import Path

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

try:
    from mooncake import pg
except ImportError as exc:
    raise unittest.SkipTest(f"Mooncake PG is unavailable: {exc}") from exc

# Reuse the source-tree PG harness without copying its process lifecycle logic.
# Keep this import available to spawned workers and both pytest and unittest.
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "mooncake-pg" / "tests"))
from pg_test_utils import (  # noqa: E402
    MooncakePGCUDABackendTestCase,
    MooncakePGWorkerContext,
    wait_until,
)


BROKEN_RANK = 1
_NCCL_ELASTIC_MAX_TOKENS = 128
_NCCL_ELASTIC_HIDDEN = 4096
_NCCL_ELASTIC_NUM_EXPERTS = 256
_NCCL_ELASTIC_NUM_TOPK = 8
_NCCL_ELASTIC_NUM_SMS = 24


def _make_nccl_elastic_buffer():
    """Construct the production two-rank NCCL elastic-kernel configuration."""
    from mooncake.mooncake_elastic_buffer import ElasticBuffer

    return ElasticBuffer(
        dist.group.WORLD,
        num_max_tokens_per_rank=_NCCL_ELASTIC_MAX_TOKENS,
        hidden=_NCCL_ELASTIC_HIDDEN,
        num_topk=_NCCL_ELASTIC_NUM_TOPK,
        use_fp8_dispatch=False,
        deterministic=False,
        allow_hybrid_mode=True,
        allow_multiple_reduction=True,
        num_gpu_timeout_secs=10,
        transport="nccl",
    )


def _run_nccl_elastic_data_round(
    buffer,
    logical_rank: int,
    generation: int,
):
    """Dispatch and combine data through both logical ranks."""
    num_tokens = 8
    world_size = buffer.num_ranks
    if world_size != 2:
        raise AssertionError("NCCL ElasticBuffer recovery test expects two ranks")

    local_experts = _NCCL_ELASTIC_NUM_EXPERTS // world_size
    expert_offsets = torch.arange(
        _NCCL_ELASTIC_NUM_TOPK, device="cuda", dtype=torch.long
    )
    dst_ranks = (
        logical_rank
        + torch.arange(_NCCL_ELASTIC_NUM_TOPK, device="cuda", dtype=torch.long)
    ) % world_size
    topk_idx = (
        (dst_ranks * local_experts + expert_offsets)
        .view(1, _NCCL_ELASTIC_NUM_TOPK)
        .repeat(num_tokens, 1)
        .contiguous()
    )
    topk_weights = torch.ones(
        (num_tokens, _NCCL_ELASTIC_NUM_TOPK),
        device="cuda",
        dtype=torch.float32,
    )

    base = torch.arange(
        num_tokens * _NCCL_ELASTIC_HIDDEN,
        device="cuda",
        dtype=torch.float32,
    ).view(num_tokens, _NCCL_ELASTIC_HIDDEN)
    rank_offset = logical_rank * 100000
    generation_offset = generation * 17
    x = (base + rank_offset + generation_offset).to(torch.bfloat16).contiguous()

    recv_x, _recv_idx, recv_weights, handle, _event = buffer.dispatch(
        x,
        topk_idx=topk_idx,
        topk_weights=topk_weights,
        num_experts=_NCCL_ELASTIC_NUM_EXPERTS,
        num_max_tokens_per_rank=_NCCL_ELASTIC_MAX_TOKENS,
        expert_alignment=1,
        do_cpu_sync=True,
        num_sms=_NCCL_ELASTIC_NUM_SMS,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()

    expected_recv_tokens = num_tokens * world_size
    actual_recv_tokens = int(handle.psum_num_recv_tokens_per_scaleup_rank[-1].item())
    if actual_recv_tokens != expected_recv_tokens:
        raise AssertionError(
            f"rank {logical_rank}: expected {expected_recv_tokens}"
            f" received tokens, got {actual_recv_tokens}"
        )

    src_global = handle.recv_src_metadata[:actual_recv_tokens, 0].long()
    src_rank = torch.div(
        src_global,
        _NCCL_ELASTIC_MAX_TOKENS,
        rounding_mode="floor",
    )
    src_token = src_global % _NCCL_ELASTIC_MAX_TOKENS
    expected_recv = (
        base[src_token] + src_rank.view(-1, 1).float() * 100000 + generation_offset
    ).to(torch.bfloat16)
    if not torch.equal(recv_x[:actual_recv_tokens], expected_recv):
        raise AssertionError(f"rank {logical_rank}: dispatch payload mismatch")

    combined_x, _, _ = buffer.combine(
        recv_x[:actual_recv_tokens].contiguous(),
        handle,
        topk_weights=recv_weights[:actual_recv_tokens].contiguous(),
        num_sms=_NCCL_ELASTIC_NUM_SMS,
        async_with_compute_stream=False,
    )
    torch.cuda.synchronize()
    torch.testing.assert_close(
        combined_x,
        (x.float() * world_size).to(torch.bfloat16),
        rtol=0.05,
        atol=0.001,
        msg=lambda msg: f"rank {logical_rank}: combine mismatch: {msg}",
    )

    return handle, x


def _nccl_elastic_buffer_recovery_worker(
    ctx: MooncakePGWorkerContext,
    pre_failure_barrier: mp.Barrier,
    broken_exited: mp.Event,
    start_recovery: mp.Event,
    membership_restored: mp.Event,
    max_group_size: int | None = None,
) -> None:
    """Replace rank 1 and rebuild the NCCL ElasticBuffer generation."""
    logical_rank = ctx.rank if ctx.proc_rank < ctx.world_size else BROKEN_RANK
    capacity = ctx.world_size if max_group_size is None else max_group_size
    reserved_slots = [0] * (capacity - ctx.world_size)

    if ctx.proc_rank < ctx.world_size:
        device = ctx.init_group(rank=logical_rank, max_group_size=max_group_size)
        # The control PG may use CPU tensors while EP still runs on this GPU.
        torch.cuda.set_device(logical_rank)
        backend = ctx.get_backend()
        buffer = _make_nccl_elastic_buffer()
        stale_handle, stale_x = _run_nccl_elastic_data_round(
            buffer, logical_rank, generation=0
        )
        ctx.synchronize()
        pre_failure_barrier.wait(timeout=60.0)

        if logical_rank == BROKEN_RANK:
            ctx.record_result({"role": "broken"})
            # Exit without destroying the process group or ElasticBuffer. This
            # models an abrupt worker loss with a live NCCL generation.
            os._exit(0)

        if not broken_exited.wait(timeout=60.0):
            raise TimeoutError("timed out waiting for departed NCCL EP rank")

        # Force Mooncake PG to observe the failed peer and commit the reduced
        # active-rank view before a replacement joins.
        probe = torch.tensor([logical_rank], device=device)
        work = dist.isend(probe, dst=BROKEN_RANK)
        work.wait()
        if pg.get_local_success(work):
            raise AssertionError("P2P probe to departed rank unexpectedly succeeded")
        active_ranks = pg.get_active_ranks(backend).cpu().tolist()
        if active_ranks != [1, 0] + reserved_slots:
            raise AssertionError(
                "expected rank 1 to be inactive, " f"got active_ranks={active_ranks}"
            )

        start_recovery.set()
        wait_until(
            lambda: pg.get_peer_state(backend, [BROKEN_RANK])[0],
            timeout_s=60.0,
            poll_interval_s=0.05,
            description="survivor waiting for replacement rank",
        )

        response = pg.recover_ranks(backend, [BROKEN_RANK])
        if response.status != pg.ProposalStatus.Applied:
            raise AssertionError(
                "recover_ranks should apply before NCCL rebuild, got "
                f"{response.status}: {response.reject_reason}"
            )
        membership_restored.set()
        role = "survivor"
    else:
        if not start_recovery.wait(timeout=60.0):
            raise TimeoutError("timed out waiting to start NCCL EP replacement")
        device = ctx.init_group(
            rank=logical_rank, max_group_size=max_group_size, is_extension=True
        )
        # The control PG may use CPU tensors while EP still runs on this GPU.
        torch.cuda.set_device(logical_rank)
        backend = ctx.get_backend()
        pg.join_group(backend)
        if not membership_restored.wait(timeout=60.0):
            raise TimeoutError("timed out waiting for restored PG membership")
        role = "replacement"

    # The fixed-world NCCL bootstrap is collective. Wait until every live
    # process has applied the fully restored logical-rank view before either
    # survivors or replacements enter it.
    wait_until(
        lambda: pg.get_active_ranks(backend).cpu().tolist() == [1, 1] + reserved_slots,
        timeout_s=60.0,
        poll_interval_s=0.05,
        description=f"rank {logical_rank} waiting for restored active-rank view",
    )

    if role == "survivor":
        buffer.update_ep_member()
        try:
            buffer.dispatch(
                stale_x,
                handle=stale_handle,
                num_experts=_NCCL_ELASTIC_NUM_EXPERTS,
                num_max_tokens_per_rank=_NCCL_ELASTIC_MAX_TOKENS,
                num_sms=_NCCL_ELASTIC_NUM_SMS,
            )
        except RuntimeError as exc:
            if "obsolete NCCL ElasticBuffer generation" not in str(exc):
                raise
        else:
            raise AssertionError("survivor accepted a handle from the old generation")
    else:
        buffer = _make_nccl_elastic_buffer()

    _run_nccl_elastic_data_round(buffer, logical_rank, generation=1)
    buffer.destroy()
    ctx.record_result({"role": role})


class TestNcclElasticBufferRecovery(MooncakePGCUDABackendTestCase):
    world_size = 2

    def test_nccl_elastic_buffer_recovery(self) -> None:
        """NCCL EP rebuilds after Mooncake PG replaces a logical rank."""
        self._run_nccl_elastic_buffer_recovery()

    def test_nccl_elastic_buffer_recovery_with_reserved_capacity(self) -> None:
        """Unused PG slots do not prevent NCCL construction or recovery."""
        self._run_nccl_elastic_buffer_recovery(max_group_size=4)

    def test_nccl_elastic_buffer_recovery_with_cpu_pg(self) -> None:
        """A CPU control PG can bootstrap and recover the GPU NCCL data path."""
        self.backend_name = "mooncake-cpu"
        self.device_type = "cpu"
        self._run_nccl_elastic_buffer_recovery(max_group_size=4)

    def _run_nccl_elastic_buffer_recovery(
        self, max_group_size: int | None = None
    ) -> None:
        if torch.cuda.device_count() < 2:
            self.skipTest("NCCL ElasticBuffer recovery requires two CUDA devices")
        try:
            from mooncake import ep
        except ImportError as exc:
            self.skipTest(f"Mooncake EP is unavailable: {exc}")

        if not ep.has_nccl_device_support():
            self.skipTest("Mooncake EP was built without NCCL Device API support")

        spawn_ctx = mp.get_context("spawn")
        pre_failure_barrier = spawn_ctx.Barrier(2)
        broken_exited = spawn_ctx.Event()
        start_recovery = spawn_ctx.Event()
        membership_restored = spawn_ctx.Event()

        rows = self.spawn_backend_and_collect(
            _nccl_elastic_buffer_recovery_worker,
            pre_failure_barrier,
            broken_exited,
            start_recovery,
            membership_restored,
            max_group_size,
            world_size=2,
            nprocs=3,
            timeout_s=180.0,
            process_exit_events={BROKEN_RANK: broken_exited},
        )
        self.assert_all_ok(rows)
        self.assertEqual(len([row for row in rows if row.get("role") == "survivor"]), 1)
        self.assertEqual(
            len([row for row in rows if row.get("role") == "replacement"]), 1
        )
        self.assertGreaterEqual(
            len([row for row in rows if row.get("role") == "broken"]), 1
        )


if __name__ == "__main__":
    unittest.main()
