"""Reuse of a buffer half by the legacy collective worker.

A rank must not store into a peer's receive half before that peer has read the
half's previous data. Read counts provide that order (see
TransferGroupMeta::halfReads). Each slow-reader case holds the reported
completion of rank 1's flag writes with the fault shim: the flags reach the
peers at once, so the peers may go on, while rank 1 reads late. In the replay
and stream cases every run's input differs and results are compared on the
device on every rank, so a result that holds a peer's next run counts as wrong;
the other cases give their collectives different inputs and compare them on the
host. The shim also counts the flag batches that it held and the READs of a
peer's read counts, so each such case shows that it held rank 1 and whether
rank 0 had to ask rank 1 for its counts.

Two collectives in a row use one half when the halves do not alternate between
them: a captured collective replays on the half it was captured with, a capture
takes a half without running, single-chunk collectives of one communicator on
independent streams are not ordered with each other, and removing or admitting a
rank starts the halves again. Rank 0 then has to read rank 1's counts. The two
controls alternate, so they pass without the counts and read none. Two more cases
fail such a READ and kill the rank that is waited for: the worker must give up on
that peer as it does when a write fails. The failed READ needs no hold.

On CPUs the host worker counts a read when it copies a chunk out. Two async
collectives in flight let a rank start a third on the first one's half while a
slow peer has not read it yet; waiting for each in turn alternates the halves.

Read counts order the runs of a half between ranks, not the runs of one rank. So
this fix does not cover patterns in which one rank has two runs of one
communicator's half in flight at once: multi-chunk collectives on independent
streams, captures on independent streams with other communicators' collectives
between them, a replay concurrent with unordered collectives of the same
communicator, and independent streams across a membership change. No case here
runs them.
"""

import os
import time
import unittest

import torch
import torch.distributed as dist
import torch.multiprocessing as mp
from mooncake import pg

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGCUDABackendTestCase,
    MooncakePGWorkerContext,
    wait_until,
)
from transfer_fault_injection import TransferFault, preload_transfer_fault

GPU_COLLECTIVE_BACKEND_ENV = "MOONCAKE_PG_PREFERRED_GPU_COLLECTIVE_BACKEND"

SLOW_RANK = 1
FLAG_HOLD_S = 0.003
RUNS = 100
# Long enough for the other ranks to capture and issue during the hold.
LONG_FLAG_HOLD_S = 2.0
SMALL_NUMEL = 1024
# Op i's inputs are shifted by i times this. A run adds 13 to the inputs, modulo
# 61, so two ops whose steps are equal or one apart never take the same input.
SALT_STEP = 5
# Only the root receives their result. The root is the slow rank.
ROOTED_KINDS = ("reduce", "gather")
# The admission case removes this rank and admits it again.
ADMITTED_RANK = 2
# The READ failure case fails this rank's links to rank 0, as the elastic fault
# rounds do. The Coordinator then removes it, the higher rank of the pair.
REPORTER = 1
# How long rank 0 waits, after rank 1 has given up on it, before it issues the
# same collective: long enough for rank 1 to end its run, far shorter than the
# reconciliation window before the Coordinator removes rank 1.
LATE_FLAG_GAP_S = 0.2
# Longer than the slow rank lives in the dead reader case.
DEATH_FLAG_HOLD_S = 60.0
# Lowered in the dead reader case, so that rank 0 gives up on its peer soon.
DEAD_PEER_TIMEOUT_US = 2_000_000
# Long enough for rank 0 to start the third async CPU collective during it.
CPU_FLAG_HOLD_S = 0.5


def _chunk_bytes(group_size: int) -> int:
    # The chunk size of putTaskCuda for a group without extension slots.
    return ((1 << 24) - 1) // group_size & ~7


def _start(ctx: MooncakePGWorkerContext, fault_library: str) -> TransferFault:
    # Read when a communicator is created.
    os.environ[GPU_COLLECTIVE_BACKEND_ENV] = "legacy"
    ctx.init_group()
    return TransferFault(fault_library, local_rank=ctx.rank)


def _hold_flags(ctx: MooncakePGWorkerContext, fault, seconds: float) -> None:
    if ctx.rank == SLOW_RANK:
        fault.delay_flag_completion(seconds)


class _CheckedCollective:
    """A collective that takes new input on every run and checks its result.

    run() only queues device work, so it can be captured into a graph. A rooted
    collective is checked on its root only.
    """

    def __init__(
        self, ctx: MooncakePGWorkerContext, kind: str, chunks: int, salt: int = 0
    ):
        self.kind = kind
        self.rank = ctx.rank
        self.world = ctx.world_size
        self.numel = chunks * _chunk_bytes(ctx.world_size) // 4
        device = ctx.device
        input_numel = self.numel
        output_numel = self.numel
        if kind == "reduce_scatter":
            input_numel *= self.world
        elif kind in ("all_gather", "gather"):
            output_numel *= self.world
        ramp = torch.arange(input_numel, dtype=torch.int64, device=device)
        self.ramp = ramp * 7 + salt
        self.input = torch.empty(input_numel, dtype=torch.float32, device=device)
        self.output = torch.empty(output_numel, dtype=torch.float32, device=device)
        self.checked = kind not in ROOTED_KINDS or self.rank == SLOW_RANK
        # The root gathers into views of the output.
        self.gather_list = None
        if kind == "gather" and self.rank == SLOW_RANK:
            self.gather_list = list(self.output.chunk(self.world))
        self.step = torch.zeros((), dtype=torch.int64, device=device)
        self.wrong = torch.zeros((), dtype=torch.int64, device=device)

    def run(self) -> None:
        self.step.add_(1)
        base = self.ramp + self.step * 13
        # Small integers keep sums exact in float32.
        pieces = [((base + peer) % 61).to(torch.float32) for peer in range(self.world)]
        if self.kind == "all_reduce":
            self.output.copy_(pieces[self.rank])
            dist.all_reduce(self.output)
            expected = sum(pieces)
        elif self.kind == "all_gather":
            self.input.copy_(pieces[self.rank])
            dist.all_gather_into_tensor(self.output, self.input)
            expected = torch.cat(pieces)
        elif self.kind == "broadcast":
            if self.rank == 0:
                self.output.copy_(pieces[0])
            else:
                self.output.fill_(-1)
            dist.broadcast(self.output, src=0)
            expected = pieces[0]
        elif self.kind == "reduce_scatter":
            self.input.copy_(pieces[self.rank])
            dist.reduce_scatter_tensor(self.output, self.input)
            first = self.rank * self.numel
            expected = sum(pieces)[first : first + self.numel]
        elif self.kind == "reduce":
            self.output.copy_(pieces[self.rank])
            dist.reduce(self.output, dst=SLOW_RANK)
            expected = sum(pieces)
        elif self.kind == "gather":
            self.input.copy_(pieces[self.rank])
            dist.gather(self.input, gather_list=self.gather_list, dst=SLOW_RANK)
            expected = torch.cat(pieces)
        else:
            raise ValueError(f"unsupported collective: {self.kind}")
        if self.checked:
            self.wrong.add_((self.output != expected).any().to(torch.int64))

    def report(self) -> dict:
        return {"wrong": int(self.wrong.item()), "runs": int(self.step.item())}


def _counts(fault: TransferFault) -> dict:
    return {"held": fault.held_flag_batches, "reads": fault.count_reads}


def _finish_hold(ctx: MooncakePGWorkerContext, fault) -> dict:
    """Ends the measured runs and returns their counts."""
    torch.cuda.synchronize(ctx.device)
    counts = _counts(fault)
    _hold_flags(ctx, fault, 0)
    dist.barrier()
    return counts


def _replay_worker(
    ctx: MooncakePGWorkerContext,
    fault_library: str,
    collectives: list[tuple[str, int]],
) -> None:
    """Replays captured collectives in turn while rank 1's flags are held."""
    fault = _start(ctx, fault_library)
    device = ctx.device
    ops = [
        _CheckedCollective(ctx, kind, chunks, salt=index * SALT_STEP)
        for index, (kind, chunks) in enumerate(collectives)
    ]

    for op in ops:
        op.run()  # Eager warm-up before capture.
    torch.cuda.synchronize(device)

    graphs = []
    for op in ops:
        graph = torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):
            op.run()
        graphs.append(graph)
    torch.cuda.synchronize(device)
    for op in ops:
        op.wrong.zero_()
    dist.barrier()
    # The barrier takes a half too, so the first replays after it may read counts
    # even in a control. The counts start one round later.
    for graph in graphs:
        graph.replay()
    torch.cuda.synchronize(device)
    fault.reset_transfer_counts()

    _hold_flags(ctx, fault, FLAG_HOLD_S)
    for _ in range(RUNS):
        for graph in graphs:
            graph.replay()
    counts = _finish_hold(ctx, fault)
    ctx.record_result({"ops": [op.report() for op in ops], **counts})


def _run_each_on_its_stream(ctx: MooncakePGWorkerContext, streams, ops) -> None:
    for stream, op in zip(streams, ops):
        with torch.cuda.stream(stream):
            op.run()
        torch.cuda.synchronize(ctx.device)


def _eager_worker(
    ctx: MooncakePGWorkerContext,
    fault_library: str,
    chunks: int,
    stream_count: int,
) -> None:
    """Issues eager collectives round-robin over independent streams."""
    # Multi-chunk collectives on independent streams can overlap on a half of one
    # rank, which read counts do not order.
    assert chunks == 1 or stream_count == 1
    fault = _start(ctx, fault_library)
    device = ctx.device
    streams = [torch.cuda.Stream(device=device) for _ in range(stream_count)]
    ops = [
        _CheckedCollective(ctx, "all_reduce", chunks, salt=index * SALT_STEP)
        for index in range(stream_count)
    ]
    # The ops' tensors were filled on the default stream.
    torch.cuda.synchronize(device)
    _run_each_on_its_stream(ctx, streams, ops)
    for op in ops:
        op.wrong.zero_()
    dist.barrier()
    # As in _replay_worker, the counts start one round after the barrier.
    _run_each_on_its_stream(ctx, streams, ops)
    fault.reset_transfer_counts()

    _hold_flags(ctx, fault, FLAG_HOLD_S)
    for index in range(RUNS):
        slot = index % stream_count
        with torch.cuda.stream(streams[slot]):
            ops[slot].run()
    counts = _finish_hold(ctx, fault)
    ctx.record_result({"ops": [op.report() for op in ops], **counts})


def _small_tensor(ctx: MooncakePGWorkerContext, factor: int):
    return torch.full(
        (SMALL_NUMEL,),
        float(factor * (ctx.rank + 1)),
        dtype=torch.float32,
        device=ctx.device,
    )


def _errors_in_sums(
    ctx: MooncakePGWorkerContext, tensors: dict, ranks=None
) -> list[str]:
    """Checks sums of _small_tensor over the given ranks, by default all."""
    if ranks is None:
        ranks = range(ctx.world_size)
    rank_sum = sum(rank + 1 for rank in ranks)
    errors = []
    for name, (tensor, factor) in tensors.items():
        cpu = tensor.cpu()
        if not bool((cpu == float(factor * rank_sum)).all()):
            errors.append(
                f"{name}: expected {factor * rank_sum}, first {cpu[0].item()}, "
                f"last {cpu[-1].item()}"
            )
    return errors


def _connect(ctx: MooncakePGWorkerContext) -> None:
    """Connects the ranks. Two one-chunk collectives leave the halves as found."""
    for _ in range(2):
        dist.all_reduce(_small_tensor(ctx, 1))
    torch.cuda.synchronize(ctx.device)


def _eager_after_capture_worker(
    ctx: MooncakePGWorkerContext, fault_library: str
) -> None:
    """A capture takes the other half without running, so two eager collectives
    in a row use one half."""
    fault = _start(ctx, fault_library)
    device = ctx.device
    _connect(ctx)
    first = _small_tensor(ctx, 1)
    second = _small_tensor(ctx, 10)
    captured = _small_tensor(ctx, 100)

    fault.reset_transfer_counts()
    _hold_flags(ctx, fault, LONG_FLAG_HOLD_S)
    dist.all_reduce(first)
    torch.cuda.synchronize(device)
    _hold_flags(ctx, fault, 0)
    held = fault.held_flag_batches

    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        dist.all_reduce(captured)
    # Only the reads of the second collective count.
    fault.reset_transfer_counts()
    dist.all_reduce(second)
    torch.cuda.synchronize(device)
    ctx.record_result(
        {
            "errors": _errors_in_sums(
                ctx, {"first": (first, 1), "second": (second, 10)}
            ),
            "held": held,
            "reads": fault.count_reads,
        }
    )


def _admission_worker(
    ctx: MooncakePGWorkerContext,
    fault_library: str,
    setup_done: mp.Barrier,
    x_done: mp.Event,
) -> None:
    """Rank 2 leaves and comes back between two collectives X and Y. Each change
    of membership starts the halves again, so X and Y both use half 0, and rank 1
    reads X late, after rank 2 is back. The admission must not make rank 0 forget
    that rank 1 has yet to read X."""
    fault = _start(ctx, fault_library)
    backend = ctx.get_backend()
    device = ctx.device
    # X is the second run of half 0 and Y the first after the admission restarts
    # the counts, so they use different flag sets. Rank 2's flag of Y can land at
    # rank 1 before rank 1 has finished X.
    _connect(ctx)
    x = _small_tensor(ctx, 1)
    y = _small_tensor(ctx, 10)
    # No rank still runs a collective of the old membership when it changes.
    setup_done.wait(timeout=60.0)

    if ctx.rank == ADMITTED_RANK:
        response = pg.deactivate_ranks(backend, [ADMITTED_RANK])
        assert response.status == pg.ProposalStatus.Applied, (
            "self-deactivation should apply, "
            f"got {response.status}: {response.reject_reason}"
        )
        # Asking to come back at once keeps the admission short, so that it lands
        # while rank 1 is still held.
        pg.join_group(backend)
    else:
        wait_until(
            lambda: pg.get_active_ranks(backend).cpu().tolist() == [1, 1, 0],
            timeout_s=60.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.rank} waiting for the view without rank 2",
        )
        _hold_flags(ctx, fault, LONG_FLAG_HOLD_S)
        dist.all_reduce(x)
        if ctx.rank == SLOW_RANK:
            # X's flags are posted once rank 0 has finished X. Rank 1 does not
            # synchronize, so it reads X only when the hold ends.
            if not x_done.wait(timeout=30.0):
                raise TimeoutError("timed out waiting for rank 0 to finish X")
            _hold_flags(ctx, fault, 0)
        else:
            torch.cuda.synchronize(device)
            x_done.set()
        wait_until(
            lambda: pg.get_peer_state(backend, [ADMITTED_RANK])[0],
            timeout_s=30.0,
            poll_interval_s=0.05,
            description=f"rank {ctx.rank} waiting for rank 2 to be activatable",
        )
        response = pg.recover_ranks(backend, [ADMITTED_RANK])
        assert (
            response.status == pg.ProposalStatus.Applied
        ), f"rank {ctx.rank}: recover_ranks should apply, got {response.status}"

    held = fault.held_flag_batches
    # Only the reads of Y count.
    fault.reset_transfer_counts()
    dist.all_reduce(y)
    torch.cuda.synchronize(device)
    errors = _errors_in_sums(ctx, {"y": (y, 10)})
    if ctx.rank != ADMITTED_RANK:
        errors += _errors_in_sums(ctx, {"x": (x, 1)}, ranks=(0, 1))
    ctx.record_result({"errors": errors, "held": held, "reads": fault.count_reads})


def _failed_count_read_worker(
    ctx: MooncakePGWorkerContext,
    fault_library: str,
    rank_one_gave_up: mp.Event,
    rank_one_issued_after: mp.Event,
) -> None:
    """Rank 1's READ of rank 0's read counts lands and reports failure. Rank 1
    must give up on rank 0 as for a failed write, and rank 1 then rejoins.

    This is the eager collective after a capture, without a hold: rank 1 has had
    no flag of rank 0 since the first collective, so it reads rank 0's counts for
    the second one however the ranks are timed.

    Rank 0 issues the second collective only after rank 1 has ended its run of
    it, so rank 0's flag of that run lands late and stays in rank 1's flag set.
    The admission restarts the run counts, so the collective after the rejoin
    uses that half and flag set again and must not take the old flag. Rank 1
    issues that collective first and rank 0 later, so rank 1 waits for rank 0's
    flag with the old one in place every time."""
    fault = _start(ctx, fault_library)
    backend = ctx.get_backend()
    device = ctx.device
    _connect(ctx)
    first = _small_tensor(ctx, 1)
    second = _small_tensor(ctx, 10)
    captured = _small_tensor(ctx, 100)
    dist.all_reduce(first)
    torch.cuda.synchronize(device)
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        dist.all_reduce(captured)
    epoch_before_failure = pg.get_current_epoch(backend)

    # Rank 1's fault is in place before it issues. Rank 0 issues only after
    # rank 1 has given up on it, plus LATE_FLAG_GAP_S, so rank 0's flag of this
    # run lands after rank 1's run has ended.
    with fault.failing_link(REPORTER, 0):
        fault.reset_transfer_counts()
        if ctx.rank != REPORTER:
            if not rank_one_gave_up.wait(timeout=60.0):
                raise TimeoutError("rank 1 did not give up on rank 0")
            time.sleep(LATE_FLAG_GAP_S)
        # An eager issue returns once the worker has submitted this rank's data
        # writes, which rank 1 does only after it has given up on rank 0.
        work = dist.all_reduce(second, async_op=True)
        if ctx.rank == REPORTER:
            rank_one_gave_up.set()
        work.wait()
        result = {
            "success": pg.get_local_success(work),
            "hint": pg.get_failed_ranks_hint(work).cpu().tolist(),
            "injected": fault.injected_count,
            "reads": fault.count_reads,
            "active": pg.get_active_ranks(backend).cpu().tolist(),
        }

    if ctx.rank == REPORTER:
        pg.join_group(backend)
    else:
        wait_until(
            lambda: pg.get_current_epoch(backend) > epoch_before_failure,
            timeout_s=60.0,
            poll_interval_s=0.05,
            description="waiting for the post-failure group view",
        )
        wait_until(
            lambda: pg.get_peer_state(backend, [REPORTER])[0],
            timeout_s=30.0,
            poll_interval_s=0.05,
            description="waiting for rank 1 to become activatable",
        )
        response = pg.activate_ranks(backend, [REPORTER])
        assert response.status == pg.ProposalStatus.Applied, (
            "in-place activation should apply, "
            f"got {response.status}: {response.reject_reason}"
        )
    after = _small_tensor(ctx, 1000)
    if ctx.rank != REPORTER:
        if not rank_one_issued_after.wait(timeout=60.0):
            raise TimeoutError("rank 1 did not issue the collective after rejoining")
        time.sleep(LATE_FLAG_GAP_S)
    work = dist.all_reduce(after, async_op=True)
    if ctx.rank == REPORTER:
        rank_one_issued_after.set()
    work.wait()
    result["errors"] = _errors_in_sums(ctx, {"after": (after, 1000)})
    ctx.record_result(result)


def _dead_reader_worker(
    ctx: MooncakePGWorkerContext,
    fault_library: str,
    x_done: mp.Event,
    slow_rank_exited: mp.Event,
) -> None:
    """Rank 1 dies before it reads X, and rank 0 then runs Y on X's half. Rank 0
    waits for rank 1 to read X, so it must give up on rank 1 as on a dead target
    of a write: when its READ fails, or when the collective timeout finds rank 1
    dead."""
    pg.set_collective_timeout_us(DEAD_PEER_TIMEOUT_US)
    fault = _start(ctx, fault_library)
    backend = ctx.get_backend()
    device = ctx.device
    _connect(ctx)
    x = _small_tensor(ctx, 1)
    y = _small_tensor(ctx, 10)
    captured = _small_tensor(ctx, 100)

    _hold_flags(ctx, fault, DEATH_FLAG_HOLD_S)
    dist.all_reduce(x)
    if ctx.rank == SLOW_RANK:
        # Rank 0 has X's flags once it has finished X, and rank 1 would read X
        # only after the hold. Exit without any teardown.
        if not x_done.wait(timeout=30.0):
            raise TimeoutError("timed out waiting for rank 0 to finish X")
        ctx.record_result({"role": "dead", "held": fault.held_flag_batches})
        os._exit(0)

    torch.cuda.synchronize(device)
    errors = _errors_in_sums(ctx, {"x": (x, 1)})
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        dist.all_reduce(captured)
    x_done.set()
    if not slow_rank_exited.wait(timeout=60.0):
        raise TimeoutError("timed out waiting for rank 1 to exit")

    fault.reset_transfer_counts()
    work = dist.all_reduce(y, async_op=True)
    work.wait()
    ctx.record_result(
        {
            "role": "survivor",
            "errors": errors,
            "success": pg.get_local_success(work),
            "hint": pg.get_failed_ranks_hint(work).cpu().tolist(),
            "reads": fault.count_reads,
            "active": pg.get_active_ranks(backend).cpu().tolist(),
        }
    )


def _async_cpu_worker(
    ctx: MooncakePGWorkerContext, fault_library: str, overlap: bool
) -> None:
    """Three one-chunk CPU collectives A, B and C while rank 1's flags are held.
    With overlap, A and B are in flight together, and rank 0 starts C on A's
    half as soon as A is done there, before rank 1 has read A. Without it, each
    collective is waited for before the next, so the halves alternate."""
    fault = _start(ctx, fault_library)
    # Connects the ranks. Two one-chunk collectives leave the halves as found.
    for _ in range(2):
        dist.all_reduce(_small_tensor(ctx, 1), async_op=True).wait()
    a, b, c = (_small_tensor(ctx, factor) for factor in (1, 10, 100))

    fault.reset_transfer_counts()
    _hold_flags(ctx, fault, CPU_FLAG_HOLD_S)
    if overlap:
        work_a = dist.all_reduce(a, async_op=True)
        work_b = dist.all_reduce(b, async_op=True)
        work_a.wait()
        work_c = dist.all_reduce(c, async_op=True)
        work_b.wait()
        work_c.wait()
    else:
        for tensor in (a, b, c):
            dist.all_reduce(tensor, async_op=True).wait()
    counts = _counts(fault)
    _hold_flags(ctx, fault, 0)
    dist.barrier()
    errors = _errors_in_sums(ctx, {"a": (a, 1), "b": (b, 10), "c": (c, 100)})
    ctx.record_result({"errors": errors, **counts})


class TestPGHalfReuseCUDA(MooncakePGCUDABackendTestCase):
    world_size = 2
    spawn_timeout_s = 150.0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        fault = preload_transfer_fault()
        cls.fault_library = str(fault.__enter__())
        cls.addClassCleanup(fault.__exit__, None, None, None)

    def _spawn(self, worker, *args, world: int = 2, **spawn_options) -> list[dict]:
        if torch.cuda.device_count() < world:
            self.skipTest(f"needs {world} CUDA devices")
        rows = self.spawn_backend_and_collect(
            worker,
            self.fault_library,
            *args,
            world_size=world,
            nprocs=world,
            **spawn_options,
        )
        self.assert_all_ok(rows)
        return rows

    def _assert_all_right(self, rows: list[dict], runs_of_each: list[int]) -> None:
        for row in rows:
            rank = row["rank"]
            self.assertEqual(
                [op["wrong"] for op in row["ops"]],
                [0] * len(runs_of_each),
                f"rank {rank} read wrong results: {row['ops']}",
            )
            self.assertEqual(
                [op["runs"] for op in row["ops"]], runs_of_each, f"rank {rank}"
            )

    def _assert_counts(self, rows: list[dict], reuse: bool) -> None:
        by_rank = {row["rank"]: row for row in rows}
        self.assertGreater(by_rank[SLOW_RANK]["held"], 0, "no flag batch was held")
        if reuse:
            # No flag of rank 1 showed that it had read the half's last run.
            self.assertGreater(by_rank[0]["reads"], 0, "rank 0 read no counts")
            return
        for rank, row in by_rank.items():
            self.assertEqual(row["reads"], 0, f"rank {rank} read counts")

    def _assert_replays_right(
        self, collectives: list[tuple[str, int]], world=2, reuse=True
    ):
        rows = self._spawn(_replay_worker, collectives, world=world)
        # The warm-up and the round after the setup run once each; a capture does
        # not run.
        self._assert_all_right(rows, [RUNS + 2] * len(collectives))
        self._assert_counts(rows, reuse)

    def test_slow_reader_keeps_its_data_across_replays(self) -> None:
        for world in (2, 4):
            with self.subTest(world=world):
                self._assert_replays_right([("all_reduce", 1)], world)

    def test_slow_reader_keeps_its_last_chunk_across_replays(self) -> None:
        for world in (2, 4):
            with self.subTest(world=world):
                self._assert_replays_right([("all_reduce", 3)], world)

    def test_slow_reader_keeps_its_data_of_other_collectives(self) -> None:
        for kind in ("all_gather", "broadcast", "reduce_scatter"):
            with self.subTest(kind=kind):
                self._assert_replays_right([(kind, 1)])

    def test_slow_reader_keeps_its_data_of_rooted_collectives(self) -> None:
        for world in (2, 4):
            for kind in ROOTED_KINDS:
                with self.subTest(world=world, kind=kind):
                    self._assert_replays_right([(kind, 1)], world)

    def test_slow_reader_keeps_its_data_of_two_replayed_collectives(self) -> None:
        self._assert_replays_right([("all_reduce", 1), ("all_reduce", 2)])

    def test_slow_reader_keeps_its_data_after_a_capture(self) -> None:
        for world in (2, 4):
            with self.subTest(world=world):
                rows = self._spawn(_eager_after_capture_worker, world=world)
                for row in rows:
                    self.assertEqual(row["errors"], [], f"rank {row['rank']}")
                self._assert_counts(rows, reuse=True)

    def test_slow_reader_keeps_its_data_on_independent_streams(self) -> None:
        for world in (2, 4):
            with self.subTest(world=world):
                rows = self._spawn(_eager_worker, 1, 3, world=world)
                # Each stream's warm-up and its round after the setup run once
                # each, and the rest go round-robin.
                self._assert_all_right(
                    rows, [RUNS // 3 + 3, RUNS // 3 + 2, RUNS // 3 + 2]
                )
                self._assert_counts(rows, reuse=True)

    def test_slow_reader_keeps_its_data_across_an_admission(self) -> None:
        spawn_ctx = mp.get_context("spawn")
        rows = self._spawn(
            _admission_worker, spawn_ctx.Barrier(3), spawn_ctx.Event(), world=3
        )
        for row in rows:
            self.assertEqual(row["errors"], [], f"rank {row['rank']}")
        self._assert_counts(rows, reuse=True)

    def test_control_replay_of_two_chunks(self) -> None:
        self._assert_replays_right([("all_reduce", 2)], reuse=False)

    def test_control_eager_on_one_stream(self) -> None:
        for chunks in (1, 3):
            with self.subTest(chunks=chunks):
                rows = self._spawn(_eager_worker, chunks, 1)
                self._assert_all_right(rows, [RUNS + 2])
                self._assert_counts(rows, reuse=False)

    def test_failed_read_of_read_counts_fails_the_link(self) -> None:
        spawn_ctx = mp.get_context("spawn")
        rows = {
            row["rank"]: row
            for row in self._spawn(
                _failed_count_read_worker, spawn_ctx.Event(), spawn_ctx.Event()
            )
        }
        # What the elastic fault rounds expect of rank 1 when its link to rank 0
        # fails. Its automatic sync applies the view that removes it before the
        # collective ends.
        reporter = rows[REPORTER]
        self.assertFalse(reporter["success"])
        self.assertEqual(reporter["hint"], [1, 0])
        self.assertGreater(reporter["injected"], 0)
        self.assertGreater(reporter["reads"], 0, "rank 1 read no counts")
        self.assertEqual(reporter["active"], [1, 0])
        for rank, row in rows.items():
            self.assertEqual(row["errors"], [], f"rank {rank}")

    def test_reader_that_dies_unread_is_given_up(self) -> None:
        spawn_ctx = mp.get_context("spawn")
        slow_rank_exited = spawn_ctx.Event()
        rows = self._spawn(
            _dead_reader_worker,
            spawn_ctx.Event(),
            slow_rank_exited,
            # Set by the parent once it sees that rank 1's process has exited.
            process_exit_events={SLOW_RANK: slow_rank_exited},
        )
        rows = {row["rank"]: row for row in rows}
        self.assertGreater(rows[SLOW_RANK]["held"], 0, "no flag batch was held")
        survivor = rows[0]
        self.assertEqual(survivor["errors"], [])
        self.assertFalse(survivor["success"])
        self.assertEqual(survivor["hint"], [0, 1])
        self.assertGreater(survivor["reads"], 0, "rank 0 read no counts")
        self.assertEqual(survivor["active"], [1, 0])


class TestPGHalfReuseCPU(MooncakePGCPUBackendTestCase):
    world_size = 2
    spawn_timeout_s = 120.0

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        fault = preload_transfer_fault()
        cls.fault_library = str(fault.__enter__())
        cls.addClassCleanup(fault.__exit__, None, None, None)

    def _run(self, overlap: bool) -> dict:
        rows = self.spawn_backend_and_collect(
            _async_cpu_worker,
            self.fault_library,
            overlap,
            world_size=self.world_size,
            nprocs=self.world_size,
        )
        self.assert_all_ok(rows)
        by_rank = {row["rank"]: row for row in rows}
        for rank, row in by_rank.items():
            self.assertEqual(row["errors"], [], f"rank {rank}")
        self.assertGreater(by_rank[SLOW_RANK]["held"], 0, "no flag batch was held")
        return by_rank

    def test_slow_reader_keeps_its_data_of_async_cpu_collectives(self) -> None:
        rows = self._run(overlap=True)
        self.assertGreater(rows[0]["reads"], 0, "rank 0 read no counts")

    def test_control_async_cpu_collectives_waited_in_turn(self) -> None:
        for rank, row in self._run(overlap=False).items():
            self.assertEqual(row["reads"], 0, f"rank {rank} read counts")


if __name__ == "__main__":
    unittest.main()
