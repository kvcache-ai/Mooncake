import gc
import math
import os

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from mooncake import pg
from pg_tree_c_api import c_api_worker
from pg_test_utils import (
    MooncakePGCUDABackendTestCase,
    cuda_runtime_available,
    temporary_env,
    wait_until,
)


def assert_tensor_value(tensor, expected, stage):
    if not bool(torch.all(tensor == expected).item()):
        raise AssertionError(
            f"{stage}: expected every element to equal {expected}, got {tensor[:8].cpu().tolist()}"
        )


def capture_tree_graph(device, rank_value, initial_expected):
    tensor = torch.empty(65539, dtype=torch.float32, device=device)
    for repetition in range(3):
        tensor.fill_(rank_value)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        torch.cuda.synchronize(device)
        assert_tensor_value(tensor, initial_expected, f"warmup {repetition}")
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        tensor.fill_(rank_value)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    return tensor, graph


def replay_and_check(tensor, graph, device, expected, repetitions):
    for repetition in range(repetitions):
        graph.replay()
        torch.cuda.synchronize(device)
        assert_tensor_value(tensor, expected, f"replay {repetition}")


def graph_replay_worker(context):
    device = context.init_group()
    rank_value = (context.rank + 1) ** 2
    expected = sum((rank + 1) ** 2 for rank in range(context.world_size))
    tensor, graph = capture_tree_graph(device, rank_value, expected)
    try:
        replay_and_check(tensor, graph, device, expected, 10)
    finally:
        graph.reset()
        del graph
        gc.collect()
    context.record_result({"expected": expected, "replays": 10})


def nonblocking_graph_replay_worker(context):
    device = context.init_group()
    rank_value = (context.rank + 1) ** 2
    expected = sum((rank + 1) ** 2 for rank in range(context.world_size))
    stream = torch.cuda.Stream(device=device)
    with torch.cuda.stream(stream):
        tensor, graph = capture_tree_graph(device, rank_value, expected)
        try:
            replay_and_check(tensor, graph, device, expected, 10)
        finally:
            graph.reset()
            del graph
            gc.collect()
    context.record_result({"expected": expected, "replays": 10})


def graph_membership_worker(
    context, start_join, full_group_done, shrink_committed, survivors_done
):
    initial_size = context.world_size - 1
    join_rank = initial_size
    initial_expected = sum((rank + 1) ** 2 for rank in range(initial_size))
    full_expected = sum((rank + 1) ** 2 for rank in range(context.world_size))
    rank_value = (context.rank + 1) ** 2
    original = context.rank < initial_size

    if original:
        device = context.init_group(
            world_size=initial_size, max_group_size=context.world_size
        )
        tensor, graph = capture_tree_graph(device, rank_value, initial_expected)
        replay_and_check(tensor, graph, device, initial_expected, 3)
        if context.rank == 0:
            start_join.set()
        backend = context.get_backend()
        wait_until(
            lambda: all(pg.get_peer_state(backend, [join_rank])),
            timeout_s=30.0,
            description=f"rank {context.rank} waiting for graph joiner",
        )
        response = pg.recover_ranks(backend, [join_rank])
        if response.status != pg.ProposalStatus.Applied:
            raise AssertionError(f"activation failed: {response.status}")
    else:
        if not start_join.wait(timeout=45.0):
            raise TimeoutError("original graph participants did not become ready")
        device = context.init_group(
            world_size=context.world_size,
            max_group_size=context.world_size,
            is_extension=True,
        )
        tensor, graph = capture_tree_graph(device, rank_value, rank_value)
        replay_and_check(tensor, graph, device, rank_value, 3)
        backend = context.get_backend()
        pg.join_group(backend)

    try:
        replay_and_check(tensor, graph, device, full_expected, 5)
        full_group_done.wait(timeout=45.0)
        if context.rank == 0:
            response = pg.deactivate_ranks(backend, [join_rank])
            if response.status != pg.ProposalStatus.Applied:
                raise AssertionError(f"deactivation failed: {response.status}")
            shrink_committed.set()
        if not shrink_committed.wait(timeout=45.0):
            raise TimeoutError("shrink was not committed")
        if original:
            replay_and_check(tensor, graph, device, initial_expected, 5)
            if context.rank == 0:
                survivors_done.set()
        elif not survivors_done.wait(timeout=45.0):
            raise TimeoutError("survivors did not finish graph replay")
    finally:
        graph.reset()
        del graph
        gc.collect()
    context.record_result(
        {
            "role": "original" if original else "joiner",
            "full_expected": full_expected,
            "shrink_expected": initial_expected if original else None,
            "same_graph": True,
        }
    )


def nonblocking_graph_membership_worker(
    context, start_join, full_group_done, shrink_committed, survivors_done
):
    torch.cuda.set_device(context.rank)
    stream = torch.cuda.Stream(device=context.rank)
    with torch.cuda.stream(stream):
        graph_membership_worker(
            context, start_join, full_group_done, shrink_committed, survivors_done
        )


def eager_matrix_worker(context, full_matrix):
    device = context.init_group()
    types = (
        [
            torch.float16,
            torch.bfloat16,
            torch.float32,
            torch.float64,
            torch.uint8,
            torch.int8,
            torch.int16,
            torch.int32,
            torch.int64,
            torch.bool,
        ]
        if full_matrix
        else [torch.float32]
    )
    operators = [
        (dist.ReduceOp.SUM, "sum"),
        (dist.ReduceOp.PRODUCT, "product"),
        (dist.ReduceOp.MIN, "min"),
        (dist.ReduceOp.MAX, "max"),
    ]
    checked = 0
    for dtype in types:
        for operation, name in operators:
            values = [rank + 1 for rank in range(context.world_size)]
            if name == "product":
                values = [2] * context.world_size
            if dtype == torch.bool:
                values = [bool(rank % 2) for rank in range(context.world_size)]
            expected = (
                sum(values)
                if name == "sum"
                else math.prod(values)
                if name == "product"
                else min(values)
                if name == "min"
                else max(values)
            )
            if dtype == torch.bool:
                expected = bool(expected)
            for count in [0, 3, 65539]:
                tensor = torch.full(
                    (count,), values[context.rank], dtype=dtype, device=device
                )
                work = dist.all_reduce(tensor, op=operation, async_op=True)
                work.wait()
                if not pg.get_local_success(work):
                    raise AssertionError(f"Tree failed: {dtype}/{name}/{count}")
                assert_tensor_value(tensor, expected, f"{dtype}/{name}/{count}")
                checked += 1
    # Enough tiles to cycle both slots repeatedly, with caller stream changes.
    for repetition, count in enumerate([1, 31, 33, 8388609, 7, 1048579, 3]):
        stream = torch.cuda.Stream(device=device)
        with torch.cuda.stream(stream):
            tensor = torch.full(
                (count,), context.rank + 1, dtype=torch.float32, device=device
            )
            dist.all_reduce(tensor)
        stream.synchronize()
        assert_tensor_value(
            tensor,
            context.world_size * (context.world_size + 1) // 2,
            f"slot reuse {repetition}",
        )
        checked += 1
    context.record_result({"checked": checked})


def rejected_joiner_worker(context, start_join, rejected):
    if context.rank < 2:
        device = context.init_group(world_size=2, max_group_size=3)
        value = (context.rank + 1) ** 2
        tensor, graph = capture_tree_graph(device, value, 5)
        try:
            replay_and_check(tensor, graph, device, 5, 3)
            if context.rank == 0:
                start_join.set()
            if not rejected.wait(timeout=45):
                raise TimeoutError("incompatible joiner did not report rejection")
            replay_and_check(tensor, graph, device, 5, 5)
            active = pg.get_active_ranks(context.get_backend()).cpu().tolist()
            if active != [1, 1, 0]:
                raise AssertionError(f"rejected joiner changed membership: {active}")
        finally:
            graph.reset()
            del graph
            gc.collect()
        context.record_result({"same_graph": True})
    else:
        if not start_join.wait(timeout=45):
            raise TimeoutError("existing graph was not ready")
        with temporary_env({"MOONCAKE_PG_PREFERRED_GPU_COLLECTIVE_BACKEND": "legacy"}):
            try:
                context.init_group(world_size=3, max_group_size=3, is_extension=True)
            except RuntimeError as error:
                if "explicit tree requires" not in str(error):
                    raise
                context.record_result({"rejected": True})
                rejected.set()
            else:
                raise AssertionError("Tree silently accepted the legacy joiner")


def captured_work_worker(context):
    device = context.init_group()
    tensor = torch.ones(7, device=device)
    dist.all_reduce(tensor)
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        tensor.fill_(1)
        work = dist.all_reduce(tensor, async_op=True)
    try:
        graph.replay()
        torch.cuda.synchronize(device)
        try:
            success = pg.get_local_success(work)
            hints = pg.get_failed_ranks_hint(work)
        except Exception as error:
            raise AssertionError(
                f"captured Work hint query failed after replay synchronization: {error}"
            ) from error
        if not success or bool(hints.any().item()):
            raise AssertionError("healthy captured Work has failure hints")
        assert_tensor_value(tensor, context.world_size, "captured Work")
        context.record_result({"captured_hint_query": True})
    finally:
        graph.reset()
        del graph
        gc.collect()


def graph_failure_worker(context, broken_rank, ready, broken, recovered):
    device = context.init_group()
    # Coordinator remains alive on global rank 0. The data-plane Tree has
    # root=1, internal=3, leaf=2, so all three roles can fail independently.
    group = dist.new_group(ranks=[1, 2, 3], backend=context.backend_name)
    if context.rank == 0:
        ready.wait(timeout=60)
        recovered.wait(timeout=90)
        context.record_result({"coordinator_alive": True})
        return
    value = (context.rank + 1) ** 2
    tensor = torch.empty(65539, dtype=torch.float32, device=device)
    for _ in range(3):
        tensor.fill_(value)
        dist.all_reduce(tensor, group=group)
        torch.cuda.synchronize(device)
        assert_tensor_value(tensor, 29, "subgroup warmup")
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        tensor.fill_(value)
        work = dist.all_reduce(tensor, group=group, async_op=True)
    graph.replay()
    torch.cuda.synchronize(device)
    assert_tensor_value(tensor, 29, "healthy subgroup replay")
    ready.wait(timeout=60)
    if context.rank == broken_rank:
        context.record_result({"role": "failed", "rank": broken_rank})
        broken.set()
        os._exit(0)
    if not broken.wait(timeout=45):
        raise TimeoutError("fault process did not exit")
    try:
        graph.replay()
        torch.cuda.synchronize(device)
        if pg.get_local_success(work):
            raise AssertionError("failed Graph replay was reported as successful")
        hints = pg.get_failed_ranks_hint(work)
        if not bool(hints.any().item()):
            raise AssertionError("failed Graph replay has no failed-peer hint")
        # Discard the failed output. The captured fill restores original input
        # on each replay; clear the caller-owned hint before the next attempt.
        expected = 29 - (broken_rank + 1) ** 2
        for _ in range(5):
            hints.zero_()
            graph.replay()
            torch.cuda.synchronize(device)
            assert_tensor_value(tensor, expected, "same graph after failure")
            if not pg.get_local_success(work):
                raise AssertionError("recovered replay still failed")
        context.record_result(
            {
                "role": "survivor",
                "same_graph": True,
                "failure_reported": True,
                "expected": expected,
            }
        )
        recovered.wait(timeout=45)
    finally:
        graph.reset()
        del graph
        gc.collect()
        dist.destroy_process_group(group)


class TreeGraphTests(MooncakePGCUDABackendTestCase):
    world_size = 2
    spawn_timeout_s = 180.0

    def setUp(self):
        super().setUp()
        self.enterContext(
            temporary_env(
                {
                    "MOONCAKE_PG_COLLECTIVE_ALGORITHM": "tree",
                    "MOONCAKE_PG_PREFERRED_GPU_COLLECTIVE_BACKEND": "new",
                }
            )
        )

    def test_fixed_group_replay(self):
        rows = self.spawn_backend_and_collect(graph_replay_worker)
        self.assert_all_ok(rows)
        self.assertEqual([row["replays"] for row in rows], [10, 10])

    def test_nonblocking_stream_replay(self):
        rows = self.spawn_backend_and_collect(nonblocking_graph_replay_worker)
        self.assert_all_ok(rows)
        self.assertEqual([row["replays"] for row in rows], [10, 10])

    def test_same_graph_scale_up_and_shrink(self):
        if not cuda_runtime_available(3):
            self.skipTest("requires 3 CUDA devices")
        spawn_context = mp.get_context("spawn")
        rows = self.spawn_backend_and_collect(
            graph_membership_worker,
            spawn_context.Event(),
            spawn_context.Barrier(3),
            spawn_context.Event(),
            spawn_context.Event(),
            world_size=3,
            nprocs=3,
        )
        self.assert_all_ok(rows)
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["same_graph"] for row in rows))
        self.assertEqual([row["full_expected"] for row in rows], [14, 14, 14])

    def test_nonblocking_same_graph_scale_up_and_shrink(self):
        if not cuda_runtime_available(3):
            self.skipTest("requires 3 CUDA devices")
        spawn_context = mp.get_context("spawn")
        rows = self.spawn_backend_and_collect(
            nonblocking_graph_membership_worker,
            spawn_context.Event(),
            spawn_context.Barrier(3),
            spawn_context.Event(),
            spawn_context.Event(),
            world_size=3,
            nprocs=3,
        )
        self.assert_all_ok(rows)
        self.assertEqual(len(rows), 3)
        self.assertTrue(all(row["same_graph"] for row in rows))
        self.assertEqual([row["full_expected"] for row in rows], [14, 14, 14])

    def _eager(self, world_size, full_matrix=False):
        if not cuda_runtime_available(world_size):
            self.skipTest(f"requires {world_size} CUDA devices")
        rows = self.spawn_backend_and_collect(
            eager_matrix_worker, full_matrix, world_size=world_size, nprocs=world_size
        )
        self.assert_all_ok(rows)
        self.assertTrue(all(row["checked"] >= 19 for row in rows))

    def test_eager_one_rank(self):
        self._eager(1)

    def test_eager_two_ranks(self):
        self._eager(2, True)

    def test_eager_three_ranks(self):
        self._eager(3)

    def test_eager_four_ranks(self):
        self._eager(4)

    def test_eager_five_ranks(self):
        self._eager(5, True)

    def test_c_api_out_of_place_and_shared_workspace(self):
        spawn_context = mp.get_context("spawn")
        with spawn_context.Manager() as manager:
            rows = self.spawn_backend_and_collect(
                c_api_worker,
                manager.dict(),
                spawn_context.Event(),
                spawn_context.Barrier(2),
            )
        self.assert_all_ok(rows)
        self.assertTrue(all(row["out_of_place"] and row["same_graph"] for row in rows))

    def test_rejected_joiner_preserves_existing_graph(self):
        if not cuda_runtime_available(3):
            self.skipTest("requires 3 CUDA devices")
        spawn_context = mp.get_context("spawn")
        rows = self.spawn_backend_and_collect(
            rejected_joiner_worker,
            spawn_context.Event(),
            spawn_context.Event(),
            world_size=3,
            nprocs=3,
        )
        self.assert_all_ok(rows)
        self.assertTrue(rows[0]["same_graph"] and rows[1]["same_graph"])
        self.assertTrue(rows[2]["rejected"])

    def test_captured_work_hint_query(self):
        rows = self.spawn_backend_and_collect(captured_work_worker)
        self.assert_all_ok(rows)

    def _graph_failure(self, rank):
        if not cuda_runtime_available(4):
            self.skipTest("requires 4 CUDA devices")
        spawn_context = mp.get_context("spawn")
        rows = self.spawn_backend_and_collect(
            graph_failure_worker,
            rank,
            spawn_context.Barrier(4),
            spawn_context.Event(),
            spawn_context.Barrier(3),
            world_size=4,
            nprocs=4,
        )
        self.assert_all_ok(rows)
        survivors = [row for row in rows if row.get("role") == "survivor"]
        self.assertEqual(len(survivors), 2)
        self.assertTrue(
            all(row["same_graph"] and row["failure_reported"] for row in survivors)
        )

    def test_same_graph_recovers_root_failure(self):
        self._graph_failure(1)

    def test_same_graph_recovers_internal_failure(self):
        self._graph_failure(3)

    def test_same_graph_recovers_leaf_failure(self):
        self._graph_failure(2)
