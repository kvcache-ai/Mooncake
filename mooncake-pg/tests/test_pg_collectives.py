import unittest

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGCUDABackendTestCase,
    MooncakePGMUSABackendTestCase,
    MooncakePGWorkerContext,
)


def _collective_payload(
    ctx: MooncakePGWorkerContext,
    case_name: str,
    case_arg: str | None,
) -> dict:
    device = ctx.device
    rank = ctx.rank
    world_size = ctx.world_size
    device_type = ctx.device_type

    if case_name == "world_init_without_pg_options":
        tensor = torch.tensor([rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        return {"value": int(tensor.cpu().item())}

    if case_name == "allreduce":
        if case_arg == "sum":
            tensor = torch.tensor([rank + 1], dtype=torch.int32, device=device)
            op = dist.ReduceOp.SUM
        elif case_arg == "min":
            tensor = torch.tensor([rank + 10], dtype=torch.int32, device=device)
            op = dist.ReduceOp.MIN
        elif case_arg == "max":
            tensor = torch.tensor([rank + 10], dtype=torch.int32, device=device)
            op = dist.ReduceOp.MAX
        elif case_arg == "product":
            tensor = torch.tensor([2], dtype=torch.int32, device=device)
            op = dist.ReduceOp.PRODUCT
        else:
            raise ValueError(f"unsupported allreduce case_arg: {case_arg}")
        dist.all_reduce(tensor, op=op)
        return {"value": int(tensor.cpu().item())}

    if case_name == "broadcast":
        tensor = torch.tensor(
            [111 if rank == 0 else -1], dtype=torch.int32, device=device
        )
        dist.broadcast(tensor, src=0)
        return {"value": int(tensor.cpu().item())}

    if case_name == "noncontiguous_collectives":
        def make_matrix(values: torch.Tensor, transposed: bool) -> torch.Tensor:
            storage = torch.empty((2, 2), dtype=torch.int32, device=device)
            tensor = storage.t() if transposed else storage
            tensor.copy_(values)
            return tensor

        transposed = rank % 2 == 0
        errors = []
        broadcast = make_matrix(
            torch.full((2, 2), -1, dtype=torch.int32, device=device), transposed
        )
        if rank == 0:
            broadcast.copy_(torch.tensor([[1, 2], [3, 4]], device=device))
        dist.broadcast(broadcast, src=0)
        if broadcast.cpu().tolist() != [[1, 2], [3, 4]]:
            errors.append("broadcast returned incorrect logical values")

        allreduce = make_matrix(
            torch.arange(4, dtype=torch.int32, device=device).view(2, 2) + rank,
            transposed,
        )
        dist.all_reduce(allreduce, op=dist.ReduceOp.SUM)
        rank_sum = world_size * (world_size - 1) // 2
        expected = (
            torch.arange(4, dtype=torch.int32, device=device)
            .view(2, 2)
            .mul(world_size)
            .add(rank_sum)
        )
        if allreduce.cpu().tolist() != expected.cpu().tolist():
            errors.append("all_reduce returned incorrect logical values")

        allgather_input = make_matrix(
            torch.arange(4, dtype=torch.int32, device=device).view(2, 2) + rank,
            transposed,
        )
        allgather_outputs = [
            make_matrix(
                torch.zeros((2, 2), dtype=torch.int32, device=device),
                peer % 2 != 0,
            )
            for peer in range(world_size)
        ]
        dist.all_gather(allgather_outputs, allgather_input)
        if [item.cpu().tolist() for item in allgather_outputs] != [
            (torch.arange(4, dtype=torch.int32).view(2, 2) + peer).tolist()
            for peer in range(world_size)
        ]:
            errors.append("all_gather returned incorrect logical values")

        gather_input = make_matrix(
            torch.arange(4, dtype=torch.int32, device=device).view(2, 2) + rank,
            transposed,
        )
        if rank == 0:
            gather_outputs = [
                make_matrix(
                    torch.zeros((2, 2), dtype=torch.int32, device=device),
                    peer % 2 != 0,
                )
                for peer in range(world_size)
            ]
            dist.gather(gather_input, gather_outputs, dst=0)
            if [item.cpu().tolist() for item in gather_outputs] != [
                (torch.arange(4).view(2, 2) + peer).tolist()
                for peer in range(world_size)
            ]:
                errors.append("gather returned incorrect logical values")
        else:
            dist.gather(gather_input, dst=0)

        reduce_input = make_matrix(
            torch.arange(4, dtype=torch.int32, device=device).view(2, 2) + rank,
            transposed,
        )
        dist.reduce(reduce_input, dst=0, op=dist.ReduceOp.SUM)
        if rank == 0:
            expected_reduce = (
                torch.arange(4, dtype=torch.int32)
                .view(2, 2)
                .mul(world_size)
                .add(rank_sum)
            )
            if reduce_input.cpu().tolist() != expected_reduce.tolist():
                errors.append("reduce returned incorrect logical values")

        scatter_output = make_matrix(
            torch.zeros((2, 2), dtype=torch.int32, device=device), transposed
        )
        if rank == 0:
            scatter_inputs = []
            for peer in range(world_size):
                scatter_inputs.append(
                    make_matrix(
                        torch.arange(4, dtype=torch.int32, device=device)
                        .view(2, 2)
                        .add(peer * 10),
                        peer % 2 == 0,
                    )
                )
            dist.scatter(scatter_output, scatter_inputs, src=0)
        else:
            dist.scatter(scatter_output, src=0)
        expected_scatter = (torch.arange(4).view(2, 2) + rank * 10).tolist()
        if scatter_output.cpu().tolist() != expected_scatter:
            errors.append("scatter returned incorrect logical values")

        allgather_base_input_storage = torch.empty(
            (2, 2), dtype=torch.int32, device=device
        )
        allgather_base_input = allgather_base_input_storage[:, 0]
        allgather_base_input.copy_(
            torch.tensor([rank * 10, rank * 10 + 1], dtype=torch.int32, device=device)
        )
        allgather_base_output_storage = torch.empty(
            (world_size * 2, 2), dtype=torch.int32, device=device
        )
        allgather_base_output = allgather_base_output_storage[:, 0]
        dist.all_gather_into_tensor(allgather_base_output, allgather_base_input)
        expected_allgather_base = [
            value
            for peer in range(world_size)
            for value in (peer * 10, peer * 10 + 1)
        ]
        if allgather_base_output.cpu().tolist() != expected_allgather_base:
            errors.append("all_gather_into_tensor returned incorrect values")

        reduce_scatter_base_input_storage = torch.empty(
            (world_size * 2, 2), dtype=torch.int32, device=device
        )
        reduce_scatter_base_input = reduce_scatter_base_input_storage[:, 0]
        reduce_scatter_base_input.copy_(
            torch.arange(world_size * 2, dtype=torch.int32, device=device) * 10
            + rank
        )
        reduce_scatter_base_output_storage = torch.empty(
            (2, 2), dtype=torch.int32, device=device
        )
        reduce_scatter_base_output = reduce_scatter_base_output_storage[:, 0]
        dist.reduce_scatter_tensor(
            reduce_scatter_base_output,
            reduce_scatter_base_input,
            op=dist.ReduceOp.SUM,
        )
        rank_sum = world_size * (world_size - 1) // 2
        expected_reduce_scatter_base = [
            rank * 20 * world_size + rank_sum,
            (rank * 20 + 10) * world_size + rank_sum,
        ]
        if reduce_scatter_base_output.cpu().tolist() != expected_reduce_scatter_base:
            errors.append("reduce_scatter_tensor returned incorrect values")

        if errors:
            raise AssertionError("; ".join(errors))
        return {"value": "ok"}

    if case_name == "all_gather_into_tensor":
        local = torch.tensor([rank], dtype=torch.int32, device=device)
        gathered = torch.empty(world_size, dtype=torch.int32, device=device)
        dist.all_gather_into_tensor(gathered, local)
        return {"value": gathered.cpu().tolist()}

    if case_name == "all_gather_list":
        local = torch.tensor([rank], dtype=torch.int32, device=device)
        gathered = [torch.empty_like(local) for _ in range(world_size)]
        dist.all_gather(gathered, local)
        return {"value": [int(t.cpu().item()) for t in gathered]}

    if case_name == "reduce_scatter_sum":
        input_buf = torch.arange(
            rank * world_size,
            (rank + 1) * world_size,
            dtype=torch.int32,
            device=device,
        )
        output = torch.empty(1, dtype=torch.int32, device=device)
        if hasattr(dist, "reduce_scatter_tensor"):
            dist.reduce_scatter_tensor(output, input_buf, op=dist.ReduceOp.SUM)
        else:
            dist.reduce_scatter(
                output, list(input_buf.chunk(world_size)), op=dist.ReduceOp.SUM
            )
        return {"value": output.cpu().tolist()}

    if case_name == "barrier":
        dist.barrier()
        return {"value": "ok"}

    if case_name == "gather":
        tensor = torch.tensor([rank], dtype=torch.int32, device=device)
        if rank == 0:
            gather_list = [torch.empty_like(tensor) for _ in range(world_size)]
            dist.gather(tensor, gather_list, dst=0)
            return {"value": [int(item.cpu().item()) for item in gather_list]}
        dist.gather(tensor, dst=0)
        return {"value": None}

    if case_name == "scatter":
        tensor = torch.zeros(1, dtype=torch.int32, device=device)
        if rank == 0:
            scatter_list = [
                torch.tensor([peer], dtype=torch.int32, device=device)
                for peer in range(world_size)
            ]
            dist.scatter(tensor, scatter_list, src=0)
        else:
            dist.scatter(tensor, src=0)
        return {"value": int(tensor.cpu().item())}

    if case_name == "reduce":
        tensor = torch.tensor([1], dtype=torch.int32, device=device)
        dist.reduce(tensor, dst=0, op=dist.ReduceOp.SUM)
        return {"value": int(tensor.cpu().item()) if rank == 0 else None}

    if case_name == "async_allreduce":
        tensor = torch.tensor([rank + 1], dtype=torch.int32, device=device)
        work = dist.all_reduce(tensor, op=dist.ReduceOp.SUM, async_op=True)
        work.wait()
        if device_type == "cuda":
            torch.cuda.synchronize(device)
        return {"value": int(tensor.cpu().item())}

    raise ValueError(f"unsupported collective case_name: {case_name}")


def _collective_worker(
    ctx: MooncakePGWorkerContext,
    case_name: str,
    case_arg: str | None = None,
) -> None:
    ctx.init_group(use_pg_options=case_name != "world_init_without_pg_options")
    payload = _collective_payload(ctx, case_name, case_arg)
    ctx.record_result(payload)


def _async_ops_on_independent_streams_worker(
    ctx: MooncakePGWorkerContext,
    allow_rank_one_to_start,
) -> None:
    device = ctx.init_group()
    stream_a = torch.cuda.Stream(device=device)
    stream_b = torch.cuda.Stream(device=device)

    rank_zero_submitted_both = True
    if ctx.rank == 1:
        rank_zero_submitted_both = allow_rank_one_to_start.wait(timeout=5.0)

    with torch.cuda.stream(stream_a):
        tensor_a = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
        work_a = dist.all_reduce(tensor_a, op=dist.ReduceOp.SUM, async_op=True)
    with torch.cuda.stream(stream_b):
        tensor_b = torch.tensor([(ctx.rank + 1) * 10], dtype=torch.int32, device=device)
        work_b = dist.all_reduce(tensor_b, op=dist.ReduceOp.SUM, async_op=True)

    if ctx.rank == 0:
        # Rank 1 does not submit either operation until both calls on rank 0
        # return.
        allow_rank_one_to_start.set()

    work_a.wait()
    work_b.wait()
    stream_a.synchronize()
    stream_b.synchronize()
    ctx.record_result(
        {
            "rank_zero_submitted_both": rank_zero_submitted_both,
            "values": [int(tensor_a.cpu().item()), int(tensor_b.cpu().item())],
        }
    )


class _CollectiveTestMixin:
    def test_world_init_without_pg_options(self) -> None:
        rows = self.spawn_backend_and_collect(
            _collective_worker, "world_init_without_pg_options"
        )
        self.assert_all_ok(rows)

        expected = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_allreduce_sum(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "allreduce", "sum")
        self.assert_all_ok(rows)
        expected = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_allreduce_min(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "allreduce", "min")
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["value"], 10)

    def test_allreduce_max(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "allreduce", "max")
        self.assert_all_ok(rows)
        expected = 10 + self.world_size - 1
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_allreduce_product(self) -> None:
        rows = self.spawn_backend_and_collect(
            _collective_worker, "allreduce", "product"
        )
        self.assert_all_ok(rows)
        expected = 2**self.world_size
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_broadcast(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "broadcast")
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["value"], 111)

    def test_noncontiguous_collectives(self) -> None:
        rows = self.spawn_backend_and_collect(
            _collective_worker, "noncontiguous_collectives"
        )
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["value"], "ok")

    def test_all_gather_into_tensor(self) -> None:
        rows = self.spawn_backend_and_collect(
            _collective_worker, "all_gather_into_tensor"
        )
        self.assert_all_ok(rows)
        expected = list(range(self.world_size))
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_all_gather_list(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "all_gather_list")
        self.assert_all_ok(rows)
        expected = list(range(self.world_size))
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_reduce_scatter_sum(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "reduce_scatter_sum")
        self.assert_all_ok(rows)
        for row in rows:
            rank = row["rank"]
            expected = self.world_size * (
                self.world_size * (self.world_size - 1) // 2 + rank
            )
            self.assertEqual(row["value"], [expected])

    def test_barrier(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "barrier")
        self.assert_all_ok(rows)

    def test_gather(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "gather")
        self.assert_all_ok(rows)
        self.assertEqual(rows[0]["value"], list(range(self.world_size)))

    def test_scatter(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "scatter")
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["value"], row["rank"])

    def test_reduce(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "reduce")
        self.assert_all_ok(rows)
        self.assertEqual(rows[0]["value"], self.world_size)

    def test_async_allreduce_work_functional(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "async_allreduce")
        self.assert_all_ok(rows)
        expected = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["value"], expected)


class TestMooncakePGCollectivesCPU(_CollectiveTestMixin, MooncakePGCPUBackendTestCase):
    world_size = 4


class TestMooncakePGCollectivesCUDA(
    _CollectiveTestMixin, MooncakePGCUDABackendTestCase
):
    world_size = 2

    def test_async_ops_on_independent_streams(self) -> None:
        spawn_ctx = mp.get_context("spawn")
        allow_rank_one_to_start = spawn_ctx.Event()
        rows = self.spawn_backend_and_collect(
            _async_ops_on_independent_streams_worker,
            allow_rank_one_to_start,
        )
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["values"], [3, 30])

        rank_one = next(row for row in rows if row["rank"] == 1)
        self.assertTrue(
            rank_one["rank_zero_submitted_both"],
            "rank 0 blocked before submitting both async operations",
        )


class TestMooncakePGCollectivesMUSA(
    _CollectiveTestMixin, MooncakePGMUSABackendTestCase
):
    world_size = 2


if __name__ == "__main__":
    unittest.main()
