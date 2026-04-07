import unittest

import torch
import torch.distributed as dist

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGCUDABackendTestCase,
    MooncakePGWorkerContext,
    wait_until,
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
        tensor = torch.tensor([111 if rank == 0 else -1], dtype=torch.int32, device=device)
        dist.broadcast(tensor, src=0)
        return {"value": int(tensor.cpu().item())}

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
            dist.reduce_scatter(output, list(input_buf.chunk(world_size)), op=dist.ReduceOp.SUM)
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


class _CollectiveTestMixin:
    def test_world_init_without_pg_options(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "world_init_without_pg_options")
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
        rows = self.spawn_backend_and_collect(_collective_worker, "allreduce", "product")
        self.assert_all_ok(rows)
        expected = 2 ** self.world_size
        for row in rows:
            self.assertEqual(row["value"], expected)

    def test_broadcast(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "broadcast")
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["value"], 111)

    def test_all_gather_into_tensor(self) -> None:
        rows = self.spawn_backend_and_collect(_collective_worker, "all_gather_into_tensor")
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
            expected = self.world_size * (self.world_size * (self.world_size - 1) // 2 + rank)
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


class TestMooncakePGCollectivesCUDA(_CollectiveTestMixin, MooncakePGCUDABackendTestCase):
    world_size = 2


if __name__ == "__main__":
    unittest.main()
