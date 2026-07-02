import gc
import unittest

import torch
import torch.distributed as dist

from mooncake import pg
from pg_test_utils import MooncakePGCUDABackendTestCase, temporary_env


def _runtime_snapshot_worker(ctx, enable_runtime: bool, reinit: bool = False) -> None:
    env_value = "1" if enable_runtime else "0"
    with temporary_env({"MOONCAKE_PG_DEVICE_API_COLLECTIVES": env_value}):
        device = ctx.init_group()
        backend = ctx.get_backend()
        snapshot = dict(pg._device_runtime_snapshot(backend))

        tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
        dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
        expected_sum = ctx.world_size * (ctx.world_size + 1) // 2
        if int(tensor.cpu().item()) != expected_sum:
            raise AssertionError(
                f"rank {ctx.rank}: expected all_reduce sum {expected_sum}, "
                f"got {int(tensor.cpu().item())}"
            )

        second_snapshot = None
        if reinit:
            dist.destroy_process_group(dist.group.WORLD)
            del backend
            gc.collect()
            device = ctx.init_group()
            backend = ctx.get_backend()
            second_snapshot = dict(pg._device_runtime_snapshot(backend))

        ctx.record_result(
            {
                "snapshot": snapshot,
                "second_snapshot": second_snapshot,
            }
        )


class TestMooncakePGDeviceRuntimeFrameworkCUDA(MooncakePGCUDABackendTestCase):
    world_size = 2

    def test_runtime_snapshot_disabled_by_default(self) -> None:
        rows = self.spawn_backend_and_collect(_runtime_snapshot_worker, False)
        self.assert_all_ok(rows)
        for row in rows:
            snapshot = row["snapshot"]
            self.assertFalse(snapshot["enabled"])
            self.assertFalse(snapshot["direct_p2p_ready"])
            self.assertFalse(snapshot["rdma_ready"])
            self.assertFalse(snapshot["has_sequence_counter"])
            self.assertFalse(snapshot["has_sequence_slots"])
            self.assertEqual(snapshot["p2p_peer_ptr_count"], 0)

    def test_runtime_snapshot_enabled_same_host(self) -> None:
        rows = self.spawn_backend_and_collect(_runtime_snapshot_worker, True)
        self.assert_all_ok(rows)
        for row in rows:
            snapshot = row["snapshot"]
            self.assertTrue(snapshot["enabled"])
            self.assertTrue(snapshot["direct_p2p_ready"])
            self.assertTrue(snapshot["has_sequence_counter"])
            self.assertTrue(snapshot["has_sequence_slots"])
            self.assertEqual(snapshot["active_size"], self.world_size)
            self.assertEqual(snapshot["p2p_peer_ptr_count"], self.world_size)

    def test_runtime_snapshot_reinit(self) -> None:
        rows = self.spawn_backend_and_collect(_runtime_snapshot_worker, True, True)
        self.assert_all_ok(rows)
        for row in rows:
            first = row["snapshot"]
            second = row["second_snapshot"]
            self.assertIsNotNone(second)
            self.assertTrue(first["enabled"])
            self.assertTrue(second["enabled"])
            self.assertTrue(first["direct_p2p_ready"])
            self.assertTrue(second["direct_p2p_ready"])
            self.assertTrue(first["has_sequence_counter"])
            self.assertTrue(second["has_sequence_counter"])


if __name__ == "__main__":
    unittest.main()
