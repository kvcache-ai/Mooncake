import os
import shlex
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

import torch
import torch.distributed as dist
import torch.multiprocessing as mp

from pg_test_utils import (
    MooncakePGCPUBackendTestCase,
    MooncakePGCUDABackendTestCase,
    MooncakePGWorkerContext,
)


def _basic_init_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test basic init works and rank/world_size are correct."""
    device = ctx.init_group()
    # Verify rank and world_size are accessible
    assert (
        dist.get_rank() == ctx.rank
    ), f"rank mismatch: {dist.get_rank()} != {ctx.rank}"
    assert dist.get_world_size() == ctx.world_size, "world_size mismatch"
    # Simple collective to verify group works
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    ctx.record_result({"sum": int(tensor.cpu().item())})


def _null_init_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test init without pg_options (using defaults)."""
    device = ctx.init_group(use_pg_options=False)
    tensor = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor, op=dist.ReduceOp.SUM)
    ctx.record_result({"sum": int(tensor.cpu().item())})


def _subgroup_create_destroy_worker(ctx: MooncakePGWorkerContext) -> None:
    """Test subgroup creation and destruction."""
    device = ctx.init_group()
    world_size = ctx.world_size
    rank = ctx.rank

    # Create two subgroups: even ranks and odd ranks
    # Note: new_group is collective, all ranks must call it
    even_ranks = list(range(0, world_size, 2))
    odd_ranks = list(range(1, world_size, 2))

    even_group = None
    odd_group = None
    subgroup_sum = None

    try:
        # All ranks collectively create both groups
        even_group = dist.new_group(ranks=even_ranks, backend=ctx.backend_name)
        odd_group = dist.new_group(ranks=odd_ranks, backend=ctx.backend_name)

        # Each rank uses its respective group
        if rank in even_ranks and even_group is not None:
            tensor = torch.tensor([rank], dtype=torch.int32, device=device)
            dist.all_reduce(tensor, group=even_group, op=dist.ReduceOp.SUM)
            subgroup_sum = int(tensor.cpu().item())
        elif rank in odd_ranks and odd_group is not None:
            tensor = torch.tensor([rank], dtype=torch.int32, device=device)
            dist.all_reduce(tensor, group=odd_group, op=dist.ReduceOp.SUM)
            subgroup_sum = int(tensor.cpu().item())

        # Verify world group still works after subgroup operations
        world_tensor = torch.tensor([1], dtype=torch.int32, device=device)
        dist.all_reduce(world_tensor, op=dist.ReduceOp.SUM)

        ctx.record_result(
            {
                "subgroup_sum": subgroup_sum,
                "world_sum": int(world_tensor.cpu().item()),
            }
        )
    finally:
        for g in (even_group, odd_group):
            if g is not None:
                try:
                    dist.destroy_process_group(g)
                except Exception:
                    pass


def _destroy_and_reinit_worker(ctx: MooncakePGWorkerContext, destroyed) -> None:
    """Test destroy and re-init process group."""
    # First init
    device = ctx.init_group()
    tensor1 = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor1, op=dist.ReduceOp.SUM)
    sum1 = int(tensor1.cpu().item())

    # Destroy WORLD group
    dist.destroy_process_group(dist.group.WORLD)
    # Reinitialization should start after every old communicator unregisters.
    destroyed.wait(timeout=10)

    # Re-init
    device = ctx.init_group()
    tensor2 = torch.tensor([ctx.rank + 1], dtype=torch.int32, device=device)
    dist.all_reduce(tensor2, op=dist.ReduceOp.SUM)
    sum2 = int(tensor2.cpu().item())

    ctx.record_result({"sum1": sum1, "sum2": sum2})


class TestMooncakePGEndpointCompatibility(unittest.TestCase):
    def test_old_and_independent_algorithm_endpoints(self) -> None:
        """Exercise the real wire types without starting a PG or using GPUs.

        The compiler must find the repository-pinned yalantinglibs headers,
        e.g. through CPATH, just like the transfer fault injection helper.
        """
        compiler = shlex.split(os.environ.get("CXX", "c++"))
        if not compiler or not shutil.which(compiler[0]):
            self.skipTest("endpoint compatibility test requires a C++ compiler")
        include = Path(__file__).resolve().parents[1] / "include"
        flags = ["-std=c++20", f"-I{include}"]
        probe = subprocess.run(
            [*compiler, *flags, "-E", "-x", "c++", "-"],
            input="#include <ylt/struct_pack.hpp>\n",
            capture_output=True,
            text=True,
            timeout=30,
        )
        if probe.returncode and (
            "file not found" in probe.stderr
            or "No such file or directory" in probe.stderr
        ):
            self.skipTest("yalantinglibs headers must be on the compiler include path")
        self.assertEqual(probe.returncode, 0, probe.stderr)
        source = r"""
#include <cassert>
#include <ylt/struct_pack.hpp>
#include "control_plane/control_types.h"

using namespace mooncake;

// #4372's endpoint shape. Adding an ordinary optional to the production
// type must fail these tests even if every process in a GPU test is upgraded.
struct OldEndpoint {
    uint64_t view_epoch_signal = 0;
    uint32_t view_epoch_signal_count = 0;
    std::optional<SimpleEndpoint> simple;
    std::optional<LLEndpoint> ll;
    std::optional<OneShotEndpoint> one_shot;
    bool operator==(const OldEndpoint&) const = default;
};

// Wire fixtures for the independently developed Tree extension in #4747.
// Different extension versions must neither alias nor hide one another.
struct TreeCapability {
    uint32_t version = 1;
};
struct TreeEndpointWire {
    uint64_t view_epoch_signal = 0;
    uint32_t view_epoch_signal_count = 0;
    std::optional<SimpleEndpoint> simple;
    std::optional<LLEndpoint> ll;
    std::optional<OneShotEndpoint> one_shot;
    struct_pack::compatible<TreeCapability, 20260930> tree;
};
struct CombinedEndpointWire {
    uint64_t view_epoch_signal = 0;
    uint32_t view_epoch_signal_count = 0;
    std::optional<SimpleEndpoint> simple;
    std::optional<LLEndpoint> ll;
    std::optional<OneShotEndpoint> one_shot;
    struct_pack::compatible<TreeCapability, 20260930> tree;
    struct_pack::compatible<TwoShotEndpoint, 20261030> two_shot;
};
template <class Endpoint> struct GroupEndpointWire {
    uint64_t endpoint_epoch = 0;
    uint64_t send_buffer[2] = {};
    uint64_t recv_buffer[2] = {};
    uint64_t send_sync[2] = {};
    uint64_t recv_sync[2] = {};
    uint64_t p2p_credit_region = 0;
    uint64_t p2p_ack_region = 0;
    Endpoint device_collective;
};
template <class To, class From> To decode(const From& value) {
    auto decoded = struct_pack::deserialize<To>(struct_pack::serialize(value));
    assert(decoded.has_value());
    return *decoded;
}

int main() {
    static_assert(static_cast<uint8_t>(DeviceAllReduceAlgorithm::Ring) == 1);
    static_assert(static_cast<uint8_t>(DeviceAllReduceAlgorithm::OneShot) == 2);
    static_assert(static_cast<uint8_t>(DeviceAllReduceAlgorithm::TwoShot) == 4);
    OldEndpoint old{128, 4, SimpleEndpoint{256, 640},
                    LLEndpoint{8192, 256, {}, {}},
                    OneShotEndpoint{65536, 1048576}};
    auto modern = decode<DeviceGroupEndpoint>(old);
    assert(!modern.two_shot);
    assert(decode<OldEndpoint>(modern) == old);
    modern.two_shot.emplace();
    modern.two_shot->device_uuid[0] = 42;
    modern.two_shot->native_atomic_peer_uuids.push_back(DeviceUUID{17});
    assert(modern.two_shot->version == kTwoShotEndpointVersion);
    assert(decode<DeviceGroupEndpoint>(modern) == modern);
    assert(decode<OldEndpoint>(modern) == old);

    auto tree = decode<TreeEndpointWire>(old);
    tree.tree.emplace();
    assert(!decode<DeviceGroupEndpoint>(tree).two_shot);
    assert(!decode<TreeEndpointWire>(modern).tree);
    auto combined = decode<CombinedEndpointWire>(modern);
    assert(!combined.tree && combined.two_shot);
    combined.tree.emplace();
    auto combined_reader = decode<DeviceGroupEndpoint>(combined);
    assert(combined_reader.tree && combined_reader.tree->version == 1);
    assert(combined_reader.two_shot == modern.two_shot);
    combined_reader.tree.reset();
    assert(combined_reader == modern);
    assert(decode<TreeEndpointWire>(combined).tree->version == 1);
    assert(decode<OldEndpoint>(combined) == old);

    // Compatibility must also hold when endpoints are nested in RPC data.
    GroupEndpointWire<OldEndpoint> old_group;
    old_group.endpoint_epoch = 17;
    old_group.send_buffer[1] = 1024;
    old_group.device_collective = old;
    auto group = decode<GroupEndpointInfo>(old_group);
    assert(group.endpoint_epoch == 17 && group.send_buffer[1] == 1024);
    assert(!group.device_collective.two_shot);
    group.device_collective = modern;
    const auto old_reader = decode<GroupEndpointWire<OldEndpoint>>(group);
    assert(old_reader.device_collective == old);
    assert(old_reader.endpoint_epoch == 17 && old_reader.send_buffer[1] == 1024);
    auto both_group = decode<GroupEndpointWire<CombinedEndpointWire>>(group);
    both_group.device_collective.tree.emplace();
    auto both_reader = decode<GroupEndpointInfo>(both_group);
    assert(both_reader.device_collective.tree);
    both_reader.device_collective.tree.reset();
    assert(both_reader == group);

    // An unknown capability version is retained for admission to reject it.
    modern.two_shot->version = kTwoShotEndpointVersion + 1;
    assert(decode<DeviceGroupEndpoint>(modern).two_shot->version ==
           kTwoShotEndpointVersion + 1);
    const std::vector<DeviceAllReduceAlgorithmChoice> choices = {
        {1024, static_cast<DeviceAllReduceAlgorithm>(3)},
        {UINT64_MAX, DeviceAllReduceAlgorithm::TwoShot}};
    const auto decoded_choices =
        decode<std::vector<DeviceAllReduceAlgorithmChoice>>(choices);
    assert(decoded_choices == choices);
    assert(decoded_choices[0].algorithm != decoded_choices[1].algorithm);
}
"""
        with tempfile.TemporaryDirectory(prefix="mooncake-pg-wire-") as tmp:
            binary = Path(tmp) / "endpoint_compatibility"
            result = subprocess.run(
                [*compiler, *flags, "-x", "c++", "-", "-o", str(binary)],
                input=source,
                capture_output=True,
                text=True,
                timeout=90,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            result = subprocess.run(
                [str(binary)], capture_output=True, text=True, timeout=30
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


class _InitFunctionalMixin:
    def test_basic_init(self) -> None:
        """Test basic init works and rank/world_size are correct."""
        rows = self.spawn_backend_and_collect(_basic_init_worker)
        self.assert_all_ok(rows)
        expected_sum = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["sum"], expected_sum)

    def test_null_init(self) -> None:
        """Test init without pg_options (using defaults)."""
        rows = self.spawn_backend_and_collect(_null_init_worker)
        self.assert_all_ok(rows)
        expected_sum = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["sum"], expected_sum)

    def test_world_size_one(self) -> None:
        """Test a process group with world size 1 initializes and works."""
        rows = self.spawn_backend_and_collect(_basic_init_worker, world_size=1)
        self.assert_all_ok(rows)
        for row in rows:
            self.assertEqual(row["sum"], 1)

    def test_subgroup_create_destroy(self) -> None:
        """Test subgroup creation and destruction."""
        if self.world_size < 4:
            self.skipTest("subgroup test requires at least 4 ranks")

        rows = self.spawn_backend_and_collect(_subgroup_create_destroy_worker)
        self.assert_all_ok(rows)

        # Verify world sum is correct
        expected_world = self.world_size
        for row in rows:
            self.assertEqual(row["world_sum"], expected_world)

        # Verify subgroup sums
        even_ranks = list(range(0, self.world_size, 2))
        odd_ranks = list(range(1, self.world_size, 2))
        expected_even = sum(even_ranks)
        expected_odd = sum(odd_ranks)

        for row in rows:
            if row["rank"] in even_ranks:
                self.assertEqual(row["subgroup_sum"], expected_even)
            else:
                self.assertEqual(row["subgroup_sum"], expected_odd)

    def test_destroy_and_reinit(self) -> None:
        """Test destroy and re-init process group."""
        destroyed = mp.get_context("spawn").Barrier(self.world_size)
        rows = self.spawn_backend_and_collect(_destroy_and_reinit_worker, destroyed)
        self.assert_all_ok(rows)

        expected = sum(range(1, self.world_size + 1))
        for row in rows:
            self.assertEqual(row["sum1"], expected)
            self.assertEqual(row["sum2"], expected)


class TestMooncakePGInitFunctionalCPU(
    _InitFunctionalMixin, MooncakePGCPUBackendTestCase
):
    world_size = 4


class TestMooncakePGInitFunctionalCUDA(
    _InitFunctionalMixin, MooncakePGCUDABackendTestCase
):
    world_size = 4

    @classmethod
    def configure_for_cuda_device_count(cls, device_count: int) -> None:
        if device_count < 2:
            return
        if device_count >= 4:
            cls.world_size = 4
            return
        cls.world_size = 2


if __name__ == "__main__":
    unittest.main()
