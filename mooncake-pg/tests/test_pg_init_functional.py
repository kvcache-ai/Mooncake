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


class TestMooncakePGRpcRuntime(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        compiler = shlex.split(os.environ.get("CXX", "c++"))
        if not compiler or not shutil.which(compiler[0]):
            raise unittest.SkipTest("RPC runtime tests require a C++ compiler")
        root = Path(__file__).resolve().parents[1]
        flags = ["-std=c++20", "-pthread", f"-I{root / 'include'}"]
        flags += shlex.split(os.environ.get("CXXFLAGS", ""))
        probe = subprocess.run(
            [*compiler, *flags, "-E", "-x", "c++", "-"],
            input="#include <ylt/coro_rpc/coro_rpc_client.hpp>\n#include <glog/logging.h>\n",
            capture_output=True,
            text=True,
            timeout=30,
        )
        if probe.returncode and (
            "file not found" in probe.stderr
            or "No such file or directory" in probe.stderr
        ):
            raise unittest.SkipTest("RPC tests require yalantinglibs and glog headers")
        if probe.returncode:
            raise AssertionError(probe.stderr)
        temporary = tempfile.TemporaryDirectory(prefix="mooncake-pg-rpc-")
        cls.addClassCleanup(temporary.cleanup)
        source = Path(temporary.name) / "rpc_runtime_test.cpp"
        cls.binary = Path(temporary.name) / "rpc_runtime_test"
        source.write_text(
            r"""
#include "control_plane/rpc_runtime.h"
#include <future>
#include <iostream>
#include <thread>
#include <vector>
using namespace std::chrono_literals;

void require(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}
struct Echo {
    std::atomic<int> calls{0};
    std::string echo(std::string value) {
        ++calls;
        // Keep a response in flight while another request starts writing.
        std::this_thread::sleep_for(1ms);
        return value;
    }
};
struct Server {
    Echo echo;
    mooncake::RpcServer rpc;
    std::string address;
    Server() {
        rpc.registerHandler<&Echo::echo>(&echo);
        require(rpc.start(), "server failed to start");
        address = rpc.getListenAddr("127.0.0.1");
    }
    ~Server() { rpc.shutdown(); }
};

void addressOwnership() {
    Server server;
    mooncake::RpcClient client(2s, 2s);
    auto executors = coro_io::g_io_context_pool().get_all_executor();
    std::promise<void> release;
    auto gate = release.get_future().share();
    std::vector<std::promise<void>> entered(executors.size());
    for (size_t i = 0; i < executors.size(); ++i) {
        asio::post(executors[i]->get_asio_executor(), [&, i] {
            entered[i].set_value();
            gate.wait();
        });
    }
    for (auto& ready : entered) ready.get_future().wait();
    std::promise<bool> response;
    {
        std::string address = server.address;
        client.callAsync<&Echo::echo>(address, std::string("call"),
            [&](auto result) { response.set_value(result && *result == "call"); });
        client.send<&Echo::echo>(address, std::string("send"));
        // Neither async API may borrow storage that the caller can reuse.
        address.assign("invalid:0");
    }
    release.set_value();
    auto result = response.get_future();
    require(result.wait_for(5s) == std::future_status::ready, "RPC timed out");
    require(result.get(), "async RPC borrowed the caller's address");
    auto deadline = std::chrono::steady_clock::now() + 5s;
    while (server.echo.calls.load() < 2 && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(1ms);
    require(server.echo.calls == 2, "send borrowed the caller's address");
    client.shutdown();
}

void callbackDrain(bool destroy) {
    Server server;
    auto client = std::make_unique<mooncake::RpcClient>(2s, 2s);
    std::promise<void> entered, release, stopping, exited;
    auto gate = release.get_future().share();
    std::atomic<bool> finished{false};
    client->callAsync<&Echo::echo>(server.address, std::string("drain"),
        [&](auto) {
            entered.set_value();
            gate.wait();
            finished = true;
            exited.set_value();
        });
    auto ready = entered.get_future();
    require(ready.wait_for(5s) == std::future_status::ready, "callback did not start");
    auto stopped = std::async(std::launch::async, [&] {
        stopping.set_value();
        if (destroy) client.reset();
        else client->shutdown();
        return finished.load();
    });
    stopping.get_future().wait();
    bool waited = stopped.wait_for(100ms) == std::future_status::timeout;
    release.set_value();
    bool completed = stopped.get();
    exited.get_future().wait();
    require(waited && completed, "shutdown returned while a callback was running");
    if (client) {
        int calls = server.echo.calls.load();
        client->send<&Echo::echo>(server.address, std::string("after shutdown"));
        client->shutdown();
        require(server.echo.calls == calls, "shutdown accepted another send");
    }
}

void peerClose() {
    for (int round = 0; round < 64; ++round) {
        Server server;
        mooncake::RpcClient client(300ms, 300ms);
        std::promise<bool> warm;
        client.callAsync<&Echo::echo>(server.address, std::string("warm"),
            [&](auto result) { warm.set_value(result && *result == "warm"); });
        auto ready = warm.get_future();
        require(ready.wait_for(5s) == std::future_status::ready && ready.get(),
                "warmup RPC failed");
        std::thread closer([&] {
            std::this_thread::sleep_for(std::chrono::microseconds(1000 + round));
            server.rpc.shutdown();
        });
        for (int i = 0; i < 64; ++i) {
            client.callAsync<&Echo::echo>(server.address, std::string(256, 'x'),
                                         [](auto) {});
            // Let one write finish before starting the next on another I/O
            // thread, with earlier replies and remote close still in flight.
            std::this_thread::sleep_for(100us);
        }
        closer.join();
        client.shutdown();
    }
}

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    easylog::logger<>::instance().set_min_severity(easylog::Severity::CRITICAL);
    coro_io::get_global_executor(4);
    try {
        require(argc == 2, "expected a test name");
        std::string test = argv[1];
        if (test == "address") addressOwnership();
        else if (test == "drain") callbackDrain(false);
        else if (test == "destroy") callbackDrain(true);
        else if (test == "peer-close") peerClose();
        else require(false, "unknown test");
        coro_io::g_io_context_pool().stop();
        std::cout << "PASS " << test << std::endl;
    } catch (const std::exception& error) {
        std::cerr << error.what() << std::endl;
        return 1;
    }
}
"""
        )
        result = subprocess.run(
            [
                *compiler,
                *flags,
                str(source),
                str(root / "src/control_plane/rpc_runtime.cpp"),
                "-lglog",
                "-o",
                str(cls.binary),
            ],
            capture_output=True,
            text=True,
            timeout=240,
        )
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)

    def _run_native(self, case: str) -> None:
        result = subprocess.run(
            [str(self.binary), case], capture_output=True, text=True, timeout=60
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"PASS {case}", result.stdout)

    def test_async_calls_own_address(self) -> None:
        self._run_native("address")

    def test_shutdown_drains_callback(self) -> None:
        self._run_native("drain")

    def test_destructor_drains_callback(self) -> None:
        self._run_native("destroy")

    def test_peer_close_during_async_sends(self) -> None:
        self._run_native("peer-close")


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
