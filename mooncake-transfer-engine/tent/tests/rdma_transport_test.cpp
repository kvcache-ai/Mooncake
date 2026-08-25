// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <infiniband/verbs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/transfer_engine.h"
#include "tent/runtime/topology.h"
#include "tent/transport/rdma/context.h"
#include "tent/transport/rdma/params.h"
#include "tent/transport/rdma/quota.h"
#include "tent/transport/rdma/rdma_transport.h"
#include "tent/transport/rdma/workers.h"

namespace mooncake {
namespace tent {

// Friend accessor for driving initializeContexts() without a full install().
class RdmaTransportTestPeer {
   public:
    static void bindTopology(RdmaTransport& transport,
                             std::shared_ptr<Topology> topology) {
        transport.local_topology_ = topology;
        transport.local_buffer_manager_.setTopology(topology);
        transport.params_ = std::make_shared<RdmaParams>();
        transport.conf_ = std::make_shared<Config>();
    }

    // Runs the monitorThread() 1 Hz reclaim tick without starting any worker
    // threads.
    static void reclaimEndpoints(RdmaTransport& transport) {
        Workers workers(&transport);
        workers.reclaimEndpoints();
    }

    static size_t initializeContexts(RdmaTransport& transport) {
        return transport.initializeContexts();
    }

    static const RdmaContextSet& contextSet(const RdmaTransport& transport) {
        return transport.context_set_;
    }
};

namespace {

bool hasRdmaDevice() {
    int count = 0;
    ibv_device** devices = ibv_get_device_list(&count);
    const bool available = devices != nullptr && count > 0;
    if (devices) ibv_free_device_list(devices);
    return available;
}

class ChildProcessGuard {
   public:
    ChildProcessGuard(pid_t pid, int stop_fd) : pid_(pid), stop_fd_(stop_fd) {}

    ~ChildProcessGuard() {
        if (pid_ <= 0) return;
        close(stop_fd_);
        (void)waitpid(pid_, nullptr, 0);
    }

    int finish() {
        close(stop_fd_);
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

    int reap() {
        int status = 0;
        (void)waitpid(pid_, &status, 0);
        close(stop_fd_);
        pid_ = -1;
        stop_fd_ = -1;
        return status;
    }

   private:
    pid_t pid_;
    int stop_fd_;
};

std::shared_ptr<Config> makeRdmaConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "P2PHANDSHAKE");
    config->set("transports/rdma/enable", true);
    config->set("transports/rdma/num_lanes", 1);
    config->set("transports/rdma/endpoint/max_qp_wr", 1);
    config->set("transports/tcp/enable", false);
    config->set("transports/shm/enable", false);
    return config;
}

std::shared_ptr<Topology> topologyWithRdmaNics(size_t count) {
    auto topology = std::make_shared<Topology>();
    for (size_t i = 0; i < count; ++i) {
        Topology::NicEntry nic;
        nic.name = "mlx5_" + std::to_string(i);
        nic.type = Topology::NIC_RDMA;
        nic.numa_node = 0;
        topology->nic_list_.push_back(std::move(nic));
    }

    Topology::MemEntry memory;
    memory.name = "cpu:0";
    memory.type = Topology::MEM_HOST;
    memory.numa_node = 0;
    for (size_t i = 0; i < count; ++i) {
        memory.device_list[0].push_back(static_cast<int>(i));
    }
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

bool waitBatchDone(TransferEngine& engine, BatchID batch) {
    TransferStatus status;
    for (int i = 0; i < 10000; ++i) {
        auto result = engine.getTransferStatus(batch, status);
        if (!result.ok() || status.s == TransferStatusEnum::FAILED)
            return false;
        if (status.s == TransferStatusEnum::COMPLETED) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    return false;
}

TEST(RdmaParamsTest, DefaultsKeepLaneCountsAligned) {
    RdmaParams params;

    EXPECT_EQ(params.num_lanes, 6);
    EXPECT_EQ(params.device.num_cq_list, params.num_lanes);
    EXPECT_EQ(params.endpoint.qp_mul_factor, params.num_lanes);
    EXPECT_EQ(params.workers.num_workers, params.num_lanes);
    EXPECT_EQ(params.endpoint.path_mtu, IBV_MTU_4096);
}

TEST(RdmaSubBatchTest, ReportsTaskCount) {
    RdmaSubBatch batch;
    batch.max_size = 8;

    EXPECT_EQ(batch.size(), 0);
    batch.task_list.push_back(nullptr);
    batch.task_list.push_back(nullptr);
    EXPECT_EQ(batch.size(), 2);
}

TEST(DeviceSelectorTest, PerSliceAllocationHonorsPolicy) {
    auto topology = topologyWithRdmaNics(3);

    {
        DeviceSelector selector;
        ASSERT_TRUE(selector.loadTopology(topology).ok());
        int chosen_device = -1;
        ASSERT_TRUE(selector.allocate(4096, "cpu:0", chosen_device,
                                       PRIO_HIGH, 1ULL << 1)
                        .ok());
        EXPECT_EQ(chosen_device, 1);
    }

    {
        DeviceSelector selector;
        ASSERT_TRUE(selector.loadTopology(topology).ok());
        auto params = selector.getSchedulingParams();
        params.device_base_priorities = {0, 1, 2};
        params.local_rotation_interval_us = 0;
        params.numa_tier_weights[0] = 1.0;
        params.numa_tier_weights[1] = 1.0;
        params.numa_tier_weights[2] = 1.0;
        params.score_jitter_range = 0.0;
        selector.setSchedulingParams(params);

        int chosen_device = -1;
        ASSERT_TRUE(selector.allocate(4096, "cpu:0", chosen_device,
                                       PRIO_LOW, (1ULL << 1) | (1ULL << 2))
                        .ok());
        EXPECT_EQ(chosen_device, 2);
    }
}

// context_set_ is subscripted by NicID, so it must keep one slot per NIC even
// when a device is skipped. It used to push_back only on the success path,
// compacting the array so a later dev_id named the wrong RNIC or ran off it.
void expectInertContextPerNic(const RdmaContextSet& contexts, size_t expected) {
    ASSERT_EQ(contexts.size(), expected);
    for (size_t i = 0; i < contexts.size(); ++i) {
        ASSERT_NE(contexts[i], nullptr) << "slot " << i << " must be occupied";
        EXPECT_NE(contexts[i]->status(), RdmaContext::DEVICE_ENABLED)
            << "slot " << i << " must not report itself usable";
        // Inert contexts must stay safe for the whole-list consumers.
        EXPECT_EQ(contexts[i]->cq(0), nullptr);
        EXPECT_EQ(contexts[i]->notifyCq(), nullptr);
        EXPECT_LT(contexts[i]->eventFd(), 0);
        EXPECT_EQ(contexts[i]->nativeContext(), nullptr);
        // construct() sets device_name_ before it can fail, so an empty name
        // is what tells a fresh placeholder from a kept half-built context.
        EXPECT_TRUE(contexts[i]->name().empty())
            << "slot " << i << " kept a half-built context for "
            << contexts[i]->name();
    }
}

// Non-RDMA entries are skipped before construct() is ever called, so this runs
// without libibverbs devices.
TEST(RdmaNicIndexAlignmentTest, ContextSetKeepsOneSlotPerNonRdmaNic) {
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-unknown-1","type":2,"numa_node":0},
                        {"name":"mc-tcp-2","type":1,"numa_node":0}]})")
                    .ok());
    ASSERT_EQ(topology->getNicCount(), static_cast<size_t>(3));

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);

    EXPECT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));
    expectInertContextPerNic(RdmaTransportTestPeer::contextSet(transport),
                             topology->getNicCount());
}

// monitorThread()'s 1 Hz tick walks every slot. An inert context never built
// an endpoint store, so an unguarded endpointStore()->reclaim() would crash --
// and only on the first heartbeat, well after startup.
TEST(RdmaNicIndexAlignmentTest, ReclaimTickSkipsInertContexts) {
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-tcp-0","type":1,"numa_node":0},
                        {"name":"mc-unknown-1","type":2,"numa_node":0}]})")
                    .ok());

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);
    ASSERT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));

    // Precondition that makes the unguarded call fatal.
    for (const auto& context : RdmaTransportTestPeer::contextSet(transport)) {
        ASSERT_EQ(context->endpointStore(), nullptr);
    }

    RdmaTransportTestPeer::reclaimEndpoints(transport);
}

// The construct()-failure branch. Runs on any host: this binary links
// libibverbs directly, so IbvLoader's dlclose does not unmap it.
TEST(RdmaNicIndexAlignmentTest, ContextSetKeepsOneSlotWhenConstructFails) {
    // Device names that resolve to no real RNIC, so construct() fails on any
    // host, with a non-RDMA entry in the middle to offset the indexes.
    auto topology = std::make_shared<Topology>();
    ASSERT_TRUE(topology
                    ->parse(R"({"nics":[
                        {"name":"mc-absent-rnic-0","type":0,"numa_node":0},
                        {"name":"mc-tcp-1","type":1,"numa_node":0},
                        {"name":"mc-absent-rnic-2","type":0,"numa_node":0}]})")
                    .ok());
    ASSERT_EQ(topology->getNicCount(), static_cast<size_t>(3));

    RdmaTransport transport;
    RdmaTransportTestPeer::bindTopology(transport, topology);

    EXPECT_EQ(RdmaTransportTestPeer::initializeContexts(transport),
              static_cast<size_t>(0));
    expectInertContextPerNic(RdmaTransportTestPeer::contextSet(transport),
                             topology->getNicCount());
}

TEST(RdmaTransportIntegrationTest, WriteThenReadAcrossProcesses) {
    if (!hasRdmaDevice()) GTEST_SKIP() << "no RDMA device detected";

    constexpr size_t kDataLength = 4 * 1024 * 1024;
    constexpr size_t kCancelTaskCount = 16;
    constexpr size_t kCancelStride = 8 * 1024 * 1024;
    constexpr size_t kBufferLength = kCancelTaskCount * kCancelStride;
    int ready_pipe[2];
    int stop_pipe[2];
    ASSERT_EQ(pipe(ready_pipe), 0);
    ASSERT_EQ(pipe(stop_pipe), 0);

    pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        close(ready_pipe[0]);
        close(stop_pipe[1]);

        TransferEngine server(makeRdmaConfig());
        if (!server.available()) _exit(2);
        std::vector<uint8_t> buffer(kBufferLength);
        if (!server.registerLocalMemory(buffer.data(), buffer.size()).ok())
            _exit(3);

        const std::string segment = server.getSegmentName();
        uint32_t length = static_cast<uint32_t>(segment.size());
        if (write(ready_pipe[1], &length, sizeof(length)) != sizeof(length))
            _exit(4);
        if (write(ready_pipe[1], segment.data(), length) !=
            static_cast<ssize_t>(length))
            _exit(5);

        char stop = 0;
        const ssize_t stop_result = read(stop_pipe[0], &stop, 1);
        (void)stop_result;
        (void)server.unregisterLocalMemory(buffer.data(), buffer.size());
        _exit(0);
    }

    close(ready_pipe[1]);
    close(stop_pipe[0]);
    ChildProcessGuard child_guard(child, stop_pipe[1]);

    uint32_t segment_length = 0;
    ssize_t received =
        read(ready_pipe[0], &segment_length, sizeof(segment_length));
    if (received != static_cast<ssize_t>(sizeof(segment_length))) {
        const int status = child_guard.reap();
        GTEST_SKIP() << "RDMA server initialization failed, child status "
                     << status;
    }
    std::string server_segment(segment_length, '\0');
    ASSERT_EQ(read(ready_pipe[0], server_segment.data(), segment_length),
              static_cast<ssize_t>(segment_length));

    TransferEngine client(makeRdmaConfig());
    ASSERT_TRUE(client.available());
    std::vector<uint8_t> buffer(kBufferLength);
    for (size_t i = 0; i < kDataLength; ++i) {
        buffer[i] = static_cast<uint8_t>((i * 31) & 0xff);
    }
    ASSERT_TRUE(client.registerLocalMemory(buffer.data(), buffer.size()).ok());

    SegmentID segment = 0;
    Status result;
    for (int i = 0; i < 100; ++i) {
        result = client.openSegment(segment, server_segment);
        if (result.ok()) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(result.ok()) << result.ToString();

    SegmentInfo info;
    ASSERT_TRUE(client.getSegmentInfo(segment, info).ok());
    ASSERT_FALSE(info.buffers.empty());

    Request request{};
    request.opcode = Request::WRITE;
    request.source = buffer.data();
    request.target_id = segment;
    request.target_offset = info.buffers[0].base;
    request.length = kDataLength;
    request.transport_hint = RDMA;

    BatchID batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());

    request.opcode = Request::READ;
    request.source = buffer.data() + kDataLength;
    batch = client.allocateBatch(1);
    ASSERT_TRUE(client.submitTransfer(batch, {request}).ok());
    ASSERT_TRUE(waitBatchDone(client, batch));
    ASSERT_TRUE(client.freeBatch(batch).ok());
    EXPECT_EQ(
        std::memcmp(buffer.data(), buffer.data() + kDataLength, kDataLength),
        0);

    // Keep one QP/worker and one outstanding WR so the tail task remains in
    // the worker's unposted set long enough to exercise real cancellation.
    std::vector<Request> cancel_requests;
    cancel_requests.reserve(kCancelTaskCount);
    for (size_t i = 0; i < kCancelTaskCount; ++i) {
        Request cancel_request{};
        cancel_request.opcode = Request::WRITE;
        cancel_request.source = buffer.data() + i * kCancelStride;
        cancel_request.target_id = segment;
        cancel_request.target_offset = info.buffers[0].base + i * kCancelStride;
        cancel_request.length = kDataLength;
        cancel_request.transport_hint = RDMA;
        cancel_requests.push_back(cancel_request);
    }

    batch = client.allocateBatch(kCancelTaskCount);
    ASSERT_TRUE(client.submitTransfer(batch, cancel_requests).ok());
    const size_t cancel_task_id = kCancelTaskCount - 1;
    ASSERT_TRUE(client.cancelTransfer(batch, cancel_task_id).ok());
    ASSERT_TRUE(client.cancelTransfer(batch, cancel_task_id).ok());

    std::vector<TransferStatus> statuses;
    for (int poll = 0; poll < 10000; ++poll) {
        ASSERT_TRUE(client.getTransferStatus(batch, statuses).ok());
        if (std::all_of(statuses.begin(), statuses.end(),
                        [](const TransferStatus& task_status) {
                            return task_status.s != TransferStatusEnum::PENDING;
                        }))
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(statuses.size(), kCancelTaskCount);
    for (size_t i = 0; i < cancel_task_id; ++i) {
        EXPECT_EQ(statuses[i].s, TransferStatusEnum::COMPLETED);
    }
    EXPECT_EQ(statuses[cancel_task_id].s, TransferStatusEnum::CANCELED);
    EXPECT_LE(statuses[cancel_task_id].transferred_bytes, kDataLength);
    ASSERT_TRUE(client.freeBatch(batch).ok());

    EXPECT_TRUE(client.closeSegment(segment).ok());
    EXPECT_TRUE(
        client.unregisterLocalMemory(buffer.data(), buffer.size()).ok());

    const int status = child_guard.finish();
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
