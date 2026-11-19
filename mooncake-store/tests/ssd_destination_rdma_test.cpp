// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_transfer_target.h"
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <array>
#include <cstring>

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void Install(TransferEngine& engine,
                        std::shared_ptr<Transport> transport, uint64_t local,
                        size_t local_bytes, uint64_t remote,
                        size_t remote_bytes, bool remote_writable) {
        engine.impl_->multi_transports_->transport_map_.clear();
        engine.impl_->multi_transports_->transport_map_.emplace(
            "rdma", std::move(transport));
        auto metadata = engine.impl_->getMetadata();
        auto self = metadata->getSegmentDescByID(LOCAL_SEGMENT_ID);
        if (!self) {
            self = std::make_shared<TransferMetadata::SegmentDesc>();
            self->name = engine.getLocalIpAndPort();
            auto held = self;
            metadata->addLocalSegment(LOCAL_SEGMENT_ID, self->name,
                                      std::move(held));
        }
        self->protocol = "rdma";
        TransferMetadata::BufferDesc source{};
        source.addr = local;
        source.length = local_bytes;
        source.name = "cpu:0";
        source.lkey = {11};
        source.rkey = {12};
        self->buffers = {source};
        auto peer = std::make_shared<TransferMetadata::SegmentDesc>();
        peer->name = "remote-rdma";
        peer->protocol = "rdma";
        TransferMetadata::BufferDesc destination{};
        destination.addr = remote;
        destination.length = remote_bytes;
        destination.name = "cpu:0";
        destination.lkey = {21};
        if (remote_writable) destination.rkey = {22};
        peer->buffers = {destination};
        metadata->addLocalSegment(12, "remote-rdma", std::move(peer));
    }
};
namespace ssd_offload {
namespace {
class RdmaCompletion final : public Transport {
   public:
    size_t submitted{0};
    bool fail{false}, ack_requested{false};
    bool supportsGroupedScatter() const override { return true; }
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("grouped submission only");
    }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        for (auto* task : tasks) {
            task->slice_count = task->request_count;
            for (size_t i = 0; i < task->request_count; ++i) {
                const auto& request = task->request[i];
                ++submitted;
                ack_requested |= request.require_tcp_write_ack;
                auto* slice = new Slice{};
                slice->task = task;
                slice->length = request.length;
                slice->status = Slice::PENDING;
                task->slice_list.push_back(slice);
                if (fail)
                    slice->markFailed();
                else {
                    if (request.opcode == TransferRequest::WRITE)
                        std::memcpy(
                            reinterpret_cast<void*>(request.target_offset),
                            request.source, request.length);
                    else
                        std::memcpy(
                            request.source,
                            reinterpret_cast<void*>(request.target_offset),
                            request.length);
                    slice->markSuccess();
                }
            }
            task->is_finished = true;
        }
        return Status::OK();
    }
    Status getTransferStatus(BatchID batch, size_t task_id,
                             TransferStatus& status) override {
        const auto& task = toBatchDesc(batch).task_list[task_id];
        status.s =
            fail ? TransferStatusEnum::FAILED : TransferStatusEnum::COMPLETED;
        status.transferred_bytes = task.total_bytes;
        return Status::OK();
    }
    Status getTaskDrainStatus(BatchID batch, size_t task_id,
                              bool& drained) override {
        // This CPU fixture copies synchronously, or rejects before any copy.
        // Match real RDMA's local WC barrier explicitly; the generic default
        // deliberately cannot prove a failed asynchronous transport drained.
        const auto& task = toBatchDesc(batch).task_list[task_id];
        drained =
            __atomic_load_n(&task.success_slice_count, __ATOMIC_ACQUIRE) +
                __atomic_load_n(&task.failed_slice_count, __ATOMIC_ACQUIRE) ==
            __atomic_load_n(&task.slice_count, __ATOMIC_ACQUIRE);
        return Status::OK();
    }
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "rdma"; }
};
class SsdRdmaDestinationTest : public ::testing::Test {
   protected:
    std::shared_ptr<TransferEngine> engine;
    std::shared_ptr<RdmaCompletion> transport =
        std::make_shared<RdmaCompletion>();
    std::array<std::byte, 64> slot{}, destination{};
    void SetUp() override {
        unsetenv("MC_USE_TENT");
        unsetenv("MC_USE_TEV1");
        engine = std::make_shared<TransferEngine>(false);
        ASSERT_EQ(engine->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
        for (size_t i = 0; i < slot.size(); ++i) slot[i] = std::byte(i);
        destination.fill(std::byte{0xA5});
        Install(true);
    }
    void Install(bool writable) {
        TransferEngineImplTestPeer::Install(
            *engine, transport, reinterpret_cast<uint64_t>(slot.data()),
            slot.size(), reinterpret_cast<uint64_t>(destination.data()),
            destination.size(), writable);
    }
    ResolvedDestination View() {
        return {"remote-rdma", reinterpret_cast<uint64_t>(destination.data()),
                destination.size(), DestinationMemory::kHost};
    }
    ReadChunk Chunk() {
        return {0, 0, 32, 0, 32, {{0, {1, 1, 9, 16}}, {16, {1, 1, 32, 16}}}};
    }
};
TEST_F(SsdRdmaDestinationTest,
       WritesThroughProductionTargetAndReadsThroughOwnedScatter) {
    auto made = PrepareSsdRdmaWriteTarget(engine, {View()});
    ASSERT_EQ(made.error, DestinationError::kOk);
    ASSERT_TRUE(made.target);
    EXPECT_EQ(made.target->WriteAndDrain(Chunk(), {slot.data(), slot.size()}),
              Error::kOk);
    EXPECT_TRUE(made.target->DestinationsDrained());
    EXPECT_EQ(std::memcmp(destination.data() + 9, slot.data(), 16), 0);
    EXPECT_EQ(std::memcmp(destination.data() + 32, slot.data() + 16, 16), 0);
    EXPECT_EQ(destination[8], std::byte{0xA5});
    EXPECT_TRUE(transport->ack_requested);
    slot.fill(std::byte{});
    const std::array<size_t, 2> local_offsets{0, 16}, remote_offsets{9, 32},
        lengths{16, 16};
    auto read =
        engine->submitScatter({{TransferRequest::READ,
                                "remote-rdma",
                                reinterpret_cast<uint64_t>(destination.data()),
                                destination.size(),
                                slot.data(),
                                slot.size(),
                                local_offsets,
                                remote_offsets,
                                lengths,
                                {}}});
    ASSERT_TRUE(read.wait().ok());
    for (size_t i = 0; i < 32; ++i) EXPECT_EQ(slot[i], std::byte(i));
}
TEST_F(SsdRdmaDestinationTest,
       DescriptorSwitchToLegacyTcpCannotFabricateDrain) {
    auto peer = std::make_shared<TransferEngine>(false);
    ASSERT_EQ(peer->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    ASSERT_NE(peer->installTransport("tcp", nullptr), nullptr);
    ASSERT_EQ(peer->registerLocalMemory(destination.data(), destination.size(),
                                        "cpu:0", true, true),
              0);
    ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
    ASSERT_EQ(engine->registerLocalMemory(slot.data(), slot.size(), "cpu:0",
                                          false, true),
              0);
    auto self = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    self->buffers[0].lkey = {11};
    auto metadata = engine->getMetadata();
    auto descriptor = std::make_shared<TransferMetadata::SegmentDesc>(
        *peer->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID));
    descriptor->protocol = "rdma";
    descriptor->buffers[0].rkey = {22};
    auto current = descriptor;
    metadata->addLocalSegment(13, peer->getLocalIpAndPort(),
                              std::move(descriptor));
    auto view = View();
    view.transport_endpoint = peer->getLocalIpAndPort();
    auto made = PrepareSsdRdmaWriteTarget(engine, {view});
    ASSERT_EQ(made.error, DestinationError::kOk);
    current->protocol = "tcp";
    current->tcp_proto_version = 1;
    setenv("MC_TCP_PROTO", "1", 1);
    const auto result =
        made.target->WriteAndDrain(Chunk(), {slot.data(), slot.size()});
    unsetenv("MC_TCP_PROTO");
    EXPECT_NE(result, Error::kOk);
    for (auto value : destination) EXPECT_EQ(value, std::byte{0xA5});
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data(), true), 0);
}
TEST_F(SsdRdmaDestinationTest,
       RefreshesSameAddressWhenWritableKeysAppearAfterCachedRegistration) {
    auto peer = std::make_shared<TransferEngine>(false);
    ASSERT_EQ(peer->init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    ASSERT_NE(peer->installTransport("tcp", nullptr), nullptr);
    ASSERT_EQ(peer->registerLocalMemory(destination.data(), destination.size(),
                                        "cpu:0", true, true),
              0);
    auto real = peer->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
    real->protocol = "rdma";
    real->buffers[0].lkey = {21};
    real->buffers[0].rkey = {22};
    real->devices.resize(1);
    real->devices[0].name = "fake-hca";
    real->devices[0].gid = "::1";
    auto cached = std::make_shared<TransferMetadata::SegmentDesc>(*real);
    cached->buffers[0].rkey.clear();
    engine->getMetadata()->addLocalSegment(13, peer->getLocalIpAndPort(),
                                           std::move(cached));
    auto view = View();
    view.transport_endpoint = peer->getLocalIpAndPort();
    auto made = PrepareSsdRdmaWriteTarget(engine, {view});
    EXPECT_EQ(made.error, DestinationError::kOk);
    EXPECT_TRUE(made.target);
    EXPECT_EQ(transport->submitted, 0u);
    EXPECT_EQ(peer->unregisterLocalMemory(destination.data(), true), 0);
}
TEST_F(SsdRdmaDestinationTest, RejectsMissingRkeyBeforeAnySubmission) {
    Install(false);
    auto made = PrepareSsdRdmaWriteTarget(engine, {View()});
    EXPECT_EQ(made.error, DestinationError::kInvalidArgument);
    EXPECT_FALSE(made.target);
    EXPECT_EQ(transport->submitted, 0u);
}
TEST_F(SsdRdmaDestinationTest, ChecksWholeChunkBeforeWritingAnyFragment) {
    auto made = PrepareSsdRdmaWriteTarget(engine, {View()});
    ASSERT_TRUE(made.target);
    auto chunk = Chunk();
    chunk.fragments.back().destination.offset = 63;
    EXPECT_EQ(made.target->WriteAndDrain(chunk, {slot.data(), slot.size()}),
              Error::kInvalidArgument);
    EXPECT_EQ(transport->submitted, 0u);
    for (auto byte : destination) EXPECT_EQ(byte, std::byte{0xA5});
}
TEST_F(SsdRdmaDestinationTest, CompletionErrorNeverProvesRemoteDrain) {
    auto made = PrepareSsdRdmaWriteTarget(engine, {View()});
    ASSERT_TRUE(made.target);
    transport->fail = true;
    EXPECT_EQ(made.target->WriteAndDrain(Chunk(), {slot.data(), slot.size()}),
              Error::kTransferError);
    EXPECT_FALSE(made.target->DestinationsDrained());
}
}  // namespace
}  // namespace ssd_offload
}  // namespace mooncake
