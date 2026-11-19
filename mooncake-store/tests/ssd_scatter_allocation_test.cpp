// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated test executable: inject one caller-thread error-message allocation
// failure after a fake transport publishes one of two grouped fragments.
#include "transfer_engine.h"
#include "transfer_engine_impl.h"
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <array>
#include <cstring>
#include <cstdlib>
#include <future>
#include <new>

namespace {
thread_local bool fail_message = false, message_failure_fired = false;
}
void* operator new(size_t bytes) {
    if (fail_message && bytes == 25) {
        fail_message = false;
        message_failure_fired = true;
        throw std::bad_alloc();
    }
    if (void* pointer = std::malloc(bytes ? bytes : 1)) return pointer;
    throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new[](size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new(size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    static void Install(TransferEngine& engine,
                        std::shared_ptr<Transport> transport) {
        engine.impl_->multi_transports_->transport_map_.clear();
        engine.impl_->multi_transports_->transport_map_.emplace(
            "throwing", std::move(transport));
        auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
        desc->name = "remote";
        desc->protocol = "throwing";
        engine.impl_->getMetadata()->addLocalSegment(12, "remote",
                                                     std::move(desc));
    }
};
class PartialThrowTransport final : public Transport {
   public:
    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::NotImplemented("use grouped submission");
    }
    bool supportsGroupedScatter() const override { return true; }
    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        task_ = tasks.front();
        auto* slice = new Slice{};
        slice->task = task_;
        slice->length = task_->request[0].length;
        slice->status = Slice::PENDING;
        task_->slice_list.push_back(slice);
        task_->slice_count = 1;
        entered.set_value();
        fail_message = true;
        throw std::runtime_error("throw-after-publishing-one-fragment");
    }
    Status getTransferStatus(BatchID, size_t, TransferStatus& status) override {
        status.s = finished.load(std::memory_order_acquire)
                       ? TransferStatusEnum::COMPLETED
                       : TransferStatusEnum::WAITING;
        return Status::OK();
    }
    void Finish() {
        const auto& request = task_->request[0];
        std::memcpy(reinterpret_cast<void*>(request.target_offset),
                    request.source, request.length);
        task_->slice_list[0]->markSuccess();
        task_->is_finished = true;
        finished.store(true, std::memory_order_release);
    }
    std::promise<void> entered;

   private:
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
    const char* getName() const override { return "throwing"; }
    TransferTask* task_{};
    std::atomic<bool> finished{false};
};
class SsdScatterAllocation : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ssd_scatter_allocation_test");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};
TEST_F(SsdScatterAllocation, ErrorMessageFailureNeverPublishesUnsentFragments) {
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "127.0.0.1:0"), 0);
    auto transport = std::make_shared<PartialThrowTransport>();
    TransferEngineImplTestPeer::Install(engine, transport);
    std::array<char, 32> source, destination{};
    source.fill('s');
    std::array<size_t, 2> offsets{0, 16}, lengths{16, 16};
    std::vector<bool> publishable;
    TransferEngine::ScatterTransferRange range{
        TransferRequest::WRITE,
        "remote",
        reinterpret_cast<uint64_t>(destination.data()),
        destination.size(),
        source.data(),
        source.size(),
        offsets,
        offsets,
        lengths,
        [&](size_t, const Status& status) {
            publishable.push_back(status.ok());
        }};
    auto entered = transport->entered.get_future();
    auto work = std::async(std::launch::async, [&] {
        bool threw = false;
        try {
            auto operation = engine.submitScatter({range});
            operation.wait();
        } catch (const std::bad_alloc&) {
            threw = true;
        }
        fail_message = false;
        return threw && message_failure_fired;
    });
    const auto ready = entered.wait_for(std::chrono::seconds(2));
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready == std::future_status::ready) transport->Finish();
    EXPECT_TRUE(work.get())
        << "Error message allocation fault must fire after publication";
    EXPECT_EQ(publishable, (std::vector<bool>{false, false}));
    EXPECT_EQ(destination[0], 's');
    EXPECT_EQ(destination[16], 0);
}
}  // namespace mooncake
