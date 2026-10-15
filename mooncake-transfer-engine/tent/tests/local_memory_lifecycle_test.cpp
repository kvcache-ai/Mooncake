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

#include <algorithm>
#include <array>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/common/config.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"

namespace mooncake {
namespace tent {
namespace {

class FailOnceFreeTransport : public Transport {
   public:
    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        return *addr ? Status::OK()
                     : Status::InternalError("malloc failed" LOC_MARK);
    }

    Status freeLocalMemory(void* addr, size_t) override {
        ++free_calls;
        if (free_calls == 1) {
            return Status::InternalError("injected free failure" LOC_MARK);
        }
        std::free(addr);
        return Status::OK();
    }

    const char* getName() const override { return "fail-once-free"; }

    int free_calls{0};
};

std::shared_ptr<Config> makeConfig() {
    auto config = std::make_shared<Config>();
    config->set("metadata_type", "p2p");
    config->set("metadata_servers", "");
    config->set("rpc_server_hostname", "127.0.0.1");
    config->set("rpc_server_port", "0");
    config->set("log_level", "warning");

    for (const char* transport : {"tcp", "shm", "rdma", "io_uring", "nvlink",
                                  "mnnvl", "gds", "ascend_direct"}) {
        config->set(std::string("transports/") + transport + "/enable", false);
    }
    return config;
}

// Keep failures in locking code from hanging the entire test binary. Worker
// functions own their context: a timed-out worker may safely outlive the test.
class ConcurrentCall {
   public:
    template <typename Function>
    explicit ConcurrentCall(Function function) {
        std::packaged_task<Status()> task(std::move(function));
        result_ = task.get_future();
        worker_ = std::thread(std::move(task));
    }

    ~ConcurrentCall() {
        if (!result_.valid() || result_.wait_for(std::chrono::seconds(0)) ==
                                    std::future_status::ready) {
            worker_.join();
        } else {
            worker_.detach();
        }
    }

    Status get() {
        // The transport gate opens after five seconds even when the engine
        // correctly prevents another callback from entering it.
        if (result_.wait_for(std::chrono::seconds(6)) !=
            std::future_status::ready) {
            ADD_FAILURE() << "Concurrent memory operation did not finish";
            return Status::InternalError("memory operation timed out" LOC_MARK);
        }
        return result_.get();
    }

    bool ready() const {
        return !result_.valid() || result_.wait_for(std::chrono::seconds(0)) ==
                                       std::future_status::ready;
    }

   private:
    std::future<Status> result_;
    std::thread worker_;
};

class ControlledRegistrationTransport : public Transport {
   public:
    enum class Operation { Add, Remove };

    struct Counts {
        size_t adds;
        size_t removes;
        size_t registered;
        bool overlapping_callbacks;
        bool gate_released_by_peer;
    };

    void pauseNext(Operation operation, bool fail = false) {
        std::lock_guard<std::mutex> lock(mutex_);
        operation_ = operation;
        gate_armed_ = true;
        gate_entered_ = false;
        gate_released_by_peer_ = false;
        fail_gated_add_ = fail;
    }

    bool waitForGate() {
        std::unique_lock<std::mutex> lock(mutex_);
        return condition_.wait_for(lock, std::chrono::seconds(5),
                                   [&] { return gate_entered_; });
    }

    Counts counts() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return {adds_, removes_, registered_.size(), overlapping_callbacks_,
                gate_released_by_peer_};
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& buffers,
                           const MemoryOptions&) override {
        if (buffers.empty()) return Status::OK();
        std::unique_lock<std::mutex> lock(mutex_);
        std::vector<uint64_t> bases;
        for (const auto& buffer : buffers) bases.push_back(buffer.addr);
        adds_ += buffers.size();
        const bool fail = enter(Operation::Add, bases, lock);
        Status status = Status::OK();
        if (fail) {
            status =
                Status::InternalError("injected registration failure" LOC_MARK);
        } else {
            // Model HP TCP's rejection of an already registered address.
            // Validate the whole batch before accepting any entry.
            for (const auto& buffer : buffers) {
                if (registered_.count(buffer.addr)) {
                    status = Status::InvalidArgument(
                        "duplicate registration" LOC_MARK);
                    break;
                }
            }
            if (status.ok()) {
                for (auto& buffer : buffers) {
                    registered_.emplace(buffer.addr, buffer.length);
                    buffer.transports.push_back(HP_TCP);
                }
            }
        }
        leave(bases);
        return status;
    }

    Status removeMemoryBuffer(BufferDesc& buffer) override {
        std::unique_lock<std::mutex> lock(mutex_);
        ++removes_;
        enter(Operation::Remove, {buffer.addr}, lock);
        const auto removed = registered_.erase(buffer.addr);
        leave({buffer.addr});
        return removed == 1
                   ? Status::OK()
                   : Status::InvalidArgument("registration missing" LOC_MARK);
    }

    const char* getName() const override { return "controlled-registration"; }

   private:
    bool enter(Operation operation, const std::vector<uint64_t>& bases,
               std::unique_lock<std::mutex>& lock) {
        for (auto base : bases) {
            if (active_[base]++ != 0) overlapping_callbacks_ = true;
        }
        const auto entry = ++callback_entries_;
        condition_.notify_all();
        if (!gate_armed_ || operation != operation_) return false;
        gate_armed_ = false;
        gate_entered_ = true;
        condition_.notify_all();
        // Old code admits the second callback and releases this gate. Correct
        // code blocks a conflicting operation until this gate's deadline.
        // An unrelated address must still be able to release it immediately.
        gate_released_by_peer_ =
            condition_.wait_for(lock, std::chrono::seconds(5),
                                [&] { return callback_entries_ > entry; });
        return operation == Operation::Add && fail_gated_add_;
    }

    void leave(const std::vector<uint64_t>& bases) {
        for (auto base : bases) {
            if (--active_[base] == 0) active_.erase(base);
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::map<uint64_t, uint64_t> registered_;
    std::map<uint64_t, size_t> active_;
    Operation operation_{Operation::Add};
    size_t adds_{0};
    size_t removes_{0};
    size_t callback_entries_{0};
    bool gate_armed_{false};
    bool gate_entered_{false};
    bool gate_released_by_peer_{false};
    bool fail_gated_add_{false};
    bool overlapping_callbacks_{false};
};

struct ConcurrentMemoryContext {
    ConcurrentMemoryContext()
        : buffers{std::vector<char>(4096), std::vector<char>(4096)},
          transport(std::make_shared<ControlledRegistrationTransport>()),
          engine(makeConfig()) {
        engine.swapTransportForTest(HP_TCP, transport);
    }

    Status add(std::vector<size_t> indices) {
        MemoryOptions options;
        options.type = HP_TCP;
        std::vector<void*> addresses;
        std::vector<size_t> sizes;
        for (auto index : indices) {
            addresses.push_back(buffers[index].data());
            sizes.push_back(buffers[index].size());
        }
        return engine.registerLocalMemory(addresses, sizes, options);
    }

    Status remove(size_t index) {
        return engine.unregisterLocalMemory(buffers[index].data(),
                                            buffers[index].size());
    }

    // Storage and the injected transport outlive the engine, including when
    // an assertion aborts a test while a worker is finishing.
    std::array<std::vector<char>, 2> buffers;
    std::shared_ptr<ControlledRegistrationTransport> transport;
    TransferEngineImpl engine;
};

TEST(LocalMemoryLifecycle,
     ConcurrentFirstRegistrationsShareOneTransportBuffer) {
    auto context = std::make_shared<ConcurrentMemoryContext>();
    ASSERT_TRUE(context->engine.available());
    context->transport->pauseNext(
        ControlledRegistrationTransport::Operation::Add);
    ConcurrentCall first([context] { return context->add({0}); });
    ASSERT_TRUE(context->transport->waitForGate());
    ConcurrentCall second([context] { return context->add({0}); });
    auto first_status = first.get();
    auto second_status = second.get();
    ASSERT_TRUE(first.ready() && second.ready());
    EXPECT_TRUE(first_status.ok()) << first_status.ToString();
    EXPECT_TRUE(second_status.ok()) << second_status.ToString();
    auto counts = context->transport->counts();
    EXPECT_EQ(counts.adds, 1u);
    EXPECT_FALSE(counts.overlapping_callbacks);

    ASSERT_TRUE(context->remove(0).ok());
    EXPECT_EQ(context->transport->counts().removes, 0u);
    ASSERT_TRUE(context->remove(0).ok());
    counts = context->transport->counts();
    EXPECT_EQ(counts.removes, 1u);
    EXPECT_EQ(counts.registered, 0u);
}

TEST(LocalMemoryLifecycle, ConcurrentReregistrationWaitsForFinalRemoval) {
    auto context = std::make_shared<ConcurrentMemoryContext>();
    ASSERT_TRUE(context->engine.available());
    ASSERT_TRUE(context->add({0}).ok());
    context->transport->pauseNext(
        ControlledRegistrationTransport::Operation::Remove);
    ConcurrentCall removal([context] { return context->remove(0); });
    ASSERT_TRUE(context->transport->waitForGate());
    ConcurrentCall registration([context] { return context->add({0}); });
    auto remove_status = removal.get();
    auto add_status = registration.get();
    ASSERT_TRUE(removal.ready() && registration.ready());
    EXPECT_TRUE(remove_status.ok()) << remove_status.ToString();
    EXPECT_TRUE(add_status.ok()) << add_status.ToString();
    auto counts = context->transport->counts();
    EXPECT_EQ(counts.adds, 2u);
    EXPECT_EQ(counts.removes, 1u);
    EXPECT_EQ(counts.registered, 1u);
    EXPECT_FALSE(counts.overlapping_callbacks);
    ASSERT_TRUE(context->remove(0).ok());
    counts = context->transport->counts();
    EXPECT_EQ(counts.removes, 2u);
    EXPECT_EQ(counts.registered, 0u);
}

TEST(LocalMemoryLifecycle, ConcurrentWaiterRetriesAfterRegistrationFailure) {
    auto context = std::make_shared<ConcurrentMemoryContext>();
    ASSERT_TRUE(context->engine.available());
    context->transport->pauseNext(
        ControlledRegistrationTransport::Operation::Add, true);
    ConcurrentCall failing([context] { return context->add({0}); });
    ASSERT_TRUE(context->transport->waitForGate());
    ConcurrentCall waiting([context] { return context->add({0}); });
    auto failed_status = failing.get();
    auto waiting_status = waiting.get();
    ASSERT_TRUE(failing.ready() && waiting.ready());
    EXPECT_FALSE(failed_status.ok());
    EXPECT_TRUE(waiting_status.ok()) << waiting_status.ToString();
    EXPECT_FALSE(context->transport->counts().overlapping_callbacks);
    ASSERT_TRUE(context->add({0}).ok());
    ASSERT_TRUE(context->remove(0).ok());
    EXPECT_EQ(context->transport->counts().removes, 0u);
    ASSERT_TRUE(context->remove(0).ok());
    EXPECT_EQ(context->transport->counts().removes, 1u);
    EXPECT_EQ(context->transport->counts().registered, 0u);
    // Neither the failed attempt nor its waiter may leave a stale reservation.
    ASSERT_TRUE(context->add({0}).ok());
    ASSERT_TRUE(context->remove(0).ok());
    auto counts = context->transport->counts();
    EXPECT_EQ(counts.adds, 3u);
    EXPECT_EQ(counts.removes, 2u);
    EXPECT_EQ(counts.registered, 0u);
}

TEST(LocalMemoryLifecycle, ConcurrentDifferentAddressesRegisterInParallel) {
    auto context = std::make_shared<ConcurrentMemoryContext>();
    ASSERT_TRUE(context->engine.available());
    context->transport->pauseNext(
        ControlledRegistrationTransport::Operation::Add);
    ConcurrentCall first([context] { return context->add({0}); });
    ASSERT_TRUE(context->transport->waitForGate());
    ConcurrentCall second([context] { return context->add({1}); });
    auto first_status = first.get();
    auto second_status = second.get();
    ASSERT_TRUE(first.ready() && second.ready());
    EXPECT_TRUE(first_status.ok()) << first_status.ToString();
    EXPECT_TRUE(second_status.ok()) << second_status.ToString();
    auto counts = context->transport->counts();
    EXPECT_TRUE(counts.gate_released_by_peer)
        << "An unrelated range was blocked behind an in-flight registration";
    EXPECT_FALSE(counts.overlapping_callbacks);
    EXPECT_EQ(counts.adds, 2u);
    ASSERT_TRUE(context->remove(0).ok());
    ASSERT_TRUE(context->remove(1).ok());
    EXPECT_EQ(context->transport->counts().removes, 2u);
    EXPECT_EQ(context->transport->counts().registered, 0u);
}

TEST(LocalMemoryLifecycle, ConcurrentReversedBatchesShareRegistrations) {
    auto context = std::make_shared<ConcurrentMemoryContext>();
    ASSERT_TRUE(context->engine.available());
    context->transport->pauseNext(
        ControlledRegistrationTransport::Operation::Add);
    ConcurrentCall first([context] { return context->add({0, 1}); });
    ASSERT_TRUE(context->transport->waitForGate());
    ConcurrentCall second([context] { return context->add({1, 0}); });
    auto first_status = first.get();
    auto second_status = second.get();
    ASSERT_TRUE(first.ready() && second.ready());
    EXPECT_TRUE(first_status.ok()) << first_status.ToString();
    EXPECT_TRUE(second_status.ok()) << second_status.ToString();
    auto counts = context->transport->counts();
    EXPECT_EQ(counts.adds, 2u);
    EXPECT_FALSE(counts.overlapping_callbacks);
    for (auto index : {1u, 0u}) ASSERT_TRUE(context->remove(index).ok());
    EXPECT_EQ(context->transport->counts().removes, 0u);
    for (auto index : {0u, 1u}) ASSERT_TRUE(context->remove(index).ok());
    EXPECT_EQ(context->transport->counts().removes, 2u);
    EXPECT_EQ(context->transport->counts().registered, 0u);
}

TEST(LocalMemoryLifecycle, ConcurrentReferencesRejectIncompatibleOptions) {
    auto context = std::make_shared<ConcurrentMemoryContext>();
    ASSERT_TRUE(context->engine.available());
    ASSERT_TRUE(context->add({0}).ok());
    MemoryOptions original;
    original.type = HP_TCP;
    std::vector<MemoryOptions> incompatible(6, original);
    incompatible[0].perm = kGlobalReadOnly;
    incompatible[1].type = UNSPEC;
    incompatible[2].location = "cpu:0";
    incompatible[3].shm_path = "/different-mapping";
    incompatible[4].shm_offset = 4096;
    incompatible[5].internal = true;
    for (size_t i = 0; i < incompatible.size(); ++i) {
        SCOPED_TRACE(i);
        auto status = context->engine.registerLocalMemory(
            {context->buffers[0].data()}, {context->buffers[0].size()},
            incompatible[i]);
        EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    }
    EXPECT_EQ(context->transport->counts().adds, 1u);
    ASSERT_TRUE(context->remove(0).ok());
    EXPECT_EQ(context->transport->counts().removes, 1u);
    EXPECT_EQ(context->transport->counts().registered, 0u);
}

TEST(LocalMemoryLifecycle, RetainsOwnershipWhenTransportFreeFails) {
    TransferEngineImpl engine(makeConfig());
    ASSERT_TRUE(engine.available());

    auto transport = std::make_shared<FailOnceFreeTransport>();
    engine.swapTransportForTest(RDMA, transport);

    MemoryOptions options;
    options.type = RDMA;
    options.location = "cpu:0";
    void* addr = nullptr;
    ASSERT_TRUE(engine.allocateLocalMemory(&addr, 4096, options).ok());

    auto first = engine.freeLocalMemory(addr);
    EXPECT_FALSE(first.ok());
    EXPECT_EQ(transport->free_calls, 1);

    auto second = engine.freeLocalMemory(addr);
    EXPECT_TRUE(second.ok()) << second.ToString();
    EXPECT_EQ(transport->free_calls, 2);

    auto third = engine.freeLocalMemory(addr);
    EXPECT_TRUE(third.IsInvalidArgument());
    EXPECT_EQ(transport->free_calls, 2);
}

TEST(LocalMemoryLifecycle, SegmentInfoReplacesPreviouslyReportedBuffers) {
    auto config = makeConfig();
    config->set("transports/tcp/enable", true);
    std::vector<char> buffer(4096);
    TransferEngineImpl engine(config);
    ASSERT_TRUE(engine.available());
    ASSERT_TRUE(engine.registerLocalMemory(buffer.data(), buffer.size()).ok());

    SegmentInfo info;
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    ASSERT_EQ(info.buffers.size(), 1u);
    EXPECT_EQ(info.buffers.front().base,
              reinterpret_cast<uint64_t>(buffer.data()));
    EXPECT_EQ(info.buffers.front().length, buffer.size());

    // Reusing the output must not duplicate buffers from the previous call.
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_EQ(info.buffers.size(), 1u);

    ASSERT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty())
        << "An unregistered buffer must not survive a segment-info refresh";
}

TEST(LocalMemoryLifecycle, HpTcpDuplicateBatchRetainsRegistration) {
    auto config = makeConfig();
    config->set("transports/hp_tcp/enable", true);
    config->set("transports/hp_tcp/bind_address", "127.0.0.1");
    config->set("transports/hp_tcp/advertise_address", "127.0.0.1");
    config->set("transports/hp_tcp/worker_count", 1);
    config->set("transports/hp_tcp/progress_timeout_ms", 1000);
    // Storage outlives both engines, including on an assertion failure.
    std::vector<unsigned char> remote(4096);
    std::vector<unsigned char> local(remote.size());
    TransferEngineImpl target(config);
    TransferEngineImpl initiator(config);
    ASSERT_TRUE(target.available());
    ASSERT_TRUE(initiator.available());

    MemoryOptions options;
    options.type = HP_TCP;
    options.perm = kGlobalReadWrite;
    auto status =
        target.registerLocalMemory({remote.data(), remote.data()},
                                   {remote.size(), remote.size()}, options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    ASSERT_TRUE(
        initiator.registerLocalMemory({local.data()}, {local.size()}, options)
            .ok());

    SegmentInfo info;
    ASSERT_TRUE(target.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    ASSERT_EQ(info.buffers.size(), 1u);
    EXPECT_EQ(info.buffers.front().base,
              reinterpret_cast<uint64_t>(remote.data()));

    SegmentID peer;
    ASSERT_TRUE(initiator.openSegment(peer, target.getSegmentName()).ok());
    Request request;
    request.source = local.data();
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(remote.data());
    request.length = remote.size();
    request.transport_hint = HP_TCP;
    for (int remaining_references : {2, 1}) {
        SCOPED_TRACE(remaining_references);
        // Exercise the real socket path before and after dropping one logical
        // reference: both READ and WRITE must retain the same registration.
        for (auto opcode : {Request::READ, Request::WRITE}) {
            request.opcode = opcode;
            std::memset(remote.data(), 0x6b, remote.size());
            std::memset(local.data(), 0x5a, local.size());
            status = initiator.transferSync({request});
            ASSERT_TRUE(status.ok()) << status.ToString();
            EXPECT_EQ(std::memcmp(remote.data(), local.data(), local.size()),
                      0);
            const auto expected_byte = opcode == Request::READ ? 0x6b : 0x5a;
            EXPECT_TRUE(std::all_of(local.begin(), local.end(),
                                    [expected_byte](unsigned char byte) {
                                        return byte == expected_byte;
                                    }));
        }
        ASSERT_TRUE(
            target.unregisterLocalMemory(remote.data(), remote.size()).ok());
        ASSERT_TRUE(target.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        EXPECT_EQ(info.buffers.size(), remaining_references == 2 ? 1u : 0u);
    }

    // The final unregister must also retire the transport registration;
    // otherwise a fresh registration of this range is rejected as overlapping.
    ASSERT_TRUE(
        target.registerLocalMemory({remote.data()}, {remote.size()}, options)
            .ok());
    EXPECT_TRUE(
        target.unregisterLocalMemory(remote.data(), remote.size()).ok());
    EXPECT_TRUE(initiator.closeSegment(peer).ok());
    EXPECT_TRUE(
        initiator.unregisterLocalMemory(local.data(), local.size()).ok());
}

TEST(LocalMemoryLifecycle,
     HpTcpConcurrentSingleAndBatchReferencesRetainTransfers) {
    auto config = makeConfig();
    config->set("transports/hp_tcp/enable", true);
    config->set("transports/hp_tcp/bind_address", "127.0.0.1");
    config->set("transports/hp_tcp/advertise_address", "127.0.0.1");
    config->set("transports/hp_tcp/worker_count", 1);
    config->set("transports/hp_tcp/progress_timeout_ms", 1000);
    struct Context {
        explicit Context(const std::shared_ptr<Config>& config)
            : remote(4096), local(4096), target(config), initiator(config) {}

        std::vector<unsigned char> remote;
        std::vector<unsigned char> local;
        TransferEngineImpl target;
        TransferEngineImpl initiator;
    };
    auto context = std::make_shared<Context>(config);
    ASSERT_TRUE(context->target.available());
    ASSERT_TRUE(context->initiator.available());
    // Keep an owner reference while workers mix both public overloads. The
    // deterministic transport tests above cover transitions to/from zero.
    ASSERT_TRUE(
        context->target
            .registerLocalMemory(context->remote.data(), context->remote.size())
            .ok());
    ASSERT_TRUE(
        context->initiator
            .registerLocalMemory(context->local.data(), context->local.size())
            .ok());

    std::promise<void> start;
    auto started = start.get_future().share();
    std::vector<std::unique_ptr<ConcurrentCall>> workers;
    for (size_t i = 0; i < 4; ++i) {
        workers.emplace_back(std::make_unique<ConcurrentCall>([context, started,
                                                               i] {
            started.wait();
            void* address = context->remote.data();
            const auto size = context->remote.size();
            if (i % 2 == 0) {
                CHECK_STATUS(
                    context->target.registerLocalMemory(address, size));
            } else {
                CHECK_STATUS(context->target.registerLocalMemory(
                    {address, address}, {size, size}));
            }
            if (i == 1 || i == 2) {
                return context->target.unregisterLocalMemory(
                    std::vector<void*>(i == 1 ? 2 : 1, address),
                    std::vector<size_t>(i == 1 ? 2 : 1, size));
            }
            CHECK_STATUS(context->target.unregisterLocalMemory(address, size));
            return i == 3 ? context->target.unregisterLocalMemory(address, size)
                          : Status::OK();
        }));
    }
    start.set_value();
    for (auto& worker : workers) {
        auto status = worker->get();
        ASSERT_TRUE(worker->ready());
        ASSERT_TRUE(status.ok()) << status.ToString();
    }

    SegmentInfo info;
    ASSERT_TRUE(context->target.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    ASSERT_EQ(info.buffers.size(), 1u);
    SegmentID peer;
    ASSERT_TRUE(
        context->initiator.openSegment(peer, context->target.getSegmentName())
            .ok());
    Request request;
    request.source = context->local.data();
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(context->remote.data());
    request.length = context->remote.size();
    request.transport_hint = HP_TCP;
    for (auto opcode : {Request::READ, Request::WRITE}) {
        request.opcode = opcode;
        std::fill(context->remote.begin(), context->remote.end(), 0x6b);
        std::fill(context->local.begin(), context->local.end(), 0x5a);
        auto status = context->initiator.transferSync({request});
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(context->local, context->remote);
        const auto expected = opcode == Request::READ ? 0x6b : 0x5a;
        EXPECT_TRUE(std::all_of(
            context->local.begin(), context->local.end(),
            [expected](unsigned char byte) { return byte == expected; }));
    }
    ASSERT_TRUE(context->target
                    .unregisterLocalMemory(context->remote.data(),
                                           context->remote.size())
                    .ok());
    ASSERT_TRUE(context->target.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty());
    ASSERT_TRUE(
        context->target
            .registerLocalMemory(context->remote.data(), context->remote.size())
            .ok());
    EXPECT_TRUE(context->target
                    .unregisterLocalMemory(context->remote.data(),
                                           context->remote.size())
                    .ok());
    EXPECT_TRUE(context->initiator.closeSegment(peer).ok());
    EXPECT_TRUE(
        context->initiator
            .unregisterLocalMemory(context->local.data(), context->local.size())
            .ok());
}

TEST(LocalMemoryLifecycle, HpTcpLocalOnlyDuplicatesReleasePrivateRegistration) {
    auto config = makeConfig();
    config->set("transports/hp_tcp/enable", true);
    config->set("transports/hp_tcp/bind_address", "127.0.0.1");
    config->set("transports/hp_tcp/advertise_address", "127.0.0.1");
    config->set("transports/hp_tcp/worker_count", 1);
    std::vector<char> buffer(4096);
    TransferEngineImpl engine(config);
    ASSERT_TRUE(engine.available());
    MemoryOptions options;
    options.type = HP_TCP;
    options.perm = kLocalReadWrite;
    for (int reference = 0; reference < 2; ++reference) {
        auto status = engine.registerLocalMemory({buffer.data()},
                                                 {buffer.size()}, options);
        ASSERT_TRUE(status.ok()) << status.ToString();
    }
    // A different permission must not silently reuse the private registration
    // or acquire another reference to it.
    auto global_options = options;
    global_options.perm = kGlobalReadWrite;
    auto status = engine.registerLocalMemory({buffer.data()}, {buffer.size()},
                                             global_options);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    SegmentInfo info;
    for (size_t remaining : {1u, 0u}) {
        ASSERT_TRUE(
            engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
        ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
        EXPECT_EQ(info.buffers.size(), remaining);
    }
    // Local-only HP TCP registrations are not advertised in transports. The
    // final release still has to remove the private registry entry.
    status =
        engine.registerLocalMemory({buffer.data()}, {buffer.size()}, options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(
        engine.unregisterLocalMemory(buffer.data(), buffer.size()).ok());
    ASSERT_TRUE(engine.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty());
}

TEST(LocalMemoryLifecycle, HpTcpDifferentLengthFailurePreservesOriginalRange) {
    auto config = makeConfig();
    config->set("transports/hp_tcp/enable", true);
    config->set("transports/hp_tcp/bind_address", "127.0.0.1");
    config->set("transports/hp_tcp/advertise_address", "127.0.0.1");
    config->set("transports/hp_tcp/worker_count", 1);
    config->set("transports/hp_tcp/progress_timeout_ms", 1000);
    std::vector<unsigned char> remote(4096);
    std::vector<unsigned char> local(remote.size());
    TransferEngineImpl target(config);
    TransferEngineImpl initiator(config);
    ASSERT_TRUE(target.available());
    ASSERT_TRUE(initiator.available());
    MemoryOptions options;
    options.type = HP_TCP;
    ASSERT_TRUE(
        target.registerLocalMemory({remote.data()}, {remote.size()}, options)
            .ok());
    ASSERT_TRUE(
        initiator.registerLocalMemory({local.data()}, {local.size()}, options)
            .ok());

    // This is a different range, not another reference to the original one.
    // Let HP TCP reject the overlap without cleaning up the existing buffer.
    auto status = target.registerLocalMemory({remote.data()},
                                             {remote.size() / 2}, options);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    SegmentInfo info;
    ASSERT_TRUE(target.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    ASSERT_EQ(info.buffers.size(), 1u);
    EXPECT_EQ(info.buffers.front().base,
              reinterpret_cast<uint64_t>(remote.data()));
    EXPECT_EQ(info.buffers.front().length, remote.size());
    SegmentID peer;
    ASSERT_TRUE(initiator.openSegment(peer, target.getSegmentName()).ok());
    Request request;
    request.source = local.data();
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(remote.data());
    request.length = remote.size();
    request.transport_hint = HP_TCP;
    for (auto opcode : {Request::READ, Request::WRITE}) {
        request.opcode = opcode;
        std::fill(remote.begin(), remote.end(), 0x6b);
        std::fill(local.begin(), local.end(), 0x5a);
        status = initiator.transferSync({request});
        ASSERT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(local, remote);
        const auto expected = opcode == Request::READ ? 0x6b : 0x5a;
        EXPECT_TRUE(std::all_of(
            local.begin(), local.end(),
            [expected](unsigned char byte) { return byte == expected; }));
    }
    ASSERT_TRUE(
        target.unregisterLocalMemory(remote.data(), remote.size()).ok());
    ASSERT_TRUE(target.getSegmentInfo(LOCAL_SEGMENT_ID, info).ok());
    EXPECT_TRUE(info.buffers.empty())
        << "The rejected range must not add a reference to the original";
    status =
        target.registerLocalMemory({remote.data()}, {remote.size()}, options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_TRUE(
        target.unregisterLocalMemory(remote.data(), remote.size()).ok());
    EXPECT_TRUE(initiator.closeSegment(peer).ok());
    EXPECT_TRUE(
        initiator.unregisterLocalMemory(local.data(), local.size()).ok());
}

TEST(LocalMemoryLifecycle, ShmAcceptsDefaultAllocationLocation) {
    auto config = makeConfig();
    config->set("transports/shm/enable", true);
    TransferEngineImpl engine(config);
    ASSERT_TRUE(engine.available());

    void* address = nullptr;
    auto status = engine.allocateLocalMemory(&address, 4096, kWildcardLocation);
    ASSERT_TRUE(status.ok()) << status.ToString();
    std::memset(address, 0x5a, 4096);
    EXPECT_TRUE(engine.freeLocalMemory(address).ok());
}

TEST(LocalMemoryLifecycle, DefaultHostOptionsAllocateUsableSharedMemory) {
    auto config = makeConfig();
    config->set("transports/shm/enable", true);
    TransferEngineImpl target(config);
    TransferEngineImpl initiator(config);
    ASSERT_TRUE(target.available());
    ASSERT_TRUE(initiator.available());

    MemoryOptions target_options;
    MemoryOptions source_options;
    source_options.location = "cpu:0";
    void* source = nullptr;
    void* destination = nullptr;
    constexpr size_t kSize = 4096;
    auto status = target.allocateLocalMemory(&source, kSize, target_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    status = initiator.allocateLocalMemory(&destination, kSize, source_options);
    ASSERT_TRUE(status.ok()) << status.ToString();
    EXPECT_EQ(target_options.type, SHM);
    EXPECT_EQ(source_options.type, SHM);
    EXPECT_EQ(target_options.location, kWildcardLocation);
    EXPECT_EQ(source_options.location, "cpu:0");
    ASSERT_FALSE(target_options.shm_path.empty());
    ASSERT_TRUE(
        target.registerLocalMemory({source}, {kSize}, target_options).ok());
    ASSERT_TRUE(
        initiator.registerLocalMemory({destination}, {kSize}, source_options)
            .ok());
    std::memset(source, 0x6b, kSize);
    std::memset(destination, 0, kSize);

    SegmentID peer;
    ASSERT_TRUE(initiator.openSegment(peer, target.getSegmentName()).ok());
    Request request;
    request.opcode = Request::READ;
    request.source = destination;
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(source);
    request.length = kSize;
    ASSERT_TRUE(initiator.transferSync({request}).ok());
    EXPECT_EQ(std::memcmp(source, destination, kSize), 0);

    EXPECT_TRUE(initiator.closeSegment(peer).ok());
    EXPECT_TRUE(initiator.unregisterLocalMemory(destination).ok());
    EXPECT_TRUE(target.unregisterLocalMemory(source).ok());
    EXPECT_TRUE(initiator.freeLocalMemory(destination).ok());
    EXPECT_TRUE(target.freeLocalMemory(source).ok());
}

TEST(LocalMemoryLifecycle, BasicShmAllocationsRegisterForReadAndWrite) {
    auto config = makeConfig();
    config->set("transports/shm/enable", true);
    TransferEngineImpl target(config);
    TransferEngineImpl initiator(config);
    ASSERT_TRUE(target.available());
    ASSERT_TRUE(initiator.available());

    constexpr size_t kSize = 4096;
    void* source = nullptr;
    void* destination = nullptr;
    ASSERT_TRUE(target.allocateLocalMemory(&source, kSize).ok());
    ASSERT_TRUE(initiator.allocateLocalMemory(&destination, kSize).ok());
    ASSERT_TRUE(target.registerLocalMemory(source, kSize).ok());
    ASSERT_TRUE(initiator
                    .registerLocalMemory(std::vector<void*>{destination},
                                         std::vector<size_t>{kSize})
                    .ok());

    SegmentID peer;
    ASSERT_TRUE(initiator.openSegment(peer, target.getSegmentName()).ok());
    Request request;
    request.source = destination;
    request.target_id = peer;
    request.target_offset = reinterpret_cast<uint64_t>(source);
    request.length = kSize;
    for (auto opcode : {Request::READ, Request::WRITE}) {
        request.opcode = opcode;
        std::memset(source, 0x6b, kSize);
        std::memset(destination, 0x5a, kSize);
        auto status = initiator.transferSync({request});
        EXPECT_TRUE(status.ok()) << status.ToString();
        EXPECT_EQ(std::memcmp(source, destination, kSize), 0);
    }

    EXPECT_TRUE(initiator.closeSegment(peer).ok());
    EXPECT_TRUE(initiator.unregisterLocalMemory(destination).ok());
    EXPECT_TRUE(target.unregisterLocalMemory(source).ok());
    EXPECT_TRUE(initiator.freeLocalMemory(destination).ok());
    EXPECT_TRUE(target.freeLocalMemory(source).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
