// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated allocation barrier: no hooks in the production runtime.
#include "real_client.h"
#include "ssd_direct_runtime.h"
#include "ssd_fixed_l2_requester.h"
#include "test_server_helpers.h"
#include "transfer_engine.h"
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <atomic>
#include <cstdlib>
#include <future>
#include <new>
#include <thread>
#include <array>
#include <bit>
#if defined(__linux__)
#include <unwind.h>
#endif

namespace {
thread_local bool pause_next_allocation = false;
std::atomic<bool> entered{false}, resume{false};
#if defined(__linux__)
enum class AllocationFault {
    kNone,
    kRequesterReserve,
    kRuntimeMap,
    kBookkeeping,
    kPhysicalUnregister
};
thread_local AllocationFault allocation_fault = AllocationFault::kNone;
thread_local bool allocation_failed = false;
thread_local size_t imports_to_skip = 0;
thread_local bool fail_physical_rollback = false;
thread_local size_t failed_allocations = 0;
thread_local const std::unordered_set<void*>* physical_fault_markers = nullptr;
thread_local void* physical_fault_pool = nullptr;
void* real_register_start = nullptr;
template <typename Method>
void* MethodStart(Method method) {
    // Nonvirtual member functions use the platform's two-word Itanium ABI.
    static_assert(sizeof(Method) == 2 * sizeof(uintptr_t));
    return reinterpret_cast<void*>(
        std::bit_cast<std::array<uintptr_t, 2>>(method)[0]);
}
struct AllocationFrames {
    bool importer{false}, requester{false}, registry{false}, real{false},
        transport{false}, unregister{false};
};
_Unwind_Reason_Code InspectAllocation(_Unwind_Context* context,
                                      void* argument) {
    auto pc = _Unwind_GetIP(context);
    if (!pc) return _URC_NO_REASON;
    // Exception unwind metadata gives exact function starts without exported
    // symbols, linker changes, heap allocation, or hooks in production code.
    auto start = _Unwind_FindEnclosingFunction(reinterpret_cast<void*>(pc - 1));
    auto& frames = *static_cast<AllocationFrames*>(argument);
    frames.importer |=
        start ==
        MethodStart(&mooncake::ssd_offload::SsdDirectRuntime::ImportFixedPool);
    frames.requester |=
        start ==
        MethodStart(&mooncake::ssd_offload::SsdFixedL2Requester::Register);
    frames.registry |=
        start ==
        MethodStart(&mooncake::ssd_offload::DestinationRegistry::Register);
    frames.real |= start == real_register_start;
    frames.transport |=
        start == MethodStart(&mooncake::Client::RegisterLocalMemory);
    frames.unregister |=
        start ==
        MethodStart(&mooncake::TransferMetadata::removeLocalMemoryBuffer);
    return _URC_NO_REASON;
}
bool FailAllocation(size_t bytes) {
    if (allocation_fault == AllocationFault::kNone) return false;
    AllocationFrames frames;
    _Unwind_Backtrace(InspectAllocation, &frames);
    if (allocation_fault == AllocationFault::kRequesterReserve &&
        frames.importer && frames.requester && !frames.registry) {
        if (imports_to_skip) {
            --imports_to_skip;
            return false;
        }
        return true;
    }
    if (allocation_fault == AllocationFault::kRuntimeMap)
        return frames.importer && !frames.requester && !frames.registry;
    if (allocation_fault == AllocationFault::kBookkeeping)
        // Skip the small CPU accelerator-view allocation before bookkeeping.
        // A map node must accommodate both its value and its link.
        return frames.real && !frames.transport && !frames.importer &&
               bytes >= sizeof(std::pair<void* const, size_t>) + sizeof(void*);
    if (allocation_fault == AllocationFault::kPhysicalUnregister)
        return frames.unregister &&
               (!physical_fault_pool ||
                (physical_fault_markers &&
                 physical_fault_markers->contains(physical_fault_pool)));
    return false;
}
#endif
}  // namespace
void* operator new(size_t bytes) {
#if defined(__linux__)
    if (FailAllocation(bytes)) {
        allocation_fault = fail_physical_rollback
                               ? AllocationFault::kPhysicalUnregister
                               : AllocationFault::kNone;
        fail_physical_rollback = false;
        allocation_failed = true;
        ++failed_allocations;
        throw std::bad_alloc();
    }
#endif
    if (pause_next_allocation) {
        pause_next_allocation = false;
        entered = true;
        while (!resume) std::this_thread::yield();
    }
    if (auto* ptr = std::malloc(bytes ? bytes : 1)) return ptr;
    throw std::bad_alloc();
}
void operator delete(void* ptr) noexcept { std::free(ptr); }
void operator delete(void* ptr, size_t) noexcept { std::free(ptr); }
void* operator new[](size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* ptr) noexcept { std::free(ptr); }
void operator delete[](void* ptr, size_t) noexcept { std::free(ptr); }
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
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
    std::free(ptr);
}
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
    std::free(ptr);
}

namespace mooncake::testing {
// Uses the existing friend fixture name in this separate executable.
class RealClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ssd_runtime_lifetime_test");
        setenv("MOONCAKE_OFFLOAD_SSD_PUSH", "true", 1);
    }
    void TearDown() override {
#if defined(__linux__)
        allocation_fault = AllocationFault::kNone;
        fail_physical_rollback = false;
        physical_fault_markers = nullptr;
        physical_fault_pool = nullptr;
#endif
        unsetenv("MOONCAKE_OFFLOAD_SSD_PUSH");
        google::ShutdownGoogleLogging();
    }
    auto PinRuntime(RealClient& client) { return client.ssd_direct_; }
    auto Engine(RealClient& client) {
        return client.client_->GetTransferEngine();
    }
    bool StartMaster(InProcMaster& master) {
        // All clients use P2PHANDSHAKE; an unused HTTP metadata listener only
        // adds an unnecessary ephemeral-port reservation race to this test.
        const auto config =
            InProcMasterConfigBuilder().set_http_metadata_port(0).build();
        // The common fixture releases its temporary reservations before bind.
        // Retry only startup with fresh ports; never retry a tested operation.
        for (size_t attempt = 0; attempt < 3; ++attempt) {
            if (master.Start(config)) return true;
            master.Stop();
        }
        return false;
    }
    bool HasMr(TransferEngine& engine, void* pointer) {
        auto desc = engine.getMetadata()->getSegmentDescByID(0);
        return desc &&
               std::any_of(desc->buffers.begin(), desc->buffers.end(),
                           [&](const auto& mr) {
                               return mr.addr ==
                                      reinterpret_cast<uint64_t>(pointer);
                           });
    }
#if defined(__linux__)
    void CheckFailedRegistration(AllocationFault selected) {
        InProcMaster master;
        ASSERT_TRUE(StartMaster(master));
        auto client = RealClient::create();
        ASSERT_EQ(
            client->setup_real("localhost:17813", "P2PHANDSHAKE", 0,
                               1024 * 1024, "tcp", "", master.master_address()),
            0);
        auto engine = Engine(*client);
        auto runtime = PinRuntime(*client);
        std::array<char, 64> l2{};
        real_register_start =
            MethodStart(&RealClient::register_buffer_internal);
        allocation_failed = false;
        imports_to_skip = 0;
        allocation_fault = selected;
        int result = 0;
        EXPECT_NO_THROW(result = client->register_buffer(l2.data(), l2.size()));
        allocation_fault = AllocationFault::kNone;
        EXPECT_TRUE(allocation_failed);
        EXPECT_NE(result, 0);
        EXPECT_FALSE(HasMr(*engine, l2.data()));
        EXPECT_FALSE(runtime->HasPending(l2.data(), l2.size()));
        EXPECT_TRUE(client->registered_buffer_sizes_.empty());
        EXPECT_TRUE(client->incomplete_buffer_registrations_.empty());
        EXPECT_EQ(client->register_buffer(l2.data(), l2.size()), 0);
        EXPECT_TRUE(HasMr(*engine, l2.data()));
        EXPECT_EQ(client->unregister_buffer(l2.data()), 0);
        EXPECT_EQ(client->tearDownAll(), 0);
        EXPECT_FALSE(HasMr(*engine, l2.data()));
    }
#endif
};
TEST_F(RealClientTest,
       CloseWaitsForAcceptedBootstrapCallBeforeResettingRuntime) {
    InProcMaster master;
    ASSERT_TRUE(StartMaster(master));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("localhost:17813", "P2PHANDSHAKE", 0, 1024 * 1024,
                           "tcp", "", master.master_address()),
        0);
    // Keep the old object alive for a clean RED assertion. Production must
    // wait for the admitted API call before releasing its runtime pointer.
    auto pin = PinRuntime(*client);
    ASSERT_TRUE(pin);
    entered = false;
    resume = false;
    auto read = std::async(std::launch::async, [&] {
        pause_next_allocation = true;
        return client->get_ssd_push_peer_info();
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!entered && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_TRUE(entered);
    auto closing =
        std::async(std::launch::async, [&] { return client->tearDownAll(); });
    EXPECT_EQ(closing.wait_for(std::chrono::seconds(3)),
              std::future_status::timeout);
    resume = true;
    EXPECT_NE(read.get(), "{}");
    EXPECT_EQ(closing.get(), 0);
    EXPECT_EQ(client->get_ssd_push_peer_info(), "{}");
    pin.reset();
    client.reset();
    master.Stop();
}
#if defined(__linux__)
TEST_F(RealClientTest, FailedRequesterReserveRollsBackCommittedMr) {
    CheckFailedRegistration(AllocationFault::kRequesterReserve);
}
TEST_F(RealClientTest, FailedRuntimeMapInsertRollsBackCommittedMr) {
    CheckFailedRegistration(AllocationFault::kRuntimeMap);
}
TEST_F(RealClientTest, FailedBookkeepingDoesNotPublishMr) {
    CheckFailedRegistration(AllocationFault::kBookkeeping);
}
TEST_F(RealClientTest, FailedBatchImportRollsBackEarlierOwnedRegistration) {
    InProcMaster master;
    ASSERT_TRUE(StartMaster(master));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("localhost:17813", "P2PHANDSHAKE", 0, 1024 * 1024,
                           "tcp", "", master.master_address()),
        0);
    auto engine = Engine(*client);
    std::array<char, 64> first{}, second{};
    allocation_failed = false;
    imports_to_skip = 1;  // The first pool's requester reserve must succeed.
    allocation_fault = AllocationFault::kRequesterReserve;
    int result = 0;
    EXPECT_NO_THROW(
        result = client->register_buffers({first.data(), second.data()},
                                          {first.size(), second.size()}));
    allocation_fault = AllocationFault::kNone;
    EXPECT_TRUE(allocation_failed);
    EXPECT_NE(result, 0);
    EXPECT_FALSE(HasMr(*engine, first.data()));
    EXPECT_FALSE(HasMr(*engine, second.data()));
    EXPECT_TRUE(client->registered_buffer_sizes_.empty());
    EXPECT_TRUE(client->incomplete_buffer_registrations_.empty());
    EXPECT_EQ(client->register_buffers({first.data(), second.data()},
                                       {first.size(), second.size()}),
              0);
    EXPECT_EQ(client->unregister_buffer(first.data()), 0);
    EXPECT_EQ(client->unregister_buffer(second.data()), 0);
    EXPECT_EQ(client->tearDownAll(), 0);
}
TEST_F(RealClientTest, FailedPhysicalRollbackKeepsPoolAndCloseBusyUntilRetry) {
    InProcMaster master;
    ASSERT_TRUE(StartMaster(master));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("localhost:17813", "P2PHANDSHAKE", 0, 1024 * 1024,
                           "tcp", "", master.master_address()),
        0);
    auto engine = Engine(*client);
    std::array<char, 64> l2{};
    // Fail the import, then the first allocation inside physical unregister.
    // No remote attempt is ever published, but the MR must remain owned until
    // the transport positively confirms a later unregister.
    imports_to_skip = 0;
    failed_allocations = 0;
    fail_physical_rollback = true;
    allocation_fault = AllocationFault::kRequesterReserve;
    int result = 0;
    EXPECT_NO_THROW(result = client->register_buffer(l2.data(), l2.size()));
    allocation_fault = AllocationFault::kNone;
    fail_physical_rollback = false;
    EXPECT_EQ(failed_allocations, 2u);
    EXPECT_EQ(result, int(ErrorCode::OBJECT_HAS_LEASE));
    EXPECT_TRUE(HasMr(*engine, l2.data()));
    EXPECT_TRUE(client->registered_buffer_sizes_.contains(l2.data()));
    EXPECT_TRUE(client->incomplete_buffer_registrations_.contains(l2.data()));
    EXPECT_EQ(client->tearDownAll(), int(ErrorCode::OBJECT_HAS_LEASE));
    EXPECT_NE(client->get_ssd_push_peer_info(), "{}");
    EXPECT_TRUE(HasMr(*engine, l2.data()));
    EXPECT_EQ(client->unregister_buffer(l2.data()), 0);
    EXPECT_FALSE(HasMr(*engine, l2.data()));
    EXPECT_TRUE(client->incomplete_buffer_registrations_.empty());
    EXPECT_EQ(client->tearDownAll(), 0);
}
TEST_F(RealClientTest, FailedBatchRollbackReportsBusyAndRetainsOwnedPool) {
    InProcMaster master;
    ASSERT_TRUE(StartMaster(master));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("localhost:17813", "P2PHANDSHAKE", 0, 1024 * 1024,
                           "tcp", "", master.master_address()),
        0);
    auto engine = Engine(*client);
    std::array<char, 64> first{}, second{};
    imports_to_skip = 1;
    failed_allocations = 0;
    physical_fault_markers = &client->incomplete_buffer_registrations_;
    physical_fault_pool = first.data();
    fail_physical_rollback = true;
    allocation_fault = AllocationFault::kRequesterReserve;
    int result = 0;
    EXPECT_NO_THROW(
        result = client->register_buffers({first.data(), second.data()},
                                          {first.size(), second.size()}));
    allocation_fault = AllocationFault::kNone;
    fail_physical_rollback = false;
    physical_fault_markers = nullptr;
    physical_fault_pool = nullptr;
    EXPECT_EQ(failed_allocations, 2u);
    EXPECT_EQ(result, int(ErrorCode::OBJECT_HAS_LEASE));
    EXPECT_TRUE(HasMr(*engine, first.data()));
    EXPECT_FALSE(HasMr(*engine, second.data()));
    EXPECT_TRUE(client->registered_buffer_sizes_.contains(first.data()));
    EXPECT_FALSE(client->registered_buffer_sizes_.contains(second.data()));
    EXPECT_TRUE(
        client->incomplete_buffer_registrations_.contains(first.data()));
    EXPECT_EQ(client->tearDownAll(), int(ErrorCode::OBJECT_HAS_LEASE));
    EXPECT_TRUE(HasMr(*engine, first.data()));
    EXPECT_EQ(client->unregister_buffer(first.data()), 0);
    EXPECT_FALSE(HasMr(*engine, first.data()));
    EXPECT_TRUE(client->incomplete_buffer_registrations_.empty());
    EXPECT_EQ(client->tearDownAll(), 0);
}
#endif
TEST_F(RealClientTest, FailedRegistrationDoesNotDeleteExistingEngineMr) {
    InProcMaster master;
    ASSERT_TRUE(StartMaster(master));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("localhost:17813", "P2PHANDSHAKE", 0, 1024 * 1024,
                           "tcp", "", master.master_address()),
        0);
    auto engine = Engine(*client);
    std::array<char, 64> l2{};
    ASSERT_EQ(engine->registerLocalMemory(l2.data(), l2.size()), 0);
    EXPECT_LT(client->register_buffer(l2.data(), l2.size()), 0);
    EXPECT_TRUE(HasMr(*engine, l2.data()));
    EXPECT_TRUE(client->registered_buffer_sizes_.empty());
    EXPECT_TRUE(client->incomplete_buffer_registrations_.empty());
    EXPECT_EQ(engine->unregisterLocalMemory(l2.data(), true), 0);
    EXPECT_EQ(client->register_buffer(l2.data(), l2.size()), 0);
    EXPECT_EQ(client->unregister_buffer(l2.data()), 0);
    EXPECT_EQ(client->tearDownAll(), 0);
}
TEST_F(RealClientTest, CloseWaitsForAcceptedRegistrationAndRemovesItsMr) {
    InProcMaster master;
    ASSERT_TRUE(StartMaster(master));
    auto client = RealClient::create();
    ASSERT_EQ(
        client->setup_real("localhost:17813", "P2PHANDSHAKE", 0, 1024 * 1024,
                           "tcp", "", master.master_address()),
        0);
    auto engine = Engine(*client);
    std::array<char, 64> l2{};
    entered = false;
    resume = false;
    auto registration = std::async(std::launch::async, [&] {
        pause_next_allocation = true;
        return client->register_buffer(l2.data(), l2.size());
    });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (!entered && std::chrono::steady_clock::now() < deadline)
        std::this_thread::yield();
    EXPECT_TRUE(entered);
    auto closing =
        std::async(std::launch::async, [&] { return client->tearDownAll(); });
    EXPECT_EQ(closing.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    resume = true;
    EXPECT_EQ(registration.get(), 0);
    EXPECT_EQ(closing.get(), 0);
    EXPECT_FALSE(HasMr(*engine, l2.data()));
    EXPECT_LT(client->register_buffer(l2.data(), l2.size()), 0);
    EXPECT_LT(client->unregister_buffer(l2.data()), 0);
}
}  // namespace mooncake::testing
