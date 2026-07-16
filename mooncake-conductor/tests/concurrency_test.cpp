// Focus tests for Stop/HandleBatch interaction and concurrent
// register/unregister, designed to run under TSAN (build-tsan) to verify
// the lock rules:
//  - handlers take the manager read lock; UnsubscribeFromService must
//    call client->Stop() OUTSIDE the manager lock (deadlock otherwise);
//  - subscribers_/active_configs_/services_ stay consistent under
//    concurrent register/unregister;
//  - PrefixCacheTable registration, owner mutation, and query race freedom
//    under the global-map -> context-state lock order.

#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <chrono>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/prefixindex/prefix_indexer.h"
#include "event_manager_test_peer.h"
#include "prefix_indexer_test_peer.h"

namespace {

using conductor::common::PublisherKind;
using conductor::common::ServiceConfig;
using conductor::kvevent::EventManager;
using conductor::kvevent::EventManagerTestPeer;
using conductor::kvevent::KVEventHandler;
using conductor::kvevent::KVEventHandlerTestPeer;
using conductor::prefixindex::ContextKey;
using conductor::prefixindex::EngineOwner;
using conductor::prefixindex::EngineRegistration;
using conductor::prefixindex::GpuClear;
using conductor::prefixindex::GpuMutation;
using conductor::prefixindex::HashBlock;
using conductor::prefixindex::HashProfile;
using conductor::prefixindex::PrefixCacheTable;
using conductor::prefixindex::PrefixCacheTableTestPeer;
using conductor::prefixindex::ProjectedPrefix;
using conductor::prefixindex::SharedClear;
using conductor::prefixindex::SharedMutation;
using conductor::prefixindex::SharedObjectOwner;
using conductor::prefixindex::StorageTier;
using conductor::zmq::MessageMetadata;
using conductor::zmq::VllmEvent;
using conductor::zmq::VllmEventBatch;
using conductor::zmq::VllmRemovedEvent;
using conductor::zmq::VllmStoredEvent;

constexpr char kRaceRootDigest[] =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";

HashProfile RaceProfile() {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256_cbor",
            .root_digest = kRaceRootDigest,
            .index_projection = "low64_be"};
}

ServiceConfig RaceService(const std::string& instance_id, int endpoint_slot,
                          int dp_rank = 0) {
    ServiceConfig svc;
    const int live_port = 50000 + endpoint_slot * 4 + dp_rank * 2;
    svc.endpoint = "tcp://127.0.0.1:" + std::to_string(live_port);
    svc.replay_endpoint = "tcp://127.0.0.1:" + std::to_string(live_port + 1);
    svc.publisher_kind = PublisherKind::kVllm;
    svc.model_name = "race-model";
    svc.instance_id = instance_id;
    svc.tenant_id = "default";
    svc.block_size = 16;
    svc.dp_rank = dp_rank;
    svc.cache_group = 0;
    svc.hash_profile = {.strategy = "vllm_v1",
                        .algorithm = "sha256_cbor",
                        .root_digest = kRaceRootDigest,
                        .index_projection = "low64_be"};
    return svc;
}

// Handlers keep dispatching events while the manager stops. This is the
// exact interaction behind Go's documented deadlock: HandleBatch takes
// the manager read lock, Stop takes it exclusively.
TEST(Concurrency, StopWhileHandlingEvents) {
    EventManager mgr({}, 0);
    ServiceConfig svc = RaceService("race-instance", 0);
    svc.block_size = 4;
    ASSERT_TRUE(mgr.GetIndexer()
                    ->Register({.context = {.tenant_id = svc.tenant_id,
                                            .model_name = svc.model_name,
                                            .lora_name = svc.lora_name,
                                            .block_size = svc.block_size},
                                .profile = RaceProfile(),
                                .instance_id = svc.instance_id,
                                .dp_rank = svc.dp_rank,
                                .effective_block_size = svc.block_size,
                                .cache_group = svc.cache_group})
                    .error.empty());
    KVEventHandler handler(&mgr, svc);
    const MessageMetadata metadata{.publisher_kind = PublisherKind::kVllm,
                                   .endpoint = svc.endpoint,
                                   .topic = "race"};

    std::atomic<bool> done{false};
    std::vector<std::thread> workers;
    for (int w = 0; w < 4; ++w) {
        workers.emplace_back([&handler, &done, &metadata, w] {
            VllmStoredEvent stored;
            stored.block_size = 4;
            stored.token_ids = {1, 2, 3, 4};
            stored.medium = "GPU";
            VllmRemovedEvent removed;
            removed.medium = "GPU";
            uint64_t hash = 1000 * (w + 1);
            while (!done.load()) {
                stored.block_hashes = {hash};
                removed.block_hashes = {hash};
                ++hash;
                VllmEventBatch stored_batch;
                stored_batch.events.push_back({.event = VllmEvent{stored}});
                VllmEventBatch removed_batch;
                removed_batch.events.push_back({.event = VllmEvent{removed}});
                // After Stop, HandleBatch returns "manager stopped" --
                // either outcome is fine; the assertion is no deadlock
                // and no TSAN report.
                (void)handler.HandleBatch(stored_batch, metadata);
                (void)handler.HandleBatch(removed_batch, metadata);
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    mgr.Stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    done.store(true);
    for (auto& t : workers) t.join();
    EXPECT_TRUE(mgr.IsStopped());
}

TEST(Concurrency, StartConcurrentWithServiceRegistration) {
    constexpr int kStaticServices = 8;
    constexpr int kDynamicServices = 8;

    std::vector<ServiceConfig> services;
    for (int i = 0; i < kStaticServices; ++i) {
        services.push_back(RaceService("static-" + std::to_string(i), i));
    }
    EventManager mgr(std::move(services), 0);

    std::barrier start(2);
    std::atomic<int> registration_errors{0};
    std::thread starter([&mgr, &start] {
        start.arrive_and_wait();
        mgr.Start();
    });
    std::thread registrar([&mgr, &start, &registration_errors] {
        start.arrive_and_wait();
        for (int i = 0; i < kDynamicServices; ++i) {
            const auto result =
                conductor::kvevent::EventManagerTestPeer::Register(
                    mgr, RaceService("dynamic-" + std::to_string(i),
                                     kStaticServices + i));
            if (!result.second.empty()) {
                registration_errors.fetch_add(1);
            }
        }
    });

    starter.join();
    registrar.join();

    EXPECT_EQ(registration_errors.load(), 0);
    EXPECT_EQ(conductor::kvevent::EventManagerTestPeer::ServicesLen(mgr),
              static_cast<size_t>(kStaticServices + kDynamicServices));
    EXPECT_EQ(conductor::kvevent::EventManagerTestPeer::SubscriberCount(mgr),
              static_cast<size_t>(kStaticServices + kDynamicServices));
    EXPECT_EQ(conductor::kvevent::EventManagerTestPeer::ActiveConfigCount(mgr),
              static_cast<size_t>(kStaticServices + kDynamicServices));
    mgr.Stop();
}

// Concurrent registrations of distinct and duplicate services. ZMQ
// connect is async, so subscriptions to unreachable endpoints succeed
// and exercise the full path including client startup/teardown.
TEST(Concurrency, ConcurrentRegisterUnregister) {
    EventManager mgr({}, 0);

    constexpr int kThreads = 8;
    constexpr int kRounds = 5;
    std::atomic<int> unexpected_errors{0};
    std::barrier start(kThreads);
    std::vector<std::thread> workers;
    for (int w = 0; w < kThreads; ++w) {
        workers.emplace_back([&mgr, &start, &unexpected_errors, w] {
            start.arrive_and_wait();
            for (int round = 0; round < kRounds; ++round) {
                // Half the threads fight over the same key; half use
                // distinct keys.
                const bool shared = w % 2 == 0;
                ServiceConfig svc =
                    RaceService(shared ? "shared-instance"
                                       : "instance-" + std::to_string(w),
                                shared ? 100 : 100 + w, round % 2);

                const auto result =
                    conductor::kvevent::EventManagerTestPeer::Subscribe(mgr,
                                                                        svc);
                if (!result.second.empty() &&
                    !result.second.starts_with(
                        "service is being unregistered")) {
                    unexpected_errors.fetch_add(1);
                }
                conductor::kvevent::EventManagerTestPeer::Unsubscribe(
                    mgr, svc.instance_id, svc.tenant_id, svc.dp_rank);
            }
        });
    }
    for (auto& t : workers) t.join();
    EXPECT_EQ(unexpected_errors.load(), 0);
    EXPECT_EQ(conductor::kvevent::EventManagerTestPeer::SubscriberCount(mgr),
              0u);
    EXPECT_EQ(conductor::kvevent::EventManagerTestPeer::ActiveConfigCount(mgr),
              0u);
    mgr.Stop();
}

TEST(Concurrency, UnregisterWaitsForAdmittedCallbackBeforeEndpointCleanup) {
    EventManager mgr({}, 0);
    ServiceConfig svc = RaceService("in-flight-instance", 220);
    svc.block_size = 4;
    ASSERT_TRUE(EventManagerTestPeer::Register(mgr, svc).first);
    const std::string service_key = conductor::kvevent::MakeServiceKey(
        svc.instance_id, svc.tenant_id, svc.dp_rank);
    auto handler = EventManagerTestPeer::HandlerFor(mgr, service_key);
    ASSERT_NE(handler, nullptr);
    ASSERT_TRUE(KVEventHandlerTestPeer::BeginDispatch(*handler));

    std::pair<bool, std::string> unregister_result;
    std::atomic<bool> unregister_done{false};
    std::thread unregister_thread([&] {
        unregister_result = EventManagerTestPeer::Unsubscribe(
            mgr, svc.instance_id, svc.tenant_id, svc.dp_rank);
        unregister_done.store(true);
    });

    const auto unavailable_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (KVEventHandlerTestPeer::IsAvailable(*handler) &&
           std::chrono::steady_clock::now() < unavailable_deadline) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(KVEventHandlerTestPeer::IsAvailable(*handler));
    EXPECT_FALSE(unregister_done.load());

    VllmStoredEvent stored;
    stored.block_hashes = {uint64_t{4242}};
    stored.token_ids = {1, 2, 3, 4};
    stored.block_size = svc.block_size;
    stored.medium = "GPU";
    const MessageMetadata metadata{.publisher_kind = PublisherKind::kVllm,
                                   .endpoint = svc.endpoint,
                                   .topic = "in-flight",
                                   .sequence = 1};
    EXPECT_TRUE(
        KVEventHandlerTestPeer::HandleVllmStored(*handler, stored, metadata)
            .empty());

    const auto in_flight_snapshot =
        PrefixCacheTableTestPeer::Snapshot(*mgr.GetIndexer());
    const ContextKey context{.tenant_id = svc.tenant_id,
                             .model_name = svc.model_name,
                             .lora_name = svc.lora_name,
                             .block_size = svc.block_size};
    const auto in_flight_context = in_flight_snapshot.contexts.find(context);
    EXPECT_NE(in_flight_context, in_flight_snapshot.contexts.end());
    if (in_flight_context != in_flight_snapshot.contexts.end()) {
        EXPECT_TRUE(in_flight_context->second.blocks.contains(
            ProjectedPrefix{.value = 4242}));
    }

    const auto replacement_while_stopping =
        EventManagerTestPeer::Register(mgr, svc);
    EXPECT_FALSE(replacement_while_stopping.first);
    EXPECT_NE(replacement_while_stopping.second.find("unregistered"),
              std::string::npos);

    KVEventHandlerTestPeer::EndDispatch(*handler);
    unregister_thread.join();
    EXPECT_TRUE(unregister_result.first) << unregister_result.second;
    EXPECT_TRUE(unregister_result.second.empty());
    EXPECT_TRUE(PrefixCacheTableTestPeer::Snapshot(*mgr.GetIndexer())
                    .contexts.at(context)
                    .blocks.empty());

    VllmEventBatch late_batch;
    late_batch.events.push_back({.event = VllmEvent{stored}});
    EXPECT_FALSE(handler->HandleBatch(late_batch, metadata).empty());
    EXPECT_TRUE(EventManagerTestPeer::Register(mgr, svc).first);
}

TEST(Concurrency, ConcurrentStopCallersWaitForHandlerQuiescence) {
    EventManager mgr({}, 0);
    const ServiceConfig svc = RaceService("stop-barrier-instance", 221);
    ASSERT_TRUE(EventManagerTestPeer::Register(mgr, svc).first);
    auto handler = EventManagerTestPeer::HandlerFor(
        mgr, conductor::kvevent::MakeServiceKey(svc.instance_id, svc.tenant_id,
                                                svc.dp_rank));
    ASSERT_NE(handler, nullptr);
    ASSERT_TRUE(KVEventHandlerTestPeer::BeginDispatch(*handler));

    std::atomic<bool> first_done{false};
    std::atomic<bool> second_done{false};
    std::thread first([&] {
        mgr.Stop();
        first_done.store(true);
    });
    const auto unavailable_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (KVEventHandlerTestPeer::IsAvailable(*handler) &&
           std::chrono::steady_clock::now() < unavailable_deadline) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(KVEventHandlerTestPeer::IsAvailable(*handler));

    std::thread second([&] {
        mgr.Stop();
        second_done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(first_done.load());
    EXPECT_FALSE(second_done.load());

    KVEventHandlerTestPeer::EndDispatch(*handler);
    first.join();
    second.join();
    EXPECT_TRUE(first_done.load());
    EXPECT_TRUE(second_done.load());
}

TEST(Concurrency, PrefixTableOwnerMutationsQueryAndUnregister) {
    PrefixCacheTable table;
    const ContextKey context{.tenant_id = "default",
                             .model_name = "race-model",
                             .lora_name = "",
                             .block_size = 4};
    auto registration = [&](const std::string& instance, int64_t rank) {
        return EngineRegistration{.context = context,
                                  .profile = RaceProfile(),
                                  .instance_id = instance,
                                  .dp_rank = rank,
                                  .effective_block_size = 4,
                                  .cache_group = 0};
    };
    ASSERT_TRUE(table.Register(registration("instance-a", 0)).error.empty());
    ASSERT_TRUE(table.Register(registration("instance-a", 1)).error.empty());
    ASSERT_TRUE(table.Register(registration("instance-b", 0)).error.empty());

    const std::vector<int32_t> tokens = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    std::string strategy_error;
    auto strategy = conductor::prefixindex::CreateHashStrategy(RaceProfile(),
                                                               &strategy_error);
    ASSERT_NE(strategy, nullptr) << strategy_error;
    std::vector<HashBlock> hash_blocks;
    ASSERT_EQ(strategy->Compute(context, tokens, std::nullopt, &hash_blocks),
              "");
    ASSERT_EQ(hash_blocks.size(), 3u);
    std::vector<ProjectedPrefix> prefixes;
    for (const HashBlock& block : hash_blocks) {
        prefixes.push_back(block.projected);
    }

    const EngineOwner persistent_gpu{.source_stream = "persistent-engine",
                                     .instance_id = "instance-a",
                                     .dp_rank = 1};
    const SharedObjectOwner persistent_cpu{.source_stream = "persistent-pool",
                                           .backend_id = "backend-a",
                                           .object_id = "persistent-object"};
    ASSERT_EQ(table.StoreGpu({.context = context,
                              .prefixes = {prefixes[0]},
                              .owner = persistent_gpu,
                              .effective_block_size = 4,
                              .cache_group = 0}),
              "");
    ASSERT_EQ(table.StoreShared({.context = context,
                                 .prefixes = prefixes,
                                 .tier = StorageTier::kCpu,
                                 .owner = persistent_cpu,
                                 .effective_block_size = 4,
                                 .cache_group = 0}),
              "");

    const EngineOwner rank_zero_owner{.source_stream = "transient-rank-zero",
                                      .instance_id = "instance-a",
                                      .dp_rank = 0};
    const EngineOwner rank_one_owner{.source_stream = "transient-rank-one",
                                     .instance_id = "instance-a",
                                     .dp_rank = 1};
    const SharedObjectOwner transient_cpu{.source_stream = "transient-pool",
                                          .backend_id = "backend-a",
                                          .object_id = "transient-cpu"};
    const SharedObjectOwner transient_disk{.source_stream = "transient-pool",
                                           .backend_id = "backend-a",
                                           .object_id = "transient-disk"};
    const GpuMutation rank_zero_mutation{.context = context,
                                         .prefixes = prefixes,
                                         .owner = rank_zero_owner,
                                         .effective_block_size = 4,
                                         .cache_group = 0};
    const GpuMutation rank_one_mutation{.context = context,
                                        .prefixes = prefixes,
                                        .owner = rank_one_owner,
                                        .effective_block_size = 4,
                                        .cache_group = 0};
    const SharedMutation cpu_mutation{.context = context,
                                      .prefixes = prefixes,
                                      .tier = StorageTier::kCpu,
                                      .owner = transient_cpu,
                                      .effective_block_size = 4,
                                      .cache_group = 0};
    const SharedMutation disk_mutation{.context = context,
                                       .prefixes = prefixes,
                                       .tier = StorageTier::kDisk,
                                       .owner = transient_disk,
                                       .effective_block_size = 4,
                                       .cache_group = 0};

    constexpr int kThreads = 7;
    constexpr int kRounds = 200;
    std::barrier start(kThreads);
    std::vector<std::thread> workers;
    workers.emplace_back([&] {
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            (void)table.StoreGpu(rank_zero_mutation);
            (void)table.StoreGpu(rank_zero_mutation);
            (void)table.RemoveGpu(rank_zero_mutation);
            (void)table.RemoveGpu(rank_zero_mutation);
            if (round % 8 == 0) std::this_thread::yield();
        }
    });
    workers.emplace_back([&] {
        const GpuClear clear{.context = context,
                             .owner = rank_zero_owner,
                             .effective_block_size = 4,
                             .cache_group = 0};
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            (void)table.ClearGpu(clear);
            (void)table.ClearGpu(clear);
        }
    });
    workers.emplace_back([&] {
        const GpuClear clear{.context = context,
                             .owner = rank_one_owner,
                             .effective_block_size = 4,
                             .cache_group = 0};
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            (void)table.StoreGpu(rank_one_mutation);
            (void)table.StoreGpu(rank_one_mutation);
            (void)table.RemoveGpu(rank_one_mutation);
            (void)table.ClearGpu(clear);
        }
    });
    workers.emplace_back([&] {
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            (void)table.StoreShared(cpu_mutation);
            (void)table.StoreShared(cpu_mutation);
            (void)table.RemoveShared(cpu_mutation);
            (void)table.RemoveShared(cpu_mutation);
        }
    });
    workers.emplace_back([&] {
        const SharedClear clear{.context = context,
                                .owner = transient_disk,
                                .tier = StorageTier::kDisk,
                                .effective_block_size = 4,
                                .cache_group = 0};
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            (void)table.StoreShared(disk_mutation);
            (void)table.ClearShared(clear);
            (void)table.ClearShared(clear);
        }
    });
    workers.emplace_back([&] {
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            const auto results = table.Query(context, tokens);
            EXPECT_TRUE(results.contains("instance-a"));
            EXPECT_TRUE(results.contains("instance-b"));
            for (const auto& [unused_instance, result] : results) {
                (void)unused_instance;
                const auto completed_call_state = [](int64_t value) {
                    return value == 0 || value == 4 || value == 8 ||
                           value == 12;
                };
                EXPECT_TRUE(completed_call_state(result.longest_match_tokens));
                EXPECT_TRUE(completed_call_state(result.gpu));
                EXPECT_TRUE(completed_call_state(result.cpu));
                EXPECT_TRUE(completed_call_state(result.disk));
                for (const auto& [unused_rank, matched] : result.dp) {
                    (void)unused_rank;
                    EXPECT_TRUE(completed_call_state(matched));
                }
            }
            if (round % 10 == 0) {
                (void)table.GetGlobalView();
            }
        }
    });
    workers.emplace_back([&] {
        start.arrive_and_wait();
        for (int round = 0; round < kRounds; ++round) {
            (void)table.Unregister(context, "instance-a", 0);
            if (round % 8 == 0) std::this_thread::yield();
        }
    });

    for (auto& t : workers) t.join();

    const auto snapshot = PrefixCacheTableTestPeer::Snapshot(table);
    ASSERT_EQ(snapshot.contexts.size(), 1u);
    const auto& state = snapshot.contexts.at(context);
    EXPECT_EQ(state.instance_ranks.at("instance-a"), (std::set<int64_t>{1}));
    EXPECT_EQ(state.instance_ranks.at("instance-b"), (std::set<int64_t>{0}));
    ASSERT_EQ(state.blocks.size(), 3u);
    for (const ProjectedPrefix prefix : prefixes) {
        const auto& block = state.blocks.at(prefix);
        EXPECT_EQ(block.cpu_owners,
                  (std::set<SharedObjectOwner>{persistent_cpu}));
        EXPECT_TRUE(block.disk_owners.empty());
        for (const EngineOwner& owner : block.gpu_owners) {
            EXPECT_FALSE(owner.instance_id == "instance-a" &&
                         owner.dp_rank == 0);
            EXPECT_NE(owner, rank_one_owner);
        }
    }
    EXPECT_EQ(state.blocks.at(prefixes[0]).gpu_owners,
              (std::set<EngineOwner>{persistent_gpu}));
    EXPECT_TRUE(state.blocks.at(prefixes[1]).gpu_owners.empty());
    EXPECT_TRUE(state.blocks.at(prefixes[2]).gpu_owners.empty());

    const auto final_results = table.Query(context, tokens);
    ASSERT_EQ(final_results.size(), 2u);
    EXPECT_EQ(final_results.at("instance-a").dp,
              (std::map<int64_t, int64_t>{{1, 4}}));
    EXPECT_EQ(final_results.at("instance-a").longest_match_tokens, 12);
    EXPECT_EQ(final_results.at("instance-a").cpu, 12);
    EXPECT_EQ(final_results.at("instance-b").dp,
              (std::map<int64_t, int64_t>{{0, 0}}));
    EXPECT_EQ(final_results.at("instance-b").longest_match_tokens, 12);
}

}  // namespace
