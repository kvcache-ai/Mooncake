// Focus tests for Stop/HandleEvent interaction and concurrent
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

using conductor::common::ServiceConfig;
using conductor::kvevent::EventManager;
using conductor::kvevent::KVEventHandler;
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
using conductor::zmq::BlockRemovedEvent;
using conductor::zmq::BlockStoredEvent;
using conductor::zmq::KVEvent;

constexpr char kRaceRootDigest[] =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";

HashProfile RaceProfile() {
    return {.strategy = "vllm_v1",
            .algorithm = "sha256_cbor",
            .root_digest = kRaceRootDigest,
            .index_projection = "low64_be"};
}

ServiceConfig RaceService(const std::string& instance_id) {
    ServiceConfig svc;
    svc.endpoint = "tcp://127.0.0.1:59999";
    svc.replay_endpoint = "tcp://127.0.0.1:59998";
    svc.type = conductor::common::kServiceTypeVLLM;
    svc.model_name = "race-model";
    svc.instance_id = instance_id;
    svc.tenant_id = "default";
    svc.block_size = 16;
    svc.cache_group = 0;
    svc.hash_profile = {.strategy = "vllm_v1",
                        .algorithm = "sha256_cbor",
                        .root_digest = kRaceRootDigest,
                        .index_projection = "low64_be"};
    return svc;
}

// Handlers keep dispatching events while the manager stops. This is the
// exact interaction behind Go's documented deadlock: HandleEvent takes
// the manager read lock, Stop takes it exclusively.
TEST(Concurrency, StopWhileHandlingEvents) {
    EventManager mgr({}, 0);
    ServiceConfig svc;
    svc.block_size = 4;
    svc.model_name = "race-model";
    svc.instance_id = "race-instance";
    KVEventHandler handler(&mgr, svc);

    std::atomic<bool> done{false};
    std::vector<std::thread> workers;
    for (int w = 0; w < 4; ++w) {
        workers.emplace_back([&handler, &done, w] {
            BlockStoredEvent stored;
            stored.block_size = 4;
            stored.token_ids = {1, 2, 3, 4};
            BlockRemovedEvent removed;
            uint64_t hash = 1000 * (w + 1);
            while (!done.load()) {
                stored.block_hashes = {hash};
                removed.block_hashes = {hash};
                ++hash;
                // After Stop, HandleEvent returns "manager stopped" —
                // either outcome is fine; the assertion is no deadlock
                // and no TSAN report.
                (void)handler.HandleEvent(KVEvent(stored), w);
                (void)handler.HandleEvent(KVEvent(removed), w);
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
        services.push_back(RaceService("static-" + std::to_string(i)));
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
                    mgr, RaceService("dynamic-" + std::to_string(i)));
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
                ServiceConfig svc =
                    RaceService((w % 2 == 0) ? "shared-instance"
                                             : "instance-" + std::to_string(w));
                svc.dp_rank = round % 2;

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
