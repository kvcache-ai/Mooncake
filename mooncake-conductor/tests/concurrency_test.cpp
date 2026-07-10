// Focus tests for Stop/HandleEvent interaction and concurrent
// register/unregister, designed to run under TSAN (build-tsan) to verify
// the lock rules:
//  - handlers take the manager read lock; UnsubscribeFromService must
//    call client->Stop() OUTSIDE the manager lock (deadlock otherwise);
//  - subscribers_/active_configs_/services_ stay consistent under
//    concurrent register/unregister;
//  - PrefixCacheTable store/remove/query/global-view race freedom under
//    the hashmap_mu -> prefix_mu order.

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "conductor/common/types.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/prefix_indexer.h"
#include "event_manager_test_peer.h"

namespace {

using conductor::common::RemovedEvent;
using conductor::common::ServiceConfig;
using conductor::common::StoredEvent;
using conductor::kvevent::EventManager;
using conductor::kvevent::KVEventHandler;
using conductor::prefixindex::ModelContext;
using conductor::prefixindex::PrefixCacheTable;
using conductor::zmq::BlockRemovedEvent;
using conductor::zmq::BlockStoredEvent;
using conductor::zmq::KVEvent;

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

// Concurrent registrations of distinct and duplicate services. ZMQ
// connect is async, so subscriptions to unreachable endpoints succeed
// and exercise the full path including client startup/teardown.
TEST(Concurrency, ConcurrentRegisterUnregister) {
    EventManager mgr({}, 0);

    constexpr int kThreads = 8;
    constexpr int kRounds = 5;
    std::vector<std::thread> workers;
    for (int w = 0; w < kThreads; ++w) {
        workers.emplace_back([w] {
            // Placeholder so all threads start near-simultaneously.
            (void)w;
        });
    }
    for (auto& t : workers) t.join();
    workers.clear();

    for (int w = 0; w < kThreads; ++w) {
        workers.emplace_back([&mgr, w] {
            for (int round = 0; round < kRounds; ++round) {
                ServiceConfig svc;
                svc.endpoint = "tcp://127.0.0.1:59999";  // no publisher
                svc.replay_endpoint = "tcp://127.0.0.1:59998";
                svc.type = conductor::common::kServiceTypeVLLM;
                svc.model_name = "race-model";
                // Half the threads fight over the same key; half use
                // distinct keys.
                svc.instance_id = (w % 2 == 0)
                                      ? "shared-instance"
                                      : "instance-" + std::to_string(w);
                svc.tenant_id = "default";
                svc.block_size = 16;
                svc.dp_rank = round % 2;

                conductor::kvevent::EventManagerTestPeer::Subscribe(mgr, svc);
                conductor::kvevent::EventManagerTestPeer::Unsubscribe(
                    mgr, svc.instance_id, svc.tenant_id, svc.dp_rank);
            }
        });
    }
    for (auto& t : workers) t.join();
    mgr.Stop();
}

// PrefixCacheTable: mixed store/remove/query/global-view across threads
// sharing one ModelContext plus per-thread contexts.
TEST(Concurrency, PrefixTableMixedOperations) {
    PrefixCacheTable table;
    constexpr int kThreads = 6;
    constexpr int kRounds = 200;

    std::vector<std::thread> workers;
    for (int w = 0; w < kThreads; ++w) {
        workers.emplace_back([&table, w] {
            const std::string instance =
                (w % 2 == 0) ? "shared" : "inst-" + std::to_string(w);
            ModelContext ctx;
            ctx.model_name = "race-model";
            ctx.block_size = 4;
            ctx.tenant_id = "default";
            ctx.instance_id = instance;

            for (int round = 0; round < kRounds; ++round) {
                StoredEvent stored;
                stored.block_hashes = {
                    static_cast<uint64_t>(w * 100000 + round)};
                stored.block_size = 4;
                stored.model_name = "race-model";
                stored.instance_id = instance;
                stored.token_ids = {1, 2, 3, 4};
                stored.medium = (round % 2 == 0) ? "GPU" : "cpu";
                (void)table.ProcessStoreEvent(stored, w % 3);

                (void)table.CacheHitCompute(ctx, {1, 2, 3, 4});

                if (round % 10 == 0) {
                    (void)table.GetGlobalView();
                }

                RemovedEvent removed;
                removed.block_hashes = stored.block_hashes;
                removed.block_size = 4;
                removed.model_name = "race-model";
                removed.instance_id = instance;
                (void)table.ProcessRemoveEvent(removed, w % 3, instance);
            }
        });
    }
    for (auto& t : workers) t.join();
}

}  // namespace
