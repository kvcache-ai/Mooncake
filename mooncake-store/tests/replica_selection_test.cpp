// Copyright 2025 Mooncake Authors
//
// Unit tests for replica_selection.h: verifies the base type+locality policy is
// unchanged, and that the opt-in remote-replica scoring picks a better remote
// MEMORY replica instead of the first one the master happened to return
// (issue #2516).

#include "replica_selection.h"

#include <gtest/gtest.h>

#include <atomic>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

namespace mooncake {
namespace {

// Build a COMPLETE MEMORY replica descriptor with the given endpoint/protocol.
Replica::Descriptor MakeMemory(const std::string& endpoint,
                               const std::string& protocol,
                               ReplicaStatus status = ReplicaStatus::COMPLETE) {
    Replica::Descriptor d;
    d.id = 0;
    MemoryDescriptor mem;
    mem.buffer_descriptor.size_ = 1024;
    mem.buffer_descriptor.buffer_address_ = 0x1000;
    mem.buffer_descriptor.protocol_ = protocol;
    mem.buffer_descriptor.transport_endpoint_ = endpoint;
    d.descriptor_variant = mem;
    d.status = status;
    return d;
}

Replica::Descriptor MakeNoF(const std::string& endpoint,
                            ReplicaStatus status = ReplicaStatus::COMPLETE) {
    Replica::Descriptor d;
    d.id = 0;
    NoFDescriptor nof;
    nof.buffer_descriptor.size_ = 1024;
    nof.buffer_descriptor.buffer_address_ = 0x2000;
    nof.buffer_descriptor.protocol_ = "nvmeof";
    nof.buffer_descriptor.transport_endpoint_ = endpoint;
    d.descriptor_variant = nof;
    d.status = status;
    return d;
}

Replica::Descriptor MakeDisk(const std::string& path) {
    Replica::Descriptor d;
    d.id = 0;
    d.descriptor_variant = DiskDescriptor{path, 1024};
    d.status = ReplicaStatus::COMPLETE;
    return d;
}

Replica::Descriptor MakeLocalDisk(const std::string& endpoint) {
    Replica::Descriptor d;
    d.id = 0;
    LocalDiskDescriptor local_disk;
    local_disk.object_size = 1024;
    local_disk.transport_endpoint = endpoint;
    d.descriptor_variant = local_disk;
    d.status = ReplicaStatus::COMPLETE;
    return d;
}

Replica::Descriptor MakeDfs(const std::string& path) {
    Replica::Descriptor d;
    d.id = 0;
    d.descriptor_variant = DistributedFSDescriptor{path, 0, 1024, 4096, 0};
    d.status = ReplicaStatus::COMPLETE;
    return d;
}

// A test fixture that guarantees scoring state is reset between tests, since
// the enable flag / injected scorer are process-wide.
class ReplicaSelectionTest : public ::testing::Test {
   protected:
    void TearDown() override { SetRemoteReplicaScorer(nullptr); }
};

// --- Base policy (scoring off): behaviour must be unchanged --------------

TEST_F(ReplicaSelectionTest, LocalMemoryAlwaysWins) {
    std::unordered_set<std::string> local = {"nodeB"};
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma"),
        MakeMemory("nodeB", "tcp"),  // local, slower protocol
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeB");  // locality beats protocol
}

TEST_F(ReplicaSelectionTest, ScoringOffKeepsFirstRemoteMemory) {
    // No scorer injected and env not set -> must return the FIRST remote
    // MEMORY.
    ASSERT_FALSE(RemoteReplicaScoringEnabled());
    std::unordered_set<std::string> local;  // nothing local
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "tcp"),   // first
        MakeMemory("nodeB", "rdma"),  // "better" but must be ignored when off
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeA");
}

// --- Opt-in scoring: pick the better remote replica ---------------------

TEST_F(ReplicaSelectionTest, InjectedScorerPicksLowestScore) {
    // Inject a scorer that prefers "nodeB" regardless of order.
    SetRemoteReplicaScorer([](const Replica::Descriptor& r) {
        const auto& ep =
            r.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
        return ep == "nodeB" ? 0.0 : 10.0;
    });
    ASSERT_TRUE(RemoteReplicaScoringEnabled());

    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma"),  // first, but higher score
        MakeMemory("nodeB", "rdma"),  // lower score -> should win
        MakeMemory("nodeC", "rdma"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeB");
}

TEST_F(ReplicaSelectionTest, BuiltinScorerPrefersRdmaOverTcp) {
    // Built-in scorer active via injected scorer? No — use it directly by
    // enabling through injection of the built-in. Simulate env-on path by
    // injecting the built-in function.
    SetRemoteReplicaScorer(BuiltinRemoteReplicaScore);
    ASSERT_TRUE(RemoteReplicaScoringEnabled());

    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "tcp"),   // first, but tcp
        MakeMemory("nodeB", "rdma"),  // rdma -> preferred
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeB");
}

TEST_F(ReplicaSelectionTest, BuiltinScorerRanksUnknownAndNonMemory) {
    EXPECT_DOUBLE_EQ(BuiltinRemoteReplicaScore(MakeMemory("nodeA", "ucx")),
                     2.0);
    EXPECT_DOUBLE_EQ(BuiltinRemoteReplicaScore(MakeNoF("nodeB")), 100.0);
}

TEST_F(ReplicaSelectionTest, EnvironmentOptInUsesBuiltinScorer) {
    const char* env = std::getenv("MC_STORE_REPLICA_SCORING");
    if (env == nullptr || std::string(env) != "1") {
        GTEST_SKIP() << "covered by the env-enabled CTest process";
    }

    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "tcp"),
        MakeMemory("nodeB", "rdma"),
    };

    EXPECT_TRUE(RemoteReplicaScoringEnabled());
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(sel->get_memory_descriptor().buffer_descriptor.protocol_, "rdma");
}

TEST_F(ReplicaSelectionTest, ScorerTieKeepsMasterOrder) {
    // All equal score -> strictly-less comparison keeps the first one.
    SetRemoteReplicaScorer([](const Replica::Descriptor&) { return 5.0; });
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma"),
        MakeMemory("nodeB", "rdma"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeA");
}

TEST_F(ReplicaSelectionTest, ScorerSkipsIncompleteReplicas) {
    SetRemoteReplicaScorer([](const Replica::Descriptor& r) {
        const auto& ep =
            r.get_memory_descriptor().buffer_descriptor.transport_endpoint_;
        return ep == "nodeB" ? 0.0 : 10.0;
    });
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma"),
        MakeMemory("nodeB", "rdma",
                   ReplicaStatus::PROCESSING),  // best score
                                                // but not ready
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeA");  // nodeB skipped -> falls back to only COMPLETE one
}

TEST_F(ReplicaSelectionTest, LocalStillWinsWhenScoringOn) {
    SetRemoteReplicaScorer(BuiltinRemoteReplicaScore);
    std::unordered_set<std::string> local = {"nodeB"};
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma"),  // remote, best protocol
        MakeMemory("nodeB", "tcp"),   // local -> must still win over scoring
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(
        sel->get_memory_descriptor().buffer_descriptor.transport_endpoint_,
        "nodeB");
}

TEST_F(ReplicaSelectionTest, PickBestRemoteMemoryCanFindNoCandidate) {
    SetRemoteReplicaScorer(BuiltinRemoteReplicaScore);
    std::unordered_set<std::string> local = {"nodeA"};
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma"),
        MakeMemory("nodeB", "rdma", ReplicaStatus::PROCESSING),
        MakeNoF("nodeC"),
    };
    EXPECT_EQ(PickBestRemoteMemory(reps, local), nullptr);
}

// --- Non-MEMORY fallbacks: preserve the complete historical policy -------

TEST_F(ReplicaSelectionTest, LocalNoFPrecedesRemoteNoF) {
    std::unordered_set<std::string> local = {"nodeB"};
    std::vector<Replica::Descriptor> reps = {
        MakeNoF("nodeA"),
        MakeNoF("nodeB"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(sel->get_nof_descriptor().buffer_descriptor.transport_endpoint_,
              "nodeB");
}

TEST_F(ReplicaSelectionTest, RemoteNoFFallbackKeepsMasterOrder) {
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeNoF("nodeA"),
        MakeNoF("nodeB"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_EQ(sel->get_nof_descriptor().buffer_descriptor.transport_endpoint_,
              "nodeA");
}

TEST_F(ReplicaSelectionTest, LocalDiskPrecedesDisk) {
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeDisk("/remote/object"),
        MakeLocalDisk("nodeA"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_TRUE(sel->is_local_disk_replica());
}

TEST_F(ReplicaSelectionTest, DiskIsLastCompleteFallback) {
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeDisk("/remote/object"),
        MakeMemory("nodeA", "rdma", ReplicaStatus::FAILED),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_TRUE(sel->is_disk_replica());
}

TEST_F(ReplicaSelectionTest, DfsPrecedesDisk) {
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeDisk("/remote/object"),
        MakeDfs("/dfs/object"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_TRUE(sel->is_dfs_replica());
}

TEST_F(ReplicaSelectionTest, LocalDiskPrecedesDfs) {
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeDfs("/dfs/object"),
        MakeLocalDisk("nodeA"),
    };
    const auto* sel = SelectBestReplica(reps, local);
    ASSERT_NE(sel, nullptr);
    EXPECT_TRUE(sel->is_local_disk_replica());
}

TEST_F(ReplicaSelectionTest, NoCompleteReplicaReturnsNull) {
    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "rdma", ReplicaStatus::PROCESSING),
        MakeNoF("nodeB", ReplicaStatus::FAILED),
    };
    EXPECT_EQ(SelectBestReplica(reps, local), nullptr);
}

// --- Concurrency: verify no data race on SetRemoteReplicaScorer vs reads ---

TEST_F(ReplicaSelectionTest, ConcurrentSetAndSelectIsRaceFree) {
    constexpr int kIterations = 50000;
    constexpr int kReaderThreads = 4;

    std::unordered_set<std::string> local;
    std::vector<Replica::Descriptor> reps = {
        MakeMemory("nodeA", "tcp"),
        MakeMemory("nodeB", "rdma"),
        MakeMemory("nodeC", "rdma"),
    };

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<bool> selection_failed{false};
    std::atomic<int> read_count{0};

    auto wait_for_start = [&] {
        ready.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    };

    // Writer: repeatedly swap scorers while readers are active.
    std::thread writer([&] {
        wait_for_start();
        for (int i = 0; i < kIterations; ++i) {
            if (i % 2 == 0) {
                SetRemoteReplicaScorer([](const Replica::Descriptor& r) {
                    const auto& proto =
                        r.get_memory_descriptor().buffer_descriptor.protocol_;
                    return proto == "rdma" ? 0.0 : 10.0;
                });
            } else {
                SetRemoteReplicaScorer(nullptr);
            }
        }
    });

    // Readers: call SelectBestReplica (which reads the scorer) concurrently.
    std::vector<std::thread> readers;
    for (int t = 0; t < kReaderThreads; ++t) {
        readers.emplace_back([&] {
            wait_for_start();
            for (int i = 0; i < kIterations; ++i) {
                const auto* sel = SelectBestReplica(reps, local);
                if (sel == nullptr) {
                    selection_failed.store(true, std::memory_order_relaxed);
                }
                read_count.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    while (ready.load(std::memory_order_acquire) < kReaderThreads + 1) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);

    writer.join();
    for (auto& r : readers) r.join();

    EXPECT_FALSE(selection_failed.load());
    EXPECT_EQ(read_count.load(), kIterations * kReaderThreads);
}

}  // namespace
}  // namespace mooncake
