#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include <unistd.h>

#include "master_service/dsl/scenario.h"

namespace mooncake::test {
namespace {

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        const char* previous = ::getenv(name_.c_str());
        if (previous != nullptr) {
            previous_value_ = previous;
        }
        ::setenv(name_.c_str(), value, 1);
    }

    ~ScopedEnvVar() {
        if (previous_value_.has_value()) {
            ::setenv(name_.c_str(), previous_value_->c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::optional<std::string> previous_value_;
};

// The DFS backend reads its configuration from the environment when the
// MasterService is constructed, which the scenario defers until its first
// action; setting the variables in the test body therefore configures the
// scenario's service exactly as it configured a directly constructed one.
class ScopedDfsEnvironment {
   public:
    ScopedDfsEnvironment(const std::string& tag, const char* shard_capacity)
        : root_((std::filesystem::temp_directory_path() /
                 (tag + "_" + std::to_string(::getpid())))
                    .string()),
          enable_dfs_("MOONCAKE_ENABLE_DFS", "1"),
          fs_adapter_("MOONCAKE_DFS_FS_ADAPTER", "posix"),
          root_dir_("MOONCAKE_DFS_ROOT_DIR", root_.c_str()),
          shard_count_("MOONCAKE_DFS_SHARD_COUNT", "1"),
          shard_capacity_("MOONCAKE_DFS_SHARD_CAPACITY", shard_capacity),
          alignment_("MOONCAKE_DFS_ALIGNMENT", "4096"),
          // Keep the background path disabled so tests drive one exact
          // transaction through the EvictDfs action.
          eviction_("MOONCAKE_DFS_EVICTION_ENABLED", "0"),
          high_watermark_("MOONCAKE_DFS_EVICTION_HIGH_WATERMARK", "0.9"),
          low_watermark_("MOONCAKE_DFS_EVICTION_LOW_WATERMARK", "0.7"),
          deferred_free_("MOONCAKE_DFS_DEFERRED_FREE_SECONDS", "0"),
          single_tenant_("MOONCAKE_DFS_SINGLE_TENANT", "true") {
        std::filesystem::create_directories(root_);
    }

    ~ScopedDfsEnvironment() {
        std::error_code error;
        std::filesystem::remove_all(root_, error);
    }

   private:
    std::string root_;
    ScopedEnvVar enable_dfs_;
    ScopedEnvVar fs_adapter_;
    ScopedEnvVar root_dir_;
    ScopedEnvVar shard_count_;
    ScopedEnvVar shard_capacity_;
    ScopedEnvVar alignment_;
    ScopedEnvVar eviction_;
    ScopedEnvVar high_watermark_;
    ScopedEnvVar low_watermark_;
    ScopedEnvVar deferred_free_;
    ScopedEnvVar single_tenant_;
};

std::string DfsEvictionKey(size_t index) {
    return "dfs_evict_" + std::to_string(index);
}

// One prepared batch of four dual-replica objects, an optional set of leased
// keys, one eviction transaction, and the per-key survivor counts. A leased
// key is a rejected candidate; eviction must skip it without rolling back the
// accepted candidates that share its prepared batch.
void RunDfsEvictionCase(
    const std::string& case_name, const std::vector<size_t>& leased_indexes,
    const std::vector<std::optional<size_t>>& expected_replica_counts,
    bool evict_memory_first = false) {
    const ScopedDfsEnvironment dfs("master_dfs_scenario_evict_" + case_name,
                                   "32768");
    MasterScenario scenario("DFS eviction case " + case_name);
    scenario.Given(MemoryNode("memory"));
    for (size_t index = 0; index < 4; ++index) {
        scenario.When(PutStart(DfsEvictionKey(index), 100).DfsReplicas(1));
        scenario.When(PutEnd(DfsEvictionKey(index)).OfType(ReplicaType::ALL));
    }
    for (const size_t index : leased_indexes) {
        scenario.Then(Object(DfsEvictionKey(index)).IsReadable());
    }
    if (evict_memory_first) {
        scenario.When(EvictMemory(1.0));
    }
    scenario.When(EvictDfs());
    for (size_t index = 0; index < expected_replica_counts.size(); ++index) {
        const auto& expected = expected_replica_counts[index];
        if (expected.has_value()) {
            scenario.Then(Object(DfsEvictionKey(index)).HasReplicas(*expected));
        } else {
            scenario.Then(Object(DfsEvictionKey(index)).DoesNotExist());
        }
    }
    if (evict_memory_first) {
        scenario.When(PutStart("dfs_evict_reclaimed", 100).DfsReplicas(1));
        scenario.When(
            PutRevoke("dfs_evict_reclaimed").OfType(ReplicaType::ALL));
    }
}

}  // namespace

TEST(MasterServiceDfsScenarioTest, PutEndAllAndMismatchedUpsertAreAtomic) {
    const ScopedDfsEnvironment dfs("master_dfs_scenario_atomic", "1048576");
    MasterScenario(
        "PutEnd(ALL) commits both replicas together and a "
        "mismatched upsert changes neither")
        .Given(MemoryNode("memory"))
        .When(PutStart("dfs_atomic", 4_KB).DfsReplicas(1).ExpectReplicas(2))
        .When(PutEnd("dfs_atomic").OfType(ReplicaType::ALL))
        .Then(Object("dfs_atomic").HasReplicas(2).HasCompleteReplicas(2))
        .When(UpsertStart("dfs_atomic", 4_KB)
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .Then(Object("dfs_atomic").HasReplicas(2).HasCompleteReplicas(2))
        .When(PutStart("dfs_revoke", 4_KB).DfsReplicas(1))
        .When(PutRevoke("dfs_revoke").OfType(ReplicaType::ALL))
        .Then(Object("dfs_revoke").DoesNotExist());
}

TEST(MasterServiceDfsScenarioTest, FailedDfsPutStartDoesNotLeakTenantQuota) {
    const ScopedDfsEnvironment dfs("master_dfs_scenario_quota", "1048576");
    // The DFS shard holds 1 MB, so a 4 MB dual-replica put fails admission.
    // The follow-up memory-only put of the full 4 MB quota succeeds, which it
    // could not if the failed attempt had leaked a quota charge.
    MasterScenario("a DFS put that fails admission releases its quota charge")
        .Given(MemoryNode("memory"))
        .Given(Tenant(TenantId::Default().value()).Quota(4096_KB))
        .When(PutStart("dfs_quota_failure", 4096_KB)
                  .DfsReplicas(1)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(PutStart("quota_after_dfs_failure", 4096_KB))
        .When(PutRevoke("quota_after_dfs_failure").OfType(ReplicaType::ALL));
}

TEST(MasterServiceDfsScenarioTest, EvictionCommitsUnleasedCandidates) {
    RunDfsEvictionCase("commit", {}, {1, 1, 2, 2});
}

TEST(MasterServiceDfsScenarioTest, EvictionRejectsAllLeasedCandidates) {
    RunDfsEvictionCase("reject", {0, 1, 2, 3}, {2, 2, 2, 2});
}

TEST(MasterServiceDfsScenarioTest, EvictionSplitsSharedPreparedBatch) {
    // Key 1 shares the first prepared batch with key 0. Rejecting key 1 must
    // not roll back key 0, and the same high-watermark trigger must continue
    // to key 2 so the shard reaches its low watermark.
    RunDfsEvictionCase("mixed", {1}, {1, 2, 1, 2});
}

TEST(MasterServiceDfsScenarioTest, EvictionReclaimsLastReplicas) {
    // Memory eviction may leave DFS as the only remaining replica. DFS
    // eviction must still reclaim those allocations and erase metadata for
    // objects whose final replica was removed.
    RunDfsEvictionCase("last_replica", {},
                       {std::nullopt, std::nullopt, size_t{1}, size_t{1}},
                       /*evict_memory_first=*/true);
}

}  // namespace mooncake::test
