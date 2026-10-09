// C ABI (conductor_c.h) tests: argument handling without a server, then a
// round trip against an in-process EventManager whose results are compared
// with the C++ ConductorClient field by field.

#include "conductor/client/conductor_c.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "conductor/client/conductor_client.h"
#include "conductor/common/types.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"

namespace mooncake::conductor {
namespace {

using prefixindex::ContextKey;
using prefixindex::HashBlock;
using prefixindex::HashProfile;
using prefixindex::ProjectedPrefix;
using prefixindex::StorageTier;

// Distinct from client_server_e2e_test's port so the two suites never contend.
constexpr int kRpcPort = 19335;

struct HandleDeleter {
    void operator()(void* handle) const { mooncake_conductor_destroy(handle); }
};
using Handle = std::unique_ptr<void, HandleDeleter>;

Handle MakeHandle() {
    Handle handle(mooncake_conductor_create());
    EXPECT_NE(handle.get(), nullptr);
    return handle;
}

TEST(ConductorCApiTest, HealthIsNotInitializedBeforeSetup) {
    EXPECT_EQ(mooncake_conductor_health_check(nullptr),
              MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED);
    Handle handle = MakeHandle();
    EXPECT_EQ(mooncake_conductor_health_check(handle.get()),
              MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED);
    // close/destroy on a NULL or never-connected handle are no-ops.
    mooncake_conductor_close(nullptr);
    mooncake_conductor_close(handle.get());
    mooncake_conductor_destroy(nullptr);
}

TEST(ConductorCApiTest, CallsBeforeSetupReturnUnavailable) {
    Handle handle = MakeHandle();
    const int32_t tokens[] = {1, 2, 3};
    mooncake_conductor_query_request_t request{};
    request.model_name = "m";
    request.block_size = 16;
    request.token_ids = tokens;
    request.token_count = 3;
    mooncake_conductor_query_result_t result{};
    EXPECT_EQ(mooncake_conductor_query(handle.get(), &request, &result),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
    EXPECT_EQ(result.hits, nullptr);
    EXPECT_EQ(result.hit_count, 0u);

    mooncake_conductor_service_config_t config{};
    int is_new = -1;
    EXPECT_EQ(mooncake_conductor_register(handle.get(), &config, &is_new),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
    EXPECT_EQ(is_new, 0);
    EXPECT_EQ(mooncake_conductor_unregister(handle.get(), "i", nullptr, 0),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);

    mooncake_conductor_global_view_t view{};
    EXPECT_EQ(mooncake_conductor_get_global_view(handle.get(), &view),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
    EXPECT_EQ(view.context_count, 0u);
    mooncake_conductor_service_list_t list{};
    EXPECT_EQ(mooncake_conductor_list_services(handle.get(), &list),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
    EXPECT_EQ(list.service_count, 0u);

    // Freeing a zeroed (failed) output is harmless, as are NULL pointers.
    mooncake_conductor_query_result_free(&result);
    mooncake_conductor_global_view_free(&view);
    mooncake_conductor_service_list_free(&list);
    mooncake_conductor_query_result_free(nullptr);
    mooncake_conductor_global_view_free(nullptr);
    mooncake_conductor_service_list_free(nullptr);
}

TEST(ConductorCApiTest, InvalidArgumentsAreRejectedLocally) {
    Handle handle = MakeHandle();
    mooncake_conductor_query_request_t request{};
    mooncake_conductor_query_result_t result{};
    mooncake_conductor_service_config_t config{};
    mooncake_conductor_global_view_t view{};
    mooncake_conductor_service_list_t list{};

    EXPECT_EQ(mooncake_conductor_setup(nullptr, "127.0.0.1:1", 100, 100),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_setup(handle.get(), nullptr, 100, 100),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    // ConductorClient::Setup rejects an address without a port separator.
    EXPECT_EQ(mooncake_conductor_setup(handle.get(), "noport", 100, 100),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);

    EXPECT_EQ(mooncake_conductor_query(nullptr, &request, &result),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_query(handle.get(), nullptr, &result),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_query(handle.get(), &request, nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    request.token_count = 1;  // with token_ids == NULL
    EXPECT_EQ(mooncake_conductor_query(handle.get(), &request, &result),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);

    EXPECT_EQ(mooncake_conductor_register(nullptr, &config, nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_register(handle.get(), nullptr, nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    // publisher_kind is validated before any RPC is attempted.
    config.publisher_kind = 42;
    EXPECT_EQ(mooncake_conductor_register(handle.get(), &config, nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);

    EXPECT_EQ(mooncake_conductor_unregister(nullptr, "i", "default", 0),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_get_global_view(nullptr, &view),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_get_global_view(handle.get(), nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_list_services(nullptr, &list),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_list_services(handle.get(), nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
}

TEST(ConductorCApiTest, SetupAgainstDeadServerReturnsRpcFail) {
    Handle handle = MakeHandle();
    EXPECT_EQ(mooncake_conductor_setup(handle.get(), "127.0.0.1:1", 200, 200),
              MOONCAKE_CONDUCTOR_RPC_FAIL);
    // A failed setup does not count as initialized.
    EXPECT_EQ(mooncake_conductor_health_check(handle.get()),
              MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED);
}

ContextKey TestContext() {
    return {.tenant_id = "default",
            .model_name = "c-abi-model",
            .lora_name = "",
            .block_size = 16};
}

HashProfile TestProfile() {
    const common::HashProfileConfig source{
        .strategy = "vllm_v1",
        .algorithm = "sha256_cbor",
        .python_hash_seed = "0",
        .index_projection = "low64_be",
    };
    HashProfile profile;
    EXPECT_EQ(prefixindex::ResolveHashProfile(source, &profile), "");
    return profile;
}

std::vector<int32_t> Sequence(int32_t first, size_t count) {
    std::vector<int32_t> values;
    values.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        values.push_back(first + static_cast<int32_t>(index));
    }
    return values;
}

std::vector<ProjectedPrefix> ProjectedFor(const ContextKey& context,
                                          const HashProfile& profile,
                                          const std::vector<int32_t>& tokens) {
    std::string error;
    auto strategy = prefixindex::CreateHashStrategy(profile, &error);
    EXPECT_NE(strategy, nullptr) << error;
    if (!strategy) {
        return {};
    }
    std::vector<HashBlock> blocks;
    error = strategy->Compute(context, tokens, std::nullopt, &blocks);
    EXPECT_TRUE(error.empty()) << error;
    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const auto& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

// Same seeding as client_server_e2e_test: engine "c-abi-engine" (rank 0) holds
// the first two blocks on device; the shared pool holds all three on cpu_share
// and disk.
void SeedEngine(kvevent::EventManager& manager,
                const std::vector<int32_t>& tokens) {
    const ContextKey context = TestContext();
    const HashProfile profile = TestProfile();
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register({.context = context,
                                .profile = profile,
                                .instance_id = "c-abi-engine",
                                .dp_rank = 0,
                                .effective_block_size = context.block_size})
                    .error.empty());
    const auto prefixes = ProjectedFor(context, profile, tokens);
    ASSERT_EQ(prefixes.size(), 3u);
    ASSERT_TRUE(manager.GetIndexer()
                    ->StoreEngine({.context = context,
                                   .prefixes = {prefixes[0], prefixes[1]},
                                   .owner = {.source_stream = "c-abi-stream",
                                             .instance_id = "c-abi-engine",
                                             .dp_rank = 0},
                                   .effective_block_size = context.block_size})
                    .empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->StoreShared({.context = context,
                                   .prefixes = prefixes,
                                   .tier = StorageTier::kCpuShare,
                                   .owner = {.source_stream = "c-abi-pool",
                                             .backend_id = "cpu-backend",
                                             .object_id = "cpu-object"},
                                   .effective_block_size = context.block_size})
                    .empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->StoreShared({.context = context,
                                   .prefixes = prefixes,
                                   .tier = StorageTier::kDisk,
                                   .owner = {.source_stream = "c-abi-pool",
                                             .backend_id = "disk-backend",
                                             .object_id = "disk-object"},
                                   .effective_block_size = context.block_size})
                    .empty());
}

// The RPC server binds inside async_start; retry briefly before failing.
int SetupWithRetry(mooncake_conductor_t handle, const std::string& addr) {
    int rc = MOONCAKE_CONDUCTOR_RPC_FAIL;
    for (int attempt = 0; attempt < 40; ++attempt) {
        rc = mooncake_conductor_setup(handle, addr.c_str(), 1000, 3000);
        if (rc == MOONCAKE_CONDUCTOR_OK) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return rc;
}

// C-side strings are never NULL on success; compare them as std::string.
std::string Str(const char* s) { return s ? std::string(s) : "<null>"; }

// Every operation goes through the C ABI and is checked against the C++
// ConductorClient talking to the same in-process server.
TEST(ConductorCApiTest, RoundTripMatchesCppClient) {
    kvevent::EventManager manager({}, /*http_server_port=*/0, kRpcPort);
    ASSERT_TRUE(manager.StartHTTPServer());
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    const std::string addr = "127.0.0.1:" + std::to_string(kRpcPort);

    Handle handle = MakeHandle();
    ASSERT_EQ(SetupWithRetry(handle.get(), addr), MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(mooncake_conductor_health_check(handle.get()),
              MOONCAKE_CONDUCTOR_HEALTH_OK);
    ConductorClient reference;
    ASSERT_TRUE(reference.Setup({addr}).has_value());

    const std::vector<int32_t> tokens = Sequence(1, 48);
    ASSERT_NO_FATAL_FAILURE(SeedEngine(manager, tokens));

    // Query: tenant_id NULL normalizes to "default" like the C++ request.
    mooncake_conductor_query_request_t request{};
    request.model_name = "c-abi-model";
    request.block_size = 16;
    request.token_ids = tokens.data();
    request.token_count = tokens.size();
    QueryRequest cpp_request;
    cpp_request.context = TestContext();
    cpp_request.token_ids = tokens;
    auto expected = reference.Query(cpp_request);
    ASSERT_TRUE(expected.has_value());
    ASSERT_EQ(expected->instances.size(), 1u);
    const auto& expected_hit = expected->instances.begin()->second;

    mooncake_conductor_query_result_t result{};
    ASSERT_EQ(mooncake_conductor_query(handle.get(), &request, &result),
              MOONCAKE_CONDUCTOR_OK);
    ASSERT_EQ(result.hit_count, 1u);
    const auto& hit = result.hits[0];
    EXPECT_EQ(Str(hit.instance_id), expected->instances.begin()->first);
    EXPECT_EQ(hit.longest_matched, 48);
    EXPECT_EQ(hit.device, 32);
    EXPECT_EQ(hit.cpu_share, 48);
    EXPECT_EQ(hit.disk, 48);
    EXPECT_EQ(hit.longest_matched, expected_hit.longest_match_tokens);
    EXPECT_EQ(hit.device, expected_hit.device);
    EXPECT_EQ(hit.cpu_local, expected_hit.cpu_local);
    EXPECT_EQ(hit.cpu_share, expected_hit.cpu_share);
    EXPECT_EQ(hit.disk, expected_hit.disk);
    std::map<int64_t, int64_t> dp;
    for (size_t i = 0; i < hit.dp_count; ++i) {
        dp.emplace(hit.dp[i].rank, hit.dp[i].tokens);
    }
    EXPECT_EQ(dp, expected_hit.dp);
    ASSERT_EQ(hit.rank_match_count, expected_hit.rank_matches.size());
    for (size_t i = 0; i < hit.rank_match_count; ++i) {
        const auto& match = hit.rank_matches[i];
        const auto it = expected_hit.rank_matches.find(match.rank);
        ASSERT_NE(it, expected_hit.rank_matches.end()) << match.rank;
        EXPECT_EQ(match.device, it->second.device);
        EXPECT_EQ(match.cpu_local, it->second.cpu_local);
        EXPECT_EQ(match.cpu_share, it->second.cpu_share);
        EXPECT_EQ(match.disk, it->second.disk);
    }
    mooncake_conductor_query_result_free(&result);
    EXPECT_EQ(result.hits, nullptr);
    EXPECT_EQ(result.hit_count, 0u);

    // instance_filter narrows the result; an unknown instance yields no hits.
    request.instance_filter = "no-such-instance";
    ASSERT_EQ(mooncake_conductor_query(handle.get(), &request, &result),
              MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(result.hit_count, 0u);
    mooncake_conductor_query_result_free(&result);
    request.instance_filter = "c-abi-engine";
    ASSERT_EQ(mooncake_conductor_query(handle.get(), &request, &result),
              MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(result.hit_count, 1u);
    mooncake_conductor_query_result_free(&result);

    // Register: same context as the seeded engine, so GlobalView merges them.
    // The endpoint is never served; ZMQ connects lazily.
    mooncake_conductor_service_config_t config{};
    config.instance_id = "c-abi-svc";
    config.endpoint = "tcp://127.0.0.1:29431";
    config.publisher_kind = MOONCAKE_CONDUCTOR_PUBLISHER_VLLM;
    config.model_name = "c-abi-model";
    config.block_size = 16;
    config.hash_profile = {.strategy = "vllm_v1",
                           .algorithm = "sha256_cbor",
                           .python_hash_seed = "0",
                           .index_projection = "low64_be"};
    int is_new = -1;
    ASSERT_EQ(mooncake_conductor_register(handle.get(), &config, &is_new),
              MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(is_new, 1);
    ASSERT_EQ(mooncake_conductor_register(handle.get(), &config, &is_new),
              MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(is_new, 0);

    auto expected_services = reference.ListServices();
    ASSERT_TRUE(expected_services.has_value());
    ASSERT_EQ(expected_services->size(), 1u);
    const auto& expected_svc = expected_services->front();
    mooncake_conductor_service_list_t list{};
    ASSERT_EQ(mooncake_conductor_list_services(handle.get(), &list),
              MOONCAKE_CONDUCTOR_OK);
    ASSERT_EQ(list.service_count, 1u);
    const auto& svc = list.services[0];
    EXPECT_EQ(Str(svc.instance_id), "c-abi-svc");
    EXPECT_EQ(Str(svc.tenant_id), "default");
    EXPECT_EQ(svc.publisher_kind, MOONCAKE_CONDUCTOR_PUBLISHER_VLLM);
    EXPECT_EQ(svc.has_cache_group, 0);
    EXPECT_FALSE(Str(svc.hash_profile.root_digest).empty());
    EXPECT_EQ(Str(svc.instance_id), expected_svc.instance_id);
    EXPECT_EQ(Str(svc.endpoint), expected_svc.endpoint);
    EXPECT_EQ(Str(svc.replay_endpoint), expected_svc.replay_endpoint);
    EXPECT_EQ(Str(svc.model_name), expected_svc.model_name);
    EXPECT_EQ(Str(svc.lora_name), expected_svc.lora_name);
    EXPECT_EQ(Str(svc.tenant_id), expected_svc.tenant_id);
    EXPECT_EQ(svc.block_size, expected_svc.block_size);
    EXPECT_EQ(svc.dp_rank, expected_svc.dp_rank);
    EXPECT_EQ(svc.has_cache_group != 0, expected_svc.cache_group.has_value());
    EXPECT_EQ(Str(svc.hash_profile.strategy),
              expected_svc.hash_profile.strategy);
    EXPECT_EQ(Str(svc.hash_profile.algorithm),
              expected_svc.hash_profile.algorithm);
    EXPECT_EQ(Str(svc.hash_profile.python_hash_seed),
              expected_svc.hash_profile.python_hash_seed);
    EXPECT_EQ(Str(svc.hash_profile.root_digest),
              expected_svc.hash_profile.root_digest);
    EXPECT_EQ(Str(svc.hash_profile.index_projection),
              expected_svc.hash_profile.index_projection);
    mooncake_conductor_service_list_free(&list);
    EXPECT_EQ(list.services, nullptr);

    // GlobalView: one context holding the seeded engine and the registered
    // service.
    auto expected_view = reference.GetGlobalView();
    ASSERT_TRUE(expected_view.has_value());
    ASSERT_EQ(expected_view->contexts.size(), 1u);
    const auto& expected_context = expected_view->contexts.front();
    mooncake_conductor_global_view_t view{};
    ASSERT_EQ(mooncake_conductor_get_global_view(handle.get(), &view),
              MOONCAKE_CONDUCTOR_OK);
    ASSERT_EQ(view.context_count, 1u);
    const auto& context = view.contexts[0];
    EXPECT_EQ(context.prefix_count, 3u);
    EXPECT_EQ(context.instance_count, 2u);
    EXPECT_EQ(Str(context.model_name), expected_context.context.model_name);
    EXPECT_EQ(Str(context.lora_name), expected_context.context.lora_name);
    EXPECT_EQ(Str(context.tenant_id), expected_context.context.tenant_id);
    EXPECT_EQ(context.block_size, expected_context.context.block_size);
    EXPECT_EQ(context.prefix_count, expected_context.prefix_count);
    EXPECT_EQ(Str(context.hash_profile.root_digest),
              expected_context.profile.root_digest);
    EXPECT_EQ(Str(context.hash_profile.strategy),
              expected_context.profile.strategy);
    std::map<std::string, std::set<int64_t>> instance_ranks;
    for (size_t i = 0; i < context.instance_count; ++i) {
        const auto& instance = context.instances[i];
        instance_ranks[Str(instance.instance_id)].insert(
            instance.ranks, instance.ranks + instance.rank_count);
    }
    EXPECT_EQ(instance_ranks, expected_context.instance_ranks);
    mooncake_conductor_global_view_free(&view);
    EXPECT_EQ(view.contexts, nullptr);
    EXPECT_EQ(view.context_count, 0u);

    // Unregister: NULL tenant normalizes to "default"; a second call misses.
    EXPECT_EQ(
        mooncake_conductor_unregister(handle.get(), "c-abi-svc", nullptr, 0),
        MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(
        mooncake_conductor_unregister(handle.get(), "c-abi-svc", nullptr, 0),
        MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND);
    ASSERT_EQ(mooncake_conductor_list_services(handle.get(), &list),
              MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(list.service_count, 0u);
    mooncake_conductor_service_list_free(&list);

    // After close the handle reports UNREACHABLE (setup was once called) and
    // operations fail until setup is called again.
    mooncake_conductor_close(handle.get());
    EXPECT_EQ(mooncake_conductor_health_check(handle.get()),
              MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE);
    EXPECT_EQ(mooncake_conductor_query(handle.get(), &request, &result),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
    ASSERT_EQ(mooncake_conductor_setup(handle.get(), addr.c_str(), 1000, 3000),
              MOONCAKE_CONDUCTOR_OK);
    EXPECT_EQ(mooncake_conductor_health_check(handle.get()),
              MOONCAKE_CONDUCTOR_HEALTH_OK);

    handle.reset();
    reference.Close();
    manager.Stop();
}

}  // namespace
}  // namespace mooncake::conductor
