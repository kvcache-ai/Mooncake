// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "conductor/client/conductor_c.h"
#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "prefix_indexer_test_peer.h"

namespace {
namespace mc = mooncake::conductor;
mooncake_conductor_string S(const std::string& s) {
    return {s.data(), s.size()};
}
std::string Copy(mooncake_conductor_string s) {
    return std::string(s.data, s.size);
}
using Client = std::unique_ptr<mooncake_conductor_client,
                               decltype(&mooncake_conductor_destroy)>;

TEST(ConductorC, InvalidInputsAndUninitializedErrors) {
    mooncake_conductor_client_t raw = nullptr;
    ASSERT_EQ(mooncake_conductor_create(&raw), 0);
    Client client(raw, mooncake_conductor_destroy);
    mooncake_conductor_query_request req{};
    auto* out = reinterpret_cast<mooncake_conductor_query_result*>(1);
    EXPECT_EQ(mooncake_conductor_query(raw, &req, nullptr, 0, &out), -2000);
    EXPECT_EQ(out, nullptr);
    EXPECT_EQ(mooncake_conductor_query(raw, &req, nullptr, 1, &out), -600);
    EXPECT_EQ(out, nullptr);
    req.model_name = {nullptr, 2};
    EXPECT_EQ(mooncake_conductor_query(raw, &req, nullptr, 0, &out), -600);
    req.model_name = {"", SIZE_MAX};
    EXPECT_EQ(mooncake_conductor_query(raw, &req, nullptr, 0, &out), -600);
    EXPECT_EQ(mooncake_conductor_create(nullptr), -600);
    EXPECT_EQ(mooncake_conductor_close(raw), 0);
    EXPECT_EQ(mooncake_conductor_close(raw), 0);
    EXPECT_EQ(mooncake_conductor_destroy(nullptr), 0);
    mooncake_conductor_query_free(nullptr);
    mooncake_conductor_services_free(nullptr);
    mooncake_conductor_global_free(nullptr);
    mooncake_conductor_registration_free(nullptr);
    mooncake_conductor_unregistration_free(nullptr);
    const mooncake_conductor_cache_hit* hits =
        reinterpret_cast<const mooncake_conductor_cache_hit*>(1);
    size_t count = 99;
    EXPECT_EQ(mooncake_conductor_query_view(nullptr, &hits, &count), -600);
    EXPECT_EQ(hits, nullptr);
    EXPECT_EQ(count, 0u);
}

TEST(ConductorC, NestedViewsOwnDataAfterClientClose) {
    mc::kvevent::EventManager manager({}, 0, 19335);
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    mooncake_conductor_client_t raw = nullptr;
    ASSERT_EQ(mooncake_conductor_create(&raw), 0);
    Client client(raw, mooncake_conductor_destroy);
    const std::string address = "127.0.0.1:19335";
    mooncake_conductor_client_config cfg{S(address), 1000, 3000};
    int32_t rc = -1;
    for (int i = 0; i < 40 && rc != 0; ++i) {
        rc = mooncake_conductor_setup(raw, &cfg);
        if (rc) std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    ASSERT_EQ(rc, 0);
    int32_t health = -1;
    ASSERT_EQ(mooncake_conductor_health_check(raw, &health), 0);
    EXPECT_EQ(health, 0);
    const std::string id("engine\0suffix", 13), model("model\0suffix", 12);
    const std::string endpoint = "tcp://127.0.0.1:29335";
    const std::string strategy = "vllm_v1", algorithm = "sha256_cbor",
                      seed = "0", projection = "low64_be";
    mooncake_conductor_service_config service{};
    service.instance_id = S(id);
    service.endpoint = S(endpoint);
    service.model_name = S(model);
    service.block_size = 2;
    service.dp_rank = 12;
    service.has_cache_group = 1;
    service.hash_profile = {S(strategy), S(algorithm), S(seed), S(projection)};
    mooncake_conductor_registration_result* registration = nullptr;
    ASSERT_EQ(mooncake_conductor_register(raw, &service, &registration), 0);
    mooncake_conductor_string registered{};
    int32_t is_new = 0;
    ASSERT_EQ(mooncake_conductor_registration_view(registration, &registered,
                                                   &is_new),
              0);
    EXPECT_EQ(Copy(registered), id);
    EXPECT_EQ(is_new, 1);
    mooncake_conductor_registration_free(registration);
    ASSERT_EQ(mooncake_conductor_register(raw, &service, &registration), 0);
    ASSERT_EQ(mooncake_conductor_registration_view(registration, &registered,
                                                   &is_new),
              0);
    EXPECT_EQ(is_new, 0);
    mooncake_conductor_registration_free(registration);

    mc::prefixindex::ContextKey context{"default", model, "", 2};
    mc::common::ResolvedHashProfile profile;
    ASSERT_EQ(mc::prefixindex::ResolveHashProfile(
                  {strategy, algorithm, seed, projection}, &profile),
              "");
    std::string hash_error;
    auto hasher = mc::prefixindex::CreateHashStrategy(profile, &hash_error);
    ASSERT_NE(hasher, nullptr) << hash_error;
    std::vector<int32_t> tokens{1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<mc::prefixindex::HashBlock> blocks;
    ASSERT_EQ(hasher->Compute(context, tokens, std::nullopt, &blocks), "");
    std::vector<mc::prefixindex::ProjectedPrefix> prefixes;
    for (const auto& b : blocks) prefixes.push_back(b.projected);
    using Tier = mc::prefixindex::StorageTier;
    const mc::prefixindex::EngineOwner owner{"test", id, 12};
    ASSERT_EQ(manager.GetIndexer()->StoreEngine({.context = context,
                                                 .prefixes = {prefixes[0]},
                                                 .owner = owner,
                                                 .effective_block_size = 2}),
              "");
    ASSERT_EQ(manager.GetIndexer()->StoreEngine({.context = context,
                                                 .prefixes = {prefixes[1]},
                                                 .owner = owner,
                                                 .effective_block_size = 2,
                                                 .tier = Tier::kCpuLocal}),
              "");
    const mc::prefixindex::SharedObjectOwner shared{"store", "backend",
                                                    "object"};
    ASSERT_EQ(manager.GetIndexer()->StoreShared({.context = context,
                                                 .prefixes = {prefixes[2]},
                                                 .tier = Tier::kCpuShare,
                                                 .owner = shared,
                                                 .effective_block_size = 2}),
              "");
    ASSERT_EQ(manager.GetIndexer()->StoreShared({.context = context,
                                                 .prefixes = {prefixes[3]},
                                                 .tier = Tier::kDisk,
                                                 .owner = shared,
                                                 .effective_block_size = 2}),
              "");
    mooncake_conductor_query_request request{};
    request.model_name = S(model);
    request.block_size = 2;
    mooncake_conductor_query_result* result = nullptr;
    ASSERT_EQ(mooncake_conductor_query(raw, &request, tokens.data(),
                                       tokens.size(), &result),
              0);
    mooncake_conductor_services_result* services = nullptr;
    mooncake_conductor_global_result* global = nullptr;
    ASSERT_EQ(mooncake_conductor_list_services(raw, &services), 0);
    ASSERT_EQ(mooncake_conductor_get_global_view(raw, &global), 0);
    ASSERT_EQ(mooncake_conductor_close(raw), 0);
    const mooncake_conductor_cache_hit* hits = nullptr;
    size_t count = 0;
    ASSERT_EQ(mooncake_conductor_query_view(result, &hits, &count), 0);
    ASSERT_EQ(count, 1u);
    EXPECT_EQ(Copy(hits[0].instance_id), id);
    EXPECT_EQ(hits[0].match.device, 2);
    EXPECT_EQ(hits[0].match.cpu_local, 4);
    EXPECT_EQ(hits[0].match.cpu_share, 6);
    EXPECT_EQ(hits[0].match.disk, 8);
    EXPECT_EQ(hits[0].longest_matched, 8);
    ASSERT_EQ(hits[0].rank_count, 1u);
    ASSERT_EQ(hits[0].dp_count, 1u);
    EXPECT_EQ(hits[0].rank_matches[0].rank, 12);
    EXPECT_EQ(hits[0].rank_matches[0].match.disk, 8);
    EXPECT_EQ(hits[0].dp[0].tokens, 2);
    const mooncake_conductor_registered_service* svc = nullptr;
    ASSERT_EQ(mooncake_conductor_services_view(services, &svc, &count), 0);
    ASSERT_EQ(count, 1u);
    EXPECT_EQ(Copy(svc[0].config.instance_id), id);
    EXPECT_EQ(Copy(svc[0].root_digest), profile.root_digest);
    EXPECT_EQ(svc[0].config.has_cache_group, 1);
    const mooncake_conductor_context_view* views = nullptr;
    ASSERT_EQ(mooncake_conductor_global_view(global, &views, &count), 0);
    ASSERT_EQ(count, 1u);
    EXPECT_EQ(views[0].prefix_count, 4u);
    ASSERT_EQ(views[0].instance_count, 1u);
    EXPECT_EQ(Copy(views[0].instances[0].instance_id), id);
    ASSERT_EQ(views[0].instances[0].rank_count, 1u);
    EXPECT_EQ(views[0].instances[0].ranks[0], 12);
    mooncake_conductor_query_free(result);
    mooncake_conductor_services_free(services);
    mooncake_conductor_global_free(global);
    cfg.request_timeout_ms = 100;
    ASSERT_EQ(mooncake_conductor_setup(raw, &cfg), 0);
    {
        auto state_lock =
            mc::prefixindex::PrefixCacheTableTestPeer::LockContextState(
                *manager.GetIndexer(), context);
        ASSERT_TRUE(state_lock.owns_lock());
        result = nullptr;
        // The connection is established; a real Query is delayed by the
        // server's context lock, so this proves request timeout mapping.
        EXPECT_EQ(mooncake_conductor_query(raw, &request, tokens.data(),
                                           tokens.size(), &result),
                  MOONCAKE_CONDUCTOR_RPC_TIMEOUT);
        EXPECT_EQ(result, nullptr);
    }
    cfg.request_timeout_ms = 3000;
    ASSERT_EQ(mooncake_conductor_setup(raw, &cfg), 0);
    cfg.request_timeout_ms = -1;
    EXPECT_EQ(mooncake_conductor_setup(raw, &cfg), -600);
    EXPECT_EQ(mooncake_conductor_health_check(raw, &health), 0);
    EXPECT_EQ(health, 1);
    cfg.request_timeout_ms = 3000;
    ASSERT_EQ(mooncake_conductor_setup(raw, &cfg), 0);
    manager.Stop();
    EXPECT_NE(mooncake_conductor_health_check(raw, &health), 0);
    EXPECT_EQ(health, 2);
}
}  // namespace
