// SPDX-License-Identifier: Apache-2.0
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "conductor/client/conductor_c.h"
#include "conductor/kvevent/event_manager.h"
#include "prefix_indexer_test_peer.h"

namespace {
TEST(ConductorCExtensions, HealthErrorsAndServerUnregisterKey) {
    int status = -1;
    EXPECT_EQ(mooncake_conductor_health_check_ex(nullptr, &status), 0);
    EXPECT_EQ(status, MOONCAKE_CONDUCTOR_HEALTH_NOT_INITIALIZED);
    EXPECT_EQ(mooncake_conductor_health_check_ex(nullptr, nullptr),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    char* removed_key = reinterpret_cast<char*>(1);
    EXPECT_EQ(
        mooncake_conductor_unregister_ex(nullptr, "i", "", 0, &removed_key),
        MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(removed_key, nullptr);
    mooncake_conductor_string_free(nullptr);

    mooncake::conductor::kvevent::EventManager manager({}, 0, 19336);
    ASSERT_TRUE(manager.StartRPCServer());
    manager.Start();
    std::unique_ptr<void, decltype(&mooncake_conductor_destroy)> client(
        mooncake_conductor_create(), mooncake_conductor_destroy);
    ASSERT_NE(client.get(), nullptr);
    mooncake_conductor_query_request_t oversized{};
    int32_t token = 1;
    oversized.token_ids = &token;
    oversized.token_count = SIZE_MAX;
    mooncake_conductor_query_result_t empty{};
    EXPECT_EQ(mooncake_conductor_query(client.get(), &oversized, &empty),
              MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(empty.hits, nullptr);
    int rc = -1;
    for (int i = 0; i < 40 && rc != 0; ++i) {
        rc = mooncake_conductor_setup(client.get(), "127.0.0.1:19336", 1000,
                                      3000);
        if (rc) std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    ASSERT_EQ(rc, 0);
    EXPECT_EQ(mooncake_conductor_health_check_ex(client.get(), &status), 0);
    EXPECT_EQ(status, MOONCAKE_CONDUCTOR_HEALTH_OK);

    mooncake_conductor_service_config_t cfg{};
    cfg.instance_id = "c-go-shared";
    cfg.endpoint = "tcp://127.0.0.1:29336";
    cfg.model_name = "extensions";
    cfg.block_size = 4;
    cfg.dp_rank = 12;
    cfg.hash_profile = {"sglang", "sha256_raw", "", "first64_be"};
    cfg.publisher_kind = MOONCAKE_CONDUCTOR_PUBLISHER_SGLANG;
    ASSERT_EQ(mooncake_conductor_register(client.get(), &cfg, nullptr), 0);
    ASSERT_EQ(
        mooncake_conductor_setup(client.get(), "127.0.0.1:19336", 1000, 100),
        0);
    {
        mooncake::conductor::prefixindex::ContextKey context{
            "default", cfg.model_name, "", cfg.block_size};
        auto state_lock =
            mooncake::conductor::prefixindex::PrefixCacheTableTestPeer::
                LockContextState(*manager.GetIndexer(), context);
        ASSERT_TRUE(state_lock.owns_lock());
        int32_t tokens[] = {1, 2, 3, 4};
        mooncake_conductor_query_request_t request{};
        request.model_name = cfg.model_name;
        request.block_size = cfg.block_size;
        request.token_ids = tokens;
        request.token_count = 4;
        mooncake_conductor_query_result_t result{};
        EXPECT_EQ(mooncake_conductor_query(client.get(), &request, &result),
                  MOONCAKE_CONDUCTOR_RPC_TIMEOUT);
        EXPECT_EQ(result.hits, nullptr);
        EXPECT_EQ(result.hit_count, 0u);
    }
    ASSERT_EQ(
        mooncake_conductor_setup(client.get(), "127.0.0.1:19336", 1000, 3000),
        0);
    ASSERT_EQ(mooncake_conductor_unregister_ex(client.get(), cfg.instance_id,
                                               "", cfg.dp_rank, &removed_key),
              0);
    std::unique_ptr<char, decltype(&mooncake_conductor_string_free)> key(
        removed_key, mooncake_conductor_string_free);
    EXPECT_STREQ(key.get(), "c-go-shared|default|12");
    EXPECT_EQ(mooncake_conductor_unregister_ex(client.get(), cfg.instance_id,
                                               "", cfg.dp_rank, &removed_key),
              MOONCAKE_CONDUCTOR_SERVICE_NOT_FOUND);
    EXPECT_EQ(removed_key, nullptr);

    // Existing Rust status-only semantics remain unchanged after a Close or
    // failed replacement setup; detailed callers also retain the native code.
    EXPECT_EQ(
        mooncake_conductor_setup(client.get(), "127.0.0.1:19336", -1, 3000),
        MOONCAKE_CONDUCTOR_INVALID_PARAMS);
    EXPECT_EQ(mooncake_conductor_health_check(client.get()),
              MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE);
    EXPECT_EQ(mooncake_conductor_health_check_ex(client.get(), &status),
              MOONCAKE_CONDUCTOR_UNAVAILABLE);
    EXPECT_EQ(status, MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE);
    ASSERT_EQ(
        mooncake_conductor_setup(client.get(), "127.0.0.1:19336", 1000, 3000),
        0);
    manager.Stop();
    EXPECT_NE(mooncake_conductor_health_check_ex(client.get(), &status), 0);
    EXPECT_EQ(status, MOONCAKE_CONDUCTOR_HEALTH_UNREACHABLE);
    EXPECT_EQ(mooncake_conductor_health_check(client.get()), status);
}
}  // namespace
