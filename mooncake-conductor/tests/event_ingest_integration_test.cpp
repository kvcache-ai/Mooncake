// Integration tests from raw producer MessagePack through the semantic event
// handler and PrefixCacheTable state.

#include <gtest/gtest.h>
#include <msgpack.hpp>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <future>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include "conductor/kvevent/event_manager.h"
#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/zmq/msg_decoder.h"
#include "prefix_indexer_test_peer.h"

namespace {

using conductor::common::PublisherKind;
using conductor::common::ServiceConfig;
using conductor::kvevent::EventManager;
using conductor::kvevent::KVEventHandler;
using conductor::prefixindex::ContextKey;
using conductor::prefixindex::CreateHashStrategy;
using conductor::prefixindex::EngineOwner;
using conductor::prefixindex::EngineRegistration;
using conductor::prefixindex::HashBlock;
using conductor::prefixindex::HashProfile;
using conductor::prefixindex::PrefixCacheTableTestPeer;
using conductor::prefixindex::ProjectedPrefix;
using conductor::prefixindex::ResolveHashProfile;
using conductor::zmq::DecodedBatch;
using conductor::zmq::DecodeMooncakeEventBatch;
using conductor::zmq::DecodeVllmEventBatch;
using conductor::zmq::MessageMetadata;

using Packer = msgpack::packer<std::stringstream>;

constexpr int64_t kMooncakeTimestamp = 1700000000123LL;
constexpr std::string_view kRootDigest =
    "4e1195df020de59e0d65a33a4279f1183e7ae4e5d980e309f8b55adff2e61c3e";

HashProfile TestProfile() {
    const conductor::common::HashProfileConfig source{
        .strategy = "vllm_v1",
        .algorithm = "sha256_cbor",
        .python_hash_seed = "0",
        .index_projection = "low64_be",
    };
    HashProfile profile;
    EXPECT_EQ(ResolveHashProfile(source, &profile), "");
    EXPECT_EQ(profile.root_digest, kRootDigest);
    return profile;
}

ContextKey ContextFor(const ServiceConfig& service) {
    return {.tenant_id = service.tenant_id,
            .model_name = service.model_name,
            .lora_name = service.lora_name,
            .block_size = service.block_size};
}

EngineRegistration RegistrationFor(const ServiceConfig& service) {
    return {.context = ContextFor(service),
            .profile = TestProfile(),
            .instance_id = service.instance_id,
            .dp_rank = service.dp_rank,
            .effective_block_size = service.block_size,
            .cache_group = service.cache_group};
}

ServiceConfig VllmService(std::string instance_id, std::string endpoint,
                          std::string tenant_id = "default", int dp_rank = 0) {
    ServiceConfig service;
    service.endpoint = std::move(endpoint);
    service.publisher_kind = PublisherKind::kVllm;
    service.model_name = "integration-model";
    service.instance_id = std::move(instance_id);
    service.tenant_id = std::move(tenant_id);
    service.dp_rank = dp_rank;
    service.block_size = 4;
    service.cache_group = 0;
    service.hash_profile = TestProfile();
    return service;
}

ServiceConfig MooncakeService(const ServiceConfig& engine,
                              std::string endpoint) {
    ServiceConfig service = engine;
    service.endpoint = std::move(endpoint);
    service.publisher_kind = PublisherKind::kMooncake;
    service.instance_id = "integration-pool";
    service.dp_rank = 0;
    return service;
}

std::vector<int32_t> Sequence(int32_t first, size_t count) {
    std::vector<int32_t> tokens;
    tokens.reserve(count);
    for (size_t index = 0; index < count; ++index) {
        tokens.push_back(first + static_cast<int32_t>(index));
    }
    return tokens;
}

std::vector<ProjectedPrefix> ProjectedFor(const ContextKey& context,
                                          const std::vector<int32_t>& tokens) {
    std::string error;
    auto strategy = CreateHashStrategy(TestProfile(), &error);
    EXPECT_NE(strategy, nullptr) << error;
    if (strategy == nullptr) return {};

    std::vector<HashBlock> blocks;
    EXPECT_EQ(strategy->Compute(context, tokens, std::nullopt, &blocks), "");
    std::vector<ProjectedPrefix> prefixes;
    prefixes.reserve(blocks.size());
    for (const auto& block : blocks) {
        prefixes.push_back(block.projected);
    }
    return prefixes;
}

MessageMetadata MetadataFor(const ServiceConfig& service, std::string topic,
                            int64_t sequence) {
    return {.publisher_kind = service.publisher_kind,
            .endpoint = service.endpoint,
            .topic = std::move(topic),
            .sequence = sequence};
}

void PackVllmStored(Packer& packer, uint64_t hash) {
    packer.pack_map(9);
    packer.pack("type");
    packer.pack("BlockStored");
    packer.pack("block_hashes");
    packer.pack_array(1);
    packer.pack_uint64(hash);
    packer.pack("parent_block_hash");
    packer.pack_nil();
    packer.pack("token_ids");
    packer.pack_array(2);
    packer.pack_int32(1);
    packer.pack_int32(2);
    packer.pack("block_size");
    packer.pack_int64(4);
    packer.pack("lora_id");
    packer.pack_nil();
    packer.pack("medium");
    packer.pack("GPU");
    packer.pack("lora_name");
    packer.pack_nil();
    packer.pack("group_idx");
    packer.pack_int64(0);
}

void PackVllmRemoved(Packer& packer, uint64_t hash) {
    packer.pack_map(4);
    packer.pack("type");
    packer.pack("BlockRemoved");
    packer.pack("block_hashes");
    packer.pack_array(1);
    packer.pack_uint64(hash);
    packer.pack("medium");
    packer.pack("GPU");
    packer.pack("group_idx");
    packer.pack_int64(0);
}

template <typename PackEvents>
std::string PackVllmBatch(uint32_t event_count, int64_t dp_rank,
                          PackEvents pack_events) {
    std::stringstream buffer;
    Packer packer(buffer);
    packer.pack_array(3);
    packer.pack_double(1.25);
    packer.pack_array(event_count);
    pack_events(packer);
    packer.pack_int64(dp_rank);
    return buffer.str();
}

std::string ConnectorHashFor(uint64_t prefix, char leading = 'a') {
    char low64[17];
    std::snprintf(low64, sizeof(low64), "%016llx",
                  static_cast<unsigned long long>(prefix));
    return std::string(48, leading) + low64;
}

void PackNullableString(Packer& packer, const std::string& value) {
    if (value.empty()) {
        packer.pack_nil();
    } else {
        packer.pack(value);
    }
}

void PackMooncakeCommon(Packer& packer, uint64_t event_id,
                        std::string_view event_type,
                        std::string_view legacy_type,
                        const ServiceConfig& context, std::string_view backend,
                        std::string_view medium) {
    packer.pack("event_id");
    packer.pack_uint64(event_id);
    packer.pack("timestamp");
    packer.pack_int64(kMooncakeTimestamp);
    packer.pack("event_type");
    packer.pack(std::string(event_type));
    packer.pack("type");
    packer.pack(std::string(legacy_type));
    packer.pack("model_name");
    packer.pack(context.model_name);
    packer.pack("block_size");
    packer.pack_int64(context.block_size);
    packer.pack("additional_salt");
    packer.pack_nil();
    packer.pack("lora_name");
    PackNullableString(packer, context.lora_name);
    packer.pack("tenant_id");
    packer.pack(context.tenant_id);
    packer.pack("backend_id");
    packer.pack(std::string(backend));
    packer.pack("medium");
    if (medium.empty()) {
        packer.pack_nil();
    } else {
        packer.pack(std::string(medium));
    }
    packer.pack("dp_rank");
    packer.pack_int64(9);
}

void PackMooncakeObject(Packer& packer, uint64_t hash,
                        std::string_view object_key) {
    packer.pack("group_id");
    packer.pack("0");
    packer.pack("object_key");
    packer.pack(std::string(object_key));
    packer.pack("connector_block_hash");
    packer.pack(ConnectorHashFor(hash));
    packer.pack("seq_hashes");
    packer.pack_array(1);
    packer.pack_uint64(hash);
    packer.pack("block_hashes");
    packer.pack_array(1);
    packer.pack_uint64(hash);
    packer.pack("base_block_idx");
    packer.pack_nil();
}

void PackMooncakeStored(Packer& packer, const ServiceConfig& context,
                        uint64_t hash, std::string_view object_key,
                        std::string_view backend = "backend-a") {
    packer.pack_map(21);
    PackMooncakeCommon(packer, 1, "stored", "BlockStored", context, backend,
                       "CPU");
    PackMooncakeObject(packer, hash, object_key);
    packer.pack("parent_hash");
    packer.pack_nil();
    packer.pack("token_ids");
    packer.pack_nil();
    packer.pack("parent_block_hash");
    packer.pack_nil();
}

void PackMooncakeRemoved(Packer& packer, const ServiceConfig& context,
                         uint64_t hash, std::string_view object_key,
                         std::string_view backend = "backend-a") {
    packer.pack_map(18);
    PackMooncakeCommon(packer, 2, "removed", "BlockRemoved", context, backend,
                       "CPU");
    PackMooncakeObject(packer, hash, object_key);
}

void PackMooncakeCleared(Packer& packer, const ServiceConfig& context,
                         std::string_view backend = "backend-a") {
    packer.pack_map(12);
    PackMooncakeCommon(packer, 3, "cleared", "AllBlocksCleared", context,
                       backend, "");
}

template <typename PackEvents>
std::string PackMooncakeBatch(uint32_t event_count, PackEvents pack_events) {
    std::stringstream buffer;
    Packer packer(buffer);
    packer.pack_array(3);
    packer.pack_int64(kMooncakeTimestamp);
    packer.pack_array(event_count);
    pack_events(packer);
    packer.pack_int64(9);
    return buffer.str();
}

std::string PackInvalidVllmEnvelope(uint64_t hash) {
    std::stringstream buffer;
    Packer packer(buffer);
    packer.pack_array(2);
    packer.pack_double(1.25);
    packer.pack_array(1);
    PackVllmStored(packer, hash);
    return buffer.str();
}

std::string PackInvalidMooncakeEnvelope(const ServiceConfig& context,
                                        uint64_t hash) {
    std::stringstream buffer;
    Packer packer(buffer);
    packer.pack_array(2);
    packer.pack_int64(kMooncakeTimestamp);
    packer.pack_array(1);
    PackMooncakeStored(packer, context, hash, "invalid-envelope-object");
    return buffer.str();
}

struct DispatchAttempt {
    bool decoded = false;
    bool handler_called = false;
    std::string error;
};

DispatchAttempt DecodeAndDispatchVllm(const std::string& payload,
                                      KVEventHandler& handler,
                                      const MessageMetadata& metadata) {
    auto decoded = DecodeVllmEventBatch(payload.data(), payload.size());
    if (!decoded.ok) {
        return {.decoded = false,
                .handler_called = false,
                .error = std::move(decoded.error)};
    }
    return {.decoded = true,
            .handler_called = true,
            .error = handler.HandleBatch(DecodedBatch(std::move(decoded.batch)),
                                         metadata)};
}

DispatchAttempt DecodeAndDispatchMooncake(const std::string& payload,
                                          KVEventHandler& handler,
                                          const MessageMetadata& metadata) {
    auto decoded = DecodeMooncakeEventBatch(payload.data(), payload.size());
    if (!decoded.ok) {
        return {.decoded = false,
                .handler_called = false,
                .error = std::move(decoded.error)};
    }
    return {.decoded = true,
            .handler_called = true,
            .error = handler.HandleBatch(DecodedBatch(std::move(decoded.batch)),
                                         metadata)};
}

void ExpectDispatched(const DispatchAttempt& attempt) {
    EXPECT_TRUE(attempt.decoded) << attempt.error;
    EXPECT_TRUE(attempt.handler_called);
    EXPECT_TRUE(attempt.error.empty()) << attempt.error;
}

TEST(EventIngestIntegration, InvalidEnvelopesNeverDispatchOrMutateState) {
    EventManager manager({}, 0);
    const auto engine = VllmService("engine", "tcp://127.0.0.1:47001");
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(engine)).error.empty());
    KVEventHandler engine_handler(&manager, engine);
    const auto tokens = Sequence(1, 4);
    const auto prefixes = ProjectedFor(ContextFor(engine), tokens);
    ASSERT_EQ(prefixes.size(), 1u);
    const auto baseline =
        PrefixCacheTableTestPeer::Snapshot(*manager.GetIndexer());

    const auto vllm_attempt =
        DecodeAndDispatchVllm(PackInvalidVllmEnvelope(prefixes[0].value),
                              engine_handler, MetadataFor(engine, "", 1));
    EXPECT_FALSE(vllm_attempt.decoded);
    EXPECT_FALSE(vllm_attempt.handler_called);
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(*manager.GetIndexer()),
              baseline);

    const auto pool = MooncakeService(engine, "tcp://127.0.0.1:47002");
    KVEventHandler pool_handler(&manager, pool);
    const auto mooncake_attempt = DecodeAndDispatchMooncake(
        PackInvalidMooncakeEnvelope(engine, prefixes[0].value), pool_handler,
        MetadataFor(pool, "", 2));
    EXPECT_FALSE(mooncake_attempt.decoded);
    EXPECT_FALSE(mooncake_attempt.handler_called);
    EXPECT_EQ(PrefixCacheTableTestPeer::Snapshot(*manager.GetIndexer()),
              baseline);
}

TEST(EventIngestIntegration, VllmMalformedFinalLeavesEarlierStoreApplied) {
    EventManager manager({}, 0);
    const auto engine = VllmService("engine", "tcp://127.0.0.1:47101");
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(engine)).error.empty());
    KVEventHandler handler(&manager, engine);
    const auto tokens = Sequence(1, 4);
    const auto prefix = ProjectedFor(ContextFor(engine), tokens).front();
    const auto payload = PackVllmBatch(2, engine.dp_rank, [&](Packer& packer) {
        PackVllmStored(packer, prefix.value);
        packer.pack_map(2);
        packer.pack("type");
        packer.pack("BlockRemoved");
        packer.pack("block_hashes");
        packer.pack_array(1);
        packer.pack_uint64(prefix.value);
    });

    ExpectDispatched(DecodeAndDispatchVllm(
        payload, handler, MetadataFor(engine, "producer-topic", 10)));
    const auto result =
        manager.GetIndexer()->Query(ContextFor(engine), tokens).at("engine");
    EXPECT_EQ(result.gpu, 4);
    EXPECT_EQ(result.cpu, 0);
    EXPECT_EQ(result.disk, 0);
}

TEST(EventIngestIntegration, MooncakeMalformedFinalLeavesEarlierStoreApplied) {
    EventManager manager({}, 0);
    const auto engine = VllmService("engine", "tcp://127.0.0.1:47201");
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(engine)).error.empty());
    const auto pool = MooncakeService(engine, "tcp://127.0.0.1:47202");
    KVEventHandler handler(&manager, pool);
    const auto tokens = Sequence(1, 4);
    const auto prefix = ProjectedFor(ContextFor(engine), tokens).front();
    const auto payload = PackMooncakeBatch(2, [&](Packer& packer) {
        PackMooncakeStored(packer, engine, prefix.value, "object-a");
        packer.pack_map(1);
        packer.pack("event_type");
        packer.pack("stored");
    });

    ExpectDispatched(DecodeAndDispatchMooncake(
        payload, handler, MetadataFor(pool, "producer-topic", 11)));
    const auto result =
        manager.GetIndexer()->Query(ContextFor(engine), tokens).at("engine");
    EXPECT_EQ(result.gpu, 0);
    EXPECT_EQ(result.cpu, 4);
    EXPECT_EQ(result.disk, 0);
}

TEST(EventIngestIntegration, VllmMalformedMiddleDoesNotBlockLaterRemove) {
    EventManager manager({}, 0);
    const auto engine = VllmService("engine", "tcp://127.0.0.1:47301");
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(engine)).error.empty());
    KVEventHandler handler(&manager, engine);
    const auto tokens = Sequence(1, 4);
    const auto prefix = ProjectedFor(ContextFor(engine), tokens).front();
    const auto payload = PackVllmBatch(3, engine.dp_rank, [&](Packer& packer) {
        PackVllmStored(packer, prefix.value);
        packer.pack_map(4);
        packer.pack("type");
        packer.pack("BlockRemoved");
        packer.pack("block_hashes");
        packer.pack_array(1);
        packer.pack_uint64(prefix.value);
        packer.pack("medium");
        packer.pack("GPU");
        packer.pack("medium");
        packer.pack("CPU");
        PackVllmRemoved(packer, prefix.value);
    });

    ExpectDispatched(DecodeAndDispatchVllm(
        payload, handler, MetadataFor(engine, "producer-topic", 20)));
    const auto result =
        manager.GetIndexer()->Query(ContextFor(engine), tokens).at("engine");
    EXPECT_EQ(result.gpu, 0);
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts[0].prefix_count,
              0u);
}

TEST(EventIngestIntegration, MooncakeMalformedMiddleDoesNotBlockLaterRemove) {
    EventManager manager({}, 0);
    const auto engine = VllmService("engine", "tcp://127.0.0.1:47401");
    ASSERT_TRUE(
        manager.GetIndexer()->Register(RegistrationFor(engine)).error.empty());
    const auto pool = MooncakeService(engine, "tcp://127.0.0.1:47402");
    KVEventHandler handler(&manager, pool);
    const auto tokens = Sequence(1, 4);
    const auto prefix = ProjectedFor(ContextFor(engine), tokens).front();
    const auto payload = PackMooncakeBatch(3, [&](Packer& packer) {
        PackMooncakeStored(packer, engine, prefix.value, "object-a");
        packer.pack_map(2);
        packer.pack("event_type");
        packer.pack("removed");
        packer.pack("event_type");
        packer.pack("removed");
        PackMooncakeRemoved(packer, engine, prefix.value, "object-a");
    });

    ExpectDispatched(DecodeAndDispatchMooncake(
        payload, handler, MetadataFor(pool, "producer-topic", 21)));
    const auto result =
        manager.GetIndexer()->Query(ContextFor(engine), tokens).at("engine");
    EXPECT_EQ(result.cpu, 0);
    EXPECT_EQ(manager.GetIndexer()->GetGlobalView().contexts[0].prefix_count,
              0u);
}

TEST(EventIngestIntegration,
     EqualTopicsOnDistinctEndpointsPopulateOnlyTrustedOwners) {
    EventManager manager({}, 0);
    const auto engine_a =
        VllmService("engine-a", "tcp://127.0.0.1:47501", "default", 0);
    const auto engine_b =
        VllmService("engine-b", "tcp://127.0.0.1:47502", "default", 1);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(RegistrationFor(engine_a))
                    .error.empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(RegistrationFor(engine_b))
                    .error.empty());
    KVEventHandler handler_a(&manager, engine_a);
    KVEventHandler handler_b(&manager, engine_b);
    const auto tokens = Sequence(1, 8);
    const auto prefixes = ProjectedFor(ContextFor(engine_a), tokens);
    ASSERT_EQ(prefixes.size(), 2u);

    const auto payload_a = PackVllmBatch(
        1, engine_a.dp_rank,
        [&](Packer& packer) { PackVllmStored(packer, prefixes[0].value); });
    const auto payload_b = PackVllmBatch(
        1, engine_b.dp_rank,
        [&](Packer& packer) { PackVllmStored(packer, prefixes[1].value); });
    ExpectDispatched(DecodeAndDispatchVllm(
        payload_a, handler_a, MetadataFor(engine_a, "shared-topic", 30)));
    ExpectDispatched(DecodeAndDispatchVllm(
        payload_b, handler_b, MetadataFor(engine_b, "shared-topic", 31)));

    const auto snapshot =
        PrefixCacheTableTestPeer::Snapshot(*manager.GetIndexer());
    const auto& blocks = snapshot.contexts.at(ContextFor(engine_a)).blocks;
    ASSERT_EQ(blocks.size(), 2u);
    EXPECT_EQ(blocks.at(prefixes[0]).gpu_owners,
              (std::set<EngineOwner>{{.source_stream = engine_a.endpoint,
                                      .instance_id = engine_a.instance_id,
                                      .dp_rank = engine_a.dp_rank}}));
    EXPECT_EQ(blocks.at(prefixes[1]).gpu_owners,
              (std::set<EngineOwner>{{.source_stream = engine_b.endpoint,
                                      .instance_id = engine_b.instance_id,
                                      .dp_rank = engine_b.dp_rank}}));
    EXPECT_TRUE(blocks.at(prefixes[0]).cpu_owners.empty());
    EXPECT_TRUE(blocks.at(prefixes[0]).disk_owners.empty());
    EXPECT_TRUE(blocks.at(prefixes[1]).cpu_owners.empty());
    EXPECT_TRUE(blocks.at(prefixes[1]).disk_owners.empty());
}

TEST(EventIngestIntegration,
     MooncakeSameObjectIdentityRemainsIndependentAcrossTenants) {
    EventManager manager({}, 0);
    const auto tenant_a =
        VllmService("engine-a", "tcp://127.0.0.1:47601", "tenant-a", 0);
    const auto tenant_b =
        VllmService("engine-b", "tcp://127.0.0.1:47602", "tenant-b", 0);
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(RegistrationFor(tenant_a))
                    .error.empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(RegistrationFor(tenant_b))
                    .error.empty());
    const auto pool = MooncakeService(tenant_a, "tcp://127.0.0.1:47603");
    KVEventHandler handler(&manager, pool);
    const auto tokens = Sequence(1, 4);
    const auto prefix_a = ProjectedFor(ContextFor(tenant_a), tokens).front();
    const auto prefix_b = ProjectedFor(ContextFor(tenant_b), tokens).front();

    const auto stores = PackMooncakeBatch(2, [&](Packer& packer) {
        PackMooncakeStored(packer, tenant_a, prefix_a.value, "same-object");
        PackMooncakeStored(packer, tenant_b, prefix_b.value, "same-object");
    });
    ExpectDispatched(DecodeAndDispatchMooncake(
        stores, handler, MetadataFor(pool, "shared-topic", 40)));
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(tenant_a), tokens)
                  .at("engine-a")
                  .cpu,
              4);
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(tenant_b), tokens)
                  .at("engine-b")
                  .cpu,
              4);

    const auto clear_a = PackMooncakeBatch(
        1, [&](Packer& packer) { PackMooncakeCleared(packer, tenant_a); });
    ExpectDispatched(DecodeAndDispatchMooncake(
        clear_a, handler, MetadataFor(pool, "shared-topic", 41)));
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(tenant_a), tokens)
                  .at("engine-a")
                  .cpu,
              0);
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(tenant_b), tokens)
                  .at("engine-b")
                  .cpu,
              4);

    const auto remove_b = PackMooncakeBatch(1, [&](Packer& packer) {
        PackMooncakeRemoved(packer, tenant_b, prefix_b.value, "same-object");
    });
    ExpectDispatched(DecodeAndDispatchMooncake(
        remove_b, handler, MetadataFor(pool, "shared-topic", 42)));
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(tenant_b), tokens)
                  .at("engine-b")
                  .cpu,
              0);
}

TEST(EventIngestIntegration,
     MooncakeClearIsProgressivelyVisibleAcrossMultipleContexts) {
    EventManager manager({}, 0);
    auto context_a =
        VllmService("engine-a", "tcp://127.0.0.1:47701", "shared-tenant", 0);
    auto context_b =
        VllmService("engine-b", "tcp://127.0.0.1:47702", "shared-tenant", 0);
    context_a.model_name = "integration-model-a";
    context_b.model_name = "integration-model-b";
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(RegistrationFor(context_a))
                    .error.empty());
    ASSERT_TRUE(manager.GetIndexer()
                    ->Register(RegistrationFor(context_b))
                    .error.empty());
    const auto pool = MooncakeService(context_a, "tcp://127.0.0.1:47703");
    KVEventHandler handler(&manager, pool);
    const auto tokens = Sequence(1, 4);
    const auto prefix_a = ProjectedFor(ContextFor(context_a), tokens).front();
    const auto prefix_b = ProjectedFor(ContextFor(context_b), tokens).front();

    const auto stores = PackMooncakeBatch(2, [&](Packer& packer) {
        PackMooncakeStored(packer, context_a, prefix_a.value, "a-object");
        PackMooncakeStored(packer, context_b, prefix_b.value, "z-object");
    });
    ExpectDispatched(DecodeAndDispatchMooncake(
        stores, handler, MetadataFor(pool, "shared-topic", 50)));
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(context_a), tokens)
                  .at("engine-a")
                  .cpu,
              4);
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(context_b), tokens)
                  .at("engine-b")
                  .cpu,
              4);

    auto second_context_lock = PrefixCacheTableTestPeer::LockContextState(
        *manager.GetIndexer(), ContextFor(context_b));
    ASSERT_TRUE(second_context_lock.owns_lock());
    const auto clear = PackMooncakeBatch(
        1, [&](Packer& packer) { PackMooncakeCleared(packer, context_a); });
    auto clear_future = std::async(std::launch::async, [&] {
        return DecodeAndDispatchMooncake(clear, handler,
                                         MetadataFor(pool, "shared-topic", 51));
    });

    bool first_context_cleared = false;
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (manager.GetIndexer()
                ->Query(ContextFor(context_a), tokens)
                .at("engine-a")
                .cpu == 0) {
            first_context_cleared = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (!first_context_cleared) {
        second_context_lock.unlock();
        ExpectDispatched(clear_future.get());
        FAIL() << "first ContextKey was not cleared before the second blocked";
    }

    EXPECT_EQ(clear_future.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(context_a), tokens)
                  .at("engine-a")
                  .cpu,
              0);

    second_context_lock.unlock();
    ExpectDispatched(clear_future.get());
    EXPECT_EQ(manager.GetIndexer()
                  ->Query(ContextFor(context_b), tokens)
                  .at("engine-b")
                  .cpu,
              0);
}

}  // namespace
