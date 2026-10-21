// part_rpc_test.cpp
//
// The part arguments on the wire, against a real master over a real RPC
// connection: the calls this build makes reach the handlers the master
// registers, a read that names a part is answered with that part, and the
// master reports the capability a part-aware client has to see before it sends
// one.

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/struct_pack/compatible.hpp>

#include "common/network.h"
#include "master_client.h"
#include "master_config.h"
#include "master_service/master_service_test_peer.h"
#include "rpc_helper.h"
#include "rpc_service.h"
#include "rpc_worker_pool.h"
#include "types.h"

namespace mooncake {
namespace {

using test::MasterServiceTestPeer;

class PartRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        config.enable_metric_reporting = false;
        service_ = std::make_shared<WrappedMasterService>(config);

        const auto ports = getFreeTcpPorts(1);
        ASSERT_EQ(ports.size(), 1u);
        port_ = ports.front();
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/2, /*port=*/port_, /*address=*/"127.0.0.1",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        RegisterRpcService(*server_, *service_);
        ASSERT_FALSE(server_->async_start().hasResult());
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        client_id_ = generate_uuid();
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "part_rpc_segment";
        segment.base = 0x300000000;
        segment.size = 16 * 1024 * 1024;
        ASSERT_TRUE(service_->MountSegment(segment, client_id_).has_value());
    }

    void TearDown() override {
        if (server_) {
            server_->stop();
        }
    }

    std::string address(uint16_t port) const {
        return "127.0.0.1:" + std::to_string(port);
    }

    // Writes one part of `key` as a part-aware client does: the write names
    // the part, and the End names the replicas the write handed out.
    void WritePartOverRpc(coro_rpc::coro_rpc_client& client,
                          const std::string& key, uint32_t part_index,
                          uint32_t part_count, uint64_t checksum) {
        ReplicateConfig config;
        config.replica_num = 1;
        auto started = async_simple::coro::syncAwait(
            client.call<&WrappedMasterService::PutStart>(
                client_id_, key, uint64_t{1024}, config,
                std::string(TenantId::kDefaultValue),
                struct_pack::compatible<uint32_t>{part_index},
                struct_pack::compatible<uint32_t>{part_count}));
        ASSERT_TRUE(started.has_value()) << "PutStart call failed";
        ASSERT_TRUE(started->has_value()) << toString(started->error());
        std::vector<uint64_t> replica_ids;
        for (const auto& descriptor : **started) {
            replica_ids.push_back(descriptor.id);
        }
        auto ended = async_simple::coro::syncAwait(
            client.call<&WrappedMasterService::PutEnd>(
                client_id_, ObjectMeta{key, checksum}, ReplicaType::MEMORY,
                std::string(TenantId::kDefaultValue),
                struct_pack::compatible<uint32_t>{part_index},
                struct_pack::compatible<std::vector<uint64_t>>{replica_ids}));
        ASSERT_TRUE(ended.has_value()) << "PutEnd call failed";
        ASSERT_TRUE(ended->has_value()) << toString(ended->error());
    }

    std::shared_ptr<WrappedMasterService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    uint16_t port_{0};
    UUID client_id_{};
};

// The part count sizes the object the master builds, so the value is bounded:
// a count past the bound is refused at the RPC boundary and leaves nothing
// behind, and the bound itself is a count a client may ask for.
TEST_F(PartRpcTest, RefusesAPartCountPastTheBound) {
    coro_rpc::coro_rpc_client client;
    auto connected = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_)));
    ASSERT_FALSE(connected) << "connect failed";

    ReplicateConfig config;
    config.replica_num = 1;
    const std::string key = "rpc_over_bound_key";
    auto started = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::PutStart>(
            client_id_, key, 1024, config, std::string(TenantId::kDefaultValue),
            struct_pack::compatible<uint32_t>{0u},
            struct_pack::compatible<uint32_t>{kMaxPartsPerObject + 1}));
    ASSERT_TRUE(started.has_value()) << "call failed";
    ASSERT_FALSE(started->has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, started->error());
    auto exists = service_->ExistKey(key, std::string(TenantId::kDefaultValue));
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());
    MasterService& service = MasterServiceTestPeer::ServiceOf(*service_);
    EXPECT_EQ(0u, MasterServiceTestPeer::GetPartCount(
                      service, TenantId::Default(), key));

    auto at_bound = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::PutStart>(
            client_id_, key, 1024, config, std::string(TenantId::kDefaultValue),
            struct_pack::compatible<uint32_t>{0u},
            struct_pack::compatible<uint32_t>{kMaxPartsPerObject}));
    ASSERT_TRUE(at_bound.has_value()) << "call failed";
    ASSERT_TRUE(at_bound->has_value()) << toString(at_bound->error());
    EXPECT_EQ(kMaxPartsPerObject, MasterServiceTestPeer::GetPartCount(
                                      service, TenantId::Default(), key));
}

// A read that names a part is answered with that part and with the object's
// part count, and it carries the tenant the master resolved it under.
TEST_F(PartRpcTest, PartReadAnswersWithTheObjectPartCount) {
    coro_rpc::coro_rpc_client client;
    auto connected = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_)));
    ASSERT_FALSE(connected) << "connect failed";

    const std::string key = "rpc_part_read_key";
    WritePartOverRpc(client, key, 0, 2, 4321);
    WritePartOverRpc(client, key, 1, 2, 8765);

    auto read = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::GetReplicaList>(
            key, std::string(TenantId::kDefaultValue),
            struct_pack::compatible<uint32_t>{1u}));
    ASSERT_TRUE(read.has_value()) << "call failed";
    ASSERT_TRUE(read->has_value()) << toString(read->error());
    ASSERT_TRUE((*read)->part_count.has_value());
    EXPECT_EQ(*(*read)->part_count, 2u);
    EXPECT_EQ((*read)->replicas.size(), 1u);
    ASSERT_TRUE((*read)->object_checksum.has_value());
    EXPECT_EQ(*(*read)->object_checksum, 8765u);
    EXPECT_EQ((*read)->resolved_tenant_id.value_or(""),
              TenantId::kDefaultValue);

    // A read that names no part of an object of several is refused.
    auto legacy = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::GetReplicaList>(
            key, std::string(TenantId::kDefaultValue),
            struct_pack::compatible<uint32_t>{}));
    ASSERT_TRUE(legacy.has_value()) << "call failed";
    ASSERT_FALSE(legacy->has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, legacy->error());
}

// The capability a part-aware client looks for before it sends a part argument.
// It has to survive the wire: a field the master fills in but never serializes
// is a capability no client can see.
TEST_F(PartRpcTest, PingReportsPartSupport) {
    MasterClient master_client(client_id_);
    ASSERT_EQ(master_client.Connect(address(port_)), ErrorCode::OK);
    auto ping = master_client.Ping();
    ASSERT_TRUE(ping.has_value()) << toString(ping.error());
    ASSERT_TRUE(ping->supported_features.has_value());
    EXPECT_EQ(*ping->supported_features & kPartSupportFeature,
              kPartSupportFeature);
}

// The master serves the same calls through a worker pool, where the arguments
// travel through a coroutine frame.
TEST_F(PartRpcTest, PartArgumentsSurviveTheWorkerPool) {
    RpcWorkerPool worker_pool(2);
    WrappedMasterServiceConfig config;
    config.default_kv_lease_ttl = 0;
    config.enable_metric_reporting = false;
    auto pooled_service = std::make_shared<WrappedMasterService>(config);

    const auto ports = getFreeTcpPorts(1);
    ASSERT_EQ(ports.size(), 1u);
    coro_rpc::coro_rpc_server server(
        /*thread_num=*/2, /*port=*/ports.front(), /*address=*/"127.0.0.1",
        std::chrono::seconds(0), /*tcp_no_delay=*/true);
    RegisterRpcService(server, *pooled_service, &worker_pool);
    ASSERT_FALSE(server.async_start().hasResult());
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    const UUID client_id = generate_uuid();
    Segment segment;
    segment.id = generate_uuid();
    segment.name = "part_rpc_pool_segment";
    segment.base = 0x300000000;
    segment.size = 16 * 1024 * 1024;
    ASSERT_TRUE(pooled_service->MountSegment(segment, client_id).has_value());

    MasterClient master_client(client_id);
    ASSERT_EQ(master_client.Connect(address(ports.front())), ErrorCode::OK);
    const std::string key = "rpc_pool_key";
    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;
    auto started = master_client.PutStart(key, {1024}, replicate_config);
    ASSERT_TRUE(started.has_value()) << toString(started.error());
    ASSERT_TRUE(
        master_client.PutEnd(ObjectMeta{key, std::nullopt}, ReplicaType::MEMORY)
            .has_value());
    auto exists =
        pooled_service->ExistKey(key, std::string(TenantId::kDefaultValue));
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value());

    // This client names no part at all, so the master owes it the shape it had
    // before parts existed: an object of one part, readable, written by this
    // client.
    MasterService& service = MasterServiceTestPeer::ServiceOf(*pooled_service);
    EXPECT_EQ(
        MasterServiceTestPeer::GetPartCount(service, TenantId::Default(), key),
        1u);
    EXPECT_TRUE(
        MasterServiceTestPeer::GetReadable(service, TenantId::Default(), key));
    EXPECT_EQ(MasterServiceTestPeer::GetLastWriter(service, TenantId::Default(),
                                                   key, /*part_index=*/0),
              client_id);
    server.stop();
}

// A batch read that names one part per item is answered part by part, in the
// order it asked, and a batch that names none is the read of whole objects it
// always was.
TEST_F(PartRpcTest, BatchReadAnswersEachPartItNamed) {
    coro_rpc::coro_rpc_client client;
    auto connected = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_)));
    ASSERT_FALSE(connected) << "connect failed";

    const std::string parted = "rpc_batch_read_parted";
    WritePartOverRpc(client, parted, 0, 2, 1111);
    WritePartOverRpc(client, parted, 1, 2, 2222);
    const std::string whole = "rpc_batch_read_whole";
    WritePartOverRpc(client, whole, 0, 1, 3333);

    auto parted_batch = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::BatchGetReplicaList>(
            std::vector<std::string>{parted, parted},
            std::string(TenantId::kDefaultValue),
            struct_pack::compatible<std::vector<uint32_t>>{
                std::vector<uint32_t>{1, 0}}));
    ASSERT_TRUE(parted_batch.has_value()) << "call failed";
    ASSERT_EQ(parted_batch->size(), 2u);
    ASSERT_TRUE((*parted_batch)[0].has_value())
        << toString((*parted_batch)[0].error());
    ASSERT_TRUE((*parted_batch)[1].has_value())
        << toString((*parted_batch)[1].error());
    EXPECT_EQ(*(*parted_batch)[0]->object_checksum, 2222u);
    EXPECT_EQ(*(*parted_batch)[1]->object_checksum, 1111u);
    EXPECT_EQ((*parted_batch)[0]->replicas.size(), 1u);

    // The same batch, naming no part at all, reads whole objects.
    auto whole_batch = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::BatchGetReplicaList>(
            std::vector<std::string>{whole},
            std::string(TenantId::kDefaultValue),
            struct_pack::compatible<std::vector<uint32_t>>{}));
    ASSERT_TRUE(whole_batch.has_value()) << "call failed";
    ASSERT_EQ(whole_batch->size(), 1u);
    ASSERT_TRUE((*whole_batch)[0].has_value())
        << toString((*whole_batch)[0].error());
    EXPECT_EQ(*(*whole_batch)[0]->object_checksum, 3333u);
}

// A batch write of every part of one key starts each part and completes each
// part, and the object those parts make is one the master holds whole.
TEST_F(PartRpcTest, BatchWriteCarriesEveryPartOfOneKey) {
    coro_rpc::coro_rpc_client client;
    auto connected = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_)));
    ASSERT_FALSE(connected) << "connect failed";

    const std::string key = "rpc_batch_write_key";
    constexpr uint32_t kBatchParts = 3;
    ReplicateConfig config;
    config.replica_num = 1;
    auto started = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::BatchPutStart>(
            client_id_, std::vector<std::string>(kBatchParts, key),
            std::vector<uint64_t>(kBatchParts, 1024), config,
            std::string(TenantId::kDefaultValue),
            struct_pack::compatible<std::vector<uint32_t>>{
                std::vector<uint32_t>{0, 1, 2}},
            struct_pack::compatible<uint32_t>{kBatchParts}));
    ASSERT_TRUE(started.has_value()) << "call failed";
    ASSERT_EQ(started->size(), kBatchParts);
    std::vector<ObjectMeta> object_metas(kBatchParts,
                                         ObjectMeta{key, std::nullopt});
    std::vector<std::vector<uint64_t>> per_part_ids;
    for (size_t i = 0; i < started->size(); ++i) {
        ASSERT_TRUE((*started)[i].has_value())
            << "part " << i << ": " << toString((*started)[i].error());
        std::vector<uint64_t> ids;
        for (const auto& descriptor : *(*started)[i]) {
            ids.push_back(descriptor.id);
        }
        per_part_ids.push_back(std::move(ids));
    }

    // Until every part has arrived the object is not one a reader sees.
    auto unfinished =
        service_->ExistKey(key, std::string(TenantId::kDefaultValue));
    ASSERT_TRUE(unfinished.has_value());
    EXPECT_FALSE(unfinished.value());

    auto ended = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::BatchPutEnd>(
            client_id_, object_metas, ReplicaType::MEMORY,
            std::string(TenantId::kDefaultValue),
            struct_pack::compatible<std::vector<uint32_t>>{
                std::vector<uint32_t>{0, 1, 2}},
            struct_pack::compatible<std::vector<std::vector<uint64_t>>>{
                per_part_ids}));
    ASSERT_TRUE(ended.has_value()) << "call failed";
    ASSERT_EQ(ended->size(), kBatchParts);
    for (size_t i = 0; i < ended->size(); ++i) {
        ASSERT_TRUE((*ended)[i].has_value())
            << "part " << i << ": " << toString((*ended)[i].error());
    }

    auto exists = service_->ExistKey(key, std::string(TenantId::kDefaultValue));
    ASSERT_TRUE(exists.has_value());
    EXPECT_TRUE(exists.value());
    MasterService& service = MasterServiceTestPeer::ServiceOf(*service_);
    EXPECT_EQ(
        MasterServiceTestPeer::GetPartCount(service, TenantId::Default(), key),
        kBatchParts);
}

// A tensor-parallel, pipeline-parallel deployment keeps the KV cache of one
// block in one object: every rank computed its own shard, so every rank writes
// one part from its own segment, and a rank that needs another rank's shard
// asks for that part.

// A checksum that names the rank and a tag the test chose for the write, so a
// read answers which shard came back.
uint64_t ShardChecksum(uint32_t rank, uint32_t tag) {
    return static_cast<uint64_t>(rank) * 1000 + tag;
}

// One rank's store process: its own client, its own mounted segment. The
// segment stays here so the test can take it away and mount it again.
struct MountedRank {
    UUID client_id{};
    Segment segment;
    std::string failure;
};

MountedRank MountRank(WrappedMasterService& service, const std::string& name,
                      uintptr_t base, size_t size) {
    MountedRank rank;
    rank.client_id = generate_uuid();
    rank.segment.id = generate_uuid();
    rank.segment.name = name;
    rank.segment.te_endpoint = name;
    rank.segment.base = base;
    rank.segment.size = size;
    auto mounted = service.MountSegment(rank.segment, rank.client_id);
    if (!mounted.has_value()) {
        rank.failure = "MountSegment: " + toString(mounted.error());
    }
    return rank;
}

// The failure text of one rank's write of one part; empty when it landed.
struct ShardWrite {
    std::string failure;
};

// A rank writes its shard into its own segment, which is what a client does
// with the buffer it allocated, so the write names that segment.
ShardWrite WriteShard(coro_rpc::coro_rpc_client& client, const UUID& client_id,
                      const std::string& key, uint32_t part_index,
                      uint32_t part_count, uint64_t part_bytes,
                      uint64_t checksum, const std::string& rank_segment = {}) {
    ShardWrite write;
    ReplicateConfig config;
    config.replica_num = 1;
    if (!rank_segment.empty()) {
        config.preferred_segments = {rank_segment};
    }
    auto started = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::PutStart>(
            client_id, key, part_bytes, config,
            std::string(TenantId::kDefaultValue),
            struct_pack::compatible<uint32_t>{part_index},
            struct_pack::compatible<uint32_t>{part_count}));
    if (!started.has_value()) {
        write.failure = "PutStart did not reach the master";
        return write;
    }
    if (!started->has_value()) {
        write.failure = "PutStart: " + toString(started->error());
        return write;
    }
    std::vector<uint64_t> replica_ids;
    for (const auto& descriptor : **started) {
        replica_ids.push_back(descriptor.id);
    }
    auto ended = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::PutEnd>(
            client_id, ObjectMeta{key, checksum}, ReplicaType::MEMORY,
            std::string(TenantId::kDefaultValue),
            struct_pack::compatible<uint32_t>{part_index},
            struct_pack::compatible<std::vector<uint64_t>>{replica_ids}));
    if (!ended.has_value()) {
        write.failure = "PutEnd did not reach the master";
    } else if (!ended->has_value()) {
        write.failure = "PutEnd: " + toString(ended->error());
    }
    return write;
}

// Eight tensor-parallel ranks over four pipeline stages hold one block's KV
// cache: thirty-two parts written from thirty-two segments, none of them a
// block a reader can use until the last rank has landed.
TEST_F(PartRpcTest, TensorParallelPipelineRanksFillOneBlock) {
    constexpr uint32_t kTensorParallel = 8;
    constexpr uint32_t kPipelineStages = 4;
    constexpr uint32_t kRanks = kTensorParallel * kPipelineStages;
    constexpr uint64_t kPartBytes = 4 * 1024 * 1024;
    constexpr size_t kRankSegmentBytes = 64 * 1024 * 1024;
    const std::string key = "kv_block_tp8_pp4";
    const std::string tenant(TenantId::kDefaultValue);

    std::vector<MountedRank> ranks(kRanks);
    for (uint32_t rank = 0; rank < kRanks; ++rank) {
        ranks[rank] = MountRank(
            *service_, "kv_rank_" + std::to_string(rank),
            0x400000000ULL + static_cast<uintptr_t>(rank) * kRankSegmentBytes,
            kRankSegmentBytes);
        ASSERT_TRUE(ranks[rank].failure.empty())
            << "rank " << rank << ": " << ranks[rank].failure;
    }

    // Every rank but the last sends its shard, each over its own connection.
    std::vector<ShardWrite> writes(kRanks);
    std::vector<std::thread> writers;
    writers.reserve(kRanks - 1);
    for (uint32_t rank = 0; rank + 1 < kRanks; ++rank) {
        writers.emplace_back([&, rank] {
            coro_rpc::coro_rpc_client client;
            if (async_simple::coro::syncAwait(
                    client.connect("127.0.0.1", std::to_string(port_)))) {
                writes[rank].failure = "connect failed";
                return;
            }
            writes[rank] = WriteShard(
                client, ranks[rank].client_id, key, rank, kRanks, kPartBytes,
                ShardChecksum(rank, /*tag=*/0), ranks[rank].segment.name);
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    for (uint32_t rank = 0; rank + 1 < kRanks; ++rank) {
        ASSERT_TRUE(writes[rank].failure.empty())
            << "rank " << rank << ": " << writes[rank].failure;
    }

    coro_rpc::coro_rpc_client reader;
    ASSERT_FALSE(async_simple::coro::syncAwait(
        reader.connect("127.0.0.1", std::to_string(port_))))
        << "connect failed";

    // A block with a shard missing is not one a reader can use, whichever shard
    // the reader asked for.
    for (const uint32_t part : {uint32_t{0}, kRanks / 2, kRanks - 1}) {
        auto unfinished = async_simple::coro::syncAwait(
            reader.call<&WrappedMasterService::GetReplicaList>(
                key, tenant, struct_pack::compatible<uint32_t>{part}));
        ASSERT_TRUE(unfinished.has_value()) << "call failed";
        ASSERT_FALSE(unfinished->has_value()) << "part " << part << " readable";
        EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, unfinished->error());
    }
    auto exists = service_->ExistKey(key, tenant);
    ASSERT_TRUE(exists.has_value());
    EXPECT_FALSE(exists.value());

    // The last rank lands, and every rank can read the shard it computed.
    const ShardWrite last =
        WriteShard(reader, ranks[kRanks - 1].client_id, key, kRanks - 1, kRanks,
                   kPartBytes, ShardChecksum(kRanks - 1, /*tag=*/0),
                   ranks[kRanks - 1].segment.name);
    ASSERT_TRUE(last.failure.empty()) << last.failure;
    auto now_exists = service_->ExistKey(key, tenant);
    ASSERT_TRUE(now_exists.has_value());
    EXPECT_TRUE(now_exists.value());

    for (uint32_t rank = 0; rank < kRanks; ++rank) {
        auto read = async_simple::coro::syncAwait(
            reader.call<&WrappedMasterService::GetReplicaList>(
                key, tenant, struct_pack::compatible<uint32_t>{rank}));
        ASSERT_TRUE(read.has_value()) << "rank " << rank << ": call failed";
        ASSERT_TRUE(read->has_value())
            << "rank " << rank << ": " << toString(read->error());
        ASSERT_EQ((*read)->replicas.size(), 1u) << "rank " << rank;
        ASSERT_TRUE((*read)->part_count.has_value());
        EXPECT_EQ(*(*read)->part_count, kRanks) << "rank " << rank;
        ASSERT_TRUE((*read)->object_checksum.has_value());
        EXPECT_EQ(*(*read)->object_checksum, ShardChecksum(rank, 0))
            << "rank " << rank;
        const auto& buffer =
            std::get<MemoryDescriptor>((*read)->replicas[0].descriptor_variant)
                .buffer_descriptor;
        // The shard of rank `rank` is the one in rank `rank`'s own segment.
        EXPECT_EQ(buffer.transport_endpoint_, ranks[rank].segment.name)
            << "rank " << rank;
        EXPECT_EQ(buffer.size_, kPartBytes) << "rank " << rank;
    }
}

// A rank writes its own shard of every block of a request in one batch, and the
// stage that takes the request on reads every shard of every block in one
// batch.
TEST_F(PartRpcTest, BatchCarriesOneRankShardOfEveryBlock) {
    constexpr uint32_t kTensorParallel = 2;
    constexpr uint32_t kPipelineStages = 2;
    constexpr uint32_t kRanks = kTensorParallel * kPipelineStages;
    constexpr uint32_t kBlocks = 4;
    constexpr uint64_t kPartBytes = 1024 * 1024;
    constexpr size_t kRankSegmentBytes = 32 * 1024 * 1024;
    const std::string tenant(TenantId::kDefaultValue);

    std::vector<std::string> keys;
    keys.reserve(kBlocks);
    for (uint32_t block = 0; block < kBlocks; ++block) {
        keys.push_back("kv_request_block_" + std::to_string(block));
    }

    std::vector<MountedRank> ranks(kRanks);
    for (uint32_t rank = 0; rank < kRanks; ++rank) {
        ranks[rank] = MountRank(
            *service_, "kv_batch_rank_" + std::to_string(rank),
            0x500000000ULL + static_cast<uintptr_t>(rank) * kRankSegmentBytes,
            kRankSegmentBytes);
        ASSERT_TRUE(ranks[rank].failure.empty())
            << "rank " << rank << ": " << ranks[rank].failure;
    }

    coro_rpc::coro_rpc_client client;
    ASSERT_FALSE(async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_))))
        << "connect failed";

    for (uint32_t rank = 0; rank < kRanks; ++rank) {
        ReplicateConfig config;
        config.replica_num = 1;
        config.preferred_segments = {ranks[rank].segment.name};
        auto started = async_simple::coro::syncAwait(
            client.call<&WrappedMasterService::BatchPutStart>(
                ranks[rank].client_id, keys,
                std::vector<uint64_t>(kBlocks, kPartBytes), config, tenant,
                struct_pack::compatible<std::vector<uint32_t>>{
                    std::vector<uint32_t>(kBlocks, rank)},
                struct_pack::compatible<uint32_t>{kRanks}));
        ASSERT_TRUE(started.has_value()) << "rank " << rank << ": call failed";
        ASSERT_EQ(started->size(), kBlocks) << "rank " << rank;
        std::vector<ObjectMeta> metas;
        std::vector<std::vector<uint64_t>> per_block_ids;
        metas.reserve(kBlocks);
        per_block_ids.reserve(kBlocks);
        for (uint32_t block = 0; block < kBlocks; ++block) {
            ASSERT_TRUE((*started)[block].has_value())
                << "rank " << rank << ", block " << block << ": "
                << toString((*started)[block].error());
            std::vector<uint64_t> ids;
            for (const auto& descriptor : *(*started)[block]) {
                ids.push_back(descriptor.id);
            }
            per_block_ids.push_back(std::move(ids));
            metas.push_back(
                ObjectMeta{keys[block], ShardChecksum(rank, block)});
        }
        auto ended = async_simple::coro::syncAwait(
            client.call<&WrappedMasterService::BatchPutEnd>(
                ranks[rank].client_id, metas, ReplicaType::MEMORY, tenant,
                struct_pack::compatible<std::vector<uint32_t>>{
                    std::vector<uint32_t>(kBlocks, rank)},
                struct_pack::compatible<std::vector<std::vector<uint64_t>>>{
                    per_block_ids}));
        ASSERT_TRUE(ended.has_value()) << "rank " << rank << ": call failed";
        ASSERT_EQ(ended->size(), kBlocks) << "rank " << rank;
        for (uint32_t block = 0; block < kBlocks; ++block) {
            ASSERT_TRUE((*ended)[block].has_value())
                << "rank " << rank << ", block " << block << ": "
                << toString((*ended)[block].error());
        }
    }

    std::vector<std::string> read_keys;
    std::vector<uint32_t> read_parts;
    read_keys.reserve(kBlocks * kRanks);
    read_parts.reserve(kBlocks * kRanks);
    for (uint32_t block = 0; block < kBlocks; ++block) {
        for (uint32_t rank = 0; rank < kRanks; ++rank) {
            read_keys.push_back(keys[block]);
            read_parts.push_back(rank);
        }
    }
    auto read = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::BatchGetReplicaList>(
            read_keys, tenant,
            struct_pack::compatible<std::vector<uint32_t>>{read_parts}));
    ASSERT_TRUE(read.has_value()) << "call failed";
    ASSERT_EQ(read->size(), read_keys.size());
    for (size_t i = 0; i < read->size(); ++i) {
        ASSERT_TRUE((*read)[i].has_value()) << toString((*read)[i].error());
        const uint32_t block = static_cast<uint32_t>(i / kRanks);
        EXPECT_EQ(*(*read)[i]->object_checksum,
                  ShardChecksum(read_parts[i], block))
            << "item " << i;
        ASSERT_TRUE((*read)[i]->part_count.has_value()) << "item " << i;
        EXPECT_EQ(*(*read)[i]->part_count, kRanks) << "item " << i;
        EXPECT_EQ((*read)[i]->replicas.size(), 1u) << "item " << i;
        EXPECT_EQ(std::get<MemoryDescriptor>(
                      (*read)[i]->replicas[0].descriptor_variant)
                      .buffer_descriptor.transport_endpoint_,
                  ranks[read_parts[i]].segment.name)
            << "item " << i;
    }
}

// One rank's segment goes away with the rank: the block stops being one a
// reader can use, and it comes back when that rank mounts again and writes its
// shard, with the other ranks' shards where they were.
TEST_F(PartRpcTest, ARankWhoseSegmentWentAwayRewritesItsShard) {
    constexpr uint32_t kRanks = 4;  // Two tensor-parallel ranks, two stages.
    constexpr uint64_t kPartBytes = 1024 * 1024;
    constexpr size_t kRankSegmentBytes = 16 * 1024 * 1024;
    const std::string key = "kv_block_rank_restart";
    const std::string tenant(TenantId::kDefaultValue);

    std::vector<MountedRank> ranks(kRanks);
    for (uint32_t rank = 0; rank < kRanks; ++rank) {
        ranks[rank] = MountRank(
            *service_, "kv_restart_rank_" + std::to_string(rank),
            0x600000000ULL + static_cast<uintptr_t>(rank) * kRankSegmentBytes,
            kRankSegmentBytes);
        ASSERT_TRUE(ranks[rank].failure.empty())
            << "rank " << rank << ": " << ranks[rank].failure;
    }

    coro_rpc::coro_rpc_client client;
    ASSERT_FALSE(async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_))))
        << "connect failed";
    const auto read_part = [&](uint32_t part) {
        return async_simple::coro::syncAwait(
            client.call<&WrappedMasterService::GetReplicaList>(
                key, tenant, struct_pack::compatible<uint32_t>{part}));
    };

    for (uint32_t rank = 0; rank < kRanks; ++rank) {
        const ShardWrite write = WriteShard(
            client, ranks[rank].client_id, key, rank, kRanks, kPartBytes,
            ShardChecksum(rank, /*tag=*/3), ranks[rank].segment.name);
        ASSERT_TRUE(write.failure.empty())
            << "rank " << rank << ": " << write.failure;
    }
    auto complete = read_part(0);
    ASSERT_TRUE(complete.has_value()) << "call failed";
    ASSERT_TRUE(complete->has_value()) << toString(complete->error());

    ASSERT_TRUE(
        service_->UnmountSegment(ranks[2].segment.id, ranks[2].client_id)
            .has_value());
    for (uint32_t part = 0; part < kRanks; ++part) {
        auto gone = read_part(part);
        ASSERT_TRUE(gone.has_value()) << "part " << part << ": call failed";
        ASSERT_FALSE(gone->has_value()) << "part " << part << " still readable";
        EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, gone->error());
    }

    // The rank is back with its segment, and its shard is what is missing.
    ASSERT_TRUE(service_->ReMountSegment({ranks[2].segment}, ranks[2].client_id)
                    .has_value());
    auto remounted = read_part(1);
    ASSERT_TRUE(remounted.has_value()) << "call failed";
    ASSERT_FALSE(remounted->has_value());
    EXPECT_EQ(ErrorCode::REPLICA_IS_NOT_READY, remounted->error());

    const ShardWrite rewrite =
        WriteShard(client, ranks[2].client_id, key, 2, kRanks, kPartBytes,
                   ShardChecksum(2, /*tag=*/4), ranks[2].segment.name);
    ASSERT_TRUE(rewrite.failure.empty()) << rewrite.failure;

    for (uint32_t part = 0; part < kRanks; ++part) {
        auto read = read_part(part);
        ASSERT_TRUE(read.has_value()) << "part " << part << ": call failed";
        ASSERT_TRUE(read->has_value())
            << "part " << part << ": " << toString(read->error());
        const uint64_t expected =
            part == 2 ? ShardChecksum(2, /*tag=*/4) : ShardChecksum(part, 3);
        EXPECT_EQ(*(*read)->object_checksum, expected) << "part " << part;
        EXPECT_EQ(
            std::get<MemoryDescriptor>((*read)->replicas[0].descriptor_variant)
                .buffer_descriptor.transport_endpoint_,
            ranks[part].segment.name)
            << "part " << part;
    }
}

// The master sees a part count and no layout, so 8x4 and 16x2 are one shape to
// it: an object split either way is accepted, and the part checksum is what
// says which shard a reader holds.
TEST_F(PartRpcTest, TwoLayoutsWithTheSamePartCountFillTheSameShape) {
    constexpr uint32_t kParts = 32;
    const std::string tenant(TenantId::kDefaultValue);

    coro_rpc::coro_rpc_client client;
    ASSERT_FALSE(async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_))))
        << "connect failed";

    // Each rank writes the index its own mapping gives it, and the checksum it
    // records names the (rank, stage) pair that index stands for there.
    const auto write_block = [&](const std::string& key, uint32_t tp,
                                 uint32_t pp) {
        for (uint32_t stage = 0; stage < pp; ++stage) {
            for (uint32_t rank = 0; rank < tp; ++rank) {
                const ShardWrite write = WriteShard(
                    client, client_id_, key, stage * tp + rank, kParts,
                    uint64_t{1024}, ShardChecksum(rank, stage));
                ASSERT_TRUE(write.failure.empty())
                    << key << " part " << stage * tp + rank << ": "
                    << write.failure;
            }
        }
    };
    const auto read_part = [&](const std::string& key, uint32_t part) {
        return async_simple::coro::syncAwait(
            client.call<&WrappedMasterService::GetReplicaList>(
                key, tenant, struct_pack::compatible<uint32_t>{part}));
    };

    write_block("kv_block_tp8_pp4", /*tp=*/8, /*pp=*/4);
    write_block("kv_block_tp16_pp2", /*tp=*/16, /*pp=*/2);

    // Part 17 is stage 2 rank 1 under 8x4 and stage 1 rank 1 under 16x2: the
    // same index, a different shard, and only the checksum tells them apart.
    auto tp8 = read_part("kv_block_tp8_pp4", 17);
    ASSERT_TRUE(tp8.has_value()) << "8x4: call failed";
    ASSERT_TRUE(tp8->has_value()) << "8x4: " << toString(tp8->error());
    EXPECT_EQ(*(*tp8)->object_checksum, ShardChecksum(/*rank=*/1, /*tag=*/2));

    auto tp16 = read_part("kv_block_tp16_pp2", 17);
    ASSERT_TRUE(tp16.has_value()) << "16x2: call failed";
    ASSERT_TRUE(tp16->has_value()) << "16x2: " << toString(tp16->error());
    EXPECT_EQ(*(*tp16)->object_checksum, ShardChecksum(/*rank=*/1, /*tag=*/1));
}

// A part's replica need not sit in a segment its writer mounted: the write
// names the segment it wants, and a reader addresses that replica's own
// endpoint. Keeping a rank's shard next to the rank is the client's choice.
TEST_F(PartRpcTest, APartReplicaLandsInTheSegmentTheWriteNames) {
    constexpr uint32_t kParts = 2;
    const std::string key = "kv_block_borrowed_segment";
    const std::string tenant(TenantId::kDefaultValue);

    const MountedRank spare =
        MountRank(*service_, "kv_rank_spare", 0x500000000ULL, 16 * 1024 * 1024);
    ASSERT_TRUE(spare.failure.empty()) << spare.failure;

    coro_rpc::coro_rpc_client client;
    ASSERT_FALSE(async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(port_))))
        << "connect failed";

    // This client writes both parts, and asks for the spare rank's segment for
    // the first of them.
    const ShardWrite borrowed =
        WriteShard(client, client_id_, key, /*part_index=*/0, kParts,
                   uint64_t{1024}, ShardChecksum(0, 6), spare.segment.name);
    ASSERT_TRUE(borrowed.failure.empty()) << borrowed.failure;
    const ShardWrite own =
        WriteShard(client, client_id_, key, /*part_index=*/1, kParts,
                   uint64_t{1024}, ShardChecksum(1, 6));
    ASSERT_TRUE(own.failure.empty()) << own.failure;

    auto read = async_simple::coro::syncAwait(
        client.call<&WrappedMasterService::GetReplicaList>(
            key, tenant, struct_pack::compatible<uint32_t>{0}));
    ASSERT_TRUE(read.has_value()) << "call failed";
    ASSERT_TRUE(read->has_value()) << toString(read->error());
    EXPECT_EQ(
        std::get<MemoryDescriptor>((*read)->replicas[0].descriptor_variant)
            .buffer_descriptor.transport_endpoint_,
        spare.segment.name);
}

}  // namespace
}  // namespace mooncake
