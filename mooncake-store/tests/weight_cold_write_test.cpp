#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <memory>

#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake::testing {
namespace {

int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

class WeightColdWriteTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto pattern = (std::filesystem::temp_directory_path() /
                        "weight-cold-write-XXXXXX")
                           .string();
        ASSERT_NE(mkdtemp(pattern.data()), nullptr);
        directory_ = pattern;
        ASSERT_TRUE(server_.Start(
            InProcMasterConfigBuilder().set_enable_offload(true).build()));
        auto ports = getFreeTcpPorts(1);
        ASSERT_EQ(ports.size(), 1u);
        owner_ = std::make_unique<RealClient>();
        ASSERT_EQ(
            owner_->setup_real("127.0.0.1:" + std::to_string(ports.front()),
                               server_.metadata_url(), 64 * 1024, 1024 * 1024,
                               "tcp", "", server_.master_address(), nullptr, "",
                               true, directory_.string()),
            0);
        writer_id_ = generate_uuid();
        writer_ = std::make_unique<MasterClient>(writer_id_);
        ASSERT_EQ(writer_->Connect(server_.master_address()), ErrorCode::OK);
    }

    void TearDown() override {
        owner_.reset();
        writer_.reset();
        server_.Stop();
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }

    void Reserve(int64_t deadline_ms, unsigned index = 0) {
        identity_ = {.tenant_id = "default",
                     .name_space = "cold-writer",
                     .resource_id = "model",
                     .revision = std::to_string(index),
                     .weight_generation = 1};
        const auto key = "cold-payload-" + std::to_string(index);
        std::vector<WeightAffinityMappingEntry> layout{{key, 4096, "tensor"}};
        auto begin = writer_->BeginWeightImport({
            .identity = identity_,
            .expected_payload_count = 1,
            .expected_logical_bytes = 4096,
            .policy = WeightStoragePolicy{.preferred_residency =
                                              WeightResidencyState::COLD},
            .affinity_summary = {.affinity_count = 1,
                                 .affinity_digest =
                                     ComputeWeightAffinityMappingSha256(
                                         layout)},
            .payload_layout = layout,
        });
        ASSERT_TRUE(begin && *begin);
        generation_ = (*begin)->metadata_generation;
        ReplicateConfig config;
        config.data_type = ObjectDataType::WEIGHT;
        config.group_ids = {
            std::vector<std::string>{(*begin)->manifest.payload_group_id}};
        config.residency_affinity_ids = {std::vector<std::string>{"tensor"}};
        auto reserved = writer_->PutStart(key, {4096}, config);
        ASSERT_TRUE(reserved);
        ASSERT_EQ(reserved->size(), 1u);
        ASSERT_TRUE(reserved->front().is_local_disk_replica());
        const auto& disk = reserved->front().get_local_disk_descriptor();
        endpoint_ = disk.transport_endpoint;
        request_ = {
            writer_id_, disk.client_id, reserved->front().id, "default",
            key,        4096,           deadline_ms,          std::nullopt};
    }

    InProcMaster server_;
    std::unique_ptr<RealClient> owner_;
    std::unique_ptr<MasterClient> writer_;
    UUID writer_id_{};
    WeightRevisionIdentity identity_;
    uint64_t generation_{0};
    ColdWriteRequest request_;
    std::string endpoint_;
    std::filesystem::path directory_;
};

TEST_F(WeightColdWriteTest, ExpiredOwnerRequestCannotBeginOrPublish) {
    ASSERT_NO_FATAL_FAILURE(Reserve(NowMs() - 1));
    ClientRequester requester;
    auto begun = requester.cold_write_begin(endpoint_, request_);
    ASSERT_FALSE(begun);
    EXPECT_EQ(begun.error(), ErrorCode::INVALID_WRITE);
    EXPECT_FALSE(writer_->FinishColdWrite(request_));
}

TEST_F(WeightColdWriteTest, AbortFencesAnAlreadyStartedOwnerStream) {
    ASSERT_NO_FATAL_FAILURE(Reserve(NowMs() + 30000));
    ClientRequester requester;
    ASSERT_TRUE(requester.cold_write_begin(endpoint_, request_));
    auto aborted = writer_->AbortWeightImport(
        {.identity = identity_, .expected_metadata_generation = generation_});
    ASSERT_TRUE(aborted && *aborted);
    ColdWriteChunk chunk{request_, "unused-source", 1, 0, 4096};
    EXPECT_FALSE(requester.cold_write_chunk(endpoint_, chunk));
    EXPECT_TRUE(requester.cold_write_cancel(endpoint_, request_));
    EXPECT_FALSE(writer_->FinishColdWrite(request_));
}

TEST_F(WeightColdWriteTest, ShutdownCancelsAndJoinsAnUnfinishedOwnerStream) {
    ASSERT_NO_FATAL_FAILURE(Reserve(NowMs() + 30000));
    ClientRequester requester;
    ASSERT_TRUE(requester.cold_write_begin(endpoint_, request_));
    auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(owner_->tearDownAll(), 0);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(5));
}

TEST_F(WeightColdWriteTest, ConcurrentBeginCannotInstallAWorkerAfterShutdown) {
    ASSERT_NO_FATAL_FAILURE(Reserve(NowMs() + 30000));
    ClientRequester requester;
    auto begun = std::async(std::launch::async, [&] {
        return requester.cold_write_begin(endpoint_, request_);
    });
    auto started = std::chrono::steady_clock::now();
    EXPECT_EQ(owner_->tearDownAll(), 0);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(5));
    EXPECT_EQ(begun.wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    auto result = begun.get();
    // Either admission or shutdown may win; both outcomes must be drained.
    owner_.reset();
}

TEST_F(WeightColdWriteTest,
       BusyOwnerRejectsAnotherStreamWithoutBlockingRpcWorkers) {
    ASSERT_NO_FATAL_FAILURE(Reserve(NowMs() + 30000));
    ClientRequester requester;
    const auto first = request_;
    ASSERT_TRUE(requester.cold_write_begin(endpoint_, first));
    ASSERT_NO_FATAL_FAILURE(Reserve(NowMs() + 30000, 1));
    auto started = std::chrono::steady_clock::now();
    auto second = requester.cold_write_begin(endpoint_, request_);
    ASSERT_FALSE(second);
    EXPECT_EQ(second.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::seconds(2));
    EXPECT_TRUE(requester.cold_write_cancel(endpoint_, first));
}

}  // namespace
}  // namespace mooncake::testing
