#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <functional>
#include <thread>
#include <vector>
#include <memory>
#include <string>

#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "common/network.h"
#include "master_client.h"
#include "master_config.h"
#include "rpc_service.h"

namespace mooncake {
namespace {

class DrainJobRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 500;
        config.enable_metric_reporting = false;
        service_ = std::make_unique<WrappedMasterService>(config);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            2, 0, "127.0.0.1", std::chrono::seconds(0), true);
        RegisterRpcService(*server_, *service_);
        auto started = server_->async_start();
        ASSERT_FALSE(started.hasResult());
        auto connected = async_simple::coro::syncAwait(
            raw_client_.connect("127.0.0.1", std::to_string(server_->port())));
        ASSERT_FALSE(connected);
        address_ = "127.0.0.1:" + std::to_string(server_->port());
        client_ = std::make_unique<MasterClient>(client_id_);
        ASSERT_EQ(
            client_->Connect("127.0.0.1:" + std::to_string(server_->port())),
            ErrorCode::OK);
    }

    void TearDown() override {
        client_.reset();
        if (server_) server_->stop();
    }

    static bool WaitUntil(const std::function<bool()>& ready) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (std::chrono::steady_clock::now() < deadline) {
            if (ready()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return false;
    }

    // Metadata-only setup keeps cancellation states stable without racing
    // a real executor. The data migration test below uses real allocations.
    void MountSourceAndPut(bool hard_pin) {
        Segment source;
        source.id = generate_uuid();
        source.name = "source";
        source.te_endpoint = "source";
        source.base = 0x300000000;
        source.size = 16 * 1024 * 1024;
        ASSERT_TRUE(client_->MountSegment(source).has_value());
        ReplicateConfig config;
        config.preferred_segment = source.name;
        config.with_hard_pin = hard_pin;
        ASSERT_TRUE(
            client_->PutStart("source-key", {1024}, config).has_value());
        ASSERT_TRUE(client_
                        ->PutEnd(ObjectMeta{"source-key", std::nullopt},
                                 ReplicaType::MEMORY)
                        .has_value());
    }

    std::string address_;
    UUID client_id_ = generate_uuid();
    std::unique_ptr<WrappedMasterService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    coro_rpc::coro_rpc_client raw_client_;
    std::unique_ptr<MasterClient> client_;
};

TEST_F(DrainJobRpcTest, CreateHandlerPreservesValidationError) {
    auto result = async_simple::coro::syncAwait(
        raw_client_.call<&WrappedMasterService::CreateDrainJob>(
            CreateDrainJobRequest{}));
    ASSERT_TRUE(result.has_value()) << result.error().msg;
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(DrainJobRpcTest, QueryHandlerPreservesMissingJobError) {
    auto result = async_simple::coro::syncAwait(
        raw_client_.call<&WrappedMasterService::QueryDrainJob>(
            generate_uuid()));
    ASSERT_TRUE(result.has_value()) << result.error().msg;
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error(), ErrorCode::JOB_NOT_FOUND);
}

TEST_F(DrainJobRpcTest, CancelHandlerPreservesMissingJobError) {
    auto result = async_simple::coro::syncAwait(
        raw_client_.call<&WrappedMasterService::CancelDrainJob>(
            generate_uuid()));
    ASSERT_TRUE(result.has_value()) << result.error().msg;
    ASSERT_FALSE(result->has_value());
    EXPECT_EQ(result->error(), ErrorCode::JOB_NOT_FOUND);
}

TEST_F(DrainJobRpcTest, ClientPreservesJobAndSegmentErrors) {
    auto created = client_->CreateDrainJob({});
    ASSERT_FALSE(created.has_value());
    EXPECT_EQ(created.error(), ErrorCode::INVALID_PARAMS);
    const auto missing = generate_uuid();
    auto queried = client_->QueryDrainJob(missing);
    ASSERT_FALSE(queried.has_value());
    EXPECT_EQ(queried.error(), ErrorCode::JOB_NOT_FOUND);
    auto canceled = client_->CancelDrainJob(missing);
    ASSERT_FALSE(canceled.has_value());
    EXPECT_EQ(canceled.error(), ErrorCode::JOB_NOT_FOUND);
    auto status = client_->QuerySegmentStatus("missing-segment");
    ASSERT_FALSE(status.has_value());
    EXPECT_EQ(status.error(), ErrorCode::SEGMENT_NOT_FOUND);
}

TEST_F(DrainJobRpcTest, ClientCancelsBlockedJobAndRestoresSegment) {
    ASSERT_NO_FATAL_FAILURE(MountSourceAndPut(/*hard_pin=*/true));
    CreateDrainJobRequest request;
    request.segments = {"source"};
    request.max_concurrency = 1;
    auto created = client_->CreateDrainJob(request);
    ASSERT_TRUE(created.has_value()) << toString(created.error());
    ASSERT_TRUE(WaitUntil([&] {
        auto job = client_->QueryDrainJob(*created);
        return job.has_value() && job->blocked_units == 1;
    }));
    auto job = client_->QueryDrainJob(*created);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->id, *created);
    EXPECT_EQ(job->type, JobType::DRAIN);
    EXPECT_EQ(job->status, JobStatus::RUNNING);
    EXPECT_EQ(job->segments, request.segments);
    EXPECT_EQ(job->active_units, 0u);
    EXPECT_EQ(job->succeeded_units, 0u);
    EXPECT_EQ(job->failed_units, 0u);
    EXPECT_EQ(job->migrated_bytes, 0u);
    EXPECT_GT(job->created_at_ms_epoch, 0);
    EXPECT_GE(job->last_updated_at_ms_epoch, job->created_at_ms_epoch);
    EXPECT_FALSE(job->message.empty());
    auto status = client_->QuerySegmentStatus("source");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(*status, SegmentStatus::DRAINING);

    ASSERT_TRUE(client_->CancelDrainJob(*created).has_value());
    job = client_->QueryDrainJob(*created);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->status, JobStatus::CANCELED);
    status = client_->QuerySegmentStatus("source");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(*status, SegmentStatus::OK);
    auto second_cancel = client_->CancelDrainJob(*created);
    ASSERT_FALSE(second_cancel.has_value());
    EXPECT_EQ(second_cancel.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    ReplicateConfig config;
    config.preferred_segment = "source";
    auto allocation = client_->PutStart("after-cancel", {1024}, config);
    ASSERT_TRUE(allocation.has_value()) << toString(allocation.error());
    ASSERT_EQ(allocation->size(), 1u);
    EXPECT_EQ(allocation->front()
                  .get_memory_descriptor()
                  .buffer_descriptor.transport_endpoint_,
              "source");
}

TEST_F(DrainJobRpcTest, ClientRejectsInvalidDrainRequests) {
    ASSERT_NO_FATAL_FAILURE(MountSourceAndPut(/*hard_pin=*/true));
    for (const auto& request :
         std::vector<CreateDrainJobRequest>{{{"source"}, {}, 0},
                                            {{"source", "source"}, {}, 1},
                                            {{"source"}, {"source"}, 1}}) {
        auto result = client_->CreateDrainJob(request);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    for (const auto& request : std::vector<CreateDrainJobRequest>{
             {{"missing"}, {}, 1}, {{"source"}, {"missing"}, 1}}) {
        auto result = client_->CreateDrainJob(request);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::SEGMENT_NOT_FOUND);
    }
    auto status = client_->QuerySegmentStatus("source");
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(*status, SegmentStatus::OK);
}

TEST_F(DrainJobRpcTest, ClientCannotCancelAnActiveMove) {
    ASSERT_NO_FATAL_FAILURE(MountSourceAndPut(/*hard_pin=*/false));
    Segment target;
    target.id = generate_uuid();
    target.name = "target";
    target.te_endpoint = "target";
    target.base = 0x400000000;
    target.size = 16 * 1024 * 1024;
    ASSERT_TRUE(service_->MountSegment(target, generate_uuid()).has_value());
    auto created = client_->CreateDrainJob({{"source"}, {"target"}, 1});
    ASSERT_TRUE(created.has_value()) << toString(created.error());
    ASSERT_TRUE(WaitUntil([&] {
        auto tasks = client_->FetchTasks(1);
        return tasks.has_value() && tasks->size() == 1;
    }));
    auto canceled = client_->CancelDrainJob(*created);
    ASSERT_FALSE(canceled.has_value());
    EXPECT_EQ(canceled.error(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    auto job = client_->QueryDrainJob(*created);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->status, JobStatus::RUNNING);
    EXPECT_EQ(job->active_units, 1u);
}

TEST_F(DrainJobRpcTest, ClientDrainMigratesDataBeforeSourceUnmount) {
    constexpr size_t kSegmentSize = 16 * 1024 * 1024;
    const auto release = [](void* p) { free_memory("tcp", p); };
    std::unique_ptr<void, decltype(release)> source_buffer(
        allocate_buffer_allocator_memory(kSegmentSize, "tcp"), release);
    std::unique_ptr<void, decltype(release)> target_buffer(
        allocate_buffer_allocator_memory(kSegmentSize, "tcp"), release);
    ASSERT_NE(source_buffer.get(), nullptr);
    ASSERT_NE(target_buffer.get(), nullptr);
    std::vector<uint8_t> payload(32769);
    std::vector<uint8_t> output(payload.size(), 0);
    for (size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<uint8_t>((i * 17 + 13) % 251);
    }
    const auto ports = getFreeTcpPorts(2);
    ASSERT_EQ(ports.size(), 2u);
    const auto source_name = "127.0.0.1:" + std::to_string(ports[0]);
    auto source_result = Client::Create(source_name, "P2PHANDSHAKE", "tcp",
                                        std::nullopt, address_);
    ASSERT_TRUE(source_result.has_value());
    auto source = std::move(*source_result);
    const auto target_name = "127.0.0.1:" + std::to_string(ports[1]);
    auto target_result = Client::Create(target_name, "P2PHANDSHAKE", "tcp",
                                        std::nullopt, address_);
    ASSERT_TRUE(target_result.has_value());
    auto target = std::move(*target_result);
    ASSERT_TRUE(
        source->MountSegment(source_buffer.get(), kSegmentSize).has_value());
    ASSERT_TRUE(
        target->MountSegment(target_buffer.get(), kSegmentSize).has_value());
    ASSERT_TRUE(source
                    ->RegisterLocalMemory(payload.data(), payload.size(),
                                          kWildcardLocation)
                    .has_value());
    ASSERT_TRUE(target
                    ->RegisterLocalMemory(output.data(), output.size(),
                                          kWildcardLocation)
                    .has_value());
    const auto source_endpoint = source->GetSegmentEndpoint();
    const auto target_endpoint = target->GetSegmentEndpoint();
    ReplicateConfig config;
    config.preferred_segment = source_name;
    std::vector<Slice> put_slices{{payload.data(), payload.size()}};
    for (const auto& key : {"drain-data-1", "drain-data-2"}) {
        auto put = source->Put(key, put_slices, config);
        ASSERT_TRUE(put.has_value()) << toString(put.error());
        auto replicas = client_->GetReplicaList(key);
        ASSERT_TRUE(replicas.has_value());
        ASSERT_EQ(replicas->replicas.size(), 1u);
        ASSERT_EQ(replicas->replicas.front()
                      .get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  source_endpoint);
    }
    auto created = client_->CreateDrainJob({{source_name}, {target_name}, 1});
    ASSERT_TRUE(created.has_value()) << toString(created.error());
    ASSERT_TRUE(WaitUntil([&] {
        auto job = client_->QueryDrainJob(*created);
        return job.has_value() && job->status == JobStatus::SUCCEEDED;
    }));
    auto job = client_->QueryDrainJob(*created);
    ASSERT_TRUE(job.has_value());
    EXPECT_EQ(job->succeeded_units, 2u);
    EXPECT_EQ(job->failed_units, 0u);
    EXPECT_EQ(job->active_units, 0u);
    EXPECT_EQ(job->migrated_bytes, payload.size() * 2);
    auto status = client_->QuerySegmentStatus(source_name);
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(*status, SegmentStatus::DRAINED);
    ASSERT_TRUE(
        source->UnmountSegment(source_buffer.get(), kSegmentSize).has_value());
    source.reset();

    std::vector<Slice> get_slices{{output.data(), output.size()}};
    for (const auto& key : {"drain-data-1", "drain-data-2"}) {
        auto replicas = client_->GetReplicaList(key);
        ASSERT_TRUE(replicas.has_value());
        ASSERT_EQ(replicas->replicas.size(), 1u);
        EXPECT_EQ(replicas->replicas.front()
                      .get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  target_endpoint);
        std::fill(output.begin(), output.end(), 0);
        auto get = target->Get(key, get_slices);
        ASSERT_TRUE(get.has_value()) << toString(get.error());
        EXPECT_EQ(output, payload);
    }
    ASSERT_TRUE(
        target->UnmountSegment(target_buffer.get(), kSegmentSize).has_value());
}

}  // namespace
}  // namespace mooncake
