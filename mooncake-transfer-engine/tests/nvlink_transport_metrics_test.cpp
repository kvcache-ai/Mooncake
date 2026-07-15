// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <set>
#include <string>

#include "multi_transport.h"
#include "transport/nvlink_transport/nvlink_transport.h"

namespace mooncake {

class NvlinkTransportTestPeer {
   public:
    enum class FailureStage { IMPORT, RESERVE, MAP, SET_ACCESS, COPY };

    static void ObserveCacheLookup(NvlinkTransport& transport, bool hit) {
        transport.observeCacheLookup(hit);
    }

    static void ObserveLazyImport(NvlinkTransport& transport,
                                  uint64_t duration_us) {
        transport.observeLazyImportLatency(duration_us);
    }

    static void ObserveFailure(NvlinkTransport& transport, FailureStage stage) {
        NvlinkTransport::ConsumerFailureStage consumer_stage =
            NvlinkTransport::ConsumerFailureStage::IMPORT;
        switch (stage) {
            case FailureStage::IMPORT:
                consumer_stage = NvlinkTransport::ConsumerFailureStage::IMPORT;
                break;
            case FailureStage::RESERVE:
                consumer_stage = NvlinkTransport::ConsumerFailureStage::RESERVE;
                break;
            case FailureStage::MAP:
                consumer_stage = NvlinkTransport::ConsumerFailureStage::MAP;
                break;
            case FailureStage::SET_ACCESS:
                consumer_stage =
                    NvlinkTransport::ConsumerFailureStage::SET_ACCESS;
                break;
            case FailureStage::COPY:
                consumer_stage = NvlinkTransport::ConsumerFailureStage::COPY;
                break;
        }
        transport.observeConsumerFailure(consumer_stage);
    }

    static bool ObserveTransferResultOnce(NvlinkTransport& transport,
                                          Transport::TransferTask& task,
                                          bool success) {
        return transport.observeTransferResultOnce(task, success);
    }

    static void FinalizeSubmissionFailure(NvlinkTransport& transport,
                                          Transport::TransferTask& task,
                                          bool copy_failure) {
        transport.finalizeSubmissionFailure(task, copy_failure);
    }
};

namespace {

class ScopedEnvironmentVariable {
   public:
    ScopedEnvironmentVariable(const char* name, const char* value)
        : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        setenv(name, value, 1);
    }

    ScopedEnvironmentVariable(const ScopedEnvironmentVariable&) = delete;
    ScopedEnvironmentVariable& operator=(const ScopedEnvironmentVariable&) =
        delete;

    ~ScopedEnvironmentVariable() {
        if (had_previous_) {
            setenv(name_.c_str(), previous_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string previous_;
    bool had_previous_ = false;
};

void ExpectSample(const std::string& metrics, const std::string& sample) {
    EXPECT_NE(metrics.find(sample), std::string::npos) << metrics;
}

void ExpectOnlyBoundedLabels(const std::string& metrics) {
    const std::set<std::string> allowed = {"operation", "result", "stage"};
    size_t cursor = 0;
    while (cursor < metrics.size()) {
        const size_t line_end = metrics.find('\n', cursor);
        const size_t end =
            line_end == std::string::npos ? metrics.size() : line_end;
        const std::string line = metrics.substr(cursor, end - cursor);
        if (!line.empty() && line.front() != '#') {
            const size_t labels_begin = line.find('{');
            const size_t labels_end = line.find('}', labels_begin);
            if (labels_begin != std::string::npos &&
                labels_end != std::string::npos) {
                size_t label_cursor = labels_begin + 1;
                while (label_cursor < labels_end) {
                    const size_t equal = line.find('=', label_cursor);
                    ASSERT_NE(equal, std::string::npos) << line;
                    const std::string name =
                        line.substr(label_cursor, equal - label_cursor);
                    EXPECT_TRUE(allowed.contains(name))
                        << "unexpected label " << name << " in " << line;
                    const size_t comma = line.find(',', equal);
                    label_cursor =
                        comma == std::string::npos || comma > labels_end
                            ? labels_end
                            : comma + 1;
                }
            }
        }
        if (line_end == std::string::npos) break;
        cursor = line_end + 1;
    }
}

TEST(NvlinkTransportMetricsTest, SerializesBoundedConsumerMetrics) {
    ScopedEnvironmentVariable force_ipc("MC_USE_NVLINK_IPC", "1");
    NvlinkTransport transport;

    NvlinkTransportTestPeer::ObserveCacheLookup(transport, true);
    NvlinkTransportTestPeer::ObserveCacheLookup(transport, true);
    NvlinkTransportTestPeer::ObserveCacheLookup(transport, false);
    NvlinkTransportTestPeer::ObserveLazyImport(transport, 7);
    NvlinkTransportTestPeer::ObserveLazyImport(transport, 13);

    using FailureStage = NvlinkTransportTestPeer::FailureStage;
    NvlinkTransportTestPeer::ObserveFailure(transport, FailureStage::IMPORT);
    NvlinkTransportTestPeer::ObserveFailure(transport, FailureStage::RESERVE);
    NvlinkTransportTestPeer::ObserveFailure(transport, FailureStage::MAP);
    NvlinkTransportTestPeer::ObserveFailure(transport,
                                            FailureStage::SET_ACCESS);
    NvlinkTransportTestPeer::ObserveFailure(transport, FailureStage::COPY);
    NvlinkTransportTestPeer::ObserveFailure(transport, FailureStage::COPY);

    constexpr std::array<Transport::TransferRequest::OpCode, 7> operations = {
        Transport::TransferRequest::READ,  Transport::TransferRequest::READ,
        Transport::TransferRequest::READ,  Transport::TransferRequest::WRITE,
        Transport::TransferRequest::WRITE, Transport::TransferRequest::WRITE,
        Transport::TransferRequest::WRITE};
    constexpr std::array<bool, 7> successes = {true,  true,  false, true,
                                               false, false, false};
    std::array<Transport::TransferRequest, 7> requests{};
    std::array<Transport::TransferTask, 7> tasks{};
    for (size_t index = 0; index < tasks.size(); ++index) {
        requests[index].opcode = operations[index];
        tasks[index].request = &requests[index];
        tasks[index].operation = requests[index].opcode;
        tasks[index].operation_initialized = true;
        tasks[index].request = nullptr;
        EXPECT_TRUE(NvlinkTransportTestPeer::ObserveTransferResultOnce(
            transport, tasks[index], successes[index]));
        EXPECT_FALSE(NvlinkTransportTestPeer::ObserveTransferResultOnce(
            transport, tasks[index], successes[index]))
            << "repeated terminal polling must not duplicate task metrics";
    }

    std::string metrics;
    transport.appendMetrics(metrics);

    ExpectSample(
        metrics,
        "mooncake_nvlink_consumer_mapping_cache_total{result=\"hit\"} 2");
    ExpectSample(
        metrics,
        "mooncake_nvlink_consumer_mapping_cache_total{result=\"miss\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_lazy_import_duration_us_total 20");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_lazy_import_observations_total 2");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_failures_total{stage=\"import\"} 1");
    ExpectSample(
        metrics,
        "mooncake_nvlink_consumer_failures_total{stage=\"reserve\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_failures_total{stage=\"map\"} 1");
    ExpectSample(
        metrics,
        "mooncake_nvlink_consumer_failures_total{stage=\"set_access\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_failures_total{stage=\"copy\"} 2");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"read\",result=\"success\"} 2");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"read\",result=\"failure\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"write\",result=\"success\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"write\",result=\"failure\"} 3");

    ExpectOnlyBoundedLabels(metrics);
    for (const std::string& forbidden :
         {"endpoint", "uuid", "address", "fabric_handle"}) {
        EXPECT_EQ(metrics.find(forbidden), std::string::npos) << metrics;
    }
}

TEST(NvlinkTransportMetricsTest,
     EventDrivenBatchFastPathFinalizesStableResultsOnce) {
    ScopedEnvironmentVariable force_ipc("MC_USE_NVLINK_IPC", "1");
    NvlinkTransport transport;
    std::string local_server_name = "metrics-test";
    MultiTransport multi_transport(nullptr, local_server_name);

    const auto batch_id = multi_transport.allocateBatchID(2);
    ASSERT_NE(batch_id, 0u);
    auto& batch = Transport::toBatchDesc(batch_id);
    batch.task_list.resize(2);

    std::array<Transport::TransferRequest, 2> requests{};
    requests[0].opcode = Transport::TransferRequest::READ;
    requests[1].opcode = Transport::TransferRequest::WRITE;
    for (size_t index = 0; index < batch.task_list.size(); ++index) {
        auto& task = batch.task_list[index];
        task.batch_id = batch_id;
        task.transport_ = &transport;
        task.request = &requests[index];
        task.operation = requests[index].opcode;
        task.operation_initialized = true;
        task.slice_count = 1;
        task.is_finished = true;
        // Model the caller-owned entries going out of scope before an
        // event-driven completion is observed.
        task.request = nullptr;
    }
    batch.task_list[0].success_slice_count = 1;
    batch.task_list[0].transferred_bytes = 64;
    batch.task_list[1].failed_slice_count = 1;
    batch.has_failure.store(true, std::memory_order_relaxed);
    batch.finished_transfer_bytes.store(64, std::memory_order_relaxed);
    batch.is_finished.store(true, std::memory_order_release);

    Transport::TransferStatus status;
    ASSERT_TRUE(multi_transport.getBatchTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    EXPECT_EQ(status.transferred_bytes, 64u);
    ASSERT_TRUE(multi_transport.getBatchTransferStatus(batch_id, status).ok());

    std::string metrics;
    transport.appendMetrics(metrics);
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"read\",result=\"success\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"write\",result=\"failure\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_failures_total{stage=\"copy\"} 1");

    EXPECT_TRUE(multi_transport.freeBatchID(batch_id).ok());
}

TEST(NvlinkTransportMetricsTest,
     NonCopySubmissionFailureStaysClassifiedAcrossBatchFastPath) {
    ScopedEnvironmentVariable force_ipc("MC_USE_NVLINK_IPC", "1");
    NvlinkTransport transport;
    std::string local_server_name = "metrics-test";
    MultiTransport multi_transport(nullptr, local_server_name);

    const auto batch_id = multi_transport.allocateBatchID(1);
    ASSERT_NE(batch_id, 0u);
    auto& batch = Transport::toBatchDesc(batch_id);
    batch.task_list.resize(1);
    auto& task = batch.task_list.front();
    task.batch_id = batch_id;
    task.transport_ = &transport;
    task.operation = Transport::TransferRequest::READ;
    task.operation_initialized = true;

    NvlinkTransportTestPeer::FinalizeSubmissionFailure(transport, task, false);
    EXPECT_TRUE(task.submission_failed);
    batch.is_finished.store(true, std::memory_order_release);

    Transport::TransferStatus status;
    ASSERT_TRUE(multi_transport.getBatchTransferStatus(batch_id, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    ASSERT_TRUE(multi_transport.getBatchTransferStatus(batch_id, status).ok());

    std::string metrics;
    transport.appendMetrics(metrics);
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_transfer_results_total{operation="
                 "\"read\",result=\"failure\"} 1");
    ExpectSample(metrics,
                 "mooncake_nvlink_consumer_failures_total{stage=\"copy\"} 0");

    EXPECT_TRUE(multi_transport.freeBatchID(batch_id).ok());
}

}  // namespace
}  // namespace mooncake
