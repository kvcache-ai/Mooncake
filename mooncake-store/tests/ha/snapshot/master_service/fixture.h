#pragma once

// Shared fixture and helpers for snapshot-aware MasterService tests.

#include "ha/snapshot/master_service_test_for_snapshot_base.h"

#include <algorithm>
#include <atomic>
#include <random>
#include <unordered_set>
#include <utility>

namespace mooncake::test {

class MasterServiceSnapshotTest : public MasterServiceSnapshotTestBase {
   protected:
    static bool glog_initialized_;

    void SetUp() override {
        // Call base class SetUp first to reset MasterMetricManager state
        MasterServiceSnapshotTestBase::SetUp();

        if (!glog_initialized_) {
            google::InitGoogleLogging("MasterServiceSnapshotTest");
            FLAGS_logtostderr = true;
            glog_initialized_ = true;
        }
    }
};

bool MasterServiceSnapshotTest::glog_initialized_ = false;

std::string GenerateKeyForSegment(const UUID& client_id,
                                  const std::unique_ptr<MasterService>& service,
                                  const std::string& segment_name) {
    static std::atomic<uint64_t> counter(0);

    while (true) {
        std::string key = "key_" + std::to_string(counter.fetch_add(1));
        std::vector<Replica::Descriptor> replica_list;

        // Check if the key already exists.
        auto exist_result = service->ExistKey(key, "default");
        if (exist_result.has_value() && exist_result.value()) {
            continue;  // Retry if the key already exists
        }

        // Attempt to put the key.
        auto put_result = service->PutStart(client_id, key, "default", {1024},
                                            {.replica_num = 1});
        if (put_result.has_value()) {
            replica_list = std::move(put_result.value());
        }
        ErrorCode code =
            put_result.has_value() ? ErrorCode::OK : put_result.error();

        if (code == ErrorCode::OBJECT_ALREADY_EXISTS) {
            continue;  // Retry if the key already exists
        }
        if (code != ErrorCode::OK) {
            throw std::runtime_error("PutStart failed with code: " +
                                     std::to_string(static_cast<int>(code)));
        }
        auto put_end_result =
            service->PutEnd(client_id, key, "default", ReplicaType::MEMORY);
        if (!put_end_result.has_value()) {
            throw std::runtime_error("PutEnd failed");
        }
        if (replica_list[0]
                .get_memory_descriptor()
                .buffer_descriptor.transport_endpoint_ == segment_name) {
            return key;
        }
        // Clean up failed attempt
        auto remove_result = service->Remove(key, "default");
        if (!remove_result.has_value()) {
            // Ignore cleanup failure
        }
    }
}

}  // namespace mooncake::test
