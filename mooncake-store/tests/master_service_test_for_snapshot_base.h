#pragma once

#include "master_service.h"
#include "master_metric_manager.h"
#include "segment.h"
#include "etcd_helper.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <map>
#include <memory>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "types.h"

// TODO metrics consistency
namespace mooncake::test {

/**
 * @brief Base class for MasterService snapshot testing.
 *
 * This class provides common utilities and methods for testing
 * snapshot and restore functionality of MasterService.
 *
 * Derived classes should:
 * 1. Override SetUp() to initialize logging with appropriate test name
 * 2. Implement any test-specific helper methods
 * 3. Use service_ member variable for MasterService instance
 */
class MasterServiceSnapshotTestBase : public ::testing::Test {
   protected:
    // ==================== SetUp ====================

    void SetUp() override {
        // Reset MasterMetricManager singleton state to ensure test isolation
        // This prevents metric state leakage between tests which can cause
        // eviction behavior inconsistency and snapshot comparison failures
        MasterMetricManager::instance().reset_allocated_mem_size();
        MasterMetricManager::instance().reset_total_mem_capacity();
    }

    // ==================== Structures ====================

    // LocalDiskSegment state for comparison
    struct LocalDiskSegmentState {
        bool enable_offloading = false;
        std::map<std::string, int64_t>
            offloading_objects;  // key -> timestamp (sorted)
    };

    struct MountedSegmentContext {
        UUID segment_id;
        UUID client_id;
    };

    // Snapshot of service state for validation
    struct ServiceStateSnapshot {
        // === Basic State ===
        std::vector<std::string> all_keys;
        std::vector<std::string> all_segments;
        std::unordered_map<std::string, GetReplicaListResponse> replica_lists;
        std::unordered_map<std::string, std::pair<size_t, size_t>>
            segment_stats;

        // === Enhanced State ===
        size_t key_count = 0;

        // === LocalDiskSegment State ===
        std::map<std::string, UUID>
            client_by_name;  // segment name -> client_id (sorted)
        std::map<UUID, LocalDiskSegmentState>
            local_disk_segments;  // client_id -> segment state (sorted)
    };

    // ==================== Constants ====================

    static constexpr size_t kDefaultSegmentBase = 0x300000000;
    static constexpr size_t kDefaultSegmentSize = 1024 * 1024 * 16;

    // ==================== Segment Helper Methods ====================

    Segment MakeSegment(std::string name = "test_segment",
                        size_t base = kDefaultSegmentBase,
                        size_t size = kDefaultSegmentSize) const {
        Segment segment;
        segment.id = generate_uuid();
        segment.name = std::move(name);
        segment.base = base;
        segment.size = size;
        segment.te_endpoint = segment.name;
        return segment;
    }

    MountedSegmentContext PrepareSimpleSegment(
        MasterService& service, std::string name = "test_segment",
        size_t base = kDefaultSegmentBase,
        size_t size = kDefaultSegmentSize) const {
        Segment segment = MakeSegment(std::move(name), base, size);
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        EXPECT_TRUE(mount_result.has_value());
        return {.segment_id = segment.id, .client_id = client_id};
    }

    // ==================== Snapshot Helper Methods ====================

    // Wrapper method: Call MasterService's private method PersistState
    // This class is a friend of MasterService, so it can access private members
    static tl::expected<void, SerializationError> CallPersistState(
        MasterService* service, const std::string& snapshot_id) {
        return service->PersistState(snapshot_id);
    }

    // Generate unique snapshot ID (timestamp format)
    std::string GenerateSnapshotId() const {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()) %
                  1000;

        std::tm tm_now;
        localtime_r(&time_t_now, &tm_now);

        char buffer[32];
        std::snprintf(buffer, sizeof(buffer), "%04d%02d%02d_%02d%02d%02d_%03d",
                      tm_now.tm_year + 1900, tm_now.tm_mon + 1, tm_now.tm_mday,
                      tm_now.tm_hour, tm_now.tm_min, tm_now.tm_sec,
                      static_cast<int>(ms.count()));
        return std::string(buffer);
    }

    bool UseEtcdSnapshotBackend() const {
        return service_ != nullptr &&
               service_->snapshot_backend_type_ == SnapshotBackendType::ETCD;
    }

    std::string GetEtcdSnapshotPrefix(const std::string& snapshot_id) const {
        return "master_snapshot/" + snapshot_id + "/";
    }

    bool EnsureEtcdConnected() const {
        if (!service_) {
            LOG(ERROR) << "Service is null when ensuring etcd connection";
            return false;
        }
        if (service_->etcd_endpoints_.empty()) {
            LOG(ERROR) << "Etcd endpoints are empty in snapshot test";
            return false;
        }
        auto result =
            EtcdHelper::ConnectToEtcdStoreClient(service_->etcd_endpoints_);
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd endpoints: "
                       << service_->etcd_endpoints_
                       << ", error=" << static_cast<int>(result);
            return false;
        }
        return true;
    }

    bool GetEtcdValue(const std::string& key, std::string& value) const {
        EtcdRevisionId revision;
        auto result =
            EtcdHelper::Get(key.c_str(), key.size(), value, revision);
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Etcd get failed for key: " << key
                       << ", error=" << static_cast<int>(result);
            return false;
        }
        return true;
    }

    bool StartSnapshotDaemonForTest() const {
        if (!service_) {
            LOG(ERROR) << "Service is null when starting snapshot daemon";
            return false;
        }
        return service_->StartSnapshotDaemon();
    }

    // Get msgpack snapshot directory path or etcd key prefix
    std::string GetSnapshotDir(const std::string& snapshot_id) const {
        if (UseEtcdSnapshotBackend()) {
            return GetEtcdSnapshotPrefix(snapshot_id);
        }
        return "/tmp/mooncake_snapshots/master_snapshot/" + snapshot_id + "/";
    }

    // Get backup directory path
    std::string GetBackupDir(const std::string& snapshot_id) const {
        if (UseEtcdSnapshotBackend()) {
            auto it = etcd_backup_id_map_.find(snapshot_id);
            if (it != etcd_backup_id_map_.end()) {
                return GetEtcdSnapshotPrefix(it->second);
            }
            LOG(ERROR) << "Missing etcd backup mapping for snapshot_id: "
                       << snapshot_id;
            return GetEtcdSnapshotPrefix(snapshot_id);
        }
        return "/tmp/mooncake_snapshots/master_snapshot_backup/" + snapshot_id +
               "/";
    }

    // ==================== State Capture Methods ====================

    // Capture current service state for validation
    ServiceStateSnapshot CaptureServiceState(MasterService* service) const {
        ServiceStateSnapshot state;

        // === Basic State ===
        auto keys_result = service->GetAllKeys();
        if (keys_result.has_value()) {
            state.all_keys = std::move(keys_result.value());
            std::sort(state.all_keys.begin(), state.all_keys.end());
        }

        auto segments_result = service->GetAllSegments();
        if (segments_result.has_value()) {
            state.all_segments = std::move(segments_result.value());
            std::sort(state.all_segments.begin(), state.all_segments.end());
        }

        for (const auto& key : state.all_keys) {
            auto replica_result = service->GetReplicaList(key);
            if (replica_result.has_value()) {
                state.replica_lists[key] = std::move(replica_result.value());
            }
        }

        for (const auto& segment : state.all_segments) {
            auto query_result = service->QuerySegments(segment);
            if (query_result.has_value()) {
                state.segment_stats[segment] = query_result.value();
            }
        }

        // === Enhanced State ===
        state.key_count = service->GetKeyCount();

        // === LocalDiskSegment State (via friend access) ===
        {
            auto access = service->segment_manager_.getLocalDiskSegmentAccess();
            // Copy client_by_name_ to sorted map
            for (const auto& [name, client_id] : access.getClientByName()) {
                state.client_by_name[name] = client_id;
            }
            // Copy local_disk_segments with full state
            for (const auto& [client_id, segment] :
                 access.getClientLocalDiskSegment()) {
                LocalDiskSegmentState seg_state;
                seg_state.enable_offloading = segment->enable_offloading;
                // Copy offloading_objects to sorted map
                std::lock_guard<Mutex> lock(segment->offloading_mutex_);
                for (const auto& [key, timestamp] :
                     segment->offloading_objects) {
                    seg_state.offloading_objects[key] = timestamp;
                }
                state.local_disk_segments[client_id] = std::move(seg_state);
            }
        }

        return state;
    }

    // ==================== Comparison Methods ====================

    // Compare two Replica::Descriptor
    // @param compare_id: whether to compare replica id (default true)
    //   Set to false for PutStart consistency test where two services share
    //   the static next_id_ variable in the same process.
    bool CompareReplicaDescriptor(const Replica::Descriptor& a,
                                  const Replica::Descriptor& b,
                                  bool compare_id = true) const {
        if (compare_id && a.id != b.id) {
            LOG(ERROR) << "Replica id mismatch: a.id=" << a.id
                       << ", b.id=" << b.id;
            return false;
        }
        if (a.status != b.status) {
            return false;
        }
        // Check replica type consistency
        if (a.is_memory_replica() != b.is_memory_replica()) {
            return false;
        }
        if (a.is_disk_replica() != b.is_disk_replica()) {
            return false;
        }
        if (a.is_local_disk_replica() != b.is_local_disk_replica()) {
            return false;
        }
        // Compare replica content based on type
        if (a.is_memory_replica()) {
            const auto& ma = a.get_memory_descriptor();
            const auto& mb = b.get_memory_descriptor();
            return ma.buffer_descriptor.size_ == mb.buffer_descriptor.size_ &&
                   ma.buffer_descriptor.buffer_address_ ==
                       mb.buffer_descriptor.buffer_address_ &&
                   ma.buffer_descriptor.transport_endpoint_ ==
                       mb.buffer_descriptor.transport_endpoint_;
        }
        if (a.is_disk_replica()) {
            const auto& da = a.get_disk_descriptor();
            const auto& db = b.get_disk_descriptor();
            return da.file_path == db.file_path &&
                   da.object_size == db.object_size;
        }
        if (a.is_local_disk_replica()) {
            const auto& la = a.get_local_disk_descriptor();
            const auto& lb = b.get_local_disk_descriptor();
            return la.client_id == lb.client_id &&
                   la.object_size == lb.object_size &&
                   la.transport_endpoint == lb.transport_endpoint;
        }
        return true;
    }

    // Compare two GetReplicaListResponse
    bool CompareReplicaListResponse(const GetReplicaListResponse& a,
                                    const GetReplicaListResponse& b) const {
        if (a.replicas.size() != b.replicas.size()) {
            return false;
        }
        for (size_t i = 0; i < a.replicas.size(); ++i) {
            if (!CompareReplicaDescriptor(a.replicas[i], b.replicas[i])) {
                return false;
            }
        }
        return true;
    }

    // Compare two LocalDiskSegmentState
    bool CompareLocalDiskSegmentState(const LocalDiskSegmentState& a,
                                      const LocalDiskSegmentState& b) const {
        if (a.enable_offloading != b.enable_offloading) {
            return false;
        }
        if (a.offloading_objects != b.offloading_objects) {
            return false;
        }
        return true;
    }

    // Compare two ServiceStateSnapshot
    bool CompareServiceState(const ServiceStateSnapshot& before,
                             const ServiceStateSnapshot& after) const {
        bool all_passed = true;

        // ========== Check LocalDiskSegment state ==========
        if (before.client_by_name != after.client_by_name) {
            LOG(ERROR) << "client_by_name mismatch. before.size="
                       << before.client_by_name.size()
                       << ", after.size=" << after.client_by_name.size();
            all_passed = false;
        }
        if (before.local_disk_segments.size() !=
            after.local_disk_segments.size()) {
            LOG(ERROR) << "local_disk_segments size mismatch. before="
                       << before.local_disk_segments.size()
                       << ", after=" << after.local_disk_segments.size();
            all_passed = false;
        } else {
            for (const auto& [client_id, seg_state] :
                 before.local_disk_segments) {
                auto it = after.local_disk_segments.find(client_id);
                if (it == after.local_disk_segments.end()) {
                    LOG(ERROR) << "local_disk_segment missing for client_id";
                    all_passed = false;
                    continue;
                }
                if (!CompareLocalDiskSegmentState(seg_state, it->second)) {
                    LOG(ERROR) << "local_disk_segment state mismatch";
                    all_passed = false;
                }
            }
        }

        // ========== Level 1: Basic state comparison ==========

        if (before.all_keys != after.all_keys) {
            LOG(ERROR) << "all_keys mismatch. before.size="
                       << before.all_keys.size()
                       << ", after.size=" << after.all_keys.size();
            all_passed = false;
        }

        if (before.all_segments != after.all_segments) {
            LOG(ERROR) << "all_segments mismatch. before.size="
                       << before.all_segments.size()
                       << ", after.size=" << after.all_segments.size();
            all_passed = false;
        }

        // ========== Level 2: Enhanced state comparison ==========

        if (before.key_count != after.key_count) {
            LOG(ERROR) << "key_count mismatch. before=" << before.key_count
                       << ", after=" << after.key_count;
            all_passed = false;
        }

        // ========== Level 3: Detailed comparison ==========

        // Compare replica list for each key
        for (const auto& [key, replica_response] : before.replica_lists) {
            auto it = after.replica_lists.find(key);
            if (it == after.replica_lists.end()) {
                LOG(ERROR) << "replica_list missing for key: " << key;
                all_passed = false;
                continue;
            }
            if (!CompareReplicaListResponse(replica_response, it->second)) {
                LOG(ERROR) << "replica_list mismatch for key: " << key;
                all_passed = false;
            }
        }

        // Compare segment stats
        for (const auto& [segment, stats] : before.segment_stats) {
            auto it = after.segment_stats.find(segment);
            if (it == after.segment_stats.end()) {
                LOG(ERROR) << "segment_stats missing for: " << segment;
                all_passed = false;
                continue;
            }
            if (stats != it->second) {
                LOG(ERROR) << "segment_stats mismatch for: " << segment;
                all_passed = false;
            }
        }

        return all_passed;
    }

    // ==================== PutStart Consistency Check Methods
    // ====================

    // Find the segment with largest remaining space from a list of segments
    std::string FindBestSegment(
        MasterService* service,
        const std::vector<std::string>& segments) const {
        std::string best_segment;
        uint64_t max_remaining = 0;

        for (const auto& seg : segments) {
            auto result = service->QuerySegments(seg);
            if (result.has_value()) {
                auto [used, capacity] = result.value();
                uint64_t remaining = capacity - used;
                if (remaining > max_remaining) {
                    max_remaining = remaining;
                    best_segment = seg;
                }
            }
        }

        if (!best_segment.empty()) {
            LOG(INFO) << "[FindBestSegment] Selected: " << best_segment
                      << " (remaining=" << max_remaining << " bytes)";
        }
        return best_segment;
    }

    // Assert that PutStart results are consistent between original and restored
    // services. The best_segment is pre-selected outside this function (segment
    // with largest remaining space). Logic:
    // - If both services allocate on best_segment: compare replica descriptors
    // - If neither allocates on best_segment: both entered random allocation,
    // considered consistent
    // - If only one allocates on best_segment: state inconsistency, test fails
    void AssertPutStartConsistentWithBestSegment(
        MasterService* original, MasterService* restored,
        const std::string& best_segment) const {
        if (best_segment.empty()) {
            LOG(WARNING)
                << "Skip PutStart consistency check: no best_segment provided";
            return;
        }

        LOG(INFO) << "[PutStart Consistency] Using best_segment: "
                  << best_segment;

        ReplicateConfig config;
        config.preferred_segment = best_segment;
        config.replica_num = 1;

        const UUID client_id = generate_uuid();
        const std::string key = "snapshot_putstart_consistency_key";
        const uint64_t slice_length = 1024;

        auto before = original->PutStart(client_id, key, slice_length, config);
        auto after = restored->PutStart(client_id, key, slice_length, config);

        ASSERT_EQ(before.has_value(), after.has_value())
            << "PutStart has_value mismatch between original and restored "
               "service";

        if (!before.has_value()) {
            LOG(INFO) << "[PutStart Consistency] Both services failed PutStart";
            return;
        }

        const auto& reps_before = before.value();
        const auto& reps_after = after.value();

        ASSERT_EQ(reps_before.size(), reps_after.size())
            << "PutStart replica count mismatch: ORIGINAL="
            << reps_before.size() << ", RESTORED=" << reps_after.size();

        // Helper to get segment name from replica
        auto get_segment = [](const Replica::Descriptor& rep) -> std::string {
            if (rep.is_memory_replica()) {
                return rep.get_memory_descriptor()
                    .buffer_descriptor.transport_endpoint_;
            }
            return "";
        };

        // Check if replicas are allocated on best_segment
        bool orig_on_best = false;
        bool rest_on_best = false;

        for (const auto& rep : reps_before) {
            if (get_segment(rep) == best_segment) {
                orig_on_best = true;
                break;
            }
        }
        for (const auto& rep : reps_after) {
            if (get_segment(rep) == best_segment) {
                rest_on_best = true;
                break;
            }
        }

        LOG(INFO) << "[PutStart Consistency] ORIGINAL on best_segment: "
                  << orig_on_best
                  << ", RESTORED on best_segment: " << rest_on_best;

        ASSERT_EQ(orig_on_best, rest_on_best)
            << "PutStart state inconsistency: ORIGINAL on best_segment="
            << orig_on_best << ", RESTORED on best_segment=" << rest_on_best
            << ". One service used preferred_segment while the other didn't.";

        if (!orig_on_best && !rest_on_best) {
            // Both entered random allocation - considered consistent
            LOG(INFO) << "[PutStart Consistency] Both services entered random "
                         "allocation, considered consistent";
            return;
        }

        // Both allocated on best_segment - compare replica descriptors
        // Note: compare_id=false because next_id_ is a static variable shared
        // by both services in the same process.
        for (size_t i = 0; i < reps_before.size(); ++i) {
            ASSERT_TRUE(
                CompareReplicaDescriptor(reps_before[i], reps_after[i], false))
                << "Replica descriptor mismatch at index " << i;
        }

        LOG(INFO) << "[PutStart Consistency] Check passed: both allocated on "
                     "best_segment consistently";
    }

    // ==================== File Operation Methods ====================

    // Copy msgpack snapshot files to backup directory (or map backup id for etcd)
    void CopySnapshotToBackup(const std::string& snapshot_id,
                              const std::string& backup_id) const {
        if (UseEtcdSnapshotBackend()) {
            etcd_backup_id_map_[backup_id] = snapshot_id;
            LOG(INFO) << "ETCD snapshot backup mapping: " << backup_id
                      << " -> " << snapshot_id;
            return;
        }
        namespace fs = std::filesystem;

        fs::path src_dir = GetSnapshotDir(snapshot_id);
        fs::path dst_dir = GetBackupDir(backup_id);

        std::error_code ec;
        fs::create_directories(dst_dir, ec);
        if (ec) {
            LOG(ERROR) << "Failed to create backup directory: "
                       << dst_dir.string() << ", error: " << ec.message();
            return;
        }

        fs::copy(
            src_dir, dst_dir,
            fs::copy_options::recursive | fs::copy_options::overwrite_existing,
            ec);
        if (ec) {
            LOG(ERROR) << "Failed to copy snapshot files from "
                       << src_dir.string() << " to " << dst_dir.string()
                       << ", error: " << ec.message();
        } else {
            LOG(INFO) << "Successfully copied snapshot from "
                      << src_dir.string() << " to " << dst_dir.string();
        }
    }

    // Compare binary contents of two files
    bool CompareBinaryFiles(const std::string& file1,
                            const std::string& file2) const {
        std::ifstream f1(file1, std::ios::binary | std::ios::ate);
        std::ifstream f2(file2, std::ios::binary | std::ios::ate);

        if (!f1.is_open() || !f2.is_open()) {
            LOG(ERROR) << "Failed to open files for comparison: " << file1
                       << " or " << file2;
            return false;
        }

        // Compare file sizes first
        auto size1 = f1.tellg();
        auto size2 = f2.tellg();
        if (size1 != size2) {
            LOG(ERROR) << "File sizes differ: " << file1 << " (" << size1
                       << " bytes) vs " << file2 << " (" << size2 << " bytes)";
            return false;
        }

        // Seek back to beginning
        f1.seekg(0);
        f2.seekg(0);

        // Compare byte by byte
        std::istreambuf_iterator<char> begin1(f1);
        std::istreambuf_iterator<char> begin2(f2);
        std::istreambuf_iterator<char> end;

        bool is_equal = std::equal(begin1, end, begin2);
        if (!is_equal) {
            LOG(ERROR) << "File contents differ: " << file1 << " vs " << file2;
        }
        return is_equal;
    }

    // Compare all msgpack files in two snapshot directories or etcd key prefixes
    bool CompareSnapshotDirectories(const std::string& dir1,
                                    const std::string& dir2) const {
        LOG(INFO) << "Comparing snapshot directories: " << dir1 << " vs "
                  << dir2;

        // Actual snapshot has two files: metadata and segments
        std::vector<std::string> files_to_compare = {"metadata", "segments"};

        bool all_match = true;
        int file_count = 0;

        if (UseEtcdSnapshotBackend()) {
            if (!EnsureEtcdConnected()) {
                return false;
            }
            for (const auto& filename : files_to_compare) {
                std::string key1 = dir1 + filename;
                std::string key2 = dir2 + filename;

                LOG(INFO) << "Comparing etcd key: " << filename;

                std::string value1;
                std::string value2;
                if (!GetEtcdValue(key1, value1) ||
                    !GetEtcdValue(key2, value2)) {
                    LOG(ERROR) << "Failed to read etcd keys for file: "
                               << filename;
                    all_match = false;
                    break;
                }
                if (value1.size() != value2.size()) {
                    LOG(ERROR) << "Etcd value size differs for file: "
                               << filename << ", size1=" << value1.size()
                               << ", size2=" << value2.size();
                    all_match = false;
                    break;
                }
                if (value1 != value2) {
                    LOG(ERROR) << "Etcd value content differs for file: "
                               << filename;
                    all_match = false;
                    break;
                }
                file_count++;
            }
        } else {
            for (const auto& filename : files_to_compare) {
                std::string file1 = dir1 + filename;
                std::string file2 = dir2 + filename;

                LOG(INFO) << "Comparing file: " << filename;

                if (!CompareBinaryFiles(file1, file2)) {
                    LOG(ERROR) << "Mismatch found in file: " << filename;
                    all_match = false;
                    break;
                }
                file_count++;
            }
        }

        LOG(INFO) << "Compared " << file_count
                  << " msgpack files, all match: " << all_match;
        return all_match;
    }

    // ==================== Core Test Method ====================

    // Test snapshot and restore functionality
    void TestSnapshotAndRestore(std::unique_ptr<MasterService>& service) {
        // ========== Phase 1: Manually persist metadata ==========
        std::string snapshot_id1 = GenerateSnapshotId();
        auto persist_result1 = CallPersistState(service.get(), snapshot_id1);
        ASSERT_TRUE(persist_result1.has_value())
            << "Failed to persist state: " << persist_result1.error().message;

        // ========== Phase 2: Backup metadata to temporary path ==========
        std::string backup_id = snapshot_id1 + "_backup";
        CopySnapshotToBackup(snapshot_id1, backup_id);

        // ========== Phase 3: Restart MasterService in Restore mode ==========
        // Inherit key configurations from original service (e.g., root_fs_dir
        // for SSD support)
        ::setenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP", "1", 1);
        auto restore_config =
            MasterServiceConfig::builder()
                .set_memory_allocator(BufferAllocatorType::OFFSET)
                .set_enable_snapshot_restore(true)
                .set_enable_snapshot_restore_clean_metadata(false)
                .set_root_fs_dir(service->root_fs_dir_)
                .build();
        std::unique_ptr<MasterService> restored_service(
            new MasterService(restore_config));
        ::unsetenv("MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");

        // ========== Phase 4: Persist restored metadata again ==========
        std::string snapshot_id2 = GenerateSnapshotId();
        auto persist_result2 =
            CallPersistState(restored_service.get(), snapshot_id2);
        ASSERT_TRUE(persist_result2.has_value())
            << "Failed to persist restored state: "
            << persist_result2.error().message;

        // ========== Phase 5: Compare two metadata snapshots ==========
        bool snapshots_match = CompareSnapshotDirectories(
            GetBackupDir(backup_id), GetSnapshotDir(snapshot_id2));
        EXPECT_TRUE(snapshots_match);

        // ========== Phase 6: Capture state before snapshot ==========
        // Note: Capture state after snapshot to avoid internal state changes
        // affecting comparison
        ServiceStateSnapshot state_before = CaptureServiceState(service.get());

        // ========== Phase 7: Capture state after restore ==========
        ServiceStateSnapshot state_after =
            CaptureServiceState(restored_service.get());

        // ========== Phase 8: API-level validation ==========
        EXPECT_TRUE(CompareServiceState(state_before, state_after));

        // ========== Phase 9: PutStart consistency validation ==========
        std::string best_segment =
            FindBestSegment(service.get(), state_before.all_segments);
        if (!best_segment.empty()) {
            AssertPutStartConsistentWithBestSegment(
                service.get(), restored_service.get(), best_segment);
        } else {
            LOG(WARNING) << "[TestSnapshotAndRestore] No valid segment found, "
                            "skip PutStart consistency check";
        }
    }

    // ==================== Member Variables ====================

    std::unique_ptr<MasterService> service_;
    std::vector<Replica::Descriptor> replica_list;
    mutable std::unordered_map<std::string, std::string> etcd_backup_id_map_;

    // ==================== TearDown ====================

    void TearDown() override {
        // Ensure service_ is properly initialized - if nullptr, likely a local
        // variable shadowing issue
        ASSERT_NE(service_, nullptr)
            << "service_ is nullptr in TearDown. "
            << "Check if test declared a local 'service_' variable that "
               "shadows the base class member. "
            << "Use 'service_.reset(new MasterService(...))' instead of "
               "'std::unique_ptr<MasterService> service_(...)'";

        // Test snapshot and restore functionality for all test cases
        TestSnapshotAndRestore(service_);
        service_.reset();
        // Note: Do not call ShutdownGoogleLogging() here - glog only allows
        // single init/shutdown cycle
    }
};

}  // namespace mooncake::test
