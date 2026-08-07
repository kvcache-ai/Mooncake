#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ylt/reflection/user_reflect_macro.hpp>

#include "ha/oplog/p2p_standby_metadata_store.h"
#include "types.h"

namespace mooncake {

class P2PHotStandbyService;

inline constexpr uint32_t kMaxStandbySnapshotChunkSize = 4096;
inline constexpr size_t kMaxStandbySnapshotSessions = 16;

struct BeginStandbySnapshotRequest {
    std::string cluster_id;
    YLT_REFL(BeginStandbySnapshotRequest, cluster_id);
};

struct BeginStandbySnapshotResponse {
    int32_t error_code{toInt(ErrorCode::OK)};
    std::string session_id;
    uint64_t baseline_sequence_id{0};
    uint64_t object_count{0};
    uint64_t client_count{0};
    YLT_REFL(BeginStandbySnapshotResponse, error_code, session_id,
             baseline_sequence_id, object_count, client_count);
};

struct StandbySnapshotChunkRequest {
    std::string session_id;
    uint64_t object_offset{0};
    uint64_t client_offset{0};
    uint32_t limit{256};
    YLT_REFL(StandbySnapshotChunkRequest, session_id, object_offset,
             client_offset, limit);
};

struct StandbySnapshotObjectRecord {
    std::string key;
    StandbyObjectMetadata metadata;
    YLT_REFL(StandbySnapshotObjectRecord, key, metadata);
};

struct StandbySnapshotClientRecord {
    UUID client_id{0, 0};
    P2PStandbyClientInfo info;
    YLT_REFL(StandbySnapshotClientRecord, client_id, info);
};

struct StandbySnapshotChunkResponse {
    int32_t error_code{toInt(ErrorCode::OK)};
    uint64_t next_object_offset{0};
    uint64_t next_client_offset{0};
    bool done{false};
    std::vector<StandbySnapshotObjectRecord> objects;
    std::vector<StandbySnapshotClientRecord> clients;
    YLT_REFL(StandbySnapshotChunkResponse, error_code, next_object_offset,
             next_client_offset, done, objects, clients);
};

struct EndStandbySnapshotRequest {
    std::string session_id;
    YLT_REFL(EndStandbySnapshotRequest, session_id);
};

class P2PStandbySnapshotService {
   public:
    explicit P2PStandbySnapshotService(P2PHotStandbyService* standby);
    ~P2PStandbySnapshotService();

    P2PStandbySnapshotService(const P2PStandbySnapshotService&) = delete;
    P2PStandbySnapshotService& operator=(const P2PStandbySnapshotService&) =
        delete;

    BeginStandbySnapshotResponse BeginSnapshot(
        const BeginStandbySnapshotRequest& request);
    StandbySnapshotChunkResponse GetSnapshotChunk(
        const StandbySnapshotChunkRequest& request);
    int32_t EndSnapshot(const EndStandbySnapshotRequest& request);

   private:
    struct Session {
        uint64_t baseline_sequence_id{0};
        std::vector<std::string> object_keys;
        std::vector<UUID> client_ids;
        std::chrono::steady_clock::time_point last_access;
    };

    void CleanupExpiredSessionsLocked();
    void CleanupLoop();

    P2PHotStandbyService* standby_;
    std::mutex mutex_;
    std::condition_variable cleanup_cv_;
    std::unordered_map<std::string, Session> sessions_;
    uint64_t next_session_id_{1};
    bool stopping_{false};
    std::thread cleanup_thread_;
};

class P2PStandbySnapshotClient {
   public:
    ErrorCode Bootstrap(const std::string& endpoint,
                        const std::string& cluster_id,
                        P2PStandbyMetadataStore* target,
                        uint64_t& baseline_sequence_id,
                        uint32_t chunk_size = 256);
};

#ifdef STORE_USE_REDIS
struct RedisMasterRegistryEntry {
    std::string instance_id;
    std::string master_endpoint;
    std::string snapshot_endpoint;
    std::string role;
    bool snapshot_ready{false};
    uint64_t applied_sequence_id{0};
};

class RedisMasterRegistry {
   public:
    RedisMasterRegistry(std::string cluster_id, std::string redis_endpoint,
                        std::string username, std::string password,
                        int db_index);

    ErrorCode Refresh(const RedisMasterRegistryEntry& entry);
    ErrorCode Remove(const std::string& instance_id);
    ErrorCode DiscoverAlive(std::chrono::seconds ttl,
                            std::vector<RedisMasterRegistryEntry>& entries);

   private:
    std::string heartbeat_key_;
    std::string metadata_key_;
    std::string redis_endpoint_;
    std::string username_;
    std::string password_;
    int db_index_{0};
};

class RedisMasterRegistryHeartbeat {
   public:
    RedisMasterRegistryHeartbeat(
        std::unique_ptr<RedisMasterRegistry> registry,
        RedisMasterRegistryEntry entry,
        std::chrono::seconds interval = std::chrono::seconds(2));
    ~RedisMasterRegistryHeartbeat();

    ErrorCode Start();
    void UpdateRole(std::string role, bool snapshot_ready);
    void SetAppliedSequenceProvider(std::function<uint64_t()> provider);
    void SetSnapshotReadyProvider(std::function<bool()> provider);
    void Stop();

   private:
    void Run();

    std::unique_ptr<RedisMasterRegistry> registry_;
    RedisMasterRegistryEntry entry_;
    std::chrono::seconds interval_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    std::function<uint64_t()> applied_sequence_provider_;
    std::function<bool()> snapshot_ready_provider_;
    bool refresh_requested_{false};
    bool stopping_{false};
};
#endif

}  // namespace mooncake
