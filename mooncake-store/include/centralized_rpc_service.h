#pragma once

#include "rpc_service.h"
#include "centralized_master_service.h"

namespace mooncake {

class WrappedCentralizedMasterService final : public WrappedMasterService {
   public:
    WrappedCentralizedMasterService(const WrappedMasterServiceConfig& config);

    // Initialize centralized-specific HTTP handlers
    void init_centralized_http_server();

    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> PutStart(
        const UUID& client_id, const std::string& key,
        const uint64_t slice_length, const ReplicateConfig& config);

    tl::expected<void, ErrorCode> PutEnd(const UUID& client_id,
                                         const std::string& key,
                                         ReplicaType replica_type);

    tl::expected<void, ErrorCode> PutRevoke(const UUID& client_id,
                                            const std::string& key,
                                            ReplicaType replica_type);

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const UUID& client_id, const std::vector<std::string>& keys,
                  const std::vector<uint64_t>& slice_lengths,
                  const ReplicateConfig& config);

    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<std::string>& keys);

    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys);

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);

    tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id);

    tl::expected<std::string, ErrorCode> GetFsdir();

    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig();

    tl::expected<void, ErrorCode> MountLocalDiskSegment(const UUID& client_id,
                                                        bool enable_offloading);

    tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        const std::string& key);

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys);

   protected:
    virtual MasterService& GetMasterService() override {
        return master_service_;
    }

   private:
    CentralizedMasterService master_service_;
};

void RegisterCentralizedRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedCentralizedMasterService& wrapped_master_service);

}  // namespace mooncake
