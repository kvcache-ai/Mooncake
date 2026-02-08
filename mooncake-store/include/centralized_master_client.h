#pragma once

#include "master_client.h"

namespace mooncake {

/**
 * @brief Client for interacting with the mooncake master service
 */
class CentralizedMasterClient final : public MasterClient {
   public:
    CentralizedMasterClient(const UUID& client_id,
                            MasterClientMetric* metrics = nullptr)
        : MasterClient(client_id, metrics) {}

    CentralizedMasterClient(const CentralizedMasterClient&) = delete;
    CentralizedMasterClient& operator=(const CentralizedMasterClient&) = delete;

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_key Key to query
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] tl::expected<GetReplicaListResponse, ErrorCode>
    GetReplicaList(const std::string& object_key);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& object_keys);

    /**
     * @brief Starts a put operation
     * @param key Object key
     * @param batch_slice_lengths Vector of slice lengths
     * @param value_length Total value length
     * @param config Replication configuration
     * @return tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
     * indicating success/failure
     */
    [[nodiscard]] tl::expected<std::vector<Replica::Descriptor>, ErrorCode>
    PutStart(const std::string& key,
             const std::vector<size_t>& batch_slice_lengths,
             const ReplicateConfig& config);

    /**
     * @brief Starts a batch of put operations for N objects
     * @param keys Vector of object key
     * @param value_lengths Vector of total value lengths
     * @param slice_lengths Vector of vectors of slice lengths
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    /**
     * @brief Ends a put operation
     * @param key Object key
     * @param replica_type Type of replica (memory or disk)
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutEnd(
        const std::string& key, ReplicaType replica_type);

    /**
     * @brief Ends a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<std::string>& keys);

    /**
     * @brief Revokes a put operation
     * @param key Object key
     * @param replica_type Type of replica (memory or disk)
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> PutRevoke(
        const std::string& key, ReplicaType replica_type);

    /**
     * @brief Revokes a put operation for a batch of objects
     * @param keys Vector of object keys
     * @return ErrorCode indicating success/failure
     */
    [[nodiscard]] std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys);

    /**
     * @brief Registers a segment to master for allocation
     * @param segment Segment to register
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountSegment(
        const Segment& segment);

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @param segments Segments to remount
     * @return tl::expected<void, ErrorCode> indicating success/failure
     */
    [[nodiscard]] tl::expected<void, ErrorCode> ReMountSegment(
        const std::vector<Segment>& segments);

    /**
     * @brief Gets the cluster ID for the current client to use as subdirectory
     * name
     * @return GetClusterIdResponse containing the cluster ID
     */
    [[nodiscard]] tl::expected<std::string, ErrorCode> GetFsdir();

    [[nodiscard]] tl::expected<GetStorageConfigResponse, ErrorCode>
    GetStorageConfig();

    /**
     * @brief Mounts a local disk segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    [[nodiscard]] tl::expected<void, ErrorCode> MountLocalDiskSegment(
        const UUID& client_id, bool enable_offloading);

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-persisted objects.
     * @param enable_offloading Indicates whether persistence is enabled for
     * this segment.
     */
    [[nodiscard]] tl::expected<std::unordered_map<std::string, int64_t>,
                               ErrorCode>
    OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading);

    /**
     * @brief Adds multiple new objects to a specified client in batch.
     * @param keys         A list of object keys (names) that were successfully
     * offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    [[nodiscard]] tl::expected<void, ErrorCode> NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas);
};

}  // namespace mooncake
