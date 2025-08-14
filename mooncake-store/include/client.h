#pragma once

#include <boost/functional/hash.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "client_metric.h"
#include "ha_helper.h"
#include "master_client.h"
#include "storage_backend.h"
#include "thread_pool.h"
#include "transfer_engine.h"
#include "transfer_task.h"
#include "types.h"

namespace mooncake {

class PutOperation;

/**
 * @brief Client for interacting with the mooncake distributed object store
 */
class Client {
   public:
    ~Client();

    /**
     * @brief Creates and initializes a new Client instance
     * @param local_hostname Local host address (IP:Port)
     * @param metadata_connstring Connection string for metadata service
     * @param protocol Transfer protocol ("rdma" or "tcp")
     * @param protocol_args Protocol-specific arguments
     * @param master_server_entry The entry of master server (IP:Port of master
     *        address for non-HA mode, etcd://IP:Port;IP:Port;...;IP:Port for
     *        HA mode)
     * @return std::optional containing a shared_ptr to Client if successful,
     * std::nullopt otherwise
     */
    static std::optional<std::shared_ptr<Client>> Create(
        const std::string& local_hostname,
        const std::string& metadata_connstring, const std::string& protocol,
        void** protocol_args,
        const std::string& master_server_entry = kDefaultMasterAddress);

    /**
     * @brief Retrieves data for a given key
     * @param object_key Key to retrieve
     * @param slices Vector of slices to store the retrieved data
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Get(const std::string& object_key,
                                      std::vector<Slice>& slices);

    /**
     * @brief Batch retrieve data for multiple keys
     * @param object_keys Keys to query
     * @param slices Map of object keys to their data slices
     */
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> Query(
        const std::string& object_key);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    QueryByRegex(const std::string& str);

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     */

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchQuery(const std::vector<std::string>& object_keys);

    /**
     * @brief Transfers data using pre-queried object information
     * @param object_key Key of the object
     * @param replica_list Previously queried replica list
     * @param slices Vector of slices to store the data
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Get(
        const std::string& object_key,
        const std::vector<Replica::Descriptor>& replica_list,
        std::vector<Slice>& slices);
    /**
     * @brief Transfers data using pre-queried object information
     * @param object_keys Keys of the objects
     * @param object_infos Previously queried object metadata
     * @param slices Map of object keys to their data slices
     * @return ErrorCode indicating success/failure
     */
    std::vector<tl::expected<void, ErrorCode>> BatchGet(
        const std::vector<std::string>& object_keys,
        const std::vector<std::vector<Replica::Descriptor>>& replica_lists,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Stores data with replication
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Put(const ObjectKey& key,
                                      std::vector<Slice>& slices,
                                      const ReplicateConfig& config);

    /**
     * @brief Batch put data with replication
     * @param keys Object keys
     * @param batched_slices Vector of vectors of data slices to store (indexed
     * to match keys)
     * @param config Replication configuration
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPut(
        const std::vector<ObjectKey>& keys,
        std::vector<std::vector<Slice>>& batched_slices,
        const ReplicateConfig& config);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> Remove(const ObjectKey& key);

    tl::expected<long, ErrorCode> RemoveByRegex(const ObjectKey& str);

    /**
     * @brief Removes all objects and all its replicas
     * @return tl::expected<long, ErrorCode> number of removed objects or error
     */
    tl::expected<long, ErrorCode> RemoveAll();

    /**
     * @brief Registers a memory segment to master for allocation
     * @param buffer Memory buffer to register
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> MountSegment(const void* buffer, size_t size);

    /**
     * @brief Unregisters a memory segment from master
     * @param buffer Memory buffer to unregister
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> UnmountSegment(const void* buffer,
                                                 size_t size);

    /**
     * @brief Registers memory buffer with TransferEngine for data transfer
     * @param addr Memory address to register
     * @param length Size of the memory region
     * @param location Device location (e.g. "cpu:0")
     * @param remote_accessible Whether the memory can be accessed remotely
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> RegisterLocalMemory(
        void* addr, size_t length, const std::string& location,
        bool remote_accessible = true, bool update_metadata = true);

    /**
     * @brief Unregisters memory buffer from TransferEngine
     * @param addr Memory address to unregister
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    tl::expected<void, ErrorCode> unregisterLocalMemory(
        void* addr, bool update_metadata = true);

    /**
     * @brief Checks if an object exists
     * @param key Key to check
     * @return ErrorCode::OK if exists, ErrorCode::OBJECT_NOT_FOUND if not
     * exists, other ErrorCode for errors
     */
    tl::expected<bool, ErrorCode> IsExist(const std::string& key);

    /**
     * @brief Checks if multiple objects exist
     * @param keys Vector of keys to check
     * @param exist_results Output vector of existence results for each key
     * @return ErrorCode indicating success/failure of the batch operation
     */
    std::vector<tl::expected<bool, ErrorCode>> BatchIsExist(
        const std::vector<std::string>& keys);

    // For human-readable metrics
    tl::expected<std::string, ErrorCode> GetSummaryMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        return metrics_->summary_metrics();
    }

    // For Prometheus-style metrics
    tl::expected<std::string, ErrorCode> SerializeMetrics() {
        if (metrics_ == nullptr) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        std::string str;
        metrics_->serialize(str);
        return str;
    }

   private:
    /**
     * @brief Private constructor to enforce creation through Create() method
     */
    Client(const std::string& local_hostname,
           const std::string& metadata_connstring);

    /**
     * @brief Internal helper functions for initialization and data transfer
     */
    ErrorCode ConnectToMaster(const std::string& master_server_entry);
    ErrorCode InitTransferEngine(const std::string& local_hostname,
                                 const std::string& metadata_connstring,
                                 const std::string& protocol,
                                 void** protocol_args);
    ErrorCode TransferData(const Replica::Descriptor& replica,
                           std::vector<Slice>& slices,
                           TransferRequest::OpCode op_code);
    ErrorCode TransferWrite(const Replica::Descriptor& replica,
                            std::vector<Slice>& slices);
    ErrorCode TransferRead(const Replica::Descriptor& replica,
                           std::vector<Slice>& slices);

    /**
     * @brief Prepare and use the storage backend for persisting data
     */
    void PrepareStorageBackend(const std::string& storage_root_dir,
                               const std::string& fsdir);

    void PutToLocalFile(const std::string& object_key,
                        const std::vector<Slice>& slices,
                        const DiskDescriptor& disk_descriptor);

    /**
     * @brief Find the first complete replica from a replica list
     * @param replica_list List of replicas to search through
     * @param replica the first complete replica (file or memory)
     * @return ErrorCode::OK if found, ErrorCode::INVALID_REPLICA if no complete
     * replica
     */
    ErrorCode FindFirstCompleteReplica(
        const std::vector<Replica::Descriptor>& replica_list,
        Replica::Descriptor& replica);

    /**
     * @brief Batch put helper methods for structured approach
     */
    std::vector<PutOperation> CreatePutOperations(
        const std::vector<ObjectKey>& keys,
        const std::vector<std::vector<Slice>>& batched_slices);
    void StartBatchPut(std::vector<PutOperation>& ops,
                       const ReplicateConfig& config);
    void SubmitTransfers(std::vector<PutOperation>& ops);
    void WaitForTransfers(std::vector<PutOperation>& ops);
    void FinalizeBatchPut(std::vector<PutOperation>& ops);
    std::vector<tl::expected<void, ErrorCode>> CollectResults(
        const std::vector<PutOperation>& ops);

    // Client-side metrics
    std::unique_ptr<ClientMetric> metrics_;

    // Core components
    TransferEngine transfer_engine_;
    MasterClient master_client_;
    std::unique_ptr<TransferSubmitter> transfer_submitter_;

    // Mutex to protect mounted_segments_
    std::mutex mounted_segments_mutex_;
    std::unordered_map<UUID, Segment, boost::hash<UUID>> mounted_segments_;

    // Configuration
    const std::string local_hostname_;
    const std::string metadata_connstring_;

    // Client persistent thread pool for async operations
    ThreadPool write_thread_pool_;
    std::shared_ptr<StorageBackend> storage_backend_;

    // For high availability
    MasterViewHelper master_view_helper_;
    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};
    void PingThreadFunc();

    // Client identification
    UUID client_id_;
};

}  // namespace mooncake