#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "master_client.h"
#include "rpc_service.h"
#include "transfer_engine.h"
#include "types.h"
#include "ha_helper.h"

namespace mooncake {

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
    ErrorCode Get(const std::string& object_key, std::vector<Slice>& slices);

    /**
     * @brief Gets object metadata without transferring data
     * @param object_keys Keys to query
     * @param slices Output parameter for the retrieved data
     */
    ErrorCode BatchGet(
        const std::vector<std::string>& object_keys,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Two-step data retrieval process
     * 1. Query object information
     * 2. Transfer data based on the information
     */
    using ObjectInfo = GetReplicaListResponse;

    /**
     * @brief Two-step data retrieval process
     * 1. BatchQuery object information
     * 2. Transfer data based on the information
     */
    using BatchObjectInfo = BatchGetReplicaListResponse;

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Query(const std::string& object_key, ObjectInfo& object_info);

    /**
     * @brief Batch query object metadata without transferring data
     * @param object_keys Keys to query
     * @param object_infos Output parameter for object metadata
     */
    ErrorCode BatchQuery(const std::vector<std::string>& object_keys,
                         BatchObjectInfo& object_infos);

    /**
     * @brief Transfers data using pre-queried object information
     * @param object_key Key of the object
     * @param object_info Previously queried object metadata
     * @param slices Vector of slices to store the data
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Get(const std::string& object_key, const ObjectInfo& object_info,
                  std::vector<Slice>& slices);

    /**
     * @brief Transfers data using pre-queried object information
     * @param object_keys Keys of the objects
     * @param object_infos Previously queried object metadata
     * @param slices Vector of slices to store the data
     * @return ErrorCode indicating success/failure
     */
    ErrorCode BatchGet(
        const std::vector<std::string>& object_keys,
        BatchObjectInfo& object_infos,
        std::unordered_map<std::string, std::vector<Slice>>& slices);

    /**
     * @brief Stores data with replication
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Put(const ObjectKey& key, std::vector<Slice>& slices,
                  const ReplicateConfig& config);

    /**
     * @brief Batch put data with replication
     * @param keys Object keys
     * @param batched_slices Vector of data slices to store
     * @param config Replication configuration
     */
    ErrorCode BatchPut(
        const std::vector<ObjectKey>& keys,
        std::unordered_map<std::string, std::vector<Slice>>& batched_slices,
        ReplicateConfig& config);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Remove(const ObjectKey& key);

    /**
     * @brief Removes all objects and all its replicas
     * @return The number of objects removed, negative on error
     */
    long RemoveAll();

    /**
     * @brief Registers a memory segment to master for allocation
     * @param segment_name Unique identifier for the segment
     * @param buffer Memory buffer to register
     * @param size Size of the buffer in bytes
     * @return ErrorCode indicating success/failure
     */
    ErrorCode MountSegment(const std::string& segment_name, const void* buffer,
                           size_t size);

    /**
     * @brief Unregisters a memory segment from master
     * @param segment_name Name of the segment to unregister
     * @param addr Memory address to unregister
     * @return ErrorCode indicating success/failure
     */
    ErrorCode UnmountSegment(const std::string& segment_name, void* addr);

    /**
     * @brief Registers memory buffer with TransferEngine for data transfer
     * @param addr Memory address to register
     * @param length Size of the memory region
     * @param location Device location (e.g. "cpu:0")
     * @param remote_accessible Whether the memory can be accessed remotely
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    ErrorCode RegisterLocalMemory(void* addr, size_t length,
                                  const std::string& location,
                                  bool remote_accessible = true,
                                  bool update_metadata = true);

    /**
     * @brief Unregisters memory buffer from TransferEngine
     * @param addr Memory address to unregister
     * @param update_metadata Whether to update metadata service
     * @return ErrorCode indicating success/failure
     */
    ErrorCode unregisterLocalMemory(void* addr, bool update_metadata = true);

    /**
     * @brief Checks if an object exists
     * @param key Key to check
     * @return ErrorCode::OK if exists, ErrorCode::OBJECT_NOT_FOUND if not
     * exists, other ErrorCode for errors
     */
    ErrorCode IsExist(const std::string& key);

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
    ErrorCode TransferData(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        std::vector<Slice>& slices, TransferRequest::OpCode op_code);
    ErrorCode TransferWrite(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        std::vector<Slice>& slices);
    ErrorCode TransferRead(
        const std::vector<AllocatedBuffer::Descriptor>& handles,
        std::vector<Slice>& slices);

    // Core components
    TransferEngine transfer_engine_;
    MasterClient master_client_;

    // Client local segments
    struct Segment{
        void* buffer;
        size_t size;
    };
    // Mutex to protect mounted_segments_
    std::mutex mounted_segments_mutex_;
    std::unordered_map<std::string, Segment> mounted_segments_;

    // Configuration
    const std::string local_hostname_;
    const std::string metadata_connstring_;

    // For high availability
    MasterViewHelper master_view_helper_;
    std::thread ping_thread_;
    std::atomic<bool> ping_running_{false};
    void PingThreadFunc(int current_version);
};

}  // namespace mooncake