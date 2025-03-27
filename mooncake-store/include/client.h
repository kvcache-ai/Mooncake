#pragma once

#include <memory>
#include <string>
#include <vector>

#include "master_client.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Client for interacting with the mooncake distributed object store
 */
class Client {
   public:
    Client();
    ~Client();

    /**
     * @brief Initializes the client with connection and transfer settings
     * @param local_hostname Local host address (IP:Port)
     * @param metadata_connstring Connection string for metadata service
     * @param protocol Transfer protocol ("rdma" or "tcp")
     * @param protocol_args Protocol-specific arguments
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Init(const std::string& local_hostname,
                   const std::string& metadata_connstring,
                   const std::string& protocol, void** protocol_args,
                   const std::string& master_addr = kDefaultMasterAddress);

    ErrorCode UnInit();

    /**
     * @brief Retrieves data for a given key
     * @param object_key Key to retrieve
     * @param slices Vector of slices to store the retrieved data
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Get(const std::string& object_key, std::vector<Slice>& slices);

    /**
     * @brief Two-step data retrieval process
     * 1. Query object information
     * 2. Transfer data based on the information
     */
    using ObjectInfo = mooncake_store::GetReplicaListResponse;

    /**
     * @brief Gets object metadata without transferring data
     * @param object_key Key to query
     * @param object_info Output parameter for object metadata
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Query(const std::string& object_key,
                    ObjectInfo& object_info) const;

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
     * @brief Stores data with replication
     * @param key Object key
     * @param slices Vector of data slices to store
     * @param config Replication configuration
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Put(const ObjectKey& key, std::vector<Slice>& slices,
                  const ReplicateConfig& config);

    /**
     * @brief Removes an object and all its replicas
     * @param key Key to remove
     * @return ErrorCode indicating success/failure
     */
    ErrorCode Remove(const ObjectKey& key) const;

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
    ErrorCode IsExist(const std::string& key) const;

   private:
    /**
     * @brief Internal helper functions for initialization and data transfer
     */
    ErrorCode ConnectToMaster(const std::string& master_addr);
    ErrorCode InitTransferEngine(const std::string& local_hostname,
                                 const std::string& metadata_connstring,
                                 const std::string& protocol,
                                 void** protocol_args);
    ErrorCode TransferData(
        const std::vector<mooncake_store::BufHandle>& handles,
        std::vector<Slice>& slices, TransferRequest::OpCode op_code) const;
    ErrorCode TransferWrite(
        const std::vector<mooncake_store::BufHandle>& handles,
        std::vector<Slice>& slices) const;
    ErrorCode TransferRead(
        const std::vector<mooncake_store::BufHandle>& handles,
        std::vector<Slice>& slices) const;

    // Core components
    std::unique_ptr<TransferEngine> transfer_engine_;
    std::unique_ptr<MasterClient> master_client_;

    std::unordered_map<std::string, void*> mounted_segments_;

    // Configuration
    std::string local_hostname_;
    std::string metadata_connstring_;
};

}  // namespace mooncake
