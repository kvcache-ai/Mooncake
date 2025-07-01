#include "client.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <unordered_set>

#include "rpc_service.h"
#include "transfer_engine.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "types.h"

namespace mooncake {

[[nodiscard]] size_t CalculateSliceSize(const std::vector<Slice>& slices) {
    size_t slice_size = 0;
    for (const auto& slice : slices) {
        slice_size += slice.size;
    }
    return slice_size;
}

Client::Client(const std::string& local_hostname,
               const std::string& metadata_connstring)
    : local_hostname_(local_hostname),
      metadata_connstring_(metadata_connstring) {
    client_id_ = generate_uuid();
    LOG(INFO) << "client_id=" << client_id_;
}

Client::~Client() {
    // Make a copy of mounted_segments_ to avoid modifying while iterating
    std::vector<Segment> segments_to_unmount;
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        segments_to_unmount.reserve(mounted_segments_.size());
        for (auto& entry : mounted_segments_) {
            segments_to_unmount.push_back(entry.second);
        }
    }

    for (auto& segment : segments_to_unmount) {
        auto err_code =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (err_code != ErrorCode::OK) {
            LOG(ERROR) << "Failed to unmount segment: " << toString(err_code);
        }
    }

    // Clear any remaining segments
    {
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        mounted_segments_.clear();
    }

    // Stop ping thread only after no need to contact master anymore
    if (ping_running_) {
        ping_running_ = false;
        if (ping_thread_.joinable()) {
            ping_thread_.join();
        }
    }
}

static bool get_auto_discover() {
    const char* ev_ad = std::getenv("MC_MS_AUTO_DISC");
    if (ev_ad) {
        int iv = std::stoi(ev_ad);
        if (iv == 1) {
            LOG(INFO) << "auto discovery set by env MC_MS_AUTO_DISC";
            return true;
        }
    }
    return false;
}

static inline void ltrim(std::string& s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) {
                return !std::isspace(ch);
            }));
}

static inline void rtrim(std::string& s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         [](unsigned char ch) { return !std::isspace(ch); })
                .base(),
            s.end());
}

static std::vector<std::string> get_auto_discover_filters(bool auto_discover) {
    std::vector<std::string> whitelst_filters;
    char* ev_ad = std::getenv("MC_MS_FILTERS");
    if (ev_ad) {
        if (!auto_discover) {
            LOG(WARNING)
                << "auto discovery not set, but find whitelist filters: "
                << ev_ad;
            return whitelst_filters;
        }
        LOG(INFO) << "whitelist filters: " << ev_ad;
        char delimiter = ',';
        char* end = ev_ad + std::strlen(ev_ad);
        char *start = ev_ad, *pos = ev_ad;
        while ((pos = std::find(start, end, delimiter)) != end) {
            std::string str(start, pos);
            ltrim(str);
            rtrim(str);
            whitelst_filters.push_back(std::move(str));
            start = pos + 1;
        }
        if (start != (end + 1)) {
            std::string str(start, end);
            ltrim(str);
            rtrim(str);
            whitelst_filters.push_back(std::move(str));
        }
    }
    return whitelst_filters;
}

ErrorCode Client::ConnectToMaster(const std::string& master_server_entry) {
    if (master_server_entry.find("etcd://") == 0) {
        std::string etcd_entry = master_server_entry.substr(strlen("etcd://"));

        // Get master address from etcd
        auto err = master_view_helper_.ConnectToEtcd(etcd_entry);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to etcd";
            return err;
        }
        std::string master_address;
        ViewVersionId master_version = 0;
        err = master_view_helper_.GetMasterView(master_address, master_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get master address";
            return err;
        }

        err = master_client_.Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master";
            return err;
        }

        // Start Ping thread to monitor master view changes and remount segments
        // if needed
        ping_running_ = true;
        ping_thread_ = std::thread(&Client::PingThreadFunc, this);

        return ErrorCode::OK;
    } else {
        return master_client_.Connect(master_server_entry);
    }
}

ErrorCode Client::InitTransferEngine(const std::string& local_hostname,
                                     const std::string& metadata_connstring,
                                     const std::string& protocol,
                                     void** protocol_args) {
    // get auto_discover and filters from env
    bool auto_discover = get_auto_discover();
    transfer_engine_.setAutoDiscover(auto_discover);
    transfer_engine_.setWhitelistFilters(
        get_auto_discover_filters(auto_discover));

    auto [hostname, port] = parseHostNameWithPort(local_hostname);
    int rc = transfer_engine_.init(metadata_connstring, local_hostname,
                                   hostname, port);
    CHECK_EQ(rc, 0) << "Failed to initialize transfer engine";

    Transport* transport = nullptr;
    if (protocol == "rdma") {
        LOG(INFO) << "transport_type=rdma";
        transport = transfer_engine_.installTransport("rdma", protocol_args);
    } else if (protocol == "tcp") {
        LOG(INFO) << "transport_type=tcp";
        try {
            transport = transfer_engine_.installTransport("tcp", protocol_args);
        } catch (std::exception& e) {
            LOG(ERROR) << "tcp_transport_install_failed error_message=\""
                       << e.what() << "\"";
            return ErrorCode::INTERNAL_ERROR;
        }
    } else {
        LOG(ERROR) << "unsupported_protocol protocol=" << protocol;
        return ErrorCode::INVALID_PARAMS;
    }
    CHECK(transport) << "Failed to install transport";

    // Initialize TransferSubmitter after transfer engine is ready
    transfer_submitter_ =
        std::make_unique<TransferSubmitter>(transfer_engine_, local_hostname);

    return ErrorCode::OK;
}

std::optional<std::shared_ptr<Client>> Client::Create(
    const std::string& local_hostname, const std::string& metadata_connstring,
    const std::string& protocol, void** protocol_args,
    const std::string& master_server_entry) {
    auto client = std::shared_ptr<Client>(
        new Client(local_hostname, metadata_connstring));

    ErrorCode err = client->ConnectToMaster(master_server_entry);
    if (err != ErrorCode::OK) {
        return std::nullopt;
    }

    // Initialize transfer engine
    err = client->InitTransferEngine(local_hostname, metadata_connstring,
                                     protocol, protocol_args);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to initialize transfer engine";
        return std::nullopt;
    }

    return client;
}

ErrorCode Client::Get(const std::string& object_key,
                      std::vector<Slice>& slices) {
    ObjectInfo object_info;
    auto err = Query(object_key, object_info);
    if (err != ErrorCode::OK) return err;
    return Get(object_key, object_info, slices);
}

ErrorCode Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    std::unordered_set<std::string> seen;
    for (const auto& key : object_keys) {
        if (!seen.insert(key).second) {
            LOG(ERROR) << "Duplicate key not supported for Batch API, key: "
                       << key;
            return ErrorCode::INVALID_PARAMS;
        };
    }
    BatchObjectInfo batched_object_info;
    auto err = BatchQuery(object_keys, batched_object_info);
    if (err == ErrorCode::OK) {
        return BatchGet(object_keys, batched_object_info, slices);
    }
    return ErrorCode::OBJECT_NOT_FOUND;
}

ErrorCode Client::Query(const std::string& object_key,
                        ObjectInfo& object_info) {
    auto response = master_client_.GetReplicaList(object_key);
    // copy vec
    object_info.replica_list.resize(response.replica_list.size());
    for (size_t i = 0; i < response.replica_list.size(); ++i) {
        object_info.replica_list[i] = response.replica_list[i];
    }
    return response.error_code;
}

ErrorCode Client::BatchQuery(const std::vector<std::string>& object_keys,
                             BatchObjectInfo& batched_object_info) {
    auto response = master_client_.BatchGetReplicaList(object_keys);
    // copy vec
    if (response.batch_replica_list.size() != object_keys.size()) {
        LOG(ERROR) << "QueryBatch failed, response size is not equal to "
                      "request size";
        LOG(ERROR) << "clear batched_object_info.batch_replica_list";
        batched_object_info.batch_replica_list.clear();
        return ErrorCode::INVALID_PARAMS;
    }
    for (const auto& key : object_keys) {
        auto it = response.batch_replica_list.find(key);
        if (it == response.batch_replica_list.end()) {
            LOG(ERROR) << "QueryBatch failed, key: " << key
                       << " not found in response";
            batched_object_info.batch_replica_list.clear();
            return ErrorCode::OBJECT_NOT_FOUND;
        }
        batched_object_info.batch_replica_list.emplace(key, it->second);
    }
    return response.error_code;
}

ErrorCode Client::Get(const std::string& object_key,
                      const ObjectInfo& object_info,
                      std::vector<Slice>& slices) {
    // Find the first complete replica
    std::vector<AllocatedBuffer::Descriptor> handles;
    ErrorCode err = FindFirstCompleteReplica(object_info.replica_list, handles);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return err;
    }

    if (TransferRead(handles, slices) != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode Client::BatchGet(
    const std::vector<std::string>& object_keys,
    BatchObjectInfo& batched_object_info,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    // Collect all transfer operations for parallel execution
    std::vector<std::pair<std::string, TransferFuture>> pending_transfers;
    pending_transfers.reserve(object_keys.size());

    // Submit all transfers in parallel
    for (const auto& key : object_keys) {
        auto object_info_it = batched_object_info.batch_replica_list.find(key);
        auto slices_it = slices.find(key);
        if (object_info_it == batched_object_info.batch_replica_list.end() ||
            slices_it == slices.end()) {
            LOG(ERROR) << "Key not found: " << key;
            slices.clear();
            return ErrorCode::INVALID_PARAMS;
        }

        // Find the first complete replica for this key
        const auto& replica_list = object_info_it->second;
        std::vector<AllocatedBuffer::Descriptor> handles;
        ErrorCode err = FindFirstCompleteReplica(replica_list, handles);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            slices.clear();
            return err;
        }

        // Submit transfer operation asynchronously
        auto future = transfer_submitter_->submit(handles, slices_it->second,
                                                  TransferRequest::READ);
        if (!future) {
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            slices.clear();
            return ErrorCode::TRANSFER_FAIL;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(key, std::move(*future));
    }

    // Wait for all transfers to complete
    for (auto& [key, future] : pending_transfers) {
        ErrorCode result = future.get();
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            slices.clear();
            return result;
        }
        VLOG(1) << "Transfer completed successfully for key: " << key;
    }

    VLOG(1) << "BatchGet completed successfully for " << object_keys.size()
            << " keys";
    return ErrorCode::OK;
}

ErrorCode Client::Put(const ObjectKey& key, std::vector<Slice>& slices,
                      const ReplicateConfig& config) {
    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    size_t slice_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.push_back(slices[i].size);
        slice_size += slices[i].size;
    }

    // Start put operation
    PutStartResponse start_response =
        master_client_.PutStart(key, slice_lengths, slice_size, config);
    ErrorCode err = start_response.error_code;
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return ErrorCode::OK;
        }
        LOG(ERROR) << "Failed to start put operation: " << err;
        return err;
    }

    // Transfer data using allocated handles from all replicas
    for (const auto& replica : start_response.replica_list) {
        std::vector<AllocatedBuffer::Descriptor> handles;
        for (const auto& handle : replica.buffer_descriptors) {
            CHECK(handle.buffer_address_ != 0) << "buffer_address_ is nullptr";
            handles.push_back(handle);
        }

        ErrorCode transfer_err = TransferWrite(handles, slices);
        if (transfer_err != ErrorCode::OK) {
            // Revoke put operation
            auto revoke_err = master_client_.PutRevoke(key);
            if (revoke_err.error_code != ErrorCode::OK) {
                LOG(ERROR) << "Failed to revoke put operation";
                return revoke_err.error_code;
            }
            return transfer_err;
        }
    }

    // End put operation
    err = master_client_.PutEnd(key).error_code;
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to end put operation: " << err;
        return err;
    }
    return ErrorCode::OK;
}

ErrorCode Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::unordered_map<std::string, std::vector<Slice>>& batched_slices,
    ReplicateConfig& config) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    std::unordered_map<std::string, std::vector<size_t>> batched_slice_lengths;
    std::unordered_map<std::string, size_t> batched_value_lengths;
    for (const auto& key : keys) {
        auto slices = batched_slices.find(key);
        if (slices == batched_slices.end()) {
            LOG(ERROR) << "Cannot find slices for key: " << key;
            return ErrorCode::INVALID_PARAMS;
        }
        size_t slice_size = 0;
        std::vector<size_t> slice_lengths;
        for (const auto& slice : slices->second) {
            slice_lengths.push_back(slice.size);
            slice_size += slice.size;
        }
        batched_slice_lengths.emplace(key, std::move(slice_lengths));
        batched_value_lengths.emplace(key, slice_size);
    }
    BatchPutStartResponse start_response = master_client_.BatchPutStart(
        keys, batched_value_lengths, batched_slice_lengths, config);
    ErrorCode err = start_response.error_code;
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            LOG(INFO) << "object_already_exists key count " << keys.size();
            return ErrorCode::OK;
        }
        LOG(ERROR) << "Failed to start batch put operation: " << err;
        return err;
    }

    // Collect all transfer operations for parallel execution
    std::vector<std::tuple<std::string, size_t, TransferFuture>>
        pending_transfers;

    // Submit all transfers in parallel
    for (const auto& key : keys) {
        const auto& slices_it = batched_slices.find(key);
        if (slices_it == batched_slices.end()) {
            LOG(ERROR) << "Cannot find slices for key: " << key;
            return ErrorCode::INVALID_PARAMS;
        }
        const auto& replica_list = start_response.batch_replica_list.find(key);
        if (replica_list == start_response.batch_replica_list.end()) {
            LOG(ERROR) << "Cannot find replica_list for key: " << key;
            return ErrorCode::INVALID_PARAMS;
        }

        for (size_t replica_idx = 0; replica_idx < replica_list->second.size();
             ++replica_idx) {
            const auto& replica = replica_list->second[replica_idx];
            std::vector<AllocatedBuffer::Descriptor> handles;
            for (const auto& handle : replica.buffer_descriptors) {
                CHECK(handle.buffer_address_ != 0)
                    << "buffer_address_ is nullptr";
                handles.push_back(handle);
            }

            // Submit transfer operation asynchronously
            auto future = transfer_submitter_->submit(
                handles, slices_it->second, TransferRequest::WRITE);
            if (!future) {
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << key << " replica: " << replica_idx;
                // Revoke put operation
                auto revoke_err = master_client_.BatchPutRevoke(keys);
                if (revoke_err.error_code != ErrorCode::OK) {
                    LOG(ERROR) << "Failed to revoke put operation";
                    return revoke_err.error_code;
                }
                return ErrorCode::TRANSFER_FAIL;
            }

            VLOG(1) << "Submitted transfer for key " << key << " replica "
                    << replica_idx << " using strategy: "
                    << static_cast<int>(future->strategy());

            pending_transfers.emplace_back(key, replica_idx,
                                           std::move(*future));
        }
    }

    // Wait for all transfers to complete
    for (auto& [key, replica_idx, future] : pending_transfers) {
        ErrorCode result = future.get();
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " replica: " << replica_idx
                       << " with error: " << result;
            // Revoke put operation
            auto revoke_err = master_client_.BatchPutRevoke(keys);
            if (revoke_err.error_code != ErrorCode::OK) {
                LOG(ERROR) << "Failed to revoke put operation";
                return revoke_err.error_code;
            }
            return result;
        }
        VLOG(1) << "Transfer completed successfully for key: " << key
                << " replica: " << replica_idx;
    }

    // End put operation
    err = master_client_.BatchPutEnd(keys).error_code;
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to end put operation: " << err;
        return err;
    }

    VLOG(1) << "BatchPut completed successfully for " << keys.size()
            << " keys with " << pending_transfers.size() << " total transfers";
    return ErrorCode::OK;
}

ErrorCode Client::Remove(const ObjectKey& key) {
    return master_client_.Remove(key).error_code;
}

long Client::RemoveAll() { return master_client_.RemoveAll().removed_count; }

ErrorCode Client::MountSegment(const void* buffer, size_t size) {
    if (buffer == nullptr || size == 0 ||
        reinterpret_cast<uintptr_t>(buffer) % facebook::cachelib::Slab::kSize ||
        size % facebook::cachelib::Slab::kSize) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize;
        return ErrorCode::INVALID_PARAMS;
    }

    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);

    // Check if the segment overlaps with any existing segment
    for (auto& it : mounted_segments_) {
        auto& mtseg = it.second;
        uintptr_t l1 = reinterpret_cast<uintptr_t>(mtseg.base);
        uintptr_t r1 = reinterpret_cast<uintptr_t>(mtseg.size) + l1;
        uintptr_t l2 = reinterpret_cast<uintptr_t>(buffer);
        uintptr_t r2 = reinterpret_cast<uintptr_t>(size) + l2;
        if (std::max(l1, l2) < std::min(r1, r2)) {
            LOG(ERROR) << "segment_overlaps base1=" << mtseg.base
                       << " size1=" << mtseg.size << " base2=" << buffer
                       << " size2=" << size;
            return ErrorCode::INVALID_PARAMS;
        }
    }

    int rc = transfer_engine_.registerLocalMemory(
        (void*)buffer, size, kWildcardLocation, true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed base=" << buffer
                   << " size=" << size << ", error=" << rc;
        return ErrorCode::INVALID_PARAMS;
    }

    Segment segment(generate_uuid(), local_hostname_,
                    reinterpret_cast<uintptr_t>(buffer), size);

    ErrorCode err = master_client_.MountSegment(segment, client_id_).error_code;
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                   << " size=" << size << ", error=" << err;
        return err;
    }

    mounted_segments_[segment.id] = segment;
    return ErrorCode::OK;
}

ErrorCode Client::UnmountSegment(const void* buffer, size_t size) {
    std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
    auto segment = mounted_segments_.end();

    for (auto it = mounted_segments_.begin(); it != mounted_segments_.end();
         ++it) {
        if (it->second.base == reinterpret_cast<uintptr_t>(buffer) &&
            it->second.size == size) {
            segment = it;
            break;
        }
    }
    if (segment == mounted_segments_.end()) {
        LOG(ERROR) << "segment_not_found base=" << buffer << " size=" << size;
        return ErrorCode::INVALID_PARAMS;
    }

    ErrorCode err =
        master_client_.UnmountSegment(segment->second.id, client_id_)
            .error_code;
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return err;
    }

    int rc = transfer_engine_.unregisterLocalMemory(
        reinterpret_cast<void*>(segment->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return ErrorCode::INTERNAL_ERROR;
        }
        // Otherwise, the segment is already unregistered from transfer engine,
        // we can continue
    }

    mounted_segments_.erase(segment);
    return ErrorCode::OK;
}

ErrorCode Client::RegisterLocalMemory(void* addr, size_t length,
                                      const std::string& location,
                                      bool remote_accessible,
                                      bool update_metadata) {
    if (this->transfer_engine_.registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode Client::unregisterLocalMemory(void* addr, bool update_metadata) {
    if (this->transfer_engine_.unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return ErrorCode::INVALID_PARAMS;
    }
    return ErrorCode::OK;
}

ErrorCode Client::IsExist(const std::string& key) {
    auto response = master_client_.ExistKey(key);
    return response.error_code;
}

ErrorCode Client::BatchIsExist(const std::vector<std::string>& keys,
                               std::vector<ErrorCode>& exist_results) {
    exist_results.resize(keys.size());
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.exist_responses.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size()
                   << ", Got: " << response.exist_responses.size();
        // Fill with RPC_FAIL error codes
        std::fill(exist_results.begin(), exist_results.end(),
                  ErrorCode::RPC_FAIL);
        return ErrorCode::RPC_FAIL;
    }

    exist_results = response.exist_responses;

    return ErrorCode::OK;
}

ErrorCode Client::TransferData(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    std::vector<Slice>& slices, TransferRequest::OpCode op_code) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    auto future = transfer_submitter_->submit(handles, slices, op_code);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return ErrorCode::TRANSFER_FAIL;
    }

    VLOG(1) << "Using transfer strategy: " << future->strategy();

    return future->get();
}

ErrorCode Client::TransferWrite(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    std::vector<Slice>& slices) {
    return TransferData(handles, slices, TransferRequest::WRITE);
}

ErrorCode Client::TransferRead(
    const std::vector<AllocatedBuffer::Descriptor>& handles,
    std::vector<Slice>& slices) {
    size_t total_size = 0;
    for (const auto& handle : handles) {
        total_size += handle.size_;
    }

    size_t slices_size = CalculateSliceSize(slices);
    if (slices_size < total_size) {
        LOG(ERROR) << "Slice size " << slices_size << " is smaller than total "
                   << "size " << total_size;
        return ErrorCode::INVALID_PARAMS;
    }

    return TransferData(handles, slices, TransferRequest::READ);
}

void Client::PingThreadFunc() {
    // How many failed pings before getting latest master view from etcd
    const int max_ping_fail_count = 3;
    // How long to wait for next ping after success
    const int success_ping_interval_ms = 1000;
    // How long to wait for next ping after failure
    const int fail_ping_interval_ms = 1000;
    // Increment after a ping failure, reset after a ping success
    int ping_fail_count = 0;

    auto remount_segment = [this]() {
        // This lock must be held until the remount rpc is finished,
        // otherwise there will be corner cases, e.g., a segment is unmounted
        // successfully first, and then remounted again in this thread.
        std::lock_guard<std::mutex> lock(mounted_segments_mutex_);
        std::vector<Segment> segments;
        for (auto it : mounted_segments_) {
            auto& segment = it.second;
            segments.push_back(segment);
        }
        ErrorCode err =
            master_client_.ReMountSegment(segments, client_id_).error_code;
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to remount segments: " << err;
        }
    };
    // Use another thread to remount segments to avoid blocking the ping thread
    std::future<void> remount_segment_future;

    while (ping_running_) {
        // Join the remount segment thread if it is ready
        if (remount_segment_future.valid() &&
            remount_segment_future.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready) {
            remount_segment_future = std::future<void>();
        }

        // Ping master
        auto ping_result = master_client_.Ping(client_id_);
        if (ping_result.error_code == ErrorCode::OK) {
            // Reset ping failure count
            ping_fail_count = 0;
            if (ping_result.client_status == ClientStatus::NEED_REMOUNT &&
                !remount_segment_future.valid()) {
                // Ensure at most one remount segment thread is running
                remount_segment_future =
                    std::async(std::launch::async, remount_segment);
            }
            std::this_thread::sleep_for(
                std::chrono::milliseconds(success_ping_interval_ms));
            continue;
        }

        ping_fail_count++;
        if (ping_fail_count < max_ping_fail_count) {
            LOG(ERROR) << "Failed to ping master";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        // Too many ping failures, we need to check if the master view has
        // changed
        LOG(ERROR) << "Failed to ping master for " << ping_fail_count
                   << " times, try to get latest master view and reconnect";
        std::string master_address;
        ViewVersionId next_version = 0;
        auto err =
            master_view_helper_.GetMasterView(master_address, next_version);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to get new master view: " << toString(err);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        err = master_client_.Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to connect to master " << master_address
                       << ": " << toString(err);
            std::this_thread::sleep_for(
                std::chrono::milliseconds(fail_ping_interval_ms));
            continue;
        }

        LOG(INFO) << "Reconnected to master " << master_address;
        ping_fail_count = 0;
    }
    // Explicitly wait for the remount segment thread to finish
    if (remount_segment_future.valid()) {
        remount_segment_future.wait();
    }
}

ErrorCode Client::FindFirstCompleteReplica(
    const std::vector<Replica::Descriptor>& replica_list,
    std::vector<AllocatedBuffer::Descriptor>& handles) {
    handles.clear();

    // Find the first complete replica
    for (size_t i = 0; i < replica_list.size(); ++i) {
        if (replica_list[i].status == ReplicaStatus::COMPLETE) {
            handles = replica_list[i].buffer_descriptors;
            return ErrorCode::OK;
        }
    }

    // No complete replica found
    return ErrorCode::INVALID_REPLICA;
}

}  // namespace mooncake
