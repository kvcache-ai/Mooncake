#include "client.h"

#include <glog/logging.h>

#include <algorithm>
#include <cassert>
#include <cstdint>

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
        auto result =
            UnmountSegment(reinterpret_cast<void*>(segment.base), segment.size);
        if (!result) {
            LOG(ERROR) << "Failed to unmount segment: "
                       << toString(result.error());
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

tl::expected<void, ErrorCode> Client::Get(const std::string& object_key,
                                          std::vector<Slice>& slices) {
    auto query_result = Query(object_key);
    if (!query_result) {
        return tl::unexpected(query_result.error());
    }
    return Get(object_key, query_result.value(), slices);
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    auto batched_query_results = BatchQuery(object_keys);

    // If any queries failed, return error results immediately for failed
    // queries
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(object_keys.size());

    std::vector<std::vector<Replica::Descriptor>> valid_replica_lists;
    std::vector<size_t> valid_indices;
    std::vector<std::string> valid_keys;

    for (size_t i = 0; i < batched_query_results.size(); ++i) {
        if (batched_query_results[i]) {
            valid_replica_lists.push_back(batched_query_results[i].value());
            valid_indices.push_back(i);
            valid_keys.push_back(object_keys[i]);
            results.emplace_back();  // placeholder for successful results
        } else {
            results.emplace_back(
                tl::unexpected(batched_query_results[i].error()));
        }
    }

    // If we have any valid queries, process them
    if (!valid_keys.empty()) {
        std::unordered_map<std::string, std::vector<Slice>> valid_slices;
        for (const auto& key : valid_keys) {
            auto it = slices.find(key);
            if (it != slices.end()) {
                valid_slices[key] = it->second;
            }
        }

        auto valid_results =
            BatchGet(valid_keys, valid_replica_lists, valid_slices);

        // Merge results back
        for (size_t i = 0; i < valid_indices.size(); ++i) {
            results[valid_indices[i]] = valid_results[i];
        }
    }

    return results;
}

tl::expected<std::vector<Replica::Descriptor>, ErrorCode> Client::Query(
    const std::string& object_key) {
    return master_client_.GetReplicaList(object_key);
}

std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
Client::BatchQuery(const std::vector<std::string>& object_keys) {
    auto response = master_client_.BatchGetReplicaList(object_keys);

    if (!response) {
        LOG(ERROR) << "QueryBatch failed: RPC error";
        // Return vector of errors
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            error_results;
        error_results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            error_results.emplace_back(tl::unexpected(response.error()));
        }
        return error_results;
    }

    if (response.value().size() != object_keys.size()) {
        LOG(ERROR) << "QueryBatch failed, response size is not equal to "
                      "request size";
        // Return vector of INVALID_PARAMS errors
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            error_results;
        error_results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            error_results.emplace_back(
                tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return error_results;
    }

    return response.value();
}

tl::expected<void, ErrorCode> Client::Get(
    const std::string& object_key,
    const std::vector<Replica::Descriptor>& replica_list,
    std::vector<Slice>& slices) {
    // Find the first complete replica
    std::vector<AllocatedBuffer::Descriptor> handles;
    ErrorCode err = FindFirstCompleteReplica(replica_list, handles);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::INVALID_REPLICA) {
            LOG(ERROR) << "no_complete_replicas_found key=" << object_key;
        }
        return tl::unexpected(err);
    }

    err = TransferRead(handles, slices);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "transfer_read_failed key=" << object_key;
        return tl::unexpected(err);
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchGet(
    const std::vector<std::string>& object_keys,
    const std::vector<std::vector<Replica::Descriptor>>& replica_lists,
    std::unordered_map<std::string, std::vector<Slice>>& slices) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    // Validate input size consistency
    if (replica_lists.size() != object_keys.size()) {
        LOG(ERROR) << "Replica lists size (" << replica_lists.size()
                   << ") doesn't match object keys size (" << object_keys.size()
                   << ")";
        std::vector<tl::expected<void, ErrorCode>> results;
        results.reserve(object_keys.size());
        for (size_t i = 0; i < object_keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::INVALID_PARAMS));
        }
        return results;
    }

    // Collect all transfer operations for parallel execution
    std::vector<std::tuple<size_t, std::string, TransferFuture>>
        pending_transfers;
    std::vector<tl::expected<void, ErrorCode>> results(object_keys.size());

    // Submit all transfers in parallel
    for (size_t i = 0; i < object_keys.size(); ++i) {
        const auto& key = object_keys[i];
        const auto& replica_list = replica_lists[i];

        auto slices_it = slices.find(key);
        if (slices_it == slices.end()) {
            LOG(ERROR) << "Slices not found for key: " << key;
            results[i] = tl::unexpected(ErrorCode::INVALID_PARAMS);
            continue;
        }

        // Find the first complete replica for this key
        std::vector<AllocatedBuffer::Descriptor> handles;
        ErrorCode err = FindFirstCompleteReplica(replica_list, handles);
        if (err != ErrorCode::OK) {
            if (err == ErrorCode::INVALID_REPLICA) {
                LOG(ERROR) << "no_complete_replicas_found key=" << key;
            }
            results[i] = tl::unexpected(err);
            continue;
        }

        // Submit transfer operation asynchronously
        auto future = transfer_submitter_->submit(handles, slices_it->second,
                                                  TransferRequest::READ);
        if (!future) {
            LOG(ERROR) << "Failed to submit transfer operation for key: "
                       << key;
            results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
            continue;
        }

        VLOG(1) << "Submitted transfer for key " << key
                << " using strategy: " << static_cast<int>(future->strategy());

        pending_transfers.emplace_back(i, key, std::move(*future));
    }

    // Wait for all transfers to complete
    for (auto& [index, key, future] : pending_transfers) {
        ErrorCode result = future.get();
        if (result != ErrorCode::OK) {
            LOG(ERROR) << "Transfer failed for key: " << key
                       << " with error: " << static_cast<int>(result);
            results[index] = tl::unexpected(result);
        } else {
            VLOG(1) << "Transfer completed successfully for key: " << key;
            results[index] = {};
        }
    }

    VLOG(1) << "BatchGet completed for " << object_keys.size() << " keys";
    return results;
}

tl::expected<void, ErrorCode> Client::Put(const ObjectKey& key,
                                          std::vector<Slice>& slices,
                                          const ReplicateConfig& config) {
    // Prepare slice lengths
    std::vector<size_t> slice_lengths;
    size_t slice_size = 0;
    for (size_t i = 0; i < slices.size(); ++i) {
        slice_lengths.push_back(slices[i].size);
        slice_size += slices[i].size;
    }

    // Start put operation
    auto start_result =
        master_client_.PutStart(key, slice_lengths, slice_size, config);
    if (!start_result) {
        ErrorCode err = start_result.error();
        if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
            VLOG(1) << "object_already_exists key=" << key;
            return {};
        }
        LOG(ERROR) << "Failed to start put operation: " << err;
        return tl::unexpected(err);
    }

    // Transfer data using allocated handles from all replicas
    for (const auto& replica : start_result.value()) {
        std::vector<AllocatedBuffer::Descriptor> handles;
        for (const auto& handle : replica.buffer_descriptors) {
            CHECK(handle.buffer_address_ != 0) << "buffer_address_ is nullptr";
            handles.push_back(handle);
        }

        ErrorCode transfer_err = TransferWrite(handles, slices);
        if (transfer_err != ErrorCode::OK) {
            // Revoke put operation
            auto revoke_result = master_client_.PutRevoke(key);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation";
                return tl::unexpected(revoke_result.error());
            }
            return tl::unexpected(transfer_err);
        }
    }

    // End put operation
    auto end_result = master_client_.PutEnd(key);
    if (!end_result) {
        ErrorCode err = end_result.error();
        LOG(ERROR) << "Failed to end put operation: " << err;
        return tl::unexpected(err);
    }
    return {};
}

std::vector<tl::expected<void, ErrorCode>> Client::BatchPut(
    const std::vector<ObjectKey>& keys,
    std::vector<std::vector<Slice>>& batched_slices, ReplicateConfig& config) {
    CHECK(transfer_submitter_) << "TransferSubmitter not initialized";

    std::vector<tl::expected<void, ErrorCode>> results =
        std::vector<tl::expected<void, ErrorCode>>(keys.size());

    if (keys.size() != batched_slices.size()) {
        LOG(ERROR) << "Keys size (" << keys.size()
                   << ") doesn't match batched slices size ("
                   << batched_slices.size() << ")";
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    }

    std::vector<std::vector<uint64_t>> batched_slice_lengths;
    std::vector<uint64_t> batched_value_lengths;
    batched_slice_lengths.reserve(keys.size());
    batched_value_lengths.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& slices = batched_slices[i];
        uint64_t slice_size = 0;
        std::vector<uint64_t> slice_lengths;
        slice_lengths.reserve(slices.size());
        for (const auto& slice : slices) {
            slice_lengths.push_back(slice.size);
            slice_size += slice.size;
        }
        batched_slice_lengths.push_back(std::move(slice_lengths));
        batched_value_lengths.push_back(slice_size);
    }

    auto start_responses = master_client_.BatchPutStart(
        keys, batched_value_lengths, batched_slice_lengths, config);
    if (start_responses.size() != keys.size()) {
        LOG(ERROR) << "BatchPutStart response size mismatch. Expected: "
                   << keys.size() << ", Got: " << start_responses.size();
        return std::vector<tl::expected<void, ErrorCode>>(
            keys.size(), tl::unexpected(ErrorCode::RPC_FAIL));
    }

    // Check for errors in individual start responses and collect successful
    // replica lists
    std::vector<std::vector<Replica::Descriptor>> replica_lists;
    replica_lists.reserve(keys.size());
    bool has_object_exists = false;

    for (size_t i = 0; i < start_responses.size(); ++i) {
        if (!start_responses[i]) {
            ErrorCode err = start_responses[i].error();
            if (err == ErrorCode::OBJECT_ALREADY_EXISTS) {
                has_object_exists = true;
                results[i] =
                    tl::expected<void,
                                 ErrorCode>{};  // Success for existing object
                replica_lists
                    .emplace_back();  // Empty replica list for existing object
            } else {
                LOG(ERROR) << "Failed to start put operation for key "
                           << keys[i] << ": " << err;
                results[i] = tl::unexpected(err);
                replica_lists
                    .emplace_back();  // Empty replica list for failed operation
            }
        } else {
            replica_lists.push_back(start_responses[i].value());
        }
    }

    if (has_object_exists) {
        LOG(INFO) << "Some objects already exist in batch put operation";
    }

    // Collect all transfer operations for parallel execution
    std::vector<std::tuple<std::string, size_t, TransferFuture>>
        pending_transfers;

    // Submit all transfers in parallel
    for (size_t key_idx = 0; key_idx < keys.size(); ++key_idx) {
        const auto& key = keys[key_idx];

        // Skip if this key had an error during start operation
        if (!results[key_idx]) {
            continue;
        }

        // Skip if this key already exists (empty replica list)
        const auto& replica_list = replica_lists[key_idx];
        if (replica_list.empty()) {
            continue;
        }

        auto& slices = batched_slices[key_idx];

        for (size_t replica_idx = 0; replica_idx < replica_list.size();
             ++replica_idx) {
            const auto& replica = replica_list[replica_idx];
            std::vector<AllocatedBuffer::Descriptor> handles;
            for (const auto& handle : replica.buffer_descriptors) {
                CHECK(handle.buffer_address_ != 0)
                    << "buffer_address_ is nullptr";
                handles.push_back(handle);
            }

            // Submit transfer operation asynchronously
            auto future = transfer_submitter_->submit(handles, slices,
                                                      TransferRequest::WRITE);
            if (!future) {
                LOG(ERROR) << "Failed to submit transfer operation for key: "
                           << key << " replica: " << replica_idx;
                // Revoke put operation
                auto revoke_results = master_client_.BatchPutRevoke(keys);
                for (size_t i = 0; i < revoke_results.size(); ++i) {
                    if (!revoke_results[i]) {
                        LOG(ERROR)
                            << "Failed to revoke put operation for key "
                            << keys[i] << ": " << revoke_results[i].error();
                    }
                }
                // Mark all remaining operations as failed
                for (size_t i = 0; i < results.size(); ++i) {
                    if (results[i]) {  // Only update successful ones
                        results[i] = tl::unexpected(ErrorCode::TRANSFER_FAIL);
                    }
                }
                return results;
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
            auto revoke_results = master_client_.BatchPutRevoke(keys);
            for (size_t i = 0; i < revoke_results.size(); ++i) {
                if (!revoke_results[i]) {
                    LOG(ERROR) << "Failed to revoke put operation for key "
                               << keys[i] << ": " << revoke_results[i].error();
                }
            }
            // Mark all remaining operations as failed
            for (size_t i = 0; i < results.size(); ++i) {
                if (results[i]) {  // Only update successful ones
                    results[i] = tl::unexpected(result);
                }
            }
            return results;
        }
        VLOG(1) << "Transfer completed successfully for key: " << key
                << " replica: " << replica_idx;
    }

    // End put operation
    auto end_results = master_client_.BatchPutEnd(keys);

    // Check individual end operation results
    for (size_t i = 0; i < end_results.size(); ++i) {
        if (!end_results[i]) {
            LOG(ERROR) << "Failed to end put operation for key " << keys[i]
                       << ": " << end_results[i].error();
            results[i] = tl::unexpected(end_results[i].error());
        } else if (results[i]) {
            // Mark as successful only if not already failed
            results[i] = tl::expected<void, ErrorCode>{};
        }
    }

    VLOG(1) << "BatchPut completed successfully for " << keys.size()
            << " keys with " << pending_transfers.size() << " total transfers";
    return results;
}

tl::expected<void, ErrorCode> Client::Remove(const ObjectKey& key) {
    auto result = master_client_.Remove(key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return {};
}

tl::expected<long, ErrorCode> Client::RemoveAll() {
    return master_client_.RemoveAll();
}

tl::expected<void, ErrorCode> Client::MountSegment(const void* buffer,
                                                   size_t size) {
    if (buffer == nullptr || size == 0 ||
        reinterpret_cast<uintptr_t>(buffer) % facebook::cachelib::Slab::kSize ||
        size % facebook::cachelib::Slab::kSize) {
        LOG(ERROR) << "buffer=" << buffer << " or size=" << size
                   << " is not aligned to " << facebook::cachelib::Slab::kSize;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
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
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    int rc = transfer_engine_.registerLocalMemory(
        (void*)buffer, size, kWildcardLocation, true, true);
    if (rc != 0) {
        LOG(ERROR) << "register_local_memory_failed base=" << buffer
                   << " size=" << size << ", error=" << rc;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    Segment segment(generate_uuid(), local_hostname_,
                    reinterpret_cast<uintptr_t>(buffer), size);

    auto mount_result = master_client_.MountSegment(segment, client_id_);
    if (!mount_result) {
        ErrorCode err = mount_result.error();
        LOG(ERROR) << "mount_segment_to_master_failed base=" << buffer
                   << " size=" << size << ", error=" << err;
        return tl::unexpected(err);
    }

    mounted_segments_[segment.id] = segment;
    return {};
}

tl::expected<void, ErrorCode> Client::UnmountSegment(const void* buffer,
                                                     size_t size) {
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
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto unmount_result =
        master_client_.UnmountSegment(segment->second.id, client_id_);
    if (!unmount_result) {
        ErrorCode err = unmount_result.error();
        LOG(ERROR) << "Failed to unmount segment from master: "
                   << toString(err);
        return tl::unexpected(err);
    }

    int rc = transfer_engine_.unregisterLocalMemory(
        reinterpret_cast<void*>(segment->second.base));
    if (rc != 0) {
        LOG(ERROR) << "Failed to unregister transfer buffer with transfer "
                      "engine ret is "
                   << rc;
        if (rc != ERR_ADDRESS_NOT_REGISTERED) {
            return tl::unexpected(ErrorCode::INTERNAL_ERROR);
        }
        // Otherwise, the segment is already unregistered from transfer engine,
        // we can continue
    }

    mounted_segments_.erase(segment);
    return {};
}

tl::expected<void, ErrorCode> Client::RegisterLocalMemory(
    void* addr, size_t length, const std::string& location,
    bool remote_accessible, bool update_metadata) {
    if (this->transfer_engine_.registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata) != 0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::unregisterLocalMemory(
    void* addr, bool update_metadata) {
    if (this->transfer_engine_.unregisterLocalMemory(addr, update_metadata) !=
        0) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

tl::expected<bool, ErrorCode> Client::IsExist(const std::string& key) {
    auto result = master_client_.ExistKey(key);
    if (!result) {
        return tl::unexpected(result.error());
    }
    return result.value();
}

std::vector<tl::expected<bool, ErrorCode>> Client::BatchIsExist(
    const std::vector<std::string>& keys) {
    auto response = master_client_.BatchExistKey(keys);

    // Check if we got the expected number of responses
    if (response.size() != keys.size()) {
        LOG(ERROR) << "BatchExistKey response size mismatch. Expected: "
                   << keys.size() << ", Got: " << response.size();
        // Return vector of RPC_FAIL errors
        std::vector<tl::expected<bool, ErrorCode>> results;
        results.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); ++i) {
            results.emplace_back(tl::unexpected(ErrorCode::RPC_FAIL));
        }
        return results;
    }

    // Return the response directly as it's already in the correct format
    return response;
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
        auto remount_result =
            master_client_.ReMountSegment(segments, client_id_);
        if (!remount_result) {
            ErrorCode err = remount_result.error();
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
        if (ping_result) {
            // Reset ping failure count
            ping_fail_count = 0;
            auto [view_version, client_status] = ping_result.value();
            if (client_status == ClientStatus::NEED_REMOUNT &&
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
