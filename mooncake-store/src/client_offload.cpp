#include "client_service.h"

#include <boost/algorithm/string.hpp>
#include <glog/logging.h>

#include "ascii_string.h"
#include "allocator.h"
#include "segment.h"
#include "nof/nof_runtime.h"

#include <csignal>
#include <algorithm>
#include <cassert>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <limits>
#if defined(__linux__)
#include <numa.h>
#endif
#include <optional>
#include <ranges>
#include <span>
#include <sched.h>
#include <thread>
#include <set>
#include <utility>
#include <vector>
#include <ylt/struct_json/json_reader.h>

#include "common.h"
#include "transfer_engine.h"
#include "topology.h"
#include "transfer_task.h"
#include "transport/transport.h"
#include "config.h"
#include "ha/leadership/leader_coordinator_factory.h"
#include "types.h"
#include "client_buffer.h"
#include "common/network.h"
#include "config/client_host_identity_config.h"
#include "config/client_object_checksum_config.h"
#include "replica_selection.h"
#include "rpc_types.h"
#include "local_hot_cache.h"
#include "config/client_auto_discovery_config.h"
#include "device/accelerator_registry.h"
#ifdef USE_INTRA_NVLINK
#include "gpu_vendor/intra_nvlink.h"
#endif
#include "crc_checksum.h"
#include "config/client_numa_config.h"
#include "storage/distributed/distributed_storage_backend.h"

namespace mooncake {

tl::expected<void, ErrorCode> Client::MountLocalDiskSegment(
    bool enable_offloading) {
    auto response =
        master_client_.MountLocalDiskSegment(client_id_, enable_offloading);

    if (!response) {
        LOG(ERROR) << "MountLocalDiskSegment failed, error code is "
                   << response.error();
        return response;
    }

    EnsureStorageControlPlaneStarted();
    return response;
}

tl::expected<void, ErrorCode> Client::UnmountLocalDiskSegment() {
    auto response = master_client_.UnmountLocalDiskSegment(client_id_);
    if (!response) {
        LOG(ERROR) << "UnmountLocalDiskSegment failed, error code is "
                   << response.error();
    }
    return response;
}

tl::expected<void, ErrorCode> Client::OffloadObjectHeartbeat(
    bool enable_offloading, std::vector<OffloadTaskItem>& offloading_objects) {
    auto response =
        master_client_.OffloadObjectHeartbeat(client_id_, enable_offloading);
    if (!response) {
        LOG(ERROR) << "OffloadObjectHeartbeat failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    offloading_objects = std::move(response.value());
    return {};
}

tl::expected<bool, ErrorCode> Client::PollRemoveAll() {
    return master_client_.PollRemoveAll();
}

tl::expected<void, ErrorCode> Client::ReportSsdCapacity(
    int64_t ssd_total_capacity_bytes) {
    auto response =
        master_client_.ReportSsdCapacity(client_id_, ssd_total_capacity_bytes);
    if (!response) {
        LOG(ERROR) << "ReportSsdCapacity failed, error code is "
                   << response.error();
        return tl::make_unexpected(response.error());
    }
    return {};
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices) {
    return BatchGetOffloadObject(
        transfer_engine_addr, keys, pointers, batch_slices,
        OffloadBufferAccess::kTransferEngine, TransferIntent::kForegroundGet);
}

tl::expected<void, ErrorCode> Client::BatchGetOffloadObject(
    const std::string& transfer_engine_addr,
    const std::vector<std::string>& keys,
    const std::vector<uintptr_t>& pointers,
    const std::unordered_map<std::string, std::vector<Slice>>& batch_slices,
    OffloadBufferAccess buffer_access, TransferIntent intent) {
    auto future = transfer_submitter_->submit_batch_get_offload_object(
        transfer_engine_addr, keys, pointers, batch_slices, buffer_access,
        intent);
    if (!future) {
        LOG(ERROR) << "Failed to submit transfer operation";
        return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
    }
    VLOG(1) << "Using transfer strategy: " << future->strategy();
    auto result = future->get();
    if (result != ErrorCode::OK) {
        LOG(ERROR) << "Transfer failed, error code is " << result;
        return tl::make_unexpected(result);
    }
    return {};
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas) {
    auto response =
        master_client_.NotifyOffloadSuccess(client_id_, keys, metadatas);
    return response;
}

tl::expected<void, ErrorCode> Client::NotifyOffloadSuccess(
    const std::vector<OffloadTaskItem>& tasks,
    const std::vector<StorageObjectMetadata>& metadatas) {
    return master_client_.NotifyOffloadSuccess(client_id_, tasks, metadatas);
}

void Client::SetDfsStorageBackend(
    std::shared_ptr<DistributedStorageBackend> backend) {
    dfs_storage_backend_ = std::move(backend);
    EnsureStorageControlPlaneStarted();
}

bool Client::CanReadDfsReplicaIntoHostMemory(
    const Replica::Descriptor& replica) const {
    return replica.is_dfs_replica() && dfs_storage_backend_ &&
           dfs_storage_backend_->SupportsArbitraryHostReadBuffers();
}

tl::expected<void, ErrorCode> Client::PromotionObjectHeartbeat(
    std::vector<PromotionTaskItem>& promotion_objects) {
    auto response = master_client_.PromotionObjectHeartbeat(client_id_);
    if (!response) {
        return tl::make_unexpected(response.error());
    }
    promotion_objects = std::move(response.value());
    return {};
}

tl::expected<void, ErrorCode> Client::RegisterPrefetchTask(
    const std::string& object_key, const std::string& tenant_id) {
    // Default (empty) means this client's own tenant; a delegated prefetch
    // carries the requester's tenant explicitly.
    return master_client_.RegisterPrefetchTask(
        client_id_, object_key,
        tenant_id.empty() ? master_client_.tenant_id() : tenant_id);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, size,
                                              preferred_segments);
}

tl::expected<PromotionAllocStartResponse, ErrorCode>
Client::PromotionAllocStart(
    const std::string& key, const std::string& tenant_id, uint64_t size,
    const std::vector<std::string>& preferred_segments) {
    return master_client_.PromotionAllocStart(client_id_, key, tenant_id, size,
                                              preferred_segments);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key) {
    return master_client_.NotifyPromotionSuccess(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionSuccess(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionSuccess(client_id_, key, tenant_id);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key) {
    return master_client_.NotifyPromotionFailure(client_id_, key);
}

tl::expected<void, ErrorCode> Client::NotifyPromotionFailure(
    const std::string& key, const std::string& tenant_id) {
    return master_client_.NotifyPromotionFailure(client_id_, key, tenant_id);
}

ErrorCode Client::PromotionWrite(const Replica::Descriptor& memory_descriptor,
                                 std::vector<Slice>& slices) {
    return TransferWrite(memory_descriptor, slices);
}

void Client::PutToLocalFile(const std::string& key,
                            const std::vector<Slice>& slices,
                            const DiskDescriptor& disk_descriptor) {
    if (!storage_backend_) return;

    size_t total_size = 0;
    for (const auto& slice : slices) {
        total_size += slice.size;
    }

    std::string path = disk_descriptor.file_path;

    // Synchronous D2H staging + copy into std::string.
    // Done on the calling thread to guarantee GPU buffers are still valid
    // (BatchPut has not yet returned to Python, so blocks are not reused).
    std::string value;
    value.reserve(total_size);
    auto runtime_accelerator =
        device::GetAcceleratorRegistry().RuntimeAccelerators();

    for (const auto& slice : slices) {
        device::PointerInfo info{};
        auto* device =
            runtime_accelerator.FindDeviceForPointer(slice.ptr, &info);
        if (device) {
            device->SetContext(info.device_id);
            auto buf = pinned_buffer_pool_->Acquire(slice.size);
            if (!device->Copy(buf.data, slice.ptr, slice.size,
                              device::CopyDirection::kDeviceToHost)) {
                LOG(ERROR) << "D2H copy failed for key: " << key
                           << ", triggering PutRevoke for disk replica";
                pinned_buffer_pool_->Release(std::move(buf));
                // Must revoke to avoid phantom replica in master
                auto revoke_result =
                    master_client_.PutRevoke(key, ReplicaType::DISK);
                if (!revoke_result) {
                    LOG(ERROR)
                        << "Failed to revoke put operation for key: " << key;
                }
                return;
            }
            value.append(buf.data, slice.size);
            pinned_buffer_pool_->Release(std::move(buf));
        } else {
            value.append(static_cast<char*>(slice.ptr), slice.size);
        }
    }

    // Async StoreObject + PutEnd (unchanged from original)
    write_thread_pool_.enqueue([this, backend = storage_backend_, key,
                                value = std::move(value), path] {
        ReplicaType replica_type = ReplicaType::DISK;
        // Store the object
        auto store_result = backend->StoreObject(
            path, value, key,
            [this, replica_type](const std::vector<std::string>& evicted_keys)
                -> tl::expected<void, ErrorCode> {
                auto evict_results = master_client_.BatchEvictDiskReplica(
                    evicted_keys, replica_type);
                if (evict_results.size() != evicted_keys.size()) {
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                for (size_t i = 0; i < evict_results.size(); ++i) {
                    if (!evict_results[i]) {
                        if (evict_results[i].error() ==
                            ErrorCode::OBJECT_NOT_FOUND) {
                            VLOG(1) << "Master no longer tracks evicted key: "
                                    << evicted_keys[i];
                            continue;
                        }
                        LOG(WARNING)
                            << "Failed to notify master about evicted key: "
                            << evicted_keys[i]
                            << ", error: " << evict_results[i].error();
                        return tl::make_unexpected(evict_results[i].error());
                    }
                }
                return {};
            });

        if (!store_result) {
            // If storage failed, revoke the put operation
            LOG(ERROR) << "Failed to store object for key: " << key;
            auto revoke_result = master_client_.PutRevoke(key, replica_type);
            if (!revoke_result) {
                LOG(ERROR) << "Failed to revoke put operation for key: " << key;
            }
            return;
        }

        // If storage succeeded, end the put operation
        auto end_result =
            master_client_.PutEnd(ObjectMeta{key, std::nullopt}, replica_type);
        if (!end_result) {
            LOG(ERROR) << "Failed to end put operation for key: " << key;
        }
    });
}

}  // namespace mooncake
