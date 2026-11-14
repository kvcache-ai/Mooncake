#include "client_service.h"
#include "device/accelerator_registry.h"
#include "storage/distributed/object_replica_io.h"

namespace mooncake {
ErrorCode Client::InitObjectStorage() {
    auto config = master_client_.GetObjectStorageConfig();
    if (!config) return config.error();
    if (!config->enabled) return ErrorCode::OK;
    auto adapter = CreateObjectReplicaAdapter(*config);
    if (!adapter) return adapter.error();
    object_storage_namespace_ = config->NamespaceId();
    object_storage_backend_ = std::make_shared<ObjectStorageBackend>(
        ObjectStorageBackendConfig{config->max_object_bytes,
                                   config->max_staging_bytes},
        *adapter);
    EnsureStorageControlPlaneStarted();
    return ErrorCode::OK;
}
ErrorCode Client::WriteObjectStorageReplica(const Replica::Descriptor& replica,
                                            const std::vector<Slice>& slices) {
    if (!object_storage_backend_) return ErrorCode::NOT_SUPPORTED;
    const auto& object = replica.get_object_storage_descriptor();
    if (object.namespace_id != object_storage_namespace_)
        return ErrorCode::INVALID_PARAMS;
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    bool started = false;
    auto start = std::chrono::steady_clock::now();
    auto error = object_storage_backend_->WriteV(
        object.object_key, object.object_size, slices,
        [&](const Slice& slice) {
            return !runtime.FindDeviceForPointer(slice.ptr);
        },
        [&](void* dst, const void* src, size_t size) {
            return runtime.CopyToHost(dst, src, size);
        },
        &started);
    if (metrics_)
        metrics_->transfer_metric.object_put_latency_us.observe(
            elapsed_us_since(start));
    if (error == ErrorCode::OK) {
        // Completion is normally persisted with PutEnd, avoiding another RPC
        // and log barrier. Retain only a notification in case PutEnd fails.
        std::lock_guard lock(object_storage_completions_mutex_);
        object_storage_completions_.insert_or_assign(
            object.object_key, ObjectUploadOutcome::COMPLETE);
    } else {
        ReportObjectStorageUpload(object.object_key,
                                  started ? ObjectUploadOutcome::UNKNOWN
                                          : ObjectUploadOutcome::NOT_STARTED);
    }
    return error;
}
ErrorCode Client::ReadObjectStorageReplica(const Replica::Descriptor& replica,
                                           const std::vector<Slice>& slices,
                                           uint64_t offset) {
    if (!object_storage_backend_) return ErrorCode::NOT_SUPPORTED;
    const auto& object = replica.get_object_storage_descriptor();
    if (object.namespace_id != object_storage_namespace_)
        return ErrorCode::INVALID_PARAMS;
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    auto start = std::chrono::steady_clock::now();
    auto result = object_storage_backend_->Read(
        object.object_key, object.object_size, slices, offset,
        [&](void* dst, const void* src, size_t size) {
            return runtime.CopyFromHost(dst, src, size);
        },
        [&](const Slice& slice) {
            return !runtime.FindDeviceForPointer(slice.ptr);
        });
    if (metrics_)
        metrics_->transfer_metric.object_get_latency_us.observe(
            elapsed_us_since(start));
    return result;
}
void Client::ReportObjectStorageUpload(const std::string& name,
                                       ObjectUploadOutcome outcome) {
    if (!master_client_.FinishObjectStorageUpload(name, outcome)) {
        std::lock_guard lock(object_storage_completions_mutex_);
        object_storage_completions_.insert_or_assign(name, outcome);
    }
}
void Client::AcknowledgeObjectStorageWrite(
    const std::vector<Replica::Descriptor>& replicas) {
    if (!object_storage_backend_) return;
    std::lock_guard lock(object_storage_completions_mutex_);
    for (const auto& replica : replicas)
        if (replica.is_object_storage_replica())
            object_storage_completions_.erase(
                replica.get_object_storage_descriptor().object_key);
}
void Client::PollObjectStorage() {
    if (!object_storage_backend_) return;
    std::unordered_map<std::string, ObjectUploadOutcome> pending;
    {
        std::lock_guard lock(object_storage_completions_mutex_);
        pending.swap(object_storage_completions_);
    }
    for (const auto& [name, outcome] : pending) {
        if (!master_client_.FinishObjectStorageUpload(name, outcome)) {
            std::lock_guard lock(object_storage_completions_mutex_);
            object_storage_completions_.emplace(name, outcome);
        }
    }
}
}  // namespace mooncake
