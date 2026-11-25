#include "client_service.h"

#include <limits>

#include "device/accelerator_registry.h"
#include "storage/distributed/object_replica_io.h"

namespace mooncake {
tl::expected<std::shared_ptr<ObjectStorageBackend>, ErrorCode>
Client::GetObjectStorageBackend(const std::string& namespace_id) {
    std::lock_guard lock(object_storage_init_mutex_);
    if (object_storage_config_current_ && object_storage_backend_ &&
        namespace_id == object_storage_namespace_)
        return object_storage_backend_;
    if (!transfer_submitter_) return tl::unexpected(ErrorCode::NOT_SUPPORTED);

    // A new leader may enable object storage or select its first target.
    // Only install the target the master confirms for this descriptor.
    auto config = master_client_.GetObjectStorageConfig();
    if (!config) return tl::unexpected(config.error());
    if (!config->enabled) return tl::unexpected(ErrorCode::NOT_SUPPORTED);
    auto target_namespace = config->NamespaceId();
    if (namespace_id != target_namespace)
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    if (object_storage_backend_ &&
        object_storage_namespace_ == target_namespace) {
        object_storage_backend_->SetMaxStagingBytes(config->max_staging_bytes);
        object_storage_config_current_ = true;
        return object_storage_backend_;
    }
    auto adapter = CreateObjectReplicaAdapter(*config);
    if (!adapter) return tl::unexpected(adapter.error());
    object_storage_backend_ = std::make_shared<ObjectStorageBackend>(
        // The issuing master enforces its current write limit. Descriptors
        // can outlive a limit change across leader switches.
        ObjectStorageBackendConfig{
            .max_object_bytes = std::numeric_limits<uint64_t>::max(),
            .max_staging_bytes = config->max_staging_bytes},
        *adapter);
    object_storage_namespace_ = std::move(target_namespace);
    object_storage_config_current_ = true;
    EnsureStorageControlPlaneStarted();
    return object_storage_backend_;
}
ErrorCode Client::WriteObjectStorageReplica(const Replica::Descriptor& replica,
                                            const std::vector<Slice>& slices) {
    const auto& object = replica.get_object_storage_descriptor();
    auto backend = GetObjectStorageBackend(object.namespace_id);
    if (!backend) {
        ReportObjectStorageUpload(object.object_key,
                                  ObjectUploadOutcome::NOT_STARTED);
        return backend.error();
    }
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    bool may_exist = false;
    auto start = std::chrono::steady_clock::now();
    auto error = (*backend)->WriteV(
        object.object_key, object.object_size, slices,
        [&](const Slice& slice) {
            return !runtime.FindDeviceForPointer(slice.ptr);
        },
        [&](void* dst, const void* src, size_t size) {
            return runtime.CopyToHost(dst, src, size);
        },
        nullptr, &may_exist);
    if (metrics_)
        metrics_->transfer_metric.object_put_latency_us.observe(
            elapsed_us_since(start));
    if (error == ErrorCode::OK) {
        if (metrics_)
            metrics_->transfer_metric.total_write_bytes.inc(object.object_size);
        // Completion is normally persisted with PutEnd, avoiding another RPC
        // and log barrier. Retain only a notification in case PutEnd fails.
        std::lock_guard lock(object_storage_completions_mutex_);
        object_storage_completions_.insert_or_assign(
            object.object_key, ObjectUploadOutcome::COMPLETE);
    } else {
        ReportObjectStorageUpload(object.object_key,
                                  may_exist ? ObjectUploadOutcome::UNKNOWN
                                            : ObjectUploadOutcome::NOT_WRITTEN);
    }
    return error;
}
ErrorCode Client::ReadObjectStorageReplica(const Replica::Descriptor& replica,
                                           const std::vector<Slice>& slices,
                                           uint64_t offset) {
    const auto& object = replica.get_object_storage_descriptor();
    auto backend = GetObjectStorageBackend(object.namespace_id);
    if (!backend) return backend.error();
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    auto start = std::chrono::steady_clock::now();
    auto result = (*backend)->Read(
        object.object_key, object.object_size, slices, offset,
        [&](void* dst, const void* src, size_t size) {
            return runtime.CopyFromHost(dst, src, size);
        },
        [&](const Slice& slice) {
            return !runtime.FindDeviceForPointer(slice.ptr);
        });
    if (metrics_) {
        metrics_->transfer_metric.object_get_latency_us.observe(
            elapsed_us_since(start));
        if (result == ErrorCode::OK) {
            uint64_t bytes_read = 0;
            for (const auto& slice : slices) bytes_read += slice.size;
            metrics_->transfer_metric.total_read_bytes.inc(bytes_read);
        }
    }
    return result;
}
void Client::ReportObjectStorageUpload(const std::string& name,
                                       ObjectUploadOutcome outcome) {
    if (!master_client_.FinishObjectStorageUpload(name, outcome)) {
        {
            std::lock_guard lock(object_storage_completions_mutex_);
            object_storage_completions_.insert_or_assign(name, outcome);
        }
        EnsureStorageControlPlaneStarted();
    }
}
void Client::AcknowledgeObjectStorageWrite(
    const std::vector<Replica::Descriptor>& replicas) {
    for (const auto& replica : replicas) {
        if (!replica.is_object_storage_replica()) continue;
        std::lock_guard lock(object_storage_completions_mutex_);
        object_storage_completions_.erase(
            replica.get_object_storage_descriptor().object_key);
    }
}
void Client::PollObjectStorage() {
    std::vector<std::pair<std::string, ObjectUploadOutcome>> pending;
    {
        std::lock_guard lock(object_storage_completions_mutex_);
        auto it = object_storage_completions_.upper_bound(last_polled_object_);
        const auto count =
            std::min(kTaskBatchSize, object_storage_completions_.size());
        for (size_t i = 0; i < count; ++i) {
            if (it == object_storage_completions_.end())
                it = object_storage_completions_.begin();
            pending.push_back(*it++);
        }
    }
    for (const auto& [name, outcome] : pending) {
        if (!task_poll_running_.load()) break;
        last_polled_object_ = name;
        if (master_client_.FinishObjectStorageUpload(name, outcome)) {
            std::lock_guard lock(object_storage_completions_mutex_);
            object_storage_completions_.erase(name);
        }
    }
}
}  // namespace mooncake
