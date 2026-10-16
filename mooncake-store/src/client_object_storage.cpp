#include "client_service.h"
#include <algorithm>

#include "device/accelerator_registry.h"

namespace mooncake {

ErrorCode Client::InitObjectStorage() {
    auto config = master_client_.GetObjectStorageConfig();
    if (!config) return config.error();
    if (!config->enabled) return ErrorCode::OK;
    auto adapter = CreateObjectStorageAdapter(*config);
    if (!adapter) return adapter.error();
    auto registered = master_client_.RegisterObjectStorageClient();
    if (!registered) return registered.error();
    std::atomic_store(
        &object_storage_backend_,
        std::make_shared<ObjectStorageBackend>(*config, *adapter));
    // Writers without local storage still need completion-RPC retries.
    EnsureStorageControlPlaneStarted();
    return ErrorCode::OK;
}

ErrorCode Client::WriteObjectStorageReplica(const Replica::Descriptor& replica,
                                            const std::vector<Slice>& slices) {
    auto backend = std::atomic_load(&object_storage_backend_);
    if (!backend) return ErrorCode::NOT_SUPPORTED;
    const auto& descriptor = replica.get_object_storage_descriptor();
    auto begun = master_client_.BeginObjectStorageUpload(descriptor.object_key);
    if (!begun) {
        // The claim may have committed even if its response was lost. This
        // caller has not issued PUT, so durably cancel before redispatch.
        auto canceled = master_client_.FinishObjectStorageUpload(
            descriptor.object_key, ObjectUploadOutcome::NOT_STARTED);
        if (!canceled) {
            std::lock_guard lock(object_storage_completions_mutex_);
            object_storage_completions_[descriptor.object_key] =
                ObjectUploadOutcome::NOT_STARTED;
        }
        return begun.error();
    }
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    bool upload_started = false;
    const auto error = backend->Write(
        descriptor, slices,
        [&](void* dst, const void* src, size_t size) {
            return runtime.CopyToHost(dst, src, size);
        },
        &upload_started);
    // On an ambiguous network failure the catalog must retain its tombstone.
    // Retrying this notification is safe; retrying the PUT is not.
    const auto outcome = error == ErrorCode::OK ? ObjectUploadOutcome::COMPLETE
                         : upload_started       ? ObjectUploadOutcome::UNKNOWN
                                          : ObjectUploadOutcome::NOT_STARTED;
    auto finished = master_client_.FinishObjectStorageUpload(
        descriptor.object_key, outcome);
    if (!finished) {
        std::lock_guard lock(object_storage_completions_mutex_);
        object_storage_completions_.insert_or_assign(descriptor.object_key,
                                                     outcome);
    }
    if (error != ErrorCode::OK) return error;
    return finished ? ErrorCode::OK : finished.error();
}

ErrorCode Client::ReadObjectStorageReplica(const Replica::Descriptor& replica,
                                           const std::vector<Slice>& slices,
                                           uint64_t offset) {
    auto backend = std::atomic_load(&object_storage_backend_);
    if (!backend) return ErrorCode::NOT_SUPPORTED;
    auto runtime = device::GetAcceleratorRegistry().RuntimeAccelerators();
    return backend->Read(replica.get_object_storage_descriptor(), slices,
                         offset, [&](void* dst, const void* src, size_t size) {
                             return runtime.CopyFromHost(dst, src, size);
                         });
}

void Client::PollObjectStorage() {
    if (!std::atomic_load(&object_storage_backend_) ||
        object_storage_running_.exchange(true))
        return;
    task_thread_pool_.enqueue([this] {
        struct RunningGuard {
            std::atomic<bool>& running;
            ~RunningGuard() { running.store(false); }
        } guard{object_storage_running_};
        std::unordered_map<std::string, ObjectUploadOutcome> completions;
        {
            std::lock_guard lock(object_storage_completions_mutex_);
            completions.swap(object_storage_completions_);
        }
        for (const auto& [key, outcome] : completions) {
            auto finished =
                master_client_.FinishObjectStorageUpload(key, outcome);
            if (!finished) {
                std::lock_guard lock(object_storage_completions_mutex_);
                object_storage_completions_.emplace(key, outcome);
            }
        }
        {
            std::lock_guard lock(object_storage_completions_mutex_);
            if (!object_storage_completions_.empty()) return;
        }
        auto tasks = master_client_.PollObjectStorageTasks();
        if (!tasks) return;
        for (const auto& task : *tasks) {
            if (!task.source.is_memory_replica()) continue;
            const auto& buffer =
                task.source.get_memory_descriptor().buffer_descriptor;
            // The Master only dispatches to the owner of this exact source.
            // Serialize local unmount with source copying. No borrowed source
            // buffer survives this synchronous upload call.
            std::lock_guard lock(mounted_segments_mutex_);
            const auto contains_source = [&](const auto& entry) {
                const auto& segment = entry.second;
                return segment.te_endpoint == buffer.transport_endpoint_ &&
                       buffer.buffer_address_ >= segment.base &&
                       buffer.buffer_address_ - segment.base <= segment.size &&
                       buffer.size_ <= segment.size - (buffer.buffer_address_ -
                                                       segment.base);
            };
            const bool mounted =
                std::any_of(mounted_segments_.begin(), mounted_segments_.end(),
                            contains_source) ||
                std::any_of(gracefully_unmounting_segments_.begin(),
                            gracefully_unmounting_segments_.end(),
                            contains_source);
            if (!mounted) continue;
            std::vector<Slice> slices{
                {reinterpret_cast<void*>(buffer.buffer_address_),
                 buffer.size_}};
            const auto error = WriteObjectStorageReplica(task.target, slices);
            if (error != ErrorCode::OK)
                LOG(WARNING) << "Object storage offload failed: " << error;
        }
    });
}

}  // namespace mooncake
