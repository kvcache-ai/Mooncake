#include "nvme_kv/backend.h"

#include <algorithm>
#include <limits>

#include <glog/logging.h>

#include "nvme_kv/key_conflict_policy.h"

namespace mooncake {

namespace {

tl::expected<NvmeKvResolvedRoot, ErrorCode> ResolveRoot(
    NvmeKvConnector &connector, const std::string &logical_key) {
    const NvmeKvObjectIdentity identity{.logical_key = logical_key};
    for (uint32_t slot = 0; slot < kNvmeKvMaxPhysicalKeySlots; ++slot) {
        const auto physical_key = EncodeNvmeKvPhysicalKey(identity, slot);
        auto object_res = connector.Retrieve(physical_key);
        if (!object_res) {
            if (object_res.error() == ErrorCode::OBJECT_NOT_FOUND) {
                continue;
            }
            return tl::make_unexpected(object_res.error());
        }

        auto root = DecodeNvmeKvReadRoot(std::move(*object_res));
        if (!root) {
            return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
        }
        if (root->identity.logical_key != logical_key) {
            continue;
        }

        return NvmeKvResolvedRoot{physical_key, slot, std::move(*root)};
    }
    return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
}

class LocalNvmeKvPolicy final : public NvmeKvObjectPolicy {
   public:
    NvmeKvPhysicalKey EncodePhysicalKey(const NvmeKvObjectKey &key,
                                        uint32_t slot,
                                        uint32_t part_index) const override {
        const NvmeKvObjectIdentity identity{key.logical_key};
        if (part_index == 0) {
            return EncodeNvmeKvPhysicalKey(identity, slot);
        }
        return EncodeNvmeKvChunkPhysicalKey(identity, part_index - 1, slot);
    }

    tl::expected<NvmeKvWritePlan, ErrorCode> PrepareWrite(
        const NvmeKvObjectKey &key, std::string_view payload,
        uint32_t value_limit) const override {
        return PrepareWriteAtSlot(key, payload, value_limit, 0);
    }

    tl::expected<NvmeKvReadRoot, ErrorCode> ReadRoot(
        NvmeKvConnector &connector, const NvmeKvObjectKey &key,
        std::optional<uint64_t> expected_payload_size) const override;

    tl::expected<NvmeKvPutResult, ErrorCode> HandleWriteFailure(
        NvmeKvConnector &connector, const NvmeKvObjectKey &key,
        std::string_view payload, NvmeKvWriteAttempt &attempt) const override;

    void AbortWrite(NvmeKvConnector &connector,
                    NvmeKvWriteAttempt &attempt) const noexcept override {
        if (!attempt.write_plan) {
            return;
        }

        const auto delete_key = [&](const NvmeKvPhysicalKey &key) {
            try {
                auto result = connector.Delete(key);
                if (!result && result.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(WARNING) << "NVMe KV failed to clean local value for "
                                 << attempt.write_plan->identity.logical_key
                                 << " error=" << toString(result.error());
                }
            } catch (const std::exception &error) {
                LOG(WARNING)
                    << "NVMe KV abort cleanup failed: " << error.what();
            } catch (...) {
                LOG(WARNING) << "NVMe KV abort cleanup failed";
            }
        };
        if (attempt.root_written) {
            delete_key(attempt.write_plan->root_key);
        }
        for (const auto &key : attempt.written_chunk_keys) {
            delete_key(key);
        }
        attempt.root_written = false;
        attempt.written_chunk_keys.clear();
    }

   private:
    tl::expected<NvmeKvWritePlan, ErrorCode> PrepareWriteAtSlot(
        const NvmeKvObjectKey &key, std::string_view payload,
        uint32_t value_limit, uint32_t slot) const {
        const NvmeKvObjectIdentity identity{key.logical_key};
        return BuildNvmeKvWritePlan(
            identity, payload, value_limit,
            {EncodePhysicalKey(key, slot, 0), slot, [&](uint32_t index) {
                 return EncodePhysicalKey(key, slot, index + 1);
             }});
    }
};

tl::expected<NvmeKvReadRoot, ErrorCode> LocalNvmeKvPolicy::ReadRoot(
    NvmeKvConnector &connector, const NvmeKvObjectKey &key,
    std::optional<uint64_t> expected_payload_size) const {
    auto resolved = ResolveRoot(connector, key.logical_key);
    if (!resolved) {
        return tl::make_unexpected(resolved.error());
    }

    auto validation_res = ValidateNvmeKvReadRoot(
        {key.logical_key}, resolved->physical_key, resolved->slot,
        expected_payload_size, resolved->root);
    if (!validation_res) {
        return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
    }

    return std::move(resolved->root);
}

}  // namespace

std::unique_ptr<NvmeKvObjectStore> NvmeKvStorageBackend::CreateObjectStore(
    std::unique_ptr<NvmeKvConnector> connector) {
    return std::make_unique<NvmeKvObjectStore>(
        std::move(connector), std::make_unique<LocalNvmeKvPolicy>());
}

NvmeKvStorageBackend::NvmeKvStorageBackend(
    const FileStorageConfig &file_storage_config_)
    : StorageBackendInterface(file_storage_config_) {}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::InitDevice() {
    auto connector = std::make_unique<NvmeKvConnector>(file_storage_config_);
    auto init_res = connector->Init();
    if (!init_res) {
        LOG(ERROR) << "Failed to initialize NVMe KV device: "
                   << toString(init_res.error());
        return init_res;
    }
    object_store_ = CreateObjectStore(std::move(connector));
    return {};
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::Init() {
    bool expected = false;
    if (!initialized_.compare_exchange_strong(expected, true,
                                              std::memory_order_acq_rel)) {
        LOG(ERROR) << "NVMe KV backend already initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    auto init_res = InitDevice();
    if (!init_res) {
        initialized_.store(false, std::memory_order_release);
        return init_res;
    }
    total_size_.store(0, std::memory_order_relaxed);
    total_keys_.store(0, std::memory_order_relaxed);
    return {};
}

tl::expected<int64_t, ErrorCode> NvmeKvStorageBackend::BatchOffload(
    const std::unordered_map<std::string, std::vector<Slice>> &batch_object,
    std::function<ErrorCode(const std::vector<std::string> &keys,
                            std::vector<StorageObjectMetadata> &metadatas)>
        complete_handler,
    NvmeKvStorageBackend::EvictionHandler eviction_handler) {
    (void)eviction_handler;
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (batch_object.empty()) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    struct OffloadPlan {
        std::string key;
        const std::vector<Slice> *slices = nullptr;
        size_t payload_size = 0;
    };
    std::vector<OffloadPlan> plans;
    plans.reserve(batch_object.size());
    int64_t batch_total_size = 0;
    for (const auto &[key, slices] : batch_object) {
        if (slices.empty()) {
            continue;
        }
        size_t payload_size = 0;
        for (const auto &slice : slices) {
            if (slice.size >
                std::numeric_limits<size_t>::max() - payload_size) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            payload_size += slice.size;
        }
        if (payload_size >
                static_cast<size_t>(std::numeric_limits<int64_t>::max()) ||
            batch_total_size > std::numeric_limits<int64_t>::max() -
                                   static_cast<int64_t>(payload_size)) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        batch_total_size += static_cast<int64_t>(payload_size);
        plans.push_back(OffloadPlan{key, &slices, payload_size});
    }

    if (plans.empty()) {
        return 0;
    }

    if (total_keys_.load(std::memory_order_relaxed) +
            static_cast<int64_t>(plans.size()) >
        file_storage_config_.total_keys_limit) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    if (total_size_.load(std::memory_order_relaxed) + batch_total_size >
        file_storage_config_.total_size_limit) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }
    auto enable_offloading_res = IsEnableOffloading();
    if (!enable_offloading_res) {
        return tl::make_unexpected(enable_offloading_res.error());
    }
    if (!enable_offloading_res.value()) {
        return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
    }

    std::vector<NvmeKvObjectStore::PutRequest> requests;
    requests.reserve(plans.size());
    for (const auto &plan : plans) {
        requests.push_back({{plan.key}, *plan.slices});
    }
    const auto worker_result = object_store_->PutBatch(requests);

    std::vector<std::string> keys;
    std::vector<StorageObjectMetadata> metadatas;
    keys.reserve(plans.size());
    metadatas.reserve(plans.size());
    int64_t inserted_keys = 0;
    int64_t inserted_size = 0;
    ErrorCode first_error =
        worker_result ? ErrorCode::OK : worker_result.error();
    for (size_t i = 0; i < plans.size(); ++i) {
        if (!requests[i].result) {
            if (first_error == ErrorCode::OK) {
                first_error = requests[i].result.error();
            }
            continue;
        }
        if (!requests[i].result.value().stored) {
            continue;
        }
        if (requests[i].result.value().inserted) {
            ++inserted_keys;
            inserted_size += static_cast<int64_t>(plans[i].payload_size);
        }
        keys.push_back(plans[i].key);
        metadatas.emplace_back(StorageObjectMetadata{
            .bucket_id = 0,
            .offset = 0,
            .key_size = static_cast<int64_t>(plans[i].key.size()),
            .data_size = static_cast<int64_t>(plans[i].payload_size),
            .transport_endpoint = "",
        });
    }

    total_keys_.fetch_add(inserted_keys, std::memory_order_relaxed);
    total_size_.fetch_add(inserted_size, std::memory_order_relaxed);
    if (complete_handler != nullptr && !keys.empty()) {
        const auto error_code = complete_handler(keys, metadatas);
        if (error_code != ErrorCode::OK) {
            return tl::make_unexpected(error_code);
        }
    }
    if (first_error != ErrorCode::OK) {
        return tl::make_unexpected(first_error);
    }
    return static_cast<int64_t>(keys.size());
}

namespace {

tl::expected<NvmeKvPutResult, ErrorCode> LocalNvmeKvPolicy::HandleWriteFailure(
    NvmeKvConnector &connector_ref, const NvmeKvObjectKey &key,
    std::string_view payload, NvmeKvWriteAttempt &attempt) const {
    const size_t payload_size = payload.size();
    if (payload_size >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    auto *connector = &connector_ref;
    AbortWrite(connector_ref, attempt);
    const uint32_t effective_max_value_size =
        std::max(connector->GetCapabilities().effective_max_value_size,
                 kMinMaxValueSize);

    for (uint32_t slot = 0; slot < kNvmeKvMaxPhysicalKeySlots; ++slot) {
        auto write_plan_res =
            PrepareWriteAtSlot(key, payload, effective_max_value_size, slot);
        if (!write_plan_res) {
            return tl::make_unexpected(write_plan_res.error());
        }
        attempt.write_plan.emplace(std::move(write_plan_res.value()));
        auto &write_plan = *attempt.write_plan;
        const bool store_inline = write_plan.store_inline;
        const auto &physical_key = write_plan.root_key;
        const auto &root_blob = write_plan.root_blob;
        const auto &chunk_store_values = write_plan.chunk_values;

        auto &written_chunk_keys = attempt.written_chunk_keys;
        auto &root_written = attempt.root_written;
        const auto cleanup_new_writes = [&]() {
            if (root_written) {
                auto delete_res = connector->Delete(physical_key);
                if (!delete_res &&
                    delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(WARNING)
                        << "NVMe KV failed to clean orphan root for "
                        << key.logical_key
                        << " key=" << NvmeKvPhysicalKeyToHex(physical_key)
                        << " error=" << toString(delete_res.error());
                }
                root_written = false;
            }
            if (written_chunk_keys.empty()) {
                return;
            }
            for (const auto &chunk_key : written_chunk_keys) {
                auto delete_res = connector->Delete(chunk_key);
                if (!delete_res &&
                    delete_res.error() != ErrorCode::OBJECT_NOT_FOUND) {
                    LOG(WARNING) << "NVMe KV failed to clean orphan chunk for "
                                 << key.logical_key
                                 << " key=" << NvmeKvPhysicalKeyToHex(chunk_key)
                                 << " error=" << toString(delete_res.error());
                }
            }
            written_chunk_keys.clear();
        };
        const auto resolve_existing_root =
            [&]() -> tl::expected<NvmeKvPutResult, ErrorCode> {
            auto decision_res = NvmeKvKeyConflictPolicy::ResolveExistingObject(
                *connector, physical_key, root_blob);
            if (!decision_res) {
                return tl::make_unexpected(decision_res.error());
            }
            if (decision_res.value() ==
                NvmeKvKeyConflictPolicy::ExistingObjectDecision::kNotFound) {
                return NvmeKvPutResult{};
            }
            if (decision_res.value() ==
                NvmeKvKeyConflictPolicy::ExistingObjectDecision::
                    kDifferentObject) {
                return NvmeKvPutResult{};
            }
            return NvmeKvPutResult{.stored = true, .inserted = false};
        };

        bool write_success = true;
        bool slot_collision = false;
        ErrorCode &write_error = attempt.error;
        write_error = ErrorCode::OK;
        if (store_inline) {
            auto store_res = connector->Store(physical_key, root_blob);
            if (!store_res) {
                write_success = false;
                write_error = store_res.error();
            } else {
                root_written = true;
            }
        } else {
            const auto handle_chunk_store_error =
                [&](const NvmeKvPhysicalKey &chunk_key,
                    std::string_view chunk_blob, ErrorCode error) -> bool {
                if (error == ErrorCode::OBJECT_ALREADY_EXISTS) {
                    auto existing_chunk_res =
                        NvmeKvKeyConflictPolicy::ResolveExistingObject(
                            *connector, chunk_key, chunk_blob);
                    if (existing_chunk_res &&
                        existing_chunk_res.value() ==
                            NvmeKvKeyConflictPolicy::ExistingObjectDecision::
                                kSameObject) {
                        return true;
                    }
                    if (!existing_chunk_res) {
                        write_error = existing_chunk_res.error();
                    } else {
                        write_error = ErrorCode::OBJECT_ALREADY_EXISTS;
                    }
                    slot_collision = true;
                } else {
                    write_error = error;
                }
                write_success = false;
                cleanup_new_writes();
                return false;
            };
            std::vector<NvmeKvCommandExecutor::StoreRequest> chunk_requests;
            chunk_requests.reserve(chunk_store_values.size());
            for (const auto &[chunk_key, chunk_blob] : chunk_store_values) {
                NvmeKvCommandExecutor::StoreRequest request;
                request.key = chunk_key;
                request.value = chunk_blob;
                chunk_requests.push_back(request);
            }
            connector->StoreBatch(chunk_requests);

            for (size_t chunk_index = 0; chunk_index < chunk_requests.size();
                 ++chunk_index) {
                if (chunk_requests[chunk_index].result) {
                    written_chunk_keys.push_back(
                        chunk_store_values[chunk_index].first);
                }
            }

            for (size_t chunk_index = 0;
                 chunk_index < chunk_store_values.size(); ++chunk_index) {
                const auto &[chunk_key, chunk_blob] =
                    chunk_store_values[chunk_index];
                const auto &chunk_res = chunk_requests[chunk_index].result;
                if (!chunk_res) {
                    if (!handle_chunk_store_error(chunk_key, chunk_blob,
                                                  chunk_res.error())) {
                        break;
                    }
                    continue;
                }
            }
            if (write_success) {
                auto store_res = connector->Store(physical_key, root_blob);
                if (!store_res) {
                    write_success = false;
                    write_error = store_res.error();
                    cleanup_new_writes();
                } else {
                    root_written = true;
                }
            }
        }

        if (!write_success && write_error == ErrorCode::OBJECT_ALREADY_EXISTS) {
            auto existing_res = resolve_existing_root();
            if (existing_res && existing_res.value().stored) {
                return existing_res;
            }
            if (!existing_res) {
                write_error = existing_res.error();
            } else {
                slot_collision = true;
                cleanup_new_writes();
            }
        }

        if (!write_success) {
            if (!slot_collision && !store_inline) {
                cleanup_new_writes();
            }
            written_chunk_keys.clear();
            if (slot_collision) {
                continue;
            }
            return tl::make_unexpected(write_error);
        }

        return NvmeKvPutResult{.stored = true, .inserted = true};
    }
    return NvmeKvPutResult{};
}

}  // namespace

tl::expected<void, ErrorCode> NvmeKvStorageBackend::BatchLoad(
    std::unordered_map<std::string, Slice> &batched_slices) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    std::vector<NvmeKvObjectStore::GetRequest> requests;
    requests.reserve(batched_slices.size());
    for (const auto &[key, slice] : batched_slices) {
        requests.push_back({{key}, slice.size, slice});
    }
    object_store_->GetBatch(requests);
    for (const auto &request : requests) {
        if (!request.result) {
            return tl::make_unexpected(request.result.error());
        }
    }
    return {};
}

tl::expected<bool, ErrorCode> NvmeKvStorageBackend::IsExist(
    const std::string &key) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (object_store_ == nullptr) {
        return false;
    }
    return object_store_->IsExist({key});
}

tl::expected<bool, ErrorCode> NvmeKvStorageBackend::IsEnableOffloading() {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (object_store_ == nullptr) {
        return false;
    }
    return total_keys_.load(std::memory_order_relaxed) <
               file_storage_config_.total_keys_limit &&
           total_size_.load(std::memory_order_relaxed) <
               file_storage_config_.total_size_limit;
}

tl::expected<void, ErrorCode> NvmeKvStorageBackend::ScanMeta(
    const std::function<
        ErrorCode(const std::vector<std::string> &keys,
                  std::vector<StorageObjectMetadata> &metadatas)> &handler) {
    if (!initialized_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    (void)handler;
    return {};
}

}  // namespace mooncake
