#include "shared_nvme_kv_backend.h"

#include <limits>
#include <stdexcept>

#include "device/accelerator_registry.h"
#include "nvme_kv/key_codec.h"
#include "nvme_kv/key_conflict_policy.h"

namespace mooncake {
namespace {

class SharedNvmeKvPolicy final : public NvmeKvObjectPolicy {
   public:
    NvmeKvPhysicalKey EncodePhysicalKey(const NvmeKvObjectKey& key, uint32_t,
                                        uint32_t part_index) const override {
        return EncodeSharedNvmeKvPhysicalKey(key.allocation_id.value(),
                                             part_index);
    }

    tl::expected<NvmeKvWritePlan, ErrorCode> PrepareWrite(
        const NvmeKvObjectKey& key, std::string_view payload,
        uint32_t value_limit) const override {
        auto size = InlineSize(key, payload.size(), value_limit);
        if (!size) {
            return tl::make_unexpected(size.error());
        }

        return BuildNvmeKvWritePlan(
            {key.logical_key}, payload, value_limit,
            {EncodePhysicalKey(key, 0, 0), 0, [&](uint32_t index) {
                 return EncodePhysicalKey(key, 0, index + 1);
             }});
    }

    tl::expected<NvmeKvReadRoot, ErrorCode> ReadRoot(
        NvmeKvConnector& connector, const NvmeKvObjectKey& key,
        std::optional<uint64_t> expected_payload_size) const override {
        auto resolved = ReadFixedRoot(connector, key, expected_payload_size);
        if (!resolved) {
            return tl::make_unexpected(resolved.error());
        }

        auto validation_res = ValidateNvmeKvReadRoot(
            {key.logical_key}, resolved->physical_key, resolved->slot,
            expected_payload_size, resolved->root, NvmeKvObjectType::kInline);
        if (!validation_res) {
            return tl::make_unexpected(validation_res.error());
        }

        return std::move(resolved->root);
    }

    tl::expected<NvmeKvPutResult, ErrorCode> HandleWriteFailure(
        NvmeKvConnector& connector, const NvmeKvObjectKey&, std::string_view,
        NvmeKvWriteAttempt& attempt) const override {
        if (attempt.error != ErrorCode::OBJECT_ALREADY_EXISTS) {
            return tl::make_unexpected(attempt.error);
        }

        const auto& plan = *attempt.write_plan;
        auto existing = NvmeKvKeyConflictPolicy::ResolveExistingObject(
            connector, plan.root_key, plan.root_blob);
        if (!existing) {
            return tl::make_unexpected(existing.error());
        }

        using Decision = NvmeKvKeyConflictPolicy::ExistingObjectDecision;
        if (*existing == Decision::kNotFound) {
            return tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
        }
        if (*existing == Decision::kDifferentObject) {
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        }
        return NvmeKvPutResult{.stored = true, .inserted = false};
    }

    void AbortWrite(NvmeKvConnector&,
                    NvmeKvWriteAttempt&) const noexcept override {
        // Allocation reclamation belongs to the shared replica lifecycle.
    }

   private:
    tl::expected<NvmeKvResolvedRoot, ErrorCode> ReadFixedRoot(
        NvmeKvConnector& connector, const NvmeKvObjectKey& key,
        std::optional<uint64_t> expected_payload_size) const {
        if (!key.allocation_id || *key.allocation_id == 0) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        uint32_t hint = 0;
        if (expected_payload_size) {
            auto size = InlineSize(
                key, *expected_payload_size,
                connector.GetCapabilities().effective_max_value_size);
            if (!size) {
                return tl::make_unexpected(size.error());
            }
            hint = *size;
        }

        const auto physical_key = EncodePhysicalKey(key, 0, 0);
        auto blob = connector.Retrieve(physical_key, hint);
        if (!blob) {
            return tl::make_unexpected(blob.error());
        }
        if (hint && blob->size() > hint) {
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
        }

        auto root = DecodeNvmeKvReadRoot(std::move(*blob));
        if (!root) {
            return tl::make_unexpected(root.error());
        }
        return NvmeKvResolvedRoot{physical_key, 0, std::move(*root)};
    }

    static tl::expected<uint32_t, ErrorCode> InlineSize(
        const NvmeKvObjectKey& key, uint64_t payload_size,
        uint32_t value_limit) {
        if (!key.allocation_id || *key.allocation_id == 0 ||
            key.logical_key.empty() || !payload_size) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        return ComputeNvmeKvInlineSize(key.logical_key.size(), payload_size,
                                       value_limit);
    }
};

tl::expected<size_t, ErrorCode> ValidateHostSlices(
    const std::vector<Slice>* slices,
    const device::RuntimeAccelerator& accelerators) {
    if (!slices) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t bytes = 0;
    for (const auto& slice : *slices) {
        if (!slice.ptr || !slice.size ||
            slice.size > std::numeric_limits<size_t>::max() - bytes) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (accelerators.FindDeviceForPointer(slice.ptr)) {
            return tl::unexpected(ErrorCode::NOT_SUPPORTED);
        }
        bytes += slice.size;
    }

    if (!bytes) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return bytes;
}

}  // namespace

NvmeKvPhysicalKey EncodeSharedNvmeKvPhysicalKey(uint64_t allocation_id,
                                                uint64_t part) {
    NvmeKvPhysicalKey key{};
    for (size_t i = 0; i < 8; ++i) {
        key[i] = static_cast<uint8_t>(allocation_id >> (56 - i * 8));
        key[i + 8] = static_cast<uint8_t>(part >> (56 - i * 8));
    }
    return key;
}

std::shared_ptr<NvmeKvObjectStore> SharedNvmeKvBackend::CreateObjectStore(
    std::unique_ptr<NvmeKvConnector> connector) {
    return std::make_shared<NvmeKvObjectStore>(
        std::move(connector), std::make_unique<SharedNvmeKvPolicy>());
}

SharedNvmeKvBackend::SharedNvmeKvBackend(
    std::vector<std::shared_ptr<NvmeKvObjectStore>> object_stores)
    : object_stores_(std::move(object_stores)) {
    if (object_stores_.empty()) {
        throw std::invalid_argument(
            "shared KV namespace list must not be empty");
    }

    for (const auto& ns : object_stores_) {
        if (!ns) {
            throw std::invalid_argument("invalid shared KV namespace");
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> SharedNvmeKvBackend::BatchWrite(
    const std::vector<SharedKvIORequest>& requests) {
    return BatchTransfer(requests, true);
}

std::vector<tl::expected<void, ErrorCode>> SharedNvmeKvBackend::BatchRead(
    const std::vector<SharedKvIORequest>& requests) {
    return BatchTransfer(requests, false);
}

std::vector<tl::expected<void, ErrorCode>> SharedNvmeKvBackend::BatchTransfer(
    const std::vector<SharedKvIORequest>& requests, bool write) {
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));
    std::vector<std::vector<size_t>> groups(object_stores_.size());
    const auto accelerators =
        device::GetAcceleratorRegistry().RuntimeAccelerators();

    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& request = requests[i];
        auto length = ValidateHostSlices(request.slice_lists, accelerators);
        if (!length || *length != request.descriptor.object_size ||
            request.descriptor.allocation_id == 0) {
            if (!length) {
                results[i] = tl::unexpected(length.error());
            }
            continue;
        }

        if (!write && request.slice_lists->size() != 1) {
            results[i] = tl::unexpected(ErrorCode::NOT_SUPPORTED);
            continue;
        }

        const auto& key = request.key;
        const auto hash = ComputeNvmeKvChecksum(
            {reinterpret_cast<const uint8_t*>(key.data()), key.size()});
        groups[hash % object_stores_.size()].push_back(i);
    }

    for (size_t ns = 0; ns < groups.size(); ++ns) {
        std::vector<NvmeKvObjectStore::PutRequest> writes;
        std::vector<NvmeKvObjectStore::GetRequest> reads;
        for (const auto index : groups[ns]) {
            const auto& request = requests[index];
            NvmeKvObjectKey key{request.key, request.descriptor.allocation_id};
            if (write) {
                writes.push_back({std::move(key), *request.slice_lists});
            } else {
                reads.push_back({std::move(key), request.descriptor.object_size,
                                 request.slice_lists->front()});
            }
        }

        if (write) {
            object_stores_[ns]->PutBatch(writes);
        } else {
            object_stores_[ns]->GetBatch(reads);
        }

        for (size_t i = 0; i < groups[ns].size(); ++i) {
            auto& result = results[groups[ns][i]];
            if (write) {
                if (!writes[i].result) {
                    result = tl::unexpected(writes[i].result.error());
                } else if (!writes[i].result->stored) {
                    result = tl::unexpected(ErrorCode::FILE_WRITE_FAIL);
                } else {
                    result = {};
                }
            } else {
                result = reads[i].result
                             ? tl::expected<void, ErrorCode>{}
                             : tl::unexpected(reads[i].result.error());
            }
        }
    }

    return results;
}

}  // namespace mooncake
