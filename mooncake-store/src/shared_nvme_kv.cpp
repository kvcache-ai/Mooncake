#include "shared_nvme_kv.h"

#include <cstring>
#include <stdexcept>

#include "nvme_kv/object_layout.h"
#include "shared_nvme_kv_key.h"

namespace mooncake {
namespace {

// Bound encoded staging memory separately from the executor DMA budget.
constexpr uint64_t kBatchBytes = 256 * 1024 * 1024;
constexpr size_t kBatchKeys = 256;
}  // namespace

SharedNvmeKv::SharedNvmeKv(std::unique_ptr<NvmeKvCommandExecutor> executor)
    : executor_(std::move(executor)) {
    if (!executor_) {
        throw std::invalid_argument("shared NVMe KV executor must not be null");
    }
}

tl::expected<uint32_t, ErrorCode> SharedNvmeKv::EncodedSize(
    const std::string& key, uint64_t payload_size) const {
    if (!executor_ || key.empty() || !payload_size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const uint64_t limit =
        executor_->GetCapabilities().effective_max_value_size;

    // Bound each input before serialization and addition. The device limit is
    // uint32_t, so the subsequent total fits in uint64_t without wrapping.
    if (key.size() > limit || payload_size > limit) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Serialize the actual logical identity, including its version and key
    // length fields. Check the total before allocating/copying the payload.
    const auto encoded_identity = SerializeNvmeKvCanonicalIdentity({key});
    const uint64_t bytes = sizeof(NvmeKvObjectHeader) +
                           sizeof(NvmeKvStoredIdentityMetadata) +
                           uint64_t{encoded_identity.size()} + payload_size;
    if (bytes > limit) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    return static_cast<uint32_t>(bytes);
}

tl::expected<void, ErrorCode> SharedNvmeKv::EncodeInto(
    const std::string& key, uint64_t allocation_id,
    std::span<const char> payload, std::span<char> encoded) const {
    auto size = EncodedSize(key, payload.size());
    if (!allocation_id || !size || encoded.size() != *size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto prefix = EncodeMetadata(key, allocation_id, payload, encoded);
    if (!prefix) {
        return tl::make_unexpected(prefix.error());
    }

    // TODO: Submit metadata and registered payload as separate SGL regions to
    // avoid this copy and the executor's subsequent copy into DMA memory.
    std::memcpy(encoded.data() + *prefix, payload.data(), payload.size());
    return {};
}

tl::expected<uint32_t, ErrorCode> SharedNvmeKv::EncodeMetadata(
    const std::string& key, uint64_t allocation_id,
    std::span<const char> payload, std::span<char> encoded) const {
    const NvmeKvObjectIdentity identity{key};

    // Master-assigned address, with part zero reserved for the inline root.
    // TODO: Add manifests only with whole-object publication and reclamation.
    auto metadata = SerializeNvmeKvStoredIdentity(
        identity, BuildNvmeKvStoredIdentityMetadata(
                      EncodeSharedNvmeKvPhysicalKey(allocation_id), 0));
    const size_t prefix = sizeof(NvmeKvObjectHeader) + metadata.size();
    if (prefix > encoded.size()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    NvmeKvObjectHeader header{};
    header.magic = NvmeKvObjectHeader::kMagic;
    header.object_type = static_cast<uint32_t>(NvmeKvObjectType::kInline);
    header.payload_size = payload.size();
    header.verify_hash = ComputeNvmeKvVerifyHash(identity);
    header.identity_metadata_size = metadata.size();

    header.payload_checksum =
        ComputeNvmeKvPayloadChecksum({payload.data(), payload.size()});
    header.header_checksum = ComputeNvmeKvHeaderChecksum(header);

    std::memcpy(encoded.data(), &header, sizeof(header));
    std::memcpy(encoded.data() + sizeof(header), metadata.data(),
                metadata.size());
    return static_cast<uint32_t>(prefix);
}

tl::expected<std::string_view, ErrorCode> SharedNvmeKv::Validate(
    const std::string& key, uint64_t allocation_id,
    std::string_view encoded) const {
    if (!allocation_id || encoded.size() < sizeof(NvmeKvObjectHeader)) {
        return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }

    // string_view does not guarantee header alignment. Keep this small copy;
    // payload copies are the zero-copy optimization target.
    NvmeKvObjectHeader header;
    std::memcpy(&header, encoded.data(), sizeof(header));
    const NvmeKvObjectIdentity identity{key};
    uint64_t bytes = sizeof(header) + uint64_t{header.identity_metadata_size} +
                     header.payload_size;
    if (bytes > encoded.size() ||
        !ValidateNvmeKvHeader(header, identity,
                              ComputeNvmeKvStoredIdentityMetadataSize(identity),
                              header.payload_size, NvmeKvObjectType::kInline)) {
        return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }

    auto expected = EncodedSize(key, header.payload_size);
    if (!expected || *expected != encoded.size()) {
        return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }

    NvmeKvStoredIdentityView stored;
    if (!ParseNvmeKvStoredIdentity(
            encoded.substr(sizeof(header), header.identity_metadata_size),
            stored) ||
        stored.logical_key != identity.logical_key ||
        stored.resolved_slot != 0 ||
        stored.resolved_physical_key !=
            EncodeSharedNvmeKvPhysicalKey(allocation_id)) {
        return tl::make_unexpected(ErrorCode::INVALID_KEY);
    }

    auto payload = encoded.substr(
        sizeof(header) + header.identity_metadata_size, header.payload_size);
    if (ComputeNvmeKvPayloadChecksum(payload) != header.payload_checksum) {
        return tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
    }

    return payload;
}

tl::expected<size_t, ErrorCode> SharedNvmeKv::Decode(
    const std::string& key, uint64_t allocation_id, std::string_view encoded,
    std::span<char> output) const {
    auto payload = Validate(key, allocation_id, encoded);
    if (!payload) {
        return tl::make_unexpected(payload.error());
    }

    if (payload->size() > output.size()) {
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

    // TODO: Scatter retrieval into metadata and caller-owned registered payload
    // buffers, with explicit lifetime and invalid-output-on-error contracts.
    std::memcpy(output.data(), payload->data(), payload->size());
    return payload->size();
}

void SharedNvmeKv::PutBatch(std::vector<WriteRequest>& requests) {
    size_t next = 0;

    while (next < requests.size()) {
        std::vector<size_t> indices;
        std::vector<std::string> values;
        uint64_t bytes = 0;

        while (next < requests.size() && indices.size() < kBatchKeys) {
            auto& request = requests[next];
            auto size = EncodedSize(request.key, request.payload.size());
            if (!request.allocation_id || !size) {
                request.result = tl::make_unexpected(
                    !size ? size.error() : ErrorCode::INVALID_PARAMS);
                ++next;
                continue;
            }

            if (!indices.empty() && bytes + *size > kBatchBytes) {
                break;
            }

            std::string value(*size, '\0');
            auto encoded = EncodeInto(request.key, request.allocation_id,
                                      request.payload, value);
            if (!encoded) {
                request.result = tl::make_unexpected(encoded.error());
                ++next;
                continue;
            }

            values.push_back(std::move(value));
            indices.push_back(next++);
            bytes += *size;
        }

        std::vector<NvmeKvCommandExecutor::StoreRequest> stores;

        for (size_t i = 0; i < indices.size(); ++i) {
            stores.push_back({EncodeSharedNvmeKvPhysicalKey(
                                  requests[indices[i]].allocation_id),
                              values[i]});
        }

        executor_->StoreBatch(stores);

        for (size_t i = 0; i < stores.size(); ++i) {
            auto& request = requests[indices[i]];
            request.result = stores[i].result;
            if (request.result ||
                request.result.error() != ErrorCode::OBJECT_ALREADY_EXISTS) {
                continue;
            }

            // KEY_EXISTS does not prove identity or integrity.
            // Never delete on failure.
            auto existing =
                executor_->Retrieve(stores[i].key, values[i].size());
            if (existing) {
                auto validated =
                    Validate(request.key, request.allocation_id, *existing);
                request.result = validated
                                     ? tl::expected<void, ErrorCode>{}
                                     : tl::make_unexpected(validated.error());
            } else if (existing.error() == ErrorCode::OBJECT_NOT_FOUND) {
                // The value disappeared between KEY_EXISTS and Retrieve. Report
                // a retryable failure; neither this layer nor the current
                // Client automatically retries this Store.
                request.result =
                    tl::make_unexpected(ErrorCode::OBJECT_REPLICA_BUSY);
            } else {
                request.result = tl::make_unexpected(existing.error());
            }
        }
    }
}

void SharedNvmeKv::GetBatch(std::vector<ReadRequest>& requests) {
    size_t next = 0;

    while (next < requests.size()) {
        std::vector<size_t> indices;
        std::vector<NvmeKvCommandExecutor::RetrieveBufferRequest> reads;
        uint64_t bytes = 0;

        // request.data.size() is the expected payload length from the object
        // descriptor, excluding the header and identity metadata.
        while (next < requests.size() && indices.size() < kBatchKeys) {
            auto& request = requests[next];
            auto size = EncodedSize(request.key, request.data.size());
            if (!request.allocation_id || !size) {
                request.result = tl::make_unexpected(
                    !size ? size.error() : ErrorCode::INVALID_PARAMS);
                ++next;
                continue;
            }

            if (!indices.empty() && bytes + *size > kBatchBytes) {
                break;
            }

            reads.push_back(
                {EncodeSharedNvmeKvPhysicalKey(request.allocation_id), *size});
            indices.push_back(next++);
            bytes += *size;
        }

        executor_->RetrieveBufferBatch(reads);

        for (size_t i = 0; i < reads.size(); ++i) {
            auto& request = requests[indices[i]];
            if (!reads[i].result) {
                request.result = tl::make_unexpected(reads[i].result.error());
                continue;
            }

            // Reject returned lengths beyond the supplied buffer capacity.
            if (reads[i].result->size > reads[i].size_hint) {
                request.result =
                    tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
                continue;
            }

            if (reads[i].result->size != reads[i].size_hint) {
                request.result =
                    tl::make_unexpected(ErrorCode::CHECKSUM_MISMATCH);
                continue;
            }
            const auto& value = *reads[i].result;
            if (!value.data) {
                request.result = tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
                continue;
            }
            request.result = Decode(request.key, request.allocation_id,
                                    {value.data, value.size}, request.data);
        }
    }
}

tl::expected<void, ErrorCode> SharedNvmeKv::Put(const std::string& key,
                                                uint64_t allocation_id,
                                                std::span<const char> payload) {
    std::vector<WriteRequest> requests{{key, allocation_id, payload}};
    PutBatch(requests);
    return requests[0].result;
}

tl::expected<size_t, ErrorCode> SharedNvmeKv::Get(const std::string& key,
                                                  uint64_t allocation_id,
                                                  void* buf, size_t len) {
    if (!buf) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<ReadRequest> requests{
        {key, allocation_id, {static_cast<char*>(buf), len}}};
    GetBatch(requests);
    return requests[0].result;
}

}  // namespace mooncake
