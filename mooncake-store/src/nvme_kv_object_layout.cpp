#include "nvme_kv_object_layout.h"

#include <cstring>

namespace mooncake {
namespace {

uint32_t ResolveNvmeKvObjectBlobSizeFromHeader(const char* buffer,
                                               uint32_t prefix_size,
                                               bool enforce_prefix_limit) {
    if (prefix_size < sizeof(NvmeKvObjectHeader)) {
        return 0;
    }

    NvmeKvObjectHeader header{};
    std::memcpy(&header, buffer, sizeof(header));
    if (header.magic != NvmeKvObjectHeader::kMagic) {
        return 0;
    }

    const uint64_t object_size = static_cast<uint64_t>(sizeof(header)) +
                                 header.identity_metadata_size +
                                 header.payload_size;
    if (object_size > UINT32_MAX) {
        return 0;
    }
    if (enforce_prefix_limit && object_size > prefix_size) {
        return 0;
    }
    return static_cast<uint32_t>(object_size);
}

}  // namespace

uint32_t ComputeNvmeKvPayloadChecksum(std::string_view payload) {
    return ComputeNvmeKvChecksum(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(payload.data()), payload.size()));
}

uint32_t ComputeNvmeKvHeaderChecksum(const NvmeKvObjectHeader& header) {
    NvmeKvObjectHeader temp = header;
    temp.header_checksum = 0;
    return ComputeNvmeKvChecksum(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(&temp), sizeof(temp)));
}

uint32_t ComputeNvmeKvStoredIdentityMetadataSize(
    const NvmeKvObjectIdentity& identity) {
    return static_cast<uint32_t>(
        sizeof(NvmeKvStoredIdentityMetadata) +
        SerializeNvmeKvCanonicalIdentity(identity).size());
}

NvmeKvStoredIdentityMetadata BuildNvmeKvStoredIdentityMetadata(
    const NvmeKvPhysicalKey& physical_key, uint32_t slot) {
    return NvmeKvStoredIdentityMetadata{
        .resolved_physical_key = physical_key,
        .resolved_slot = slot,
    };
}

std::string SerializeNvmeKvStoredIdentity(
    const NvmeKvObjectIdentity& identity,
    const NvmeKvStoredIdentityMetadata& metadata) {
    const std::string canonical_identity =
        SerializeNvmeKvCanonicalIdentity(identity);
    std::string encoded(reinterpret_cast<const char*>(&metadata),
                        sizeof(metadata));
    encoded.append(canonical_identity);
    return encoded;
}

bool ParseNvmeKvStoredIdentity(std::string_view encoded_identity,
                               NvmeKvStoredIdentityView& identity_view) {
    identity_view = {};
    if (encoded_identity.size() < sizeof(NvmeKvStoredIdentityMetadata)) {
        return false;
    }

    NvmeKvStoredIdentityMetadata metadata{};
    std::memcpy(&metadata, encoded_identity.data(), sizeof(metadata));
    NvmeKvObjectIdentity identity{};
    if (!ParseNvmeKvCanonicalIdentity(
            encoded_identity.substr(sizeof(NvmeKvStoredIdentityMetadata)),
            identity)) {
        return false;
    }

    identity_view.logical_key = std::move(identity.logical_key);
    identity_view.resolved_physical_key = metadata.resolved_physical_key;
    identity_view.resolved_slot = metadata.resolved_slot;
    return true;
}

bool ValidateNvmeKvHeader(const NvmeKvObjectHeader& header,
                          const NvmeKvObjectIdentity& identity,
                          uint32_t expected_identity_size,
                          uint32_t expected_size,
                          NvmeKvObjectType expected_type) {
    if (header.magic != NvmeKvObjectHeader::kMagic) {
        return false;
    }
    if (header.object_type != static_cast<uint32_t>(expected_type)) {
        return false;
    }
    if (header.payload_size != expected_size) {
        return false;
    }
    if (header.identity_metadata_size != expected_identity_size) {
        return false;
    }
    if (header.header_checksum != ComputeNvmeKvHeaderChecksum(header)) {
        return false;
    }
    if (header.verify_hash != ComputeNvmeKvVerifyHash(identity)) {
        return false;
    }
    return true;
}

std::string BuildNvmeKvObjectBlob(const NvmeKvObjectHeader& header,
                                  std::string_view identity_metadata,
                                  std::string_view payload) {
    std::string object_blob(reinterpret_cast<const char*>(&header),
                            sizeof(header));
    object_blob.append(identity_metadata);
    object_blob.append(payload);
    return object_blob;
}

bool ParseNvmeKvObjectBlob(std::string_view object_blob,
                           NvmeKvObjectHeader& header,
                           std::string_view& identity_metadata_view,
                           std::string_view& payload_view) {
    if (object_blob.size() < sizeof(NvmeKvObjectHeader)) {
        return false;
    }

    std::memcpy(&header, object_blob.data(), sizeof(header));
    if (header.magic != NvmeKvObjectHeader::kMagic) {
        return false;
    }
    const size_t total_header_bytes =
        sizeof(NvmeKvObjectHeader) + header.identity_metadata_size;
    const size_t expected_size = total_header_bytes + header.payload_size;
    if (object_blob.size() != expected_size) {
        return false;
    }
    identity_metadata_view = object_blob.substr(sizeof(NvmeKvObjectHeader),
                                                header.identity_metadata_size);
    payload_view = object_blob.substr(total_header_bytes, header.payload_size);
    return true;
}

uint32_t ResolveNvmeKvObjectBlobSizeFromPrefix(const char* buffer,
                                               uint32_t prefix_size) {
    return ResolveNvmeKvObjectBlobSizeFromHeader(buffer, prefix_size, false);
}

uint32_t ResolveNvmeKvObjectBlobSize(const char* buffer, uint32_t returned_size,
                                     uint32_t max_size) {
    const uint32_t header_size =
        ResolveNvmeKvObjectBlobSizeFromHeader(buffer, max_size, true);
    if (header_size != 0) {
        return header_size;
    }
    if (returned_size > max_size) {
        return 0;
    }
    return returned_size;
}

std::string SerializeNvmeKvManifest(
    const NvmeKvManifestMetadata& metadata,
    const std::vector<NvmeKvManifestChunkRecord>& chunk_records) {
    std::string manifest(reinterpret_cast<const char*>(&metadata),
                         sizeof(metadata));
    for (const auto& record : chunk_records) {
        manifest.append(reinterpret_cast<const char*>(&record), sizeof(record));
    }
    return manifest;
}

bool ParseNvmeKvManifest(
    std::string_view manifest_payload, NvmeKvManifestMetadata& metadata,
    std::vector<NvmeKvManifestChunkRecord>& chunk_records) {
    if (manifest_payload.size() < sizeof(NvmeKvManifestMetadata)) {
        return false;
    }
    std::memcpy(&metadata, manifest_payload.data(), sizeof(metadata));
    const size_t records_bytes = manifest_payload.size() - sizeof(metadata);
    if (records_bytes !=
        metadata.chunk_count * sizeof(NvmeKvManifestChunkRecord)) {
        return false;
    }
    chunk_records.resize(metadata.chunk_count);
    if (!chunk_records.empty()) {
        std::memcpy(chunk_records.data(),
                    manifest_payload.data() + sizeof(metadata), records_bytes);
    }
    return true;
}

}  // namespace mooncake
