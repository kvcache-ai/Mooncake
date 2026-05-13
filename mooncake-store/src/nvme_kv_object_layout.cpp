#include "nvme_kv_object_layout.h"

#include <cstring>

namespace mooncake {

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

bool ValidateNvmeKvHeader(const NvmeKvObjectHeader& header,
                          const std::string& key, uint32_t expected_size,
                          NvmeKvObjectType expected_type) {
    if (header.magic != NvmeKvObjectHeader::kMagic ||
        header.version != NvmeKvObjectHeader::kVersion) {
        return false;
    }
    if (header.object_type != static_cast<uint32_t>(expected_type)) {
        return false;
    }
    if (header.payload_size != expected_size) {
        return false;
    }
    if (header.header_checksum != ComputeNvmeKvHeaderChecksum(header)) {
        return false;
    }
    if (header.verify_hash != ComputeNvmeKvVerifyHash(key)) {
        return false;
    }
    return true;
}

std::string BuildNvmeKvObjectValue(const NvmeKvObjectHeader& header,
                                   std::string_view payload) {
    std::string object_value;
    object_value.resize(sizeof(header) + payload.size());
    std::memcpy(object_value.data(), &header, sizeof(header));
    std::memcpy(object_value.data() + sizeof(header), payload.data(),
                payload.size());
    return object_value;
}

bool ParseNvmeKvObjectValue(std::string_view object_value,
                            NvmeKvObjectHeader& header,
                            std::string_view& payload_view) {
    if (object_value.size() < sizeof(NvmeKvObjectHeader)) {
        return false;
    }
    std::memcpy(&header, object_value.data(), sizeof(header));
    payload_view =
        std::string_view(object_value.data() + sizeof(NvmeKvObjectHeader),
                         object_value.size() - sizeof(NvmeKvObjectHeader));
    return true;
}

uint32_t ResolveNvmeKvObjectValueSize(const char* buffer,
                                      uint32_t returned_size,
                                      uint32_t max_size) {
    if (returned_size != 0) {
        return returned_size;
    }
    if (max_size < sizeof(NvmeKvObjectHeader)) {
        return 0;
    }

    NvmeKvObjectHeader header{};
    std::memcpy(&header, buffer, sizeof(header));
    if (header.magic != NvmeKvObjectHeader::kMagic ||
        header.version != NvmeKvObjectHeader::kVersion) {
        return 0;
    }

    const uint64_t object_size =
        static_cast<uint64_t>(sizeof(NvmeKvObjectHeader)) + header.payload_size;
    if (object_size > max_size) {
        return 0;
    }
    return static_cast<uint32_t>(object_size);
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
