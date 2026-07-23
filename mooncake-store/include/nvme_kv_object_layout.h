#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "nvme_kv_key_codec.h"

namespace mooncake {

enum class NvmeKvObjectType : uint32_t {
    kInline = 1,
    kManifest = 2,
    kChunk = 3,
};

struct NvmeKvObjectHeader {
    uint32_t magic;
    uint32_t object_type;
    uint32_t payload_size;
    std::array<uint8_t, 32> verify_hash;
    uint32_t payload_checksum;
    uint32_t header_checksum;
    uint32_t flags;
    uint32_t identity_metadata_size;
    uint32_t reserved0;
    uint32_t reserved1;

    static constexpr uint32_t kMagic = 0x4e564b56;
    static constexpr uint32_t kFlagHasIsolationContext = 1u << 0;
};

struct NvmeKvStoredIdentityMetadata {
    NvmeKvPhysicalKey resolved_physical_key{};
    uint32_t resolved_slot = 0;
    uint32_t canonical_identity_size = 0;
};

struct NvmeKvStoredIdentityView {
    std::string logical_key;
    std::string tenant_id;
    std::string domain_id;
    std::string namespace_id;
    NvmeKvPhysicalKey resolved_physical_key{};
    uint32_t resolved_slot = 0;
};

struct NvmeKvManifestMetadata {
    uint32_t logical_payload_size;
    uint32_t chunk_count;
};

struct NvmeKvManifestChunkRecord {
    NvmeKvPhysicalKey physical_key;
    uint32_t payload_size;
    uint32_t payload_checksum;
};

uint32_t ComputeNvmeKvPayloadChecksum(std::string_view payload);
uint32_t ComputeNvmeKvHeaderChecksum(const NvmeKvObjectHeader& header);
uint32_t ComputeNvmeKvStoredIdentityMetadataSize(
    const NvmeKvObjectIdentity& identity);
NvmeKvStoredIdentityMetadata BuildNvmeKvStoredIdentityMetadata(
    const NvmeKvObjectIdentity& identity, const NvmeKvPhysicalKey& physical_key,
    uint32_t slot);
std::string SerializeNvmeKvStoredIdentity(
    const NvmeKvObjectIdentity& identity,
    const NvmeKvStoredIdentityMetadata& metadata);
bool ParseNvmeKvStoredIdentity(std::string_view encoded_identity,
                               NvmeKvStoredIdentityView& identity_view);
bool ValidateNvmeKvHeader(const NvmeKvObjectHeader& header,
                          const NvmeKvObjectIdentity& identity,
                          uint32_t expected_identity_size,
                          uint32_t expected_size,
                          NvmeKvObjectType expected_type);
bool ValidateNvmeKvHeader(const NvmeKvObjectHeader& header,
                          const std::string& key, uint32_t expected_size,
                          NvmeKvObjectType expected_type);
std::string BuildNvmeKvObjectBlob(const NvmeKvObjectHeader& header,
                                  std::string_view identity_metadata,
                                  std::string_view payload);
bool ParseNvmeKvObjectBlob(std::string_view object_blob,
                           NvmeKvObjectHeader& header,
                           std::string_view& identity_metadata_view,
                           std::string_view& payload_view);
uint32_t ResolveNvmeKvObjectBlobSizeFromPrefix(const char* buffer,
                                               uint32_t prefix_size);
uint32_t ResolveNvmeKvObjectBlobSize(const char* buffer, uint32_t returned_size,
                                     uint32_t max_size);
std::string SerializeNvmeKvManifest(
    const NvmeKvManifestMetadata& metadata,
    const std::vector<NvmeKvManifestChunkRecord>& chunk_records);
bool ParseNvmeKvManifest(std::string_view manifest_payload,
                         NvmeKvManifestMetadata& metadata,
                         std::vector<NvmeKvManifestChunkRecord>& chunk_records);

}  // namespace mooncake
