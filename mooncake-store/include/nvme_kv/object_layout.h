#pragma once

#include <array>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "nvme_kv/key_codec.h"
#include "types.h"

namespace mooncake {

enum class NvmeKvObjectType : uint32_t {
    kInline = 1,
    kManifest = 2,
};

struct NvmeKvObjectHeader {
    uint32_t magic;
    uint32_t object_type;
    uint32_t payload_size;
    std::array<uint8_t, 32> verify_hash;
    uint32_t payload_checksum;
    uint32_t header_checksum;
    uint32_t identity_metadata_size;

    static constexpr uint32_t kMagic = 0x4e564b56;
};

struct NvmeKvStoredIdentityMetadata {
    NvmeKvPhysicalKey resolved_physical_key{};
    uint32_t resolved_slot = 0;
};

struct NvmeKvStoredIdentityView {
    std::string logical_key;
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

struct NvmeKvWritePlan {
    NvmeKvObjectIdentity identity;
    NvmeKvPhysicalKey root_key{};
    bool store_inline = false;
    std::string root_blob;
    std::vector<std::pair<NvmeKvPhysicalKey, std::string_view>> chunk_values;
    std::vector<NvmeKvManifestChunkRecord> manifest_records;
};

tl::expected<NvmeKvWritePlan, ErrorCode> BuildNvmeKvWritePlan(
    const NvmeKvObjectIdentity& identity, std::string_view payload,
    uint32_t slot, uint32_t max_value_size);

uint32_t ComputeNvmeKvPayloadChecksum(std::string_view payload);
uint32_t ComputeNvmeKvHeaderChecksum(const NvmeKvObjectHeader& header);
uint32_t ComputeNvmeKvStoredIdentityMetadataSize(
    const NvmeKvObjectIdentity& identity);
NvmeKvStoredIdentityMetadata BuildNvmeKvStoredIdentityMetadata(
    const NvmeKvPhysicalKey& physical_key, uint32_t slot);
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
