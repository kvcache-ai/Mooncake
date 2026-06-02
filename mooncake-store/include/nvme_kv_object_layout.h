#pragma once

#include <array>
#include <cstdint>
#include <span>
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
    uint32_t version;
    uint32_t object_type;
    uint32_t payload_size;
    std::array<uint8_t, 32> verify_hash;
    uint32_t payload_checksum;
    uint32_t header_checksum;

    static constexpr uint32_t kMagic = 0x4e564b56;  // NVKV
    static constexpr uint32_t kVersion = 2;
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
bool ValidateNvmeKvHeader(const NvmeKvObjectHeader& header,
                          const std::string& key, uint32_t expected_size,
                          NvmeKvObjectType expected_type);
std::string BuildNvmeKvObjectValue(const NvmeKvObjectHeader& header,
                                   std::string_view payload);
bool ParseNvmeKvObjectValue(std::string_view object_value,
                            NvmeKvObjectHeader& header,
                            std::string_view& payload_view);
uint32_t ResolveNvmeKvObjectValueSize(const char* buffer,
                                      uint32_t returned_size,
                                      uint32_t max_size);
std::string SerializeNvmeKvManifest(
    const NvmeKvManifestMetadata& metadata,
    const std::vector<NvmeKvManifestChunkRecord>& chunk_records);
bool ParseNvmeKvManifest(std::string_view manifest_payload,
                         NvmeKvManifestMetadata& metadata,
                         std::vector<NvmeKvManifestChunkRecord>& chunk_records);

}  // namespace mooncake
