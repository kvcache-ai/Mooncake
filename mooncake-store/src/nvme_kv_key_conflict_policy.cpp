#include "nvme_kv_key_conflict_policy.h"

#include <algorithm>
#include <limits>

namespace mooncake {

NvmeKvObjectIdentity NvmeKvKeyConflictPolicy::BuildIdentity(
    const std::string& key) {
    return NvmeKvObjectIdentity{.logical_key = key, .isolation = {}};
}

NvmeKvObjectIdentity NvmeKvKeyConflictPolicy::BuildIdentityFromStoredView(
    const NvmeKvStoredIdentityView& stored_identity_view) {
    return NvmeKvObjectIdentity{
        .logical_key = stored_identity_view.logical_key,
        .isolation = {.tenant_id = stored_identity_view.tenant_id,
                      .domain_id = stored_identity_view.domain_id,
                      .namespace_id = stored_identity_view.namespace_id}};
}

bool NvmeKvKeyConflictPolicy::ValidateResolvedRootPlacement(
    const NvmeKvObjectIdentity& identity,
    const NvmeKvStoredIdentityView& stored_view,
    const NvmeKvPhysicalKey& observed_physical_key) {
    return stored_view.resolved_physical_key == observed_physical_key &&
           EncodeNvmeKvPhysicalKey(identity, stored_view.resolved_slot) ==
               stored_view.resolved_physical_key;
}

uint32_t NvmeKvKeyConflictPolicy::PayloadLimitForIdentityMetadata(
    uint32_t max_value_size, uint32_t identity_metadata_size) {
    const uint64_t fixed_overhead =
        static_cast<uint64_t>(sizeof(NvmeKvObjectHeader)) +
        identity_metadata_size;
    if (max_value_size <= fixed_overhead) {
        return 0;
    }
    return static_cast<uint32_t>(max_value_size - fixed_overhead);
}

tl::expected<NvmeKvKeyConflictPolicy::WritePlan, ErrorCode>
NvmeKvKeyConflictPolicy::BuildWritePlan(const NvmeKvObjectIdentity& identity,
                                        std::string_view payload, uint32_t slot,
                                        uint32_t max_value_size) {
    if (payload.size() >
        static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    WritePlan plan;
    plan.identity = identity;
    plan.slot = slot;
    plan.root_key = EncodeNvmeKvPhysicalKey(identity, slot);

    const auto root_identity_metadata = SerializeNvmeKvStoredIdentity(
        identity,
        BuildNvmeKvStoredIdentityMetadata(identity, plan.root_key, slot));
    const uint32_t inline_payload_limit = PayloadLimitForIdentityMetadata(
        max_value_size, static_cast<uint32_t>(root_identity_metadata.size()));
    plan.store_inline = payload.size() <= inline_payload_limit;

    const auto verify_hash = ComputeNvmeKvVerifyHash(identity);
    if (plan.store_inline) {
        NvmeKvObjectHeader header{
            .magic = NvmeKvObjectHeader::kMagic,
            .object_type = static_cast<uint32_t>(NvmeKvObjectType::kInline),
            .payload_size = static_cast<uint32_t>(payload.size()),
            .verify_hash = verify_hash,
            .payload_checksum = ComputeNvmeKvPayloadChecksum(payload),
            .header_checksum = 0,
            .flags = 0,
            .identity_metadata_size =
                static_cast<uint32_t>(root_identity_metadata.size()),
            .reserved0 = 0,
            .reserved1 = 0,
        };
        header.header_checksum = ComputeNvmeKvHeaderChecksum(header);
        plan.root_blob =
            BuildNvmeKvObjectBlob(header, root_identity_metadata, payload);
        return plan;
    }

    const uint32_t chunk_payload_limit = PayloadLimitForIdentityMetadata(
        max_value_size, static_cast<uint32_t>(root_identity_metadata.size()));
    if (chunk_payload_limit == 0) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    const size_t chunk_count =
        (payload.size() + chunk_payload_limit - 1) / chunk_payload_limit;
    if (chunk_count == 0 ||
        chunk_count >
            static_cast<size_t>(std::numeric_limits<uint32_t>::max())) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::vector<NvmeKvManifestChunkRecord> manifest_records;
    manifest_records.reserve(chunk_count);
    plan.chunk_blobs.reserve(chunk_count);
    for (size_t chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
        const size_t offset = chunk_index * chunk_payload_limit;
        const size_t chunk_size = std::min(
            static_cast<size_t>(chunk_payload_limit), payload.size() - offset);
        std::string_view chunk_payload(payload.data() + offset, chunk_size);
        const auto chunk_key = EncodeNvmeKvChunkPhysicalKey(
            identity, static_cast<uint32_t>(chunk_index), slot);
        const auto chunk_identity_metadata = SerializeNvmeKvStoredIdentity(
            identity,
            BuildNvmeKvStoredIdentityMetadata(identity, chunk_key, slot));
        NvmeKvObjectHeader chunk_header{
            .magic = NvmeKvObjectHeader::kMagic,
            .object_type = static_cast<uint32_t>(NvmeKvObjectType::kChunk),
            .payload_size = static_cast<uint32_t>(chunk_size),
            .verify_hash = ComputeNvmeKvVerifyHash(identity),
            .payload_checksum = ComputeNvmeKvPayloadChecksum(chunk_payload),
            .header_checksum = 0,
            .flags = 0,
            .identity_metadata_size =
                static_cast<uint32_t>(chunk_identity_metadata.size()),
            .reserved0 = 0,
            .reserved1 = 0,
        };
        chunk_header.header_checksum =
            ComputeNvmeKvHeaderChecksum(chunk_header);
        plan.chunk_blobs.emplace_back(
            chunk_key,
            BuildNvmeKvObjectBlob(chunk_header, chunk_identity_metadata,
                                  chunk_payload));
        manifest_records.push_back(NvmeKvManifestChunkRecord{
            chunk_key, static_cast<uint32_t>(chunk_size),
            ComputeNvmeKvPayloadChecksum(chunk_payload)});
    }

    const NvmeKvManifestMetadata metadata{
        .logical_payload_size = static_cast<uint32_t>(payload.size()),
        .chunk_count = static_cast<uint32_t>(manifest_records.size()),
    };
    const std::string manifest_payload =
        SerializeNvmeKvManifest(metadata, manifest_records);
    if (manifest_payload.size() + sizeof(NvmeKvObjectHeader) +
            root_identity_metadata.size() >
        max_value_size) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    NvmeKvObjectHeader header{
        .magic = NvmeKvObjectHeader::kMagic,
        .object_type = static_cast<uint32_t>(NvmeKvObjectType::kManifest),
        .payload_size = static_cast<uint32_t>(manifest_payload.size()),
        .verify_hash = verify_hash,
        .payload_checksum = ComputeNvmeKvPayloadChecksum(manifest_payload),
        .header_checksum = 0,
        .flags = 0,
        .identity_metadata_size =
            static_cast<uint32_t>(root_identity_metadata.size()),
        .reserved0 = 0,
        .reserved1 = 0,
    };
    header.header_checksum = ComputeNvmeKvHeaderChecksum(header);
    plan.root_blob =
        BuildNvmeKvObjectBlob(header, root_identity_metadata, manifest_payload);
    return plan;
}

tl::expected<NvmeKvKeyConflictPolicy::ExistingObjectDecision, ErrorCode>
NvmeKvKeyConflictPolicy::ResolveExistingObject(
    NvmeKvConnector& connector, const PhysicalKey& physical_key,
    const std::string& expected_blob) {
    auto existing_value_res = connector.Retrieve(physical_key);
    if (!existing_value_res) {
        if (existing_value_res.error() == ErrorCode::OBJECT_NOT_FOUND) {
            return ExistingObjectDecision::kNotFound;
        }
        return tl::make_unexpected(existing_value_res.error());
    }
    if (existing_value_res.value() == expected_blob) {
        return ExistingObjectDecision::kSameObject;
    }
    return ExistingObjectDecision::kDifferentObject;
}

}  // namespace mooncake
