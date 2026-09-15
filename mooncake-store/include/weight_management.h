#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include <ylt/struct_pack.hpp>

#include "types.h"

namespace mooncake {

// RFC #4017: revision-level weight management contracts (PR1 subset).

enum class WeightAvailabilityState : uint8_t {
    IMPORTING = 0,
    READY = 1,
    DEGRADED = 2,
    DELETING = 3,
    DELETED = 4,
};

enum class WeightResidencyState : uint8_t {
    UNKNOWN = 0,
    HOT = 1,
    COLD = 2,
    MIXED = 3,
    ABSENT = 4,
};

enum class WeightMigrationMode : uint8_t {
    PINNED = 0,
    MANUAL = 1,
    AUTO = 2,
};

struct WeightRevisionIdentity {
    std::string tenant_id = "default";
    std::string ns = "default";
    std::string resource_id;
    std::string revision;
    uint64_t weight_generation = 0;

    [[nodiscard]] bool IsValid() const {
        return !resource_id.empty() && !revision.empty();
    }

    [[nodiscard]] std::string ToKey() const {
        return tenant_id + "/" + ns + "/" + resource_id + "/" + revision + "/" +
               std::to_string(weight_generation);
    }

    bool operator==(const WeightRevisionIdentity& other) const {
        return tenant_id == other.tenant_id && ns == other.ns &&
               resource_id == other.resource_id && revision == other.revision &&
               weight_generation == other.weight_generation;
    }
};
YLT_REFL(WeightRevisionIdentity, tenant_id, ns, resource_id, revision,
         weight_generation);

struct WeightStoragePolicy {
    WeightResidencyState preferred_residency = WeightResidencyState::MIXED;
    double mixed_hot_ratio = 0.5;
    WeightMigrationMode migration_mode = WeightMigrationMode::AUTO;

    [[nodiscard]] bool IsValid() const {
        if (mixed_hot_ratio <= 0.0 || mixed_hot_ratio >= 1.0) {
            return false;
        }
        switch (preferred_residency) {
            case WeightResidencyState::HOT:
            case WeightResidencyState::COLD:
            case WeightResidencyState::MIXED:
                break;
            default:
                return false;
        }
        switch (migration_mode) {
            case WeightMigrationMode::PINNED:
            case WeightMigrationMode::MANUAL:
            case WeightMigrationMode::AUTO:
                break;
            default:
                return false;
        }
        return true;
    }
};
YLT_REFL(WeightStoragePolicy, preferred_residency, mixed_hot_ratio,
         migration_mode);

struct WeightRevisionMetadata {
    WeightRevisionIdentity identity;
    WeightAvailabilityState availability = WeightAvailabilityState::IMPORTING;
    WeightResidencyState observed_residency = WeightResidencyState::UNKNOWN;
    WeightStoragePolicy policy;
    uint64_t metadata_generation = 1;
    std::string manifest_key;
    std::string manifest_sha256;
    std::string payload_group_id;
    std::string payload_keys_digest;
    uint64_t payload_count = 0;
    uint64_t logical_payload_bytes = 0;
    std::vector<std::string> payload_keys;
    std::string operation_id;  // empty = no active operation
};
YLT_REFL(WeightRevisionMetadata, identity, availability, observed_residency,
         policy, metadata_generation, manifest_key, manifest_sha256,
         payload_group_id, payload_keys_digest, payload_count,
         logical_payload_bytes, payload_keys, operation_id);

struct BeginWeightImportRequest {
    WeightRevisionIdentity identity;
    WeightStoragePolicy policy;
    std::string payload_group_id;
};
YLT_REFL(BeginWeightImportRequest, identity, policy, payload_group_id);

struct BeginWeightImportResponse {
    WeightRevisionMetadata metadata;
};
YLT_REFL(BeginWeightImportResponse, metadata);

struct CommitWeightImportRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation = 0;
    std::string manifest_key;
    std::string manifest_sha256;
    std::string payload_keys_digest;
    uint64_t payload_count = 0;
    uint64_t logical_payload_bytes = 0;
    std::vector<std::string> payload_keys;
};
YLT_REFL(CommitWeightImportRequest, identity, expected_metadata_generation,
         manifest_key, manifest_sha256, payload_keys_digest, payload_count,
         logical_payload_bytes, payload_keys);

struct CommitWeightImportResponse {
    WeightRevisionMetadata metadata;
};
YLT_REFL(CommitWeightImportResponse, metadata);

struct GetWeightMetadataRequest {
    WeightRevisionIdentity identity;
};
YLT_REFL(GetWeightMetadataRequest, identity);

struct GetWeightMetadataResponse {
    WeightRevisionMetadata metadata;
};
YLT_REFL(GetWeightMetadataResponse, metadata);

struct ListWeightRevisionsRequest {
    std::string tenant_id = "default";
    std::string ns = "default";
    std::string resource_id;  // empty = all resources in namespace
    uint64_t offset = 0;
    uint64_t limit = 100;
};
YLT_REFL(ListWeightRevisionsRequest, tenant_id, ns, resource_id, offset, limit);

struct ListWeightRevisionsResponse {
    std::vector<WeightRevisionMetadata> revisions;
    uint64_t next_offset = 0;
    bool has_more = false;
};
YLT_REFL(ListWeightRevisionsResponse, revisions, next_offset, has_more);

struct UpdateWeightPolicyRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation = 0;
    WeightStoragePolicy policy;
};
YLT_REFL(UpdateWeightPolicyRequest, identity, expected_metadata_generation,
         policy);

struct UpdateWeightPolicyResponse {
    WeightRevisionMetadata metadata;
};
YLT_REFL(UpdateWeightPolicyResponse, metadata);

// Abort an in-flight IMPORTING revision. Optional keys_to_remove lets the
// caller clean up partially written objects after a failed transfer.
struct AbortWeightImportRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation = 0;
    std::vector<std::string> keys_to_remove;
};
YLT_REFL(AbortWeightImportRequest, identity, expected_metadata_generation,
         keys_to_remove);

struct AbortWeightImportResponse {
    WeightRevisionMetadata metadata;
    std::vector<std::string> removed_keys;
};
YLT_REFL(AbortWeightImportResponse, metadata, removed_keys);

// Explicit whole-revision delete (RFC weight_remove subset for PR1).
struct RemoveWeightRevisionRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation = 0;
};
YLT_REFL(RemoveWeightRevisionRequest, identity, expected_metadata_generation);

struct RemoveWeightRevisionResponse {
    WeightRevisionMetadata metadata;
    std::vector<std::string> removed_keys;
};
YLT_REFL(RemoveWeightRevisionResponse, metadata, removed_keys);

inline const char* ToString(WeightAvailabilityState state) {
    switch (state) {
        case WeightAvailabilityState::IMPORTING:
            return "IMPORTING";
        case WeightAvailabilityState::READY:
            return "READY";
        case WeightAvailabilityState::DEGRADED:
            return "DEGRADED";
        case WeightAvailabilityState::DELETING:
            return "DELETING";
        case WeightAvailabilityState::DELETED:
            return "DELETED";
    }
    return "UNKNOWN";
}

inline const char* ToString(WeightResidencyState state) {
    switch (state) {
        case WeightResidencyState::UNKNOWN:
            return "UNKNOWN";
        case WeightResidencyState::HOT:
            return "HOT";
        case WeightResidencyState::COLD:
            return "COLD";
        case WeightResidencyState::MIXED:
            return "MIXED";
        case WeightResidencyState::ABSENT:
            return "ABSENT";
    }
    return "UNKNOWN";
}

}  // namespace mooncake
