#pragma once

#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "tenant_id.h"
#include "ylt/struct_pack.hpp"

namespace mooncake {

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

enum class WeightOperationState : uint8_t {
    NONE = 0,
    EVICTING = 1,
    REHYDRATING = 2,
    REPAIRING = 3,
};

inline constexpr bool IsValidWeightAvailabilityState(
    WeightAvailabilityState state) {
    return state == WeightAvailabilityState::IMPORTING ||
           state == WeightAvailabilityState::READY ||
           state == WeightAvailabilityState::DEGRADED ||
           state == WeightAvailabilityState::DELETING ||
           state == WeightAvailabilityState::DELETED;
}

inline constexpr bool IsValidWeightResidencyState(WeightResidencyState state) {
    return state == WeightResidencyState::UNKNOWN ||
           state == WeightResidencyState::HOT ||
           state == WeightResidencyState::COLD ||
           state == WeightResidencyState::MIXED ||
           state == WeightResidencyState::ABSENT;
}

inline constexpr bool IsValidWeightOperationState(WeightOperationState state) {
    return state == WeightOperationState::NONE ||
           state == WeightOperationState::EVICTING ||
           state == WeightOperationState::REHYDRATING ||
           state == WeightOperationState::REPAIRING;
}

enum class WeightManagementError : uint8_t {
    INVALID_ARGUMENT = 1,
    NOT_FOUND = 2,
    CONFLICT = 3,
    STALE_GENERATION = 4,
    NOT_READY = 5,
    BUSY = 6,
    LEASE_EXPIRED = 7,
    GENERATION_EXHAUSTED = 8,
    DURABILITY_FAILED = 9,
};

struct WeightRevisionIdentity {
    std::string tenant_id{"default"};
    std::string name_space;
    std::string resource_id;
    std::string revision;
    uint64_t weight_generation{0};

    friend bool operator==(const WeightRevisionIdentity&,
                           const WeightRevisionIdentity&) = default;
    friend bool operator<(const WeightRevisionIdentity& lhs,
                          const WeightRevisionIdentity& rhs) {
        return std::tie(lhs.tenant_id, lhs.name_space, lhs.resource_id,
                        lhs.revision, lhs.weight_generation) <
               std::tie(rhs.tenant_id, rhs.name_space, rhs.resource_id,
                        rhs.revision, rhs.weight_generation);
    }
};
YLT_REFL(WeightRevisionIdentity, tenant_id, name_space, resource_id, revision,
         weight_generation);

struct WeightManifestReference {
    std::string manifest_key;
    std::string manifest_sha256;
    std::string payload_group_id;
    std::string payload_keys_sha256;
    uint64_t payload_count{0};
    uint64_t logical_bytes{0};

    friend bool operator==(const WeightManifestReference&,
                           const WeightManifestReference&) = default;
};
YLT_REFL(WeightManifestReference, manifest_key, manifest_sha256,
         payload_group_id, payload_keys_sha256, payload_count, logical_bytes);

struct WeightRevisionMetadata {
    WeightRevisionIdentity identity;
    WeightManifestReference manifest;
    WeightAvailabilityState availability{WeightAvailabilityState::IMPORTING};
    WeightResidencyState residency{WeightResidencyState::UNKNOWN};
    WeightOperationState operation{WeightOperationState::NONE};
    uint64_t operation_id{0};
    uint64_t metadata_generation{1};
    uint64_t created_at_ms{0};
    uint64_t updated_at_ms{0};

    friend bool operator==(const WeightRevisionMetadata&,
                           const WeightRevisionMetadata&) = default;
};
YLT_REFL(WeightRevisionMetadata, identity, manifest, availability, residency,
         operation, operation_id, metadata_generation, created_at_ms,
         updated_at_ms);

struct WeightRevisionLease {
    uint64_t lease_id{0};
    WeightRevisionIdentity identity;
    std::string holder;
    uint64_t expires_at_ms{0};
    uint64_t fenced_metadata_generation{0};

    friend bool operator==(const WeightRevisionLease&,
                           const WeightRevisionLease&) = default;
};
YLT_REFL(WeightRevisionLease, lease_id, identity, holder, expires_at_ms,
         fenced_metadata_generation);

struct WeightResidencyOperation {
    uint64_t operation_id{0};
    WeightRevisionIdentity identity;
    WeightOperationState operation{WeightOperationState::NONE};
    WeightResidencyState target_residency{WeightResidencyState::UNKNOWN};
    uint64_t fenced_metadata_generation{0};
    uint64_t started_at_ms{0};
    uint64_t updated_at_ms{0};
    uint64_t processed_members{0};
    uint64_t total_members{0};
    std::string cursor;
    std::string message;

    friend bool operator==(const WeightResidencyOperation&,
                           const WeightResidencyOperation&) = default;
};
YLT_REFL(WeightResidencyOperation, operation_id, identity, operation,
         target_residency, fenced_metadata_generation, started_at_ms,
         updated_at_ms, processed_members, total_members, cursor, message);

struct WeightRevisionView {
    WeightRevisionMetadata metadata;
    uint64_t active_lease_count{0};
    std::optional<uint64_t> nearest_lease_expiry_ms;
};
YLT_REFL(WeightRevisionView, metadata, active_lease_count,
         nearest_lease_expiry_ms);

struct BeginWeightImportRequest {
    WeightRevisionIdentity identity;
    std::string payload_group_id;
    uint64_t expected_payload_count{0};
    uint64_t expected_logical_bytes{0};
};
YLT_REFL(BeginWeightImportRequest, identity, payload_group_id,
         expected_payload_count, expected_logical_bytes);

struct CommitWeightImportRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation{0};
    WeightManifestReference manifest;
};
YLT_REFL(CommitWeightImportRequest, identity, expected_metadata_generation,
         manifest);

struct AbortWeightImportRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation{0};
};
YLT_REFL(AbortWeightImportRequest, identity, expected_metadata_generation);

struct GetWeightRevisionRequest {
    WeightRevisionIdentity identity;
};
YLT_REFL(GetWeightRevisionRequest, identity);

struct ListWeightRevisionsRequest {
    std::string tenant_id{"default"};
    std::string name_space;
    std::string resource_id;
    std::string page_token;
    uint32_t limit{100};
};
YLT_REFL(ListWeightRevisionsRequest, tenant_id, name_space, resource_id,
         page_token, limit);

struct ListWeightRevisionsResponse {
    std::vector<WeightRevisionView> revisions;
    std::string next_page_token;
};
YLT_REFL(ListWeightRevisionsResponse, revisions, next_page_token);

struct AcquireWeightRevisionLeaseRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation{0};
    std::string holder;
    uint64_t ttl_ms{0};
};
YLT_REFL(AcquireWeightRevisionLeaseRequest, identity,
         expected_metadata_generation, holder, ttl_ms);

struct RenewWeightRevisionLeaseRequest {
    std::string tenant_id{"default"};
    uint64_t lease_id{0};
    uint64_t ttl_ms{0};
};
YLT_REFL(RenewWeightRevisionLeaseRequest, tenant_id, lease_id, ttl_ms);

struct ReleaseWeightRevisionLeaseRequest {
    std::string tenant_id{"default"};
    uint64_t lease_id{0};
};
YLT_REFL(ReleaseWeightRevisionLeaseRequest, tenant_id, lease_id);

struct StartWeightResidencyOperationRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation{0};
    WeightResidencyState target_residency{WeightResidencyState::UNKNOWN};
};
YLT_REFL(StartWeightResidencyOperationRequest, identity,
         expected_metadata_generation, target_residency);

struct QueryWeightOperationRequest {
    std::string tenant_id{"default"};
    uint64_t operation_id{0};
};
YLT_REFL(QueryWeightOperationRequest, tenant_id, operation_id);

struct ReconcileWeightRevisionRequest {
    WeightRevisionIdentity identity;
};
YLT_REFL(ReconcileWeightRevisionRequest, identity);

struct DeleteWeightRevisionRequest {
    WeightRevisionIdentity identity;
    uint64_t expected_metadata_generation{0};
};
YLT_REFL(DeleteWeightRevisionRequest, identity, expected_metadata_generation);

std::string ComputeWeightPayloadKeysSha256(
    const std::vector<std::string>& payload_keys);
std::string MakeWeightPayloadGroupId(const WeightRevisionIdentity& identity);
std::string MakeWeightRevisionMetadataKey(
    const WeightRevisionIdentity& identity);
std::string MakeWeightLeaseMetadataKey(uint64_t lease_id);

class WeightValidationResult {
   public:
    static WeightValidationResult Success() { return WeightValidationResult(); }

    static WeightValidationResult Failure(std::string message) {
        return WeightValidationResult(std::move(message));
    }

    bool ok() const noexcept { return message_.empty(); }
    const std::string& message() const noexcept { return message_; }

   private:
    WeightValidationResult() = default;
    explicit WeightValidationResult(std::string message)
        : message_(std::move(message)) {}

    std::string message_;
};

inline bool IsValidWeightComponent(std::string_view value) {
    if (value.empty() || value.size() > 1024) {
        return false;
    }
    for (const unsigned char c : value) {
        if (c < 0x20 || c == 0x7f) {
            return false;
        }
    }
    return true;
}

inline std::string EncodeWeightPathSegment(std::string_view value) {
    constexpr char kHex[] = "0123456789ABCDEF";
    std::string encoded;
    encoded.reserve(value.size());
    for (const unsigned char c : value) {
        const bool safe = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                          (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                          c == '.' || c == '~';
        if (safe) {
            encoded.push_back(static_cast<char>(c));
        } else {
            encoded.push_back('%');
            encoded.push_back(kHex[c >> 4]);
            encoded.push_back(kHex[c & 0x0f]);
        }
    }
    return encoded;
}

inline std::string MakeWeightManifestKey(
    const WeightRevisionIdentity& identity) {
    return "weights/" + EncodeWeightPathSegment(identity.name_space) + "/" +
           EncodeWeightPathSegment(identity.resource_id) + "/" +
           EncodeWeightPathSegment(identity.revision) + "/" +
           std::to_string(identity.weight_generation) + "/manifest";
}

inline bool IsValidSha256(std::string_view digest) {
    if (digest.size() != 64) {
        return false;
    }
    for (const char c : digest) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return false;
        }
    }
    return true;
}

inline WeightValidationResult ValidateWeightRevisionIdentity(
    const WeightRevisionIdentity& identity) {
    if (identity.tenant_id.empty() || !TenantId(identity.tenant_id).IsValid()) {
        return WeightValidationResult::Failure("invalid tenant_id");
    }
    if (!IsValidWeightComponent(identity.name_space)) {
        return WeightValidationResult::Failure("invalid namespace");
    }
    if (!IsValidWeightComponent(identity.resource_id)) {
        return WeightValidationResult::Failure("invalid resource_id");
    }
    if (!IsValidWeightComponent(identity.revision)) {
        return WeightValidationResult::Failure("invalid revision");
    }
    if (identity.weight_generation == 0) {
        return WeightValidationResult::Failure("invalid weight_generation");
    }
    return WeightValidationResult::Success();
}

inline WeightValidationResult ValidateWeightManifestReference(
    const WeightManifestReference& manifest) {
    if (!IsValidWeightComponent(manifest.manifest_key)) {
        return WeightValidationResult::Failure("invalid manifest_key");
    }
    if (!IsValidSha256(manifest.manifest_sha256)) {
        return WeightValidationResult::Failure("invalid manifest_sha256");
    }
    if (!IsValidWeightComponent(manifest.payload_group_id)) {
        return WeightValidationResult::Failure("invalid payload_group_id");
    }
    if (!IsValidSha256(manifest.payload_keys_sha256)) {
        return WeightValidationResult::Failure("invalid payload_keys_sha256");
    }
    if (manifest.payload_count == 0) {
        return WeightValidationResult::Failure("payload_count must be nonzero");
    }
    if (manifest.logical_bytes == 0) {
        return WeightValidationResult::Failure("logical_bytes must be nonzero");
    }
    return WeightValidationResult::Success();
}

inline bool IsValidWeightAvailabilityTransition(WeightAvailabilityState from,
                                                WeightAvailabilityState to) {
    switch (from) {
        case WeightAvailabilityState::IMPORTING:
            return to == WeightAvailabilityState::READY ||
                   to == WeightAvailabilityState::DELETING;
        case WeightAvailabilityState::READY:
            return to == WeightAvailabilityState::DEGRADED ||
                   to == WeightAvailabilityState::DELETING;
        case WeightAvailabilityState::DEGRADED:
            return to == WeightAvailabilityState::READY ||
                   to == WeightAvailabilityState::DELETING;
        case WeightAvailabilityState::DELETING:
            return to == WeightAvailabilityState::DELETED;
        case WeightAvailabilityState::DELETED:
            return false;
    }
    return false;
}

inline WeightValidationResult ValidateWeightRevisionMetadata(
    const WeightRevisionMetadata& metadata) {
    if (!IsValidWeightAvailabilityState(metadata.availability) ||
        !IsValidWeightResidencyState(metadata.residency) ||
        !IsValidWeightOperationState(metadata.operation)) {
        return WeightValidationResult::Failure("invalid weight state");
    }
    auto identity_result = ValidateWeightRevisionIdentity(metadata.identity);
    if (!identity_result.ok()) {
        return identity_result;
    }
    if (metadata.metadata_generation == 0 ||
        metadata.metadata_generation == std::numeric_limits<uint64_t>::max()) {
        return WeightValidationResult::Failure("invalid metadata_generation");
    }
    if (metadata.updated_at_ms < metadata.created_at_ms) {
        return WeightValidationResult::Failure("timestamps are not monotonic");
    }
    if ((metadata.operation == WeightOperationState::NONE) !=
        (metadata.operation_id == 0)) {
        return WeightValidationResult::Failure(
            "operation and operation_id disagree");
    }
    if (metadata.operation != WeightOperationState::NONE &&
        metadata.availability != WeightAvailabilityState::READY &&
        metadata.availability != WeightAvailabilityState::DEGRADED) {
        return WeightValidationResult::Failure(
            "active operation requires a published revision");
    }
    if (metadata.availability == WeightAvailabilityState::IMPORTING &&
        metadata.residency != WeightResidencyState::UNKNOWN) {
        return WeightValidationResult::Failure(
            "importing revision must have unknown residency");
    }
    if (metadata.availability == WeightAvailabilityState::READY ||
        metadata.availability == WeightAvailabilityState::DEGRADED) {
        auto manifest_result =
            ValidateWeightManifestReference(metadata.manifest);
        if (!manifest_result.ok()) {
            return manifest_result;
        }
    }
    if ((metadata.availability == WeightAvailabilityState::READY ||
         metadata.availability == WeightAvailabilityState::DEGRADED) &&
        metadata.residency == WeightResidencyState::UNKNOWN) {
        return WeightValidationResult::Failure(
            "published revision must have observed residency");
    }
    if (metadata.availability == WeightAvailabilityState::READY &&
        metadata.residency == WeightResidencyState::ABSENT) {
        return WeightValidationResult::Failure(
            "ready revision must have readable residency");
    }
    if (metadata.availability == WeightAvailabilityState::DELETED &&
        (metadata.residency != WeightResidencyState::ABSENT ||
         metadata.operation != WeightOperationState::NONE)) {
        return WeightValidationResult::Failure(
            "deleted revision must be absent and idle");
    }
    return WeightValidationResult::Success();
}

inline bool CanAdvanceWeightMetadataGeneration(uint64_t generation) {
    return generation > 0 &&
           generation < std::numeric_limits<uint64_t>::max() - 1;
}

}  // namespace mooncake
