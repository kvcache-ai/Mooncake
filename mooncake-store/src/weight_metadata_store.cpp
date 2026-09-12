#include "weight_metadata_store.h"

#include <algorithm>
#include <unordered_set>

namespace mooncake {

namespace {

std::vector<std::string> UniqueNonEmptyKeys(
    const std::vector<std::string>& keys) {
    std::unordered_set<std::string> seen;
    std::vector<std::string> out;
    out.reserve(keys.size());
    for (const auto& key : keys) {
        if (key.empty() || !seen.insert(key).second) {
            continue;
        }
        out.push_back(key);
    }
    return out;
}

}  // namespace

tl::expected<WeightRevisionMetadata, ErrorCode>
WeightMetadataStore::BeginImport(const BeginWeightImportRequest& request) {
    if (!request.identity.IsValid() || request.payload_group_id.empty() ||
        !request.policy.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = request.identity.ToKey();
    auto it = revisions_.find(key);
    if (it != revisions_.end()) {
        const auto& existing = it->second;
        if (existing.availability == WeightAvailabilityState::READY ||
            existing.availability == WeightAvailabilityState::DEGRADED ||
            existing.availability == WeightAvailabilityState::DELETING) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
        if (existing.availability == WeightAvailabilityState::IMPORTING) {
            // Idempotent retry of the same in-flight import.
            return existing;
        }
        // DELETED tombstone: allow a fresh import with a bumped generation.
    }

    WeightRevisionMetadata metadata;
    metadata.identity = request.identity;
    metadata.availability = WeightAvailabilityState::IMPORTING;
    metadata.observed_residency = WeightResidencyState::UNKNOWN;
    metadata.policy = request.policy;
    metadata.metadata_generation = 1;
    metadata.payload_group_id = request.payload_group_id;
    if (it != revisions_.end() &&
        it->second.availability == WeightAvailabilityState::DELETED) {
        metadata.metadata_generation = it->second.metadata_generation + 1;
    }
    revisions_[key] = metadata;
    return metadata;
}

tl::expected<WeightRevisionMetadata, ErrorCode>
WeightMetadataStore::CommitImport(const CommitWeightImportRequest& request) {
    if (!request.identity.IsValid() || request.manifest_key.empty() ||
        request.payload_count == 0 ||
        request.payload_keys.size() != request.payload_count) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = request.identity.ToKey();
    auto it = revisions_.find(key);
    if (it == revisions_.end()) {
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_FOUND);
    }

    auto& metadata = it->second;
    if (metadata.availability == WeightAvailabilityState::READY &&
        metadata.manifest_key == request.manifest_key &&
        metadata.payload_keys_digest == request.payload_keys_digest) {
        // Idempotent commit of an already-published revision.
        return metadata;
    }
    if (metadata.availability != WeightAvailabilityState::IMPORTING) {
        return tl::make_unexpected(ErrorCode::WEIGHT_CONFLICT);
    }
    if (request.expected_metadata_generation != 0 &&
        request.expected_metadata_generation != metadata.metadata_generation) {
        return tl::make_unexpected(ErrorCode::WEIGHT_STALE_GENERATION);
    }

    metadata.manifest_key = request.manifest_key;
    metadata.manifest_sha256 = request.manifest_sha256;
    metadata.payload_keys_digest = request.payload_keys_digest;
    metadata.payload_count = request.payload_count;
    metadata.logical_payload_bytes = request.logical_payload_bytes;
    metadata.payload_keys = request.payload_keys;
    metadata.availability = WeightAvailabilityState::READY;
    metadata.observed_residency = WeightResidencyState::HOT;
    metadata.metadata_generation += 1;
    metadata.operation_id.clear();
    return metadata;
}

tl::expected<WeightRevisionMetadata, ErrorCode> WeightMetadataStore::Get(
    const WeightRevisionIdentity& identity) const {
    if (!identity.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = revisions_.find(identity.ToKey());
    if (it == revisions_.end()) {
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_FOUND);
    }
    if (it->second.availability == WeightAvailabilityState::IMPORTING ||
        it->second.availability == WeightAvailabilityState::DELETED) {
        // IMPORTING/DELETED are invisible to ordinary discovery.
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_FOUND);
    }
    return it->second;
}

ListWeightRevisionsResponse WeightMetadataStore::List(
    const ListWeightRevisionsRequest& request) const {
    ListWeightRevisionsResponse response;
    const uint64_t limit = request.limit == 0 ? 100 : request.limit;

    std::vector<WeightRevisionMetadata> matched;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        matched.reserve(revisions_.size());
        for (const auto& [_, metadata] : revisions_) {
            if (metadata.identity.tenant_id != request.tenant_id ||
                metadata.identity.ns != request.ns) {
                continue;
            }
            if (!request.resource_id.empty() &&
                metadata.identity.resource_id != request.resource_id) {
                continue;
            }
            if (metadata.availability != WeightAvailabilityState::READY &&
                metadata.availability != WeightAvailabilityState::DEGRADED) {
                continue;
            }
            matched.push_back(metadata);
        }
    }

    std::sort(
        matched.begin(), matched.end(),
        [](const WeightRevisionMetadata& a, const WeightRevisionMetadata& b) {
            return a.identity.ToKey() < b.identity.ToKey();
        });

    if (request.offset >= matched.size()) {
        response.next_offset = request.offset;
        response.has_more = false;
        return response;
    }

    const size_t start = static_cast<size_t>(request.offset);
    const size_t end =
        std::min(matched.size(), start + static_cast<size_t>(limit));
    response.revisions.assign(
        matched.begin() + static_cast<std::ptrdiff_t>(start),
        matched.begin() + static_cast<std::ptrdiff_t>(end));
    response.next_offset = static_cast<uint64_t>(end);
    response.has_more = end < matched.size();
    return response;
}

tl::expected<WeightRevisionMetadata, ErrorCode>
WeightMetadataStore::UpdatePolicy(const UpdateWeightPolicyRequest& request) {
    if (!request.identity.IsValid() || !request.policy.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = revisions_.find(request.identity.ToKey());
    if (it == revisions_.end()) {
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_FOUND);
    }
    auto& metadata = it->second;
    if (metadata.availability != WeightAvailabilityState::READY &&
        metadata.availability != WeightAvailabilityState::DEGRADED) {
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_READY);
    }
    if (request.expected_metadata_generation != metadata.metadata_generation) {
        return tl::make_unexpected(ErrorCode::WEIGHT_STALE_GENERATION);
    }
    metadata.policy = request.policy;
    metadata.metadata_generation += 1;
    return metadata;
}

tl::expected<std::pair<WeightRevisionMetadata, std::vector<std::string>>,
             ErrorCode>
WeightMetadataStore::AbortImport(const AbortWeightImportRequest& request) {
    if (!request.identity.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = revisions_.find(request.identity.ToKey());
    if (it == revisions_.end()) {
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_FOUND);
    }

    auto& metadata = it->second;
    if (metadata.availability == WeightAvailabilityState::DELETED) {
        // Idempotent abort after a previous cleanup.
        return std::make_pair(metadata, std::vector<std::string>{});
    }
    if (metadata.availability != WeightAvailabilityState::IMPORTING) {
        return tl::make_unexpected(ErrorCode::WEIGHT_CONFLICT);
    }
    if (request.expected_metadata_generation != 0 &&
        request.expected_metadata_generation != metadata.metadata_generation) {
        return tl::make_unexpected(ErrorCode::WEIGHT_STALE_GENERATION);
    }

    auto keys = UniqueNonEmptyKeys(request.keys_to_remove);
    metadata.availability = WeightAvailabilityState::DELETED;
    metadata.observed_residency = WeightResidencyState::UNKNOWN;
    metadata.operation_id.clear();
    metadata.metadata_generation += 1;
    return std::make_pair(metadata, std::move(keys));
}

tl::expected<std::pair<WeightRevisionMetadata, std::vector<std::string>>,
             ErrorCode>
WeightMetadataStore::RemoveRevision(
    const RemoveWeightRevisionRequest& request) {
    if (!request.identity.IsValid()) {
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = revisions_.find(request.identity.ToKey());
    if (it == revisions_.end()) {
        return tl::make_unexpected(ErrorCode::WEIGHT_NOT_FOUND);
    }

    auto& metadata = it->second;
    if (metadata.availability == WeightAvailabilityState::DELETED) {
        return std::make_pair(metadata, std::vector<std::string>{});
    }
    if (metadata.availability != WeightAvailabilityState::READY &&
        metadata.availability != WeightAvailabilityState::DEGRADED &&
        metadata.availability != WeightAvailabilityState::IMPORTING) {
        return tl::make_unexpected(ErrorCode::WEIGHT_CONFLICT);
    }
    if (request.expected_metadata_generation != 0 &&
        request.expected_metadata_generation != metadata.metadata_generation) {
        return tl::make_unexpected(ErrorCode::WEIGHT_STALE_GENERATION);
    }

    std::vector<std::string> keys = metadata.payload_keys;
    if (!metadata.manifest_key.empty()) {
        keys.push_back(metadata.manifest_key);
    }
    keys = UniqueNonEmptyKeys(keys);

    metadata.availability = WeightAvailabilityState::DELETED;
    metadata.observed_residency = WeightResidencyState::UNKNOWN;
    metadata.operation_id.clear();
    metadata.metadata_generation += 1;
    return std::make_pair(metadata, std::move(keys));
}

size_t WeightMetadataStore::SizeForTesting() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return revisions_.size();
}

std::optional<WeightRevisionMetadata> WeightMetadataStore::GetRawForTesting(
    const WeightRevisionIdentity& identity) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = revisions_.find(identity.ToKey());
    if (it == revisions_.end()) {
        return std::nullopt;
    }
    return it->second;
}

}  // namespace mooncake
