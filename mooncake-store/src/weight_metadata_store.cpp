#include "weight_metadata_store.h"
#include "tenant_id.h"

#include <algorithm>
#include <charconv>
#include <limits>
#include <memory>
#include <sstream>
#include <set>
#include <string_view>
#include <unordered_set>

#include <openssl/evp.h>

namespace mooncake {
namespace {

constexpr uint32_t kMaxListLimit = 1000;
constexpr uint64_t kMaxLeaseTtlMs = 24ULL * 60 * 60 * 1000;
constexpr char kPageTokenSeparator = '\x1f';

bool MatchesIdempotentRetryGeneration(uint64_t current, uint64_t expected) {
    return current == expected ||
           (expected != std::numeric_limits<uint64_t>::max() &&
            current == expected + 1);
}

bool SameImport(const WeightRevisionMetadata& metadata,
                const BeginWeightImportRequest& request) {
    return metadata.identity == request.identity &&
           metadata.manifest.payload_group_id == request.payload_group_id &&
           metadata.manifest.payload_count == request.expected_payload_count &&
           metadata.manifest.logical_bytes == request.expected_logical_bytes;
}

bool SameManifest(const WeightRevisionMetadata& metadata,
                  const WeightManifestReference& manifest) {
    return metadata.manifest == manifest;
}

std::string Sha256Hex(const std::vector<std::string_view>& chunks) {
    using Context = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;
    Context context(EVP_MD_CTX_new(), EVP_MD_CTX_free);
    if (!context ||
        EVP_DigestInit_ex(context.get(), EVP_sha256(), nullptr) != 1) {
        return {};
    }
    for (const auto chunk : chunks) {
        if (EVP_DigestUpdate(context.get(), chunk.data(), chunk.size()) != 1) {
            return {};
        }
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_size = 0;
    if (EVP_DigestFinal_ex(context.get(), digest, &digest_size) != 1 ||
        digest_size != 32) {
        return {};
    }
    static constexpr char kHex[] = "0123456789abcdef";
    std::string encoded(digest_size * 2, '0');
    for (unsigned int i = 0; i < digest_size; ++i) {
        encoded[2 * i] = kHex[digest[i] >> 4];
        encoded[2 * i + 1] = kHex[digest[i] & 0x0f];
    }
    return encoded;
}

void AppendLengthPrefixed(std::vector<std::string>* storage,
                          std::vector<std::string_view>* chunks,
                          std::string_view value) {
    storage->push_back(std::to_string(value.size()));
    chunks->push_back(storage->back());
    chunks->push_back(":");
    chunks->push_back(value);
    chunks->push_back("\n");
}

std::string ComputeWeightIdentitySha256(
    const WeightRevisionIdentity& identity) {
    if (!ValidateWeightRevisionIdentity(identity).ok()) {
        return {};
    }
    std::vector<std::string> lengths;
    lengths.reserve(5);
    std::vector<std::string_view> chunks;
    chunks.reserve(20);
    AppendLengthPrefixed(&lengths, &chunks, identity.tenant_id);
    AppendLengthPrefixed(&lengths, &chunks, identity.name_space);
    AppendLengthPrefixed(&lengths, &chunks, identity.resource_id);
    AppendLengthPrefixed(&lengths, &chunks, identity.revision);
    const auto generation = std::to_string(identity.weight_generation);
    AppendLengthPrefixed(&lengths, &chunks, generation);
    return Sha256Hex(chunks);
}

}  // namespace

std::string ComputeWeightPayloadKeysSha256(
    const std::vector<std::string>& payload_keys) {
    auto sorted_keys = payload_keys;
    std::sort(sorted_keys.begin(), sorted_keys.end());
    std::vector<std::string> lengths;
    lengths.reserve(sorted_keys.size());
    std::vector<std::string_view> chunks;
    chunks.reserve(sorted_keys.size() * 4);
    for (const auto& key : sorted_keys) {
        AppendLengthPrefixed(&lengths, &chunks, key);
    }
    return Sha256Hex(chunks);
}

std::string MakeWeightPayloadGroupId(const WeightRevisionIdentity& identity) {
    const auto digest = ComputeWeightIdentitySha256(identity);
    return digest.empty() ? std::string() : "weight:" + digest;
}

std::string MakeWeightRevisionMetadataKey(
    const WeightRevisionIdentity& identity) {
    const auto digest = ComputeWeightIdentitySha256(identity);
    return digest.empty() ? std::string() : "weight-revision:" + digest;
}

std::string MakeWeightLeaseMetadataKey(uint64_t lease_id) {
    return lease_id == 0 ? std::string()
                         : "weight-lease:" + std::to_string(lease_id);
}

WeightMetadataStore::Result<WeightMetadataMutation>
WeightMetadataStore::PrepareBeginImport(const BeginWeightImportRequest& request,
                                        uint64_t now_ms) const {
    if (!ValidateWeightRevisionIdentity(request.identity).ok() ||
        !IsValidWeightComponent(request.payload_group_id) ||
        request.payload_group_id !=
            MakeWeightPayloadGroupId(request.identity) ||
        request.expected_payload_count == 0 ||
        request.expected_logical_bytes == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    std::lock_guard lock(mutex_);
    const auto group = group_index_.find(request.payload_group_id);
    if (group != group_index_.end() && group->second != request.identity) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }

    const auto current = revisions_.find(request.identity);
    if (current != revisions_.end()) {
        if ((current->second.availability ==
                 WeightAvailabilityState::IMPORTING ||
             current->second.availability == WeightAvailabilityState::READY) &&
            SameImport(current->second, request)) {
            return WeightMetadataMutation{
                .identity = request.identity,
                .previous = current->second,
                .next = current->second,
                .no_op = true,
            };
        }
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }

    WeightRevisionMetadata metadata{
        .identity = request.identity,
        .manifest =
            WeightManifestReference{
                .manifest_key = {},
                .manifest_sha256 = {},
                .payload_group_id = request.payload_group_id,
                .payload_keys_sha256 = {},
                .payload_count = request.expected_payload_count,
                .logical_bytes = request.expected_logical_bytes,
            },
        .availability = WeightAvailabilityState::IMPORTING,
        .residency = WeightResidencyState::UNKNOWN,
        .operation = WeightOperationState::NONE,
        .metadata_generation = 1,
        .created_at_ms = now_ms,
        .updated_at_ms = now_ms,
    };
    return WeightMetadataMutation{
        .kind = WeightMetadataMutationKind::UPSERT,
        .identity = request.identity,
        .previous = std::nullopt,
        .next = std::move(metadata),
        .no_op = false,
    };
}

WeightMetadataStore::Result<WeightMetadataMutation>
WeightMetadataStore::PrepareCommitImport(
    const CommitWeightImportRequest& request, uint64_t now_ms) const {
    if (!ValidateWeightRevisionIdentity(request.identity).ok() ||
        !ValidateWeightManifestReference(request.manifest).ok() ||
        request.manifest.payload_group_id !=
            MakeWeightPayloadGroupId(request.identity) ||
        request.manifest.manifest_key !=
            MakeWeightManifestKey(request.identity) ||
        request.expected_metadata_generation == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(request.identity);
    if (current == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (current->second.availability == WeightAvailabilityState::READY) {
        if (!SameManifest(current->second, request.manifest)) {
            return tl::make_unexpected(WeightManagementError::CONFLICT);
        }
        if (!MatchesIdempotentRetryGeneration(
                current->second.metadata_generation,
                request.expected_metadata_generation)) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
        return WeightMetadataMutation{
            .identity = request.identity,
            .previous = current->second,
            .next = current->second,
            .no_op = true,
        };
    }
    if (current->second.metadata_generation !=
        request.expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (current->second.availability != WeightAvailabilityState::IMPORTING) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    if (current->second.manifest.payload_group_id !=
            request.manifest.payload_group_id ||
        current->second.manifest.payload_count !=
            request.manifest.payload_count ||
        current->second.manifest.logical_bytes !=
            request.manifest.logical_bytes) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    if (!CanAdvanceWeightMetadataGeneration(
            current->second.metadata_generation)) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }

    auto next = current->second;
    next.manifest = request.manifest;
    next.availability = WeightAvailabilityState::READY;
    next.residency = WeightResidencyState::HOT;
    ++next.metadata_generation;
    next.updated_at_ms = std::max(next.updated_at_ms, now_ms);
    return WeightMetadataMutation{
        .identity = request.identity,
        .previous = current->second,
        .next = std::move(next),
    };
}

WeightMetadataStore::Result<WeightMetadataMutation>
WeightMetadataStore::PrepareAbortImport(const AbortWeightImportRequest& request,
                                        uint64_t now_ms) const {
    if (!ValidateWeightRevisionIdentity(request.identity).ok() ||
        request.expected_metadata_generation == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(request.identity);
    if (current == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (current->second.availability == WeightAvailabilityState::DELETING) {
        if (!MatchesIdempotentRetryGeneration(
                current->second.metadata_generation,
                request.expected_metadata_generation)) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
        return WeightMetadataMutation{
            .identity = request.identity,
            .previous = current->second,
            .next = current->second,
            .no_op = true,
        };
    }
    if (current->second.metadata_generation !=
        request.expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (current->second.availability != WeightAvailabilityState::IMPORTING) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    if (!CanAdvanceWeightMetadataGeneration(
            current->second.metadata_generation)) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }

    auto next = current->second;
    next.availability = WeightAvailabilityState::DELETING;
    ++next.metadata_generation;
    next.updated_at_ms = std::max(next.updated_at_ms, now_ms);
    return WeightMetadataMutation{
        .identity = request.identity,
        .previous = current->second,
        .next = std::move(next),
    };
}

WeightMetadataStore::Result<WeightRevisionMetadata>
WeightMetadataStore::Publish(const WeightMetadataMutation& mutation) {
    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(mutation.identity);
    if (mutation.previous.has_value()) {
        if (current == revisions_.end() ||
            current->second != *mutation.previous) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
    } else if (current != revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }

    if (mutation.no_op) {
        return current->second;
    }
    if (mutation.kind == WeightMetadataMutationKind::ERASE ||
        !mutation.next.has_value()) {
        if (current == revisions_.end()) {
            return tl::make_unexpected(WeightManagementError::NOT_FOUND);
        }
        auto removed = current->second;
        group_index_.erase(removed.manifest.payload_group_id);
        revisions_.erase(current);
        return removed;
    }

    const auto& next = *mutation.next;
    if (!ValidateWeightRevisionMetadata(next).ok()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    if (current != revisions_.end() &&
        current->second.availability != WeightAvailabilityState::DELETING &&
        next.availability == WeightAvailabilityState::DELETING) {
        std::optional<uint64_t> nearest;
        if (CountActiveLeasesLocked(current->second, next.updated_at_ms,
                                    &nearest) != 0) {
            return tl::make_unexpected(WeightManagementError::BUSY);
        }
    }
    const auto group = group_index_.find(next.manifest.payload_group_id);
    if (group != group_index_.end() && group->second != next.identity) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    revisions_[next.identity] = next;
    group_index_[next.manifest.payload_group_id] = next.identity;
    return next;
}

WeightMetadataStore::Result<WeightRevisionView> WeightMetadataStore::Get(
    const WeightRevisionIdentity& identity, uint64_t now_ms) const {
    if (!ValidateWeightRevisionIdentity(identity).ok()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(identity);
    if (current == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    std::optional<uint64_t> nearest;
    const auto count =
        CountActiveLeasesLocked(current->second, now_ms, &nearest);
    return WeightRevisionView{
        .metadata = current->second,
        .active_lease_count = count,
        .nearest_lease_expiry_ms = nearest,
    };
}

WeightMetadataStore::Result<ListWeightRevisionsResponse>
WeightMetadataStore::List(const ListWeightRevisionsRequest& request,
                          uint64_t now_ms) const {
    if (request.tenant_id.empty() || !TenantId(request.tenant_id).IsValid() ||
        !IsValidWeightComponent(request.name_space) ||
        !IsValidWeightComponent(request.resource_id) || request.limit == 0 ||
        request.limit > kMaxListLimit) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    std::optional<std::pair<std::string, uint64_t>> cursor;
    if (!request.page_token.empty()) {
        auto parsed = ParsePageToken(request.page_token);
        if (!parsed.has_value()) {
            return tl::make_unexpected(parsed.error());
        }
        cursor = *parsed;
    }

    std::lock_guard lock(mutex_);
    ListWeightRevisionsResponse response;
    for (const auto& [identity, metadata] : revisions_) {
        if (identity.tenant_id != request.tenant_id ||
            identity.name_space != request.name_space ||
            identity.resource_id != request.resource_id) {
            continue;
        }
        if (cursor.has_value() &&
            std::tie(identity.revision, identity.weight_generation) <=
                std::tie(cursor->first, cursor->second)) {
            continue;
        }
        if (response.revisions.size() == request.limit) {
            response.next_page_token =
                MakePageToken(response.revisions.back().metadata.identity);
            break;
        }
        std::optional<uint64_t> nearest;
        const auto count = CountActiveLeasesLocked(metadata, now_ms, &nearest);
        response.revisions.push_back(WeightRevisionView{
            .metadata = metadata,
            .active_lease_count = count,
            .nearest_lease_expiry_ms = nearest,
        });
    }
    return response;
}

WeightMetadataStore::Result<WeightLeaseMutation>
WeightMetadataStore::PrepareAcquireLease(
    const AcquireWeightRevisionLeaseRequest& request, uint64_t now_ms) {
    if (!ValidateWeightRevisionIdentity(request.identity).ok() ||
        request.expected_metadata_generation == 0 ||
        !IsValidWeightComponent(request.holder) || request.ttl_ms == 0 ||
        request.ttl_ms > kMaxLeaseTtlMs) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(request.identity);
    if (current == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (current->second.metadata_generation !=
        request.expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (current->second.availability != WeightAvailabilityState::READY) {
        return tl::make_unexpected(WeightManagementError::NOT_READY);
    }
    if (current->second.operation != WeightOperationState::NONE) {
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    for (const auto& [lease_id, lease] : leases_) {
        (void)lease_id;
        if (lease.identity == request.identity &&
            lease.holder == request.holder && lease.expires_at_ms > now_ms &&
            lease.fenced_metadata_generation <=
                current->second.metadata_generation) {
            return WeightLeaseMutation{
                .lease_id = lease.lease_id,
                .previous = lease,
                .next = lease,
                .no_op = true,
            };
        }
    }
    if (next_lease_id_ == 0 ||
        next_lease_id_ == std::numeric_limits<uint64_t>::max()) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    const uint64_t lease_id = next_lease_id_++;
    return WeightLeaseMutation{
        .kind = WeightMetadataMutationKind::UPSERT,
        .lease_id = lease_id,
        .previous = std::nullopt,
        .next =
            WeightRevisionLease{
                .lease_id = lease_id,
                .identity = request.identity,
                .holder = request.holder,
                .expires_at_ms = AddTtl(now_ms, request.ttl_ms),
                .fenced_metadata_generation =
                    request.expected_metadata_generation,
            },
        .no_op = false,
    };
}

WeightMetadataStore::Result<WeightLeaseMutation>
WeightMetadataStore::PrepareRenewLease(
    const RenewWeightRevisionLeaseRequest& request, uint64_t now_ms) const {
    if (request.tenant_id.empty() || !TenantId(request.tenant_id).IsValid() ||
        request.lease_id == 0 || request.ttl_ms == 0 ||
        request.ttl_ms > kMaxLeaseTtlMs) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto current = leases_.find(request.lease_id);
    if (current == leases_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (current->second.identity.tenant_id != request.tenant_id) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (current->second.expires_at_ms <= now_ms) {
        return tl::make_unexpected(WeightManagementError::LEASE_EXPIRED);
    }
    auto next = current->second;
    // Lease upserts must remain monotonic for standby replay.
    next.expires_at_ms =
        std::max(next.expires_at_ms, AddTtl(now_ms, request.ttl_ms));
    return WeightLeaseMutation{
        .lease_id = request.lease_id,
        .previous = current->second,
        .next = std::move(next),
    };
}

WeightMetadataStore::Result<WeightLeaseMutation>
WeightMetadataStore::PrepareReleaseLease(
    const ReleaseWeightRevisionLeaseRequest& request) const {
    if (request.tenant_id.empty() || !TenantId(request.tenant_id).IsValid() ||
        request.lease_id == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto current = leases_.find(request.lease_id);
    if (current == leases_.end()) {
        return WeightLeaseMutation{
            .kind = WeightMetadataMutationKind::ERASE,
            .lease_id = request.lease_id,
            .previous = std::nullopt,
            .next = std::nullopt,
            .no_op = true,
        };
    }
    if (current->second.identity.tenant_id != request.tenant_id) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    return WeightLeaseMutation{
        .kind = WeightMetadataMutationKind::ERASE,
        .lease_id = request.lease_id,
        .previous = current->second,
        .next = std::nullopt,
        .no_op = false,
    };
}

std::vector<WeightLeaseMutation> WeightMetadataStore::PrepareExpireLeases(
    uint64_t now_ms,
    const std::optional<WeightRevisionIdentity>& identity) const {
    std::lock_guard lock(mutex_);
    std::vector<WeightLeaseMutation> expired;
    for (const auto& [lease_id, lease] : leases_) {
        if (lease.expires_at_ms <= now_ms &&
            (!identity.has_value() || lease.identity == *identity)) {
            expired.push_back(WeightLeaseMutation{
                .kind = WeightMetadataMutationKind::ERASE,
                .lease_id = lease_id,
                .previous = lease,
                .next = std::nullopt,
                .no_op = false,
            });
        }
    }
    std::sort(expired.begin(), expired.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.lease_id < rhs.lease_id;
              });
    return expired;
}

WeightMetadataStore::Result<WeightRevisionLease> WeightMetadataStore::Publish(
    const WeightLeaseMutation& mutation) {
    std::lock_guard lock(mutex_);
    const auto current = leases_.find(mutation.lease_id);
    if (mutation.no_op) {
        if (mutation.next.has_value()) {
            return *mutation.next;
        }
        return WeightRevisionLease{
            .lease_id = mutation.lease_id,
            .identity = {},
            .holder = {},
            .expires_at_ms = 0,
            .fenced_metadata_generation = 0,
        };
    }
    if (mutation.previous.has_value()) {
        if (current == leases_.end() || current->second != *mutation.previous) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
    } else if (current != leases_.end()) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }

    if (mutation.kind == WeightMetadataMutationKind::ERASE ||
        !mutation.next.has_value()) {
        auto removed = current->second;
        leases_.erase(current);
        return removed;
    }

    const auto& next = *mutation.next;
    const auto revision = revisions_.find(next.identity);
    if (revision == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (revision->second.metadata_generation <
        next.fenced_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (revision->second.availability != WeightAvailabilityState::READY) {
        return tl::make_unexpected(WeightManagementError::NOT_READY);
    }
    if (revision->second.operation != WeightOperationState::NONE) {
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    if (!mutation.previous.has_value() &&
        next.lease_id == std::numeric_limits<uint64_t>::max()) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    leases_[next.lease_id] = next;
    if (!mutation.previous.has_value()) {
        next_lease_id_ = std::max(next_lease_id_, next.lease_id + 1);
    }
    return next;
}

bool WeightMetadataStore::HasActiveLease(const WeightRevisionIdentity& identity,
                                         uint64_t metadata_generation,
                                         uint64_t now_ms) const {
    std::lock_guard lock(mutex_);
    for (const auto& [lease_id, lease] : leases_) {
        static_cast<void>(lease_id);
        if (lease.identity == identity &&
            lease.fenced_metadata_generation <= metadata_generation &&
            lease.expires_at_ms > now_ms) {
            return true;
        }
    }
    return false;
}

WeightMetadataStore::Result<WeightOperationMutation>
WeightMetadataStore::PrepareStartOperation(
    const StartWeightResidencyOperationRequest& request, uint64_t now_ms) {
    if (!ValidateWeightRevisionIdentity(request.identity).ok() ||
        request.expected_metadata_generation == 0 ||
        (request.target_residency != WeightResidencyState::HOT &&
         request.target_residency != WeightResidencyState::COLD)) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(request.identity);
    if (current == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (current->second.operation != WeightOperationState::NONE) {
        const auto operation = operations_.find(current->second.operation_id);
        if (operation != operations_.end() &&
            operation->second.target_residency == request.target_residency) {
            if (!MatchesIdempotentRetryGeneration(
                    current->second.metadata_generation,
                    request.expected_metadata_generation)) {
                return tl::make_unexpected(
                    WeightManagementError::STALE_GENERATION);
            }
            return WeightOperationMutation{
                .metadata =
                    WeightMetadataMutation{
                        .identity = request.identity,
                        .previous = current->second,
                        .next = current->second,
                        .no_op = true,
                    },
                .previous = operation->second,
                .next = operation->second,
                .no_op = true,
            };
        }
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    if (current->second.metadata_generation !=
        request.expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (current->second.availability != WeightAvailabilityState::READY &&
        current->second.availability != WeightAvailabilityState::DEGRADED) {
        return tl::make_unexpected(WeightManagementError::NOT_READY);
    }
    std::optional<uint64_t> nearest;
    if (CountActiveLeasesLocked(current->second, now_ms, &nearest) != 0) {
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    if (!CanAdvanceWeightMetadataGeneration(
            current->second.metadata_generation) ||
        next_operation_id_ == 0 ||
        next_operation_id_ == std::numeric_limits<uint64_t>::max()) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }

    const uint64_t operation_id = next_operation_id_++;
    const uint64_t operation_time =
        std::max(current->second.updated_at_ms, now_ms);
    auto next_metadata = current->second;
    next_metadata.operation = OperationForTarget(request.target_residency);
    next_metadata.operation_id = operation_id;
    ++next_metadata.metadata_generation;
    next_metadata.updated_at_ms = operation_time;
    WeightResidencyOperation next_operation{
        .operation_id = operation_id,
        .identity = request.identity,
        .operation = next_metadata.operation,
        .target_residency = request.target_residency,
        .fenced_metadata_generation = next_metadata.metadata_generation,
        .started_at_ms = operation_time,
        .updated_at_ms = operation_time,
        .processed_members = 0,
        .total_members = 0,
        .cursor = {},
        .message = {},
    };
    return WeightOperationMutation{
        .metadata =
            WeightMetadataMutation{
                .identity = request.identity,
                .previous = current->second,
                .next = std::move(next_metadata),
            },
        .previous = std::nullopt,
        .next = std::move(next_operation),
        .no_op = false,
    };
}

WeightMetadataStore::Result<WeightOperationMutation>
WeightMetadataStore::PrepareFinishOperation(
    uint64_t operation_id, WeightResidencyState observed_residency,
    uint64_t now_ms) const {
    if (operation_id == 0 ||
        (observed_residency != WeightResidencyState::HOT &&
         observed_residency != WeightResidencyState::COLD)) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto operation = operations_.find(operation_id);
    if (operation == operations_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    const auto revision = revisions_.find(operation->second.identity);
    if (revision == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (revision->second.operation_id != operation_id ||
        revision->second.operation != operation->second.operation) {
        if (revision->second.operation == WeightOperationState::NONE &&
            revision->second.residency == observed_residency) {
            return WeightOperationMutation{
                .metadata =
                    WeightMetadataMutation{
                        .identity = revision->first,
                        .previous = revision->second,
                        .next = revision->second,
                        .no_op = true,
                    },
                .previous = operation->second,
                .next = operation->second,
                .no_op = true,
            };
        }
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    if (operation->second.target_residency != observed_residency) {
        return tl::make_unexpected(WeightManagementError::NOT_READY);
    }
    if (!CanAdvanceWeightMetadataGeneration(
            revision->second.metadata_generation)) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    auto next_metadata = revision->second;
    next_metadata.availability = WeightAvailabilityState::READY;
    next_metadata.residency = observed_residency;
    next_metadata.operation = WeightOperationState::NONE;
    next_metadata.operation_id = 0;
    ++next_metadata.metadata_generation;
    const uint64_t operation_time =
        std::max({revision->second.updated_at_ms,
                  operation->second.updated_at_ms, now_ms});
    next_metadata.updated_at_ms = operation_time;
    auto next_operation = operation->second;
    next_operation.updated_at_ms = operation_time;
    next_operation.processed_members = next_operation.total_members;
    next_operation.cursor.clear();
    next_operation.message = "completed";
    return WeightOperationMutation{
        .metadata =
            WeightMetadataMutation{
                .identity = revision->first,
                .previous = revision->second,
                .next = std::move(next_metadata),
            },
        .previous = operation->second,
        .next = std::move(next_operation),
    };
}

WeightMetadataStore::Result<WeightOperationMutation>
WeightMetadataStore::PrepareUpdateOperationProgress(
    uint64_t operation_id, uint64_t processed_members, uint64_t total_members,
    std::string cursor, WeightAvailabilityState observed_availability,
    WeightResidencyState observed_residency, uint64_t now_ms) const {
    if (operation_id == 0 || total_members == 0 ||
        processed_members > total_members ||
        (!cursor.empty() && !IsValidWeightComponent(cursor)) ||
        (observed_availability != WeightAvailabilityState::READY &&
         observed_availability != WeightAvailabilityState::DEGRADED)) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto operation = operations_.find(operation_id);
    if (operation == operations_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    const auto revision = revisions_.find(operation->second.identity);
    if (revision == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (revision->second.operation_id != operation_id ||
        revision->second.operation != operation->second.operation ||
        operation->second.message == "completed") {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    auto next_operation = operation->second;
    auto next_metadata = revision->second;
    next_metadata.availability = observed_availability;
    next_metadata.residency = observed_residency;
    if (!ValidateWeightRevisionMetadata(next_metadata).ok()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    const bool metadata_unchanged =
        revision->second.availability == observed_availability &&
        revision->second.residency == observed_residency;
    const bool unchanged =
        metadata_unchanged &&
        next_operation.processed_members == processed_members &&
        next_operation.total_members == total_members &&
        next_operation.cursor == cursor;
    if (unchanged) {
        return WeightOperationMutation{
            .metadata =
                WeightMetadataMutation{
                    .identity = revision->first,
                    .previous = revision->second,
                    .next = revision->second,
                    .no_op = true,
                },
            .previous = operation->second,
            .next = operation->second,
            .no_op = true,
        };
    }
    next_operation.processed_members = processed_members;
    next_operation.total_members = total_members;
    next_operation.cursor = std::move(cursor);
    next_operation.updated_at_ms =
        std::max(next_operation.updated_at_ms, now_ms);
    if (!metadata_unchanged) {
        if (!CanAdvanceWeightMetadataGeneration(
                next_metadata.metadata_generation)) {
            return tl::make_unexpected(
                WeightManagementError::GENERATION_EXHAUSTED);
        }
        ++next_metadata.metadata_generation;
        next_metadata.updated_at_ms =
            std::max(next_metadata.updated_at_ms, now_ms);
        // Publish the observed state and its operation fence atomically.
        next_operation.fenced_metadata_generation =
            next_metadata.metadata_generation;
    }
    return WeightOperationMutation{
        .metadata =
            WeightMetadataMutation{
                .identity = revision->first,
                .previous = revision->second,
                .next = std::move(next_metadata),
                .no_op = false,
            },
        .previous = operation->second,
        .next = std::move(next_operation),
        .no_op = false,
    };
}

WeightMetadataStore::Result<WeightResidencyOperation>
WeightMetadataStore::Publish(const WeightOperationMutation& mutation) {
    std::lock_guard lock(mutex_);
    const auto current = revisions_.find(mutation.metadata.identity);
    if (!mutation.metadata.previous.has_value() ||
        current == revisions_.end() ||
        current->second != *mutation.metadata.previous) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (mutation.no_op) {
        if (!mutation.next.has_value()) {
            return tl::make_unexpected(WeightManagementError::NOT_FOUND);
        }
        return *mutation.next;
    }
    if (!mutation.metadata.next.has_value() || !mutation.next.has_value()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    if (!mutation.previous.has_value()) {
        std::optional<uint64_t> nearest;
        if (CountActiveLeasesLocked(
                current->second, mutation.next->started_at_ms, &nearest) != 0) {
            return tl::make_unexpected(WeightManagementError::BUSY);
        }
    }
    const auto operation = operations_.find(mutation.next->operation_id);
    if (mutation.previous.has_value()) {
        if (operation == operations_.end() ||
            operation->second != *mutation.previous) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
    } else if (operation != operations_.end()) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    if (!mutation.previous.has_value() &&
        mutation.next->operation_id == std::numeric_limits<uint64_t>::max()) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    revisions_[mutation.metadata.identity] = *mutation.metadata.next;
    operations_[mutation.next->operation_id] = *mutation.next;
    if (!mutation.previous.has_value()) {
        next_operation_id_ =
            std::max(next_operation_id_, mutation.next->operation_id + 1);
    }
    return *mutation.next;
}

WeightMetadataStore::Result<WeightResidencyOperation>
WeightMetadataStore::QueryOperation(uint64_t operation_id) const {
    if (operation_id == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto operation = operations_.find(operation_id);
    if (operation == operations_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    return operation->second;
}

WeightMetadataStore::Result<WeightMetadataMutation>
WeightMetadataStore::PrepareDelete(const DeleteWeightRevisionRequest& request,
                                   uint64_t now_ms) const {
    if (!ValidateWeightRevisionIdentity(request.identity).ok() ||
        request.expected_metadata_generation == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    std::lock_guard lock(mutex_);
    const auto revision = revisions_.find(request.identity);
    if (revision == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (revision->second.availability == WeightAvailabilityState::DELETED) {
        if (!MatchesIdempotentRetryGeneration(
                revision->second.metadata_generation,
                request.expected_metadata_generation)) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
        return WeightMetadataMutation{
            .identity = request.identity,
            .previous = revision->second,
            .next = revision->second,
            .no_op = true,
        };
    }
    if (revision->second.availability == WeightAvailabilityState::DELETING) {
        if (!MatchesIdempotentRetryGeneration(
                revision->second.metadata_generation,
                request.expected_metadata_generation)) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
        return WeightMetadataMutation{
            .identity = request.identity,
            .previous = revision->second,
            .next = revision->second,
            .no_op = true,
        };
    }
    if (revision->second.metadata_generation !=
        request.expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    std::optional<uint64_t> nearest;
    if (CountActiveLeasesLocked(revision->second, now_ms, &nearest) != 0) {
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    if (revision->second.operation != WeightOperationState::NONE) {
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    if (revision->second.availability != WeightAvailabilityState::READY &&
        revision->second.availability != WeightAvailabilityState::DEGRADED) {
        return tl::make_unexpected(WeightManagementError::NOT_READY);
    }
    if (!CanAdvanceWeightMetadataGeneration(
            revision->second.metadata_generation)) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    auto next = revision->second;
    next.availability = WeightAvailabilityState::DELETING;
    ++next.metadata_generation;
    next.updated_at_ms = std::max(next.updated_at_ms, now_ms);
    return WeightMetadataMutation{
        .identity = request.identity,
        .previous = revision->second,
        .next = std::move(next),
    };
}

WeightMetadataStore::Result<WeightMetadataMutation>
WeightMetadataStore::PrepareFinishDelete(const WeightRevisionIdentity& identity,
                                         uint64_t expected_metadata_generation,
                                         uint64_t now_ms) const {
    std::lock_guard lock(mutex_);
    const auto revision = revisions_.find(identity);
    if (revision == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (revision->second.availability == WeightAvailabilityState::DELETED) {
        if (!MatchesIdempotentRetryGeneration(
                revision->second.metadata_generation,
                expected_metadata_generation)) {
            return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
        }
        return WeightMetadataMutation{
            .identity = identity,
            .previous = revision->second,
            .next = revision->second,
            .no_op = true,
        };
    }
    if (revision->second.metadata_generation != expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (revision->second.availability != WeightAvailabilityState::DELETING) {
        return tl::make_unexpected(WeightManagementError::CONFLICT);
    }
    if (!CanAdvanceWeightMetadataGeneration(expected_metadata_generation)) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    auto next = revision->second;
    next.availability = WeightAvailabilityState::DELETED;
    next.residency = WeightResidencyState::ABSENT;
    ++next.metadata_generation;
    next.updated_at_ms = std::max(next.updated_at_ms, now_ms);
    return WeightMetadataMutation{
        .identity = identity,
        .previous = revision->second,
        .next = std::move(next),
    };
}

WeightMetadataStore::Result<WeightMetadataMutation>
WeightMetadataStore::PrepareReconcile(const WeightRevisionIdentity& identity,
                                      uint64_t expected_metadata_generation,
                                      WeightAvailabilityState availability,
                                      WeightResidencyState residency,
                                      uint64_t now_ms) const {
    std::lock_guard lock(mutex_);
    const auto revision = revisions_.find(identity);
    if (revision == revisions_.end()) {
        return tl::make_unexpected(WeightManagementError::NOT_FOUND);
    }
    if (revision->second.metadata_generation != expected_metadata_generation) {
        return tl::make_unexpected(WeightManagementError::STALE_GENERATION);
    }
    if (revision->second.operation != WeightOperationState::NONE ||
        revision->second.availability == WeightAvailabilityState::IMPORTING ||
        revision->second.availability == WeightAvailabilityState::DELETING ||
        revision->second.availability == WeightAvailabilityState::DELETED) {
        return tl::make_unexpected(WeightManagementError::BUSY);
    }
    if ((availability != WeightAvailabilityState::READY &&
         availability != WeightAvailabilityState::DEGRADED) ||
        residency == WeightResidencyState::UNKNOWN) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    if (revision->second.availability == availability &&
        revision->second.residency == residency) {
        return WeightMetadataMutation{
            .identity = identity,
            .previous = revision->second,
            .next = revision->second,
            .no_op = true,
        };
    }
    if (!CanAdvanceWeightMetadataGeneration(expected_metadata_generation)) {
        return tl::make_unexpected(WeightManagementError::GENERATION_EXHAUSTED);
    }
    auto next = revision->second;
    next.availability = availability;
    next.residency = residency;
    ++next.metadata_generation;
    next.updated_at_ms = std::max(next.updated_at_ms, now_ms);
    return WeightMetadataMutation{
        .identity = identity,
        .previous = revision->second,
        .next = std::move(next),
    };
}

bool WeightMetadataStore::IsManagedGroup(
    const std::string& payload_group_id) const {
    std::lock_guard lock(mutex_);
    return group_index_.contains(payload_group_id);
}

bool WeightMetadataStore::AllowsGroupMemberMutation(
    const std::string& payload_group_id) const {
    std::lock_guard lock(mutex_);
    const auto group = group_index_.find(payload_group_id);
    if (group == group_index_.end()) {
        return true;
    }
    const auto revision = revisions_.find(group->second);
    return revision != revisions_.end() &&
           revision->second.availability == WeightAvailabilityState::IMPORTING;
}

WeightMetadataSnapshot WeightMetadataStore::ExportSnapshot() const {
    std::lock_guard lock(mutex_);
    WeightMetadataSnapshot snapshot{
        .schema_version = 1,
        .metadata = {},
        .leases = {},
        .operations = {},
        .next_lease_id = next_lease_id_,
        .next_operation_id = next_operation_id_,
    };
    snapshot.metadata.reserve(revisions_.size());
    for (const auto& [identity, metadata] : revisions_) {
        static_cast<void>(identity);
        snapshot.metadata.push_back(metadata);
    }
    snapshot.leases.reserve(leases_.size());
    for (const auto& [lease_id, lease] : leases_) {
        static_cast<void>(lease_id);
        snapshot.leases.push_back(lease);
    }
    std::sort(snapshot.leases.begin(), snapshot.leases.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.lease_id < rhs.lease_id;
              });
    snapshot.operations.reserve(operations_.size());
    for (const auto& [operation_id, operation] : operations_) {
        static_cast<void>(operation_id);
        snapshot.operations.push_back(operation);
    }
    std::sort(snapshot.operations.begin(), snapshot.operations.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.operation_id < rhs.operation_id;
              });
    return snapshot;
}

tl::expected<void, WeightManagementError> ValidateWeightMetadataSnapshot(
    const WeightMetadataSnapshot& snapshot) {
    if (snapshot.schema_version != 1 || snapshot.next_lease_id == 0 ||
        snapshot.next_operation_id == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    std::map<WeightRevisionIdentity, const WeightRevisionMetadata*> revisions;
    std::set<std::string_view> group_index;
    for (const auto& metadata : snapshot.metadata) {
        if (!ValidateWeightRevisionMetadata(metadata).ok() ||
            !IsValidWeightComponent(metadata.manifest.payload_group_id) ||
            !revisions.emplace(metadata.identity, &metadata).second ||
            !group_index.emplace(metadata.manifest.payload_group_id).second) {
            return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
        }
    }

    uint64_t max_lease_id = 0;
    std::unordered_set<uint64_t> leases;
    for (const auto& lease : snapshot.leases) {
        const auto revision = revisions.find(lease.identity);
        if (lease.lease_id == 0 ||
            !ValidateWeightRevisionIdentity(lease.identity).ok() ||
            !IsValidWeightComponent(lease.holder) || lease.expires_at_ms == 0 ||
            lease.fenced_metadata_generation == 0 ||
            revision == revisions.end() ||
            lease.fenced_metadata_generation >
                revision->second->metadata_generation ||
            !leases.emplace(lease.lease_id).second) {
            return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
        }
        max_lease_id = std::max(max_lease_id, lease.lease_id);
    }
    if (snapshot.next_lease_id <= max_lease_id) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }

    uint64_t max_operation_id = 0;
    std::unordered_set<uint64_t> operations;
    for (const auto& operation : snapshot.operations) {
        const auto revision = revisions.find(operation.identity);
        const bool completed = operation.message == "completed";
        if (operation.operation_id == 0 ||
            !ValidateWeightRevisionIdentity(operation.identity).ok() ||
            !IsValidWeightOperationState(operation.operation) ||
            operation.operation == WeightOperationState::NONE ||
            !IsValidWeightResidencyState(operation.target_residency) ||
            (operation.target_residency != WeightResidencyState::HOT &&
             operation.target_residency != WeightResidencyState::COLD) ||
            (operation.operation == WeightOperationState::EVICTING &&
             operation.target_residency != WeightResidencyState::COLD) ||
            (operation.operation == WeightOperationState::REHYDRATING &&
             operation.target_residency != WeightResidencyState::HOT) ||
            operation.fenced_metadata_generation == 0 ||
            operation.updated_at_ms < operation.started_at_ms ||
            operation.processed_members > operation.total_members ||
            (!operation.cursor.empty() &&
             !IsValidWeightComponent(operation.cursor)) ||
            revision == revisions.end() ||
            (!completed &&
             (revision->second->operation != operation.operation ||
              revision->second->operation_id != operation.operation_id ||
              revision->second->metadata_generation !=
                  operation.fenced_metadata_generation)) ||
            (completed &&
             (operation.fenced_metadata_generation ==
                  std::numeric_limits<uint64_t>::max() ||
              revision->second->metadata_generation <=
                  operation.fenced_metadata_generation ||
              (revision->second->metadata_generation ==
                   operation.fenced_metadata_generation + 1 &&
               (revision->second->operation != WeightOperationState::NONE ||
                revision->second->operation_id != 0 ||
                revision->second->residency != operation.target_residency)))) ||
            !operations.emplace(operation.operation_id).second) {
            return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
        }
        max_operation_id = std::max(max_operation_id, operation.operation_id);
    }
    if (snapshot.next_operation_id <= max_operation_id) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    for (const auto& [identity, metadata] : revisions) {
        static_cast<void>(identity);
        if (metadata->operation != WeightOperationState::NONE &&
            !operations.contains(metadata->operation_id)) {
            return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
        }
    }

    return {};
}

WeightMetadataStore::Result<void> WeightMetadataStore::RestoreSnapshot(
    const WeightMetadataSnapshot& snapshot) {
    auto validated = ValidateWeightMetadataSnapshot(snapshot);
    if (!validated) {
        return validated;
    }
    std::map<WeightRevisionIdentity, WeightRevisionMetadata> revisions;
    std::map<std::string, WeightRevisionIdentity> group_index;
    std::unordered_map<uint64_t, WeightRevisionLease> leases;
    std::unordered_map<uint64_t, WeightResidencyOperation> operations;
    for (const auto& metadata : snapshot.metadata) {
        revisions.emplace(metadata.identity, metadata);
        group_index.emplace(metadata.manifest.payload_group_id,
                            metadata.identity);
    }
    for (const auto& lease : snapshot.leases) {
        leases.emplace(lease.lease_id, lease);
    }
    for (const auto& operation : snapshot.operations) {
        operations.emplace(operation.operation_id, operation);
    }

    std::lock_guard lock(mutex_);
    revisions_ = std::move(revisions);
    group_index_ = std::move(group_index);
    leases_ = std::move(leases);
    operations_ = std::move(operations);
    next_lease_id_ = snapshot.next_lease_id;
    next_operation_id_ = snapshot.next_operation_id;
    return {};
}

void WeightMetadataStore::Clear() {
    std::lock_guard lock(mutex_);
    revisions_.clear();
    group_index_.clear();
    leases_.clear();
    operations_.clear();
    next_lease_id_ = 1;
    next_operation_id_ = 1;
}

std::string WeightMetadataStore::MakePageToken(
    const WeightRevisionIdentity& identity) {
    return identity.revision + kPageTokenSeparator +
           std::to_string(identity.weight_generation);
}

WeightMetadataStore::Result<std::pair<std::string, uint64_t>>
WeightMetadataStore::ParsePageToken(const std::string& page_token) {
    const auto separator = page_token.rfind(kPageTokenSeparator);
    if (separator == std::string::npos || separator == 0 ||
        separator + 1 == page_token.size()) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    uint64_t generation = 0;
    const auto value = std::string_view(page_token).substr(separator + 1);
    const auto [end, error] =
        std::from_chars(value.data(), value.data() + value.size(), generation);
    if (error != std::errc() || end != value.data() + value.size() ||
        generation == 0) {
        return tl::make_unexpected(WeightManagementError::INVALID_ARGUMENT);
    }
    return std::make_pair(page_token.substr(0, separator), generation);
}

uint64_t WeightMetadataStore::AddTtl(uint64_t now_ms, uint64_t ttl_ms) {
    if (ttl_ms > std::numeric_limits<uint64_t>::max() - now_ms) {
        return std::numeric_limits<uint64_t>::max();
    }
    return now_ms + ttl_ms;
}

WeightOperationState WeightMetadataStore::OperationForTarget(
    WeightResidencyState target) {
    return target == WeightResidencyState::COLD
               ? WeightOperationState::EVICTING
               : WeightOperationState::REHYDRATING;
}

uint64_t WeightMetadataStore::CountActiveLeasesLocked(
    const WeightRevisionMetadata& metadata, uint64_t now_ms,
    std::optional<uint64_t>* nearest) const {
    uint64_t count = 0;
    for (const auto& [lease_id, lease] : leases_) {
        static_cast<void>(lease_id);
        if (lease.identity != metadata.identity ||
            lease.fenced_metadata_generation > metadata.metadata_generation ||
            lease.expires_at_ms <= now_ms) {
            continue;
        }
        ++count;
        if (!nearest->has_value() || lease.expires_at_ms < **nearest) {
            *nearest = lease.expires_at_ms;
        }
    }
    return count;
}

}  // namespace mooncake
