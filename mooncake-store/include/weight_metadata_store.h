#pragma once

#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "weight_management.h"

namespace mooncake {

enum class WeightMetadataMutationKind : uint8_t {
    UPSERT = 0,
    ERASE = 1,
};

struct WeightMetadataMutation {
    WeightMetadataMutationKind kind{WeightMetadataMutationKind::UPSERT};
    WeightRevisionIdentity identity;
    std::optional<WeightRevisionMetadata> previous;
    std::optional<WeightRevisionMetadata> next;
    bool no_op{false};
};

struct WeightLeaseMutation {
    WeightMetadataMutationKind kind{WeightMetadataMutationKind::UPSERT};
    uint64_t lease_id{0};
    std::optional<WeightRevisionLease> previous;
    std::optional<WeightRevisionLease> next;
    bool no_op{false};
};

struct WeightOperationMutation {
    WeightMetadataMutation metadata;
    std::optional<WeightResidencyOperation> previous;
    std::optional<WeightResidencyOperation> next;
    bool no_op{false};
};

struct WeightMetadataSnapshot {
    uint32_t schema_version{1};
    std::vector<WeightRevisionMetadata> metadata;
    std::vector<WeightRevisionLease> leases;
    std::vector<WeightResidencyOperation> operations;
    uint64_t next_lease_id{1};
    uint64_t next_operation_id{1};

    friend bool operator==(const WeightMetadataSnapshot&,
                           const WeightMetadataSnapshot&) = default;
};
YLT_REFL(WeightMetadataSnapshot, schema_version, metadata, leases, operations,
         next_lease_id, next_operation_id);

tl::expected<void, WeightManagementError> ValidateWeightMetadataSnapshot(
    const WeightMetadataSnapshot& snapshot);

class WeightMetadataStore {
   public:
    template <typename T>
    using Result = tl::expected<T, WeightManagementError>;

    Result<WeightMetadataMutation> PrepareBeginImport(
        const BeginWeightImportRequest& request, uint64_t now_ms) const;
    Result<WeightMetadataMutation> PrepareCommitImport(
        const CommitWeightImportRequest& request, uint64_t now_ms) const;
    Result<WeightMetadataMutation> PrepareAbortImport(
        const AbortWeightImportRequest& request, uint64_t now_ms) const;
    Result<WeightRevisionMetadata> Publish(
        const WeightMetadataMutation& mutation);

    Result<WeightRevisionView> Get(const WeightRevisionIdentity& identity,
                                   uint64_t now_ms) const;
    Result<ListWeightRevisionsResponse> List(
        const ListWeightRevisionsRequest& request, uint64_t now_ms) const;

    Result<WeightLeaseMutation> PrepareAcquireLease(
        const AcquireWeightRevisionLeaseRequest& request, uint64_t now_ms);
    Result<WeightLeaseMutation> PrepareRenewLease(
        const RenewWeightRevisionLeaseRequest& request, uint64_t now_ms) const;
    Result<WeightLeaseMutation> PrepareReleaseLease(
        const ReleaseWeightRevisionLeaseRequest& request) const;
    std::vector<WeightLeaseMutation> PrepareExpireLeases(
        uint64_t now_ms, const std::optional<WeightRevisionIdentity>& identity =
                             std::nullopt) const;
    Result<WeightRevisionLease> Publish(const WeightLeaseMutation& mutation);
    bool HasActiveLease(const WeightRevisionIdentity& identity,
                        uint64_t metadata_generation, uint64_t now_ms) const;

    Result<WeightOperationMutation> PrepareStartOperation(
        const StartWeightResidencyOperationRequest& request, uint64_t now_ms);
    Result<WeightOperationMutation> PrepareFinishOperation(
        uint64_t operation_id, WeightResidencyState observed_residency,
        uint64_t now_ms) const;
    Result<WeightOperationMutation> PrepareUpdateOperationProgress(
        uint64_t operation_id, uint64_t processed_members,
        uint64_t total_members, std::string cursor,
        WeightAvailabilityState observed_availability,
        WeightResidencyState observed_residency, uint64_t now_ms) const;
    Result<WeightResidencyOperation> Publish(
        const WeightOperationMutation& mutation);
    Result<WeightResidencyOperation> QueryOperation(
        uint64_t operation_id) const;

    Result<WeightMetadataMutation> PrepareDelete(
        const DeleteWeightRevisionRequest& request, uint64_t now_ms) const;
    Result<WeightMetadataMutation> PrepareFinishDelete(
        const WeightRevisionIdentity& identity,
        uint64_t expected_metadata_generation, uint64_t now_ms) const;
    Result<WeightMetadataMutation> PrepareReconcile(
        const WeightRevisionIdentity& identity,
        uint64_t expected_metadata_generation,
        WeightAvailabilityState availability, WeightResidencyState residency,
        uint64_t now_ms) const;

    bool IsManagedGroup(const std::string& payload_group_id) const;
    bool AllowsGroupMemberMutation(const std::string& payload_group_id) const;
    WeightMetadataSnapshot ExportSnapshot() const;
    Result<void> RestoreSnapshot(const WeightMetadataSnapshot& snapshot);
    void Clear();

   private:
    static std::string MakePageToken(const WeightRevisionIdentity& identity);
    static Result<std::pair<std::string, uint64_t>> ParsePageToken(
        const std::string& page_token);
    static uint64_t AddTtl(uint64_t now_ms, uint64_t ttl_ms);
    static WeightOperationState OperationForTarget(WeightResidencyState target);

    uint64_t CountActiveLeasesLocked(const WeightRevisionMetadata& metadata,
                                     uint64_t now_ms,
                                     std::optional<uint64_t>* nearest) const;

    mutable std::mutex mutex_;
    std::map<WeightRevisionIdentity, WeightRevisionMetadata> revisions_;
    std::map<std::string, WeightRevisionIdentity> group_index_;
    std::unordered_map<uint64_t, WeightRevisionLease> leases_;
    std::unordered_map<uint64_t, WeightResidencyOperation> operations_;
    uint64_t next_lease_id_{1};
    uint64_t next_operation_id_{1};
};

}  // namespace mooncake
