#pragma once

#include <array>
#include <mutex>
#include <shared_mutex>

#include "weight_store_backend.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}

class WeightStoreManager {
    friend class test::MasterServiceTestPeer;

   public:
    explicit WeightStoreManager(WeightStoreBackend& backend)
        : backend_(backend) {}

    // A successful guard excludes append-to-publication mutations while the
    // caller captures the OpLog boundary and weight state together.
    std::unique_lock<std::shared_mutex> TryLockSnapshot() {
        return std::unique_lock(mutation_mutex_, std::try_to_lock);
    }

    WeightMetadataSnapshot ExportSnapshot() const {
        return weight_metadata_.ExportSnapshot();
    }
    WeightMetadataStore::Result<void> RestoreSnapshot(
        const WeightMetadataSnapshot& snapshot) {
        return weight_metadata_.RestoreSnapshot(snapshot);
    }
    void Clear() { weight_metadata_.Clear(); }
    bool IsManagedGroup(const std::string& group) const {
        return weight_metadata_.IsManagedGroup(group);
    }
    bool AllowsGroupMemberMutation(const std::string& group) const {
        return weight_metadata_.AllowsGroupMemberMutation(group);
    }
    std::unique_lock<std::mutex> LockGroup(const TenantId& tenant,
                                           const std::string& group) {
        const auto key = tenant.MakeScopedKey(group);
        return std::unique_lock(
            group_locks_[std::hash<std::string>{}(key) % group_locks_.size()]);
    }

    WeightMetadataStore::Result<WeightRevisionLease> AcquireWeightRevisionLease(
        const AcquireWeightRevisionLeaseRequest& request);
    WeightMetadataStore::Result<WeightRevisionLease> RenewWeightRevisionLease(
        const RenewWeightRevisionLeaseRequest& request);
    WeightMetadataStore::Result<void> ReleaseWeightRevisionLease(
        const ReleaseWeightRevisionLeaseRequest& request);

    WeightMetadataStore::Result<WeightResidencyOperation>
    StartWeightResidencyOperation(
        const StartWeightResidencyOperationRequest& request);
    WeightMetadataStore::Result<WeightResidencyOperation> QueryWeightOperation(
        const QueryWeightOperationRequest& request) const;
    WeightMetadataStore::Result<WeightRevisionMetadata> ReconcileWeightRevision(
        const ReconcileWeightRevisionRequest& request);
    WeightMetadataStore::Result<WeightRevisionMetadata> DeleteWeightRevision(
        const DeleteWeightRevisionRequest& request);

    WeightMetadataStore::Result<WeightRevisionMetadata> BeginWeightImport(
        const BeginWeightImportRequest& request);
    WeightMetadataStore::Result<WeightRevisionMetadata> CommitWeightImport(
        const CommitWeightImportRequest& request);
    WeightMetadataStore::Result<WeightRevisionMetadata> AbortWeightImport(
        const AbortWeightImportRequest& request);
    WeightMetadataStore::Result<WeightRevisionView> GetWeightRevision(
        const GetWeightRevisionRequest& request) const;
    WeightMetadataStore::Result<ListWeightRevisionsResponse>
    ListWeightRevisions(const ListWeightRevisionsRequest& request) const;

   private:
    WeightMetadataStore::Result<WeightResidencyOperation>
    PersistAndPublishWeightOperationMutation(const WeightOperationMutation& mutation);
    WeightMetadataStore::Result<WeightRevisionLease>
    PersistAndPublishWeightLeaseMutation(const WeightLeaseMutation& mutation);
    WeightMetadataStore::Result<WeightRevisionMetadata>
    PersistAndPublishWeightMutation(const WeightMetadataMutation& mutation);
    std::unique_lock<std::mutex> LockGroup(
        const WeightRevisionIdentity& identity);
    WeightMetadataStore::Result<void> ValidateWeightGroupForCommit(
        const CommitWeightImportRequest& request) const;

    WeightStoreBackend& backend_;
    WeightMetadataStore weight_metadata_;
    std::shared_mutex mutation_mutex_;
    std::array<std::mutex, 4096> group_locks_;
};

}  // namespace mooncake
