#pragma once

#include <array>
#include <mutex>

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

    WeightMetadataSnapshot ExportSnapshot() const {
        return weight_metadata_.ExportSnapshot();
    }
    WeightMetadataStore::Result<void> RestoreSnapshot(
        const WeightMetadataSnapshot& snapshot) {
        return weight_metadata_.RestoreSnapshot(snapshot);
    }
    void Clear() { weight_metadata_.Clear(); }

    WeightMetadataStore::Result<WeightRevisionLease> AcquireWeightRevisionLease(
        const AcquireWeightRevisionLeaseRequest& request);
    WeightMetadataStore::Result<WeightRevisionLease> RenewWeightRevisionLease(
        const RenewWeightRevisionLeaseRequest& request);
    WeightMetadataStore::Result<void> ReleaseWeightRevisionLease(
        const ReleaseWeightRevisionLeaseRequest& request);

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
    std::array<std::mutex, 4096> group_locks_;
};

}  // namespace mooncake
