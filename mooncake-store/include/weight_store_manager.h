#pragma once

#include <array>
#include <mutex>

#include "weight_store_backend.h"

namespace mooncake {

class WeightStoreManager {
   public:
    explicit WeightStoreManager(WeightStoreBackend& backend)
        : backend_(backend) {}

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
