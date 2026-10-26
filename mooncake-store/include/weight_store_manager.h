#pragma once

#include <array>
#include <mutex>
#include <memory>
#include <shared_mutex>
#include <unordered_map>

#include "weight_store_backend.h"

namespace mooncake {

namespace test {
class MasterServiceTestPeer;
}

class WeightStoreManager {
    friend class test::MasterServiceTestPeer;
    struct ImportActivity {
        std::mutex mutex;
        uint64_t last_write_ms{0};
        size_t in_flight{0};
        bool closing{false};
    };

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
        const WeightMetadataSnapshot& snapshot);
    void Clear();
    // Configure before starting the master workers; this is not a live setter.
    void SetImportIdleTimeoutMs(uint64_t timeout_ms) {
        import_idle_timeout_ms_ = timeout_ms;
    }
    class ImportWriteGuard {
       public:
        ImportWriteGuard() = default;
        explicit ImportWriteGuard(std::shared_ptr<ImportActivity> activity)
            : activity_(std::move(activity)) {}
        ImportWriteGuard(ImportWriteGuard&&) = default;
        ~ImportWriteGuard();
        void Accept() { accepted_ = true; }

       private:
        std::shared_ptr<ImportActivity> activity_;
        bool accepted_{false};
    };
    tl::expected<ImportWriteGuard, ErrorCode> TrackImportWrite(
        const TenantId& tenant, const std::string& group);
    void RecordImportWrite(const TenantId& tenant, const std::string& group);
    size_t ReconcileWeightMetadataStoreOnce(uint64_t now_ms, size_t limit);
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
    WeightMetadataStore::Result<WeightRevisionMetadata>
    ReconcileWeightRevisionBounded(
        const ReconcileWeightRevisionRequest& request, size_t* member_budget);
    WeightMetadataStore::Result<void> RemoveWeightMembers(
        const WeightRevisionMetadata& revision, size_t* member_budget);
    std::vector<std::string> SelectReconciliationMembers(
        const WeightRevisionMetadata& revision, size_t* member_budget);
    void ForgetReconciliationCursor(const WeightRevisionIdentity& identity);
    WeightMetadataStore::Result<WeightResidencyOperation>
    PersistAndPublishWeightOperationMutation(
        const WeightOperationMutation& mutation);
    WeightMetadataStore::Result<WeightRevisionLease>
    PersistAndPublishWeightLeaseMutation(const WeightLeaseMutation& mutation);
    WeightMetadataStore::Result<WeightRevisionMetadata>
    PersistAndPublishWeightMutation(const WeightMetadataMutation& mutation);
    std::unique_lock<std::mutex> LockGroup(
        const WeightRevisionIdentity& identity);
    WeightMetadataStore::Result<void> ValidateWeightGroupForCommit(
        const CommitWeightImportRequest& request) const;

    WeightStoreBackend& backend_;
    std::shared_ptr<ImportActivity> FindImportActivity(
        const TenantId& tenant, const std::string& group);
    void RegisterImportActivity(const WeightRevisionMetadata& metadata);
    uint64_t import_idle_timeout_ms_{5 * 60 * 1000};
    std::mutex import_activity_mutex_;
    std::unordered_map<std::string, std::shared_ptr<ImportActivity>>
        import_activity_;
    WeightMetadataStore weight_metadata_;
    std::shared_mutex mutation_mutex_;
    std::array<std::mutex, 4096> group_locks_;
    size_t weight_reconciliation_offset_{0};
    bool expire_leases_on_unit_budget_{true};
    std::mutex reconciliation_mutex_;
    std::mutex member_cursor_mutex_;
    // Retry offsets are local scheduling hints, not durable progress tokens.
    std::unordered_map<std::string, size_t> member_cursors_;
};

}  // namespace mooncake
