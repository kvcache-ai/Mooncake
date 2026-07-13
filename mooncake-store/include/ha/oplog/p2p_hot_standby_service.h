// mooncake-store/include/ha/oplog/p2p_hot_standby_service.h
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "ha/oplog/oplog_replicator.h"
#include "ha/oplog/oplog_store_factory.h"
#include "ha/oplog/p2p_oplog_applier.h"
#include "ha/oplog/p2p_standby_metadata_store.h"
#include "standby_state_machine.h"
#include "types.h"

namespace mooncake {

struct P2PHotStandbyConfig {
    std::string cluster_id;
    OpLogStoreType oplog_store_type{kDefaultOpLogStoreType};
    std::string oplog_store_root_dir{kDefaultOpLogRootDir};
    int oplog_poll_interval_ms{kDefaultOpLogPollIntervalMs};
};

struct P2PStandbySyncStatus {
    uint64_t applied_seq_id{0};
    uint64_t primary_seq_id{0};
    uint64_t lag_entries{0};
    bool is_connected{false};
    StandbyState state{StandbyState::STOPPED};
    std::chrono::milliseconds time_in_state{0};
};

// NOTE: P2PHotStandbyService intentionally does not inherit from the
// centralized HotStandbyService. The centralized service owns centralized
// metadata/apply/export semantics, while P2P promotion needs
// P2PStandbyMetadataStore, P2POpLogApplier, and an export shape that includes
// clients, segments, objects, and replicas. This class reuses the shared
// lower-level components (StandbyStateMachine, OpLogStoreFactory,
// OpLogChangeNotifier, and OpLogReplicator) and keeps only the orchestration
// layer P2P-specific.
class P2PHotStandbyService {
   public:
    explicit P2PHotStandbyService(P2PHotStandbyConfig config);
    ~P2PHotStandbyService();

    P2PHotStandbyService(const P2PHotStandbyService&) = delete;
    P2PHotStandbyService& operator=(const P2PHotStandbyService&) = delete;

    ErrorCode Start(uint64_t baseline_sequence_id = 0);
    void Stop();
    ErrorCode Promote();

    P2PStandbySyncStatus GetSyncStatus() const;
    bool IsReadyForPromotion() const;
    uint64_t GetLatestAppliedSequenceId() const;

    P2PStandbyMetadataStore::ExportedMetadata ExportMetadata() const;
    bool WaitForAppliedSequence(
        uint64_t sequence_id,
        std::chrono::milliseconds timeout = std::chrono::seconds(5)) const;

    StandbyState GetState() const { return state_machine_.GetState(); }
    P2PStandbyMetadataStore* GetMetadataStore() const {
        return metadata_store_.get();
    }

   private:
    ErrorCode StartOplogFollowingLocked(uint64_t baseline_sequence_id);
    void ResetOplogFollowingLocked();
    ErrorCode FinalCatchUpForPromotionLocked(uint64_t current_applied_seq_id);
    uint64_t GetLocalLastAppliedSequenceIdLocked() const;
    void OnWatcherEvent(StandbyEvent event);

    P2PHotStandbyConfig config_;

    std::unique_ptr<P2PStandbyMetadataStore> metadata_store_;
    std::unique_ptr<P2POpLogApplier> oplog_applier_;
    std::shared_ptr<OpLogStore> watcher_oplog_store_;
    std::unique_ptr<OpLogChangeNotifier> oplog_change_notifier_;
    std::unique_ptr<OpLogReplicator> oplog_replicator_;

    StandbyStateMachine state_machine_;
    mutable std::mutex mutex_;
};

}  // namespace mooncake
