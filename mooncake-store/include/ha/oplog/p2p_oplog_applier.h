// mooncake-store/include/ha/oplog/p2p_oplog_applier.h
#pragma once

#include <cstdint>
#include <string>

#include "ha/oplog/oplog_applier.h"
#include "ha/oplog/p2p_oplog_types.h"
#include "ha/oplog/p2p_standby_metadata_store.h"

namespace mooncake {

/// P2P-specific OpLog applier.
///
/// Extends the main branch's OpLogApplier base class to handle currently
/// defined P2P-specific OpType values (ADD_REPLICA, REMOVE_REPLICA,
/// MOUNT_SEGMENT, UNMOUNT_SEGMENT, REMOVE_ALL, REGISTER_CLIENT,
/// UNREGISTER_CLIENT). The base class handles PUT_END, PUT_REVOKE, and REMOVE
/// using the generic MetadataStore interface.
///
/// P2POpLogApplier delegates P2P OpTypes to P2PStandbyMetadataStore which
/// maintains both object metadata (via MetadataStore) and P2P-specific state
/// (client registrations, segment mappings).
///
class P2POpLogApplier : public OpLogApplier {
   public:
    /// Constructor.
    /// @param p2p_store       The P2PStandbyMetadataStore to apply ops to.
    ///                        Must outlive this applier.
    /// @param cluster_id      Cluster ID for validation.
    /// @param oplog_store     Optional OpLogStore for gap resolution.
    explicit P2POpLogApplier(P2PStandbyMetadataStore* p2p_store,
                             const std::string& cluster_id = std::string(),
                             OpLogStore* oplog_store = nullptr);

    /// Get the underlying P2P metadata store.
    P2PStandbyMetadataStore* GetP2PMetadataStore() const { return p2p_store_; }

   protected:
    bool ApplyCustomOpLogEntry(const OpLogEntry& entry) override;
    bool IsBestEffortOpLogEntry(const OpLogEntry& entry) const override;

   private:
    // Apply individual P2P OpTypes. Return true on success.
    bool ApplyAddReplica(const OpLogEntry& entry);
    bool ApplyRemoveReplica(const OpLogEntry& entry);
    bool ApplyMountSegment(const OpLogEntry& entry);
    bool ApplyUnmountSegment(const OpLogEntry& entry);
    bool ApplyRemoveAll(const OpLogEntry& entry);
    bool ApplyRegisterClient(const OpLogEntry& entry);
    bool ApplyUnregisterClient(const OpLogEntry& entry);

    P2PStandbyMetadataStore* p2p_store_;
};

}  // namespace mooncake
