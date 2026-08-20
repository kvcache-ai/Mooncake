#pragma once

#include <cstdint>

#include "p2p/ha/oplog/p2p_standby_metadata_store.h"

namespace mooncake {

/**
 * @brief Type-erased payload carried in ha::PromotionContext for the P2P
 *        deployment mode. Produced by P2PStandbyController::PromoteStandbyAndExport
 *        and consumed by MasterServiceSupervisor when wiring the new P2P
 *        primary (RestoreFromStandbyMetadata).
 */
struct P2PPromotionData {
    P2PStandbyMetadataStore::ExportedMetadata metadata;
    uint64_t applied_sequence_id{0};
};

}  // namespace mooncake
