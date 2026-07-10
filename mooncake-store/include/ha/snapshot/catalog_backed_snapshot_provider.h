#pragma once

#include <memory>

#include <ylt/util/tl/expected.hpp>

#include "ha/snapshot/snapshot_provider.h"
#include "master_config.h"

namespace mooncake {

// Build a standby-facing snapshot provider backed by the configured snapshot
// catalog + object store pair. The provider is intentionally snapshot-only: it
// loads the latest baseline but does not provide oplog catch-up state.
tl::expected<std::unique_ptr<SnapshotProvider>, ErrorCode>
CreateCatalogBackedSnapshotProvider(
    const MasterServiceSupervisorConfig& config);

}  // namespace mooncake
