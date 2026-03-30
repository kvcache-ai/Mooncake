#pragma once

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace embedded {

class EmbeddedSnapshotCatalogStore : public SnapshotCatalogStore {
   public:
    explicit EmbeddedSnapshotCatalogStore(SnapshotObjectStore* object_store);

    ErrorCode Publish(const SnapshotDescriptor& snapshot) override;

    tl::expected<std::optional<SnapshotDescriptor>, ErrorCode> GetLatest()
        override;

    tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) override;

    ErrorCode Delete(const SnapshotId& snapshot_id) override;

   private:
    SnapshotObjectStore* object_store_;
};

}  // namespace embedded
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
