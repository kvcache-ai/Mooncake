#pragma once

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace embedded {

class EmbeddedSnapshotCatalogStore : public SnapshotCatalogStore {
   public:
    EmbeddedSnapshotCatalogStore(SnapshotObjectStore* object_store,
                                 const std::string& cluster_id);

    ErrorCode Publish(const SnapshotDescriptor& snapshot) override;

    tl::expected<std::optional<SnapshotDescriptor>, ErrorCode> GetLatest()
        override;

    tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) override;

    ErrorCode Delete(const SnapshotId& snapshot_id) override;

    const std::string& GetSnapshotRoot() const override {
        return snapshot_root_;
    }

   private:
    SnapshotObjectStore* object_store_;
    std::string snapshot_root_;
};

}  // namespace embedded
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
