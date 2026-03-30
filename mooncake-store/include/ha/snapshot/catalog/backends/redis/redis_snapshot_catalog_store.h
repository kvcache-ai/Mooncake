#pragma once

#include <string>

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/object/snapshot_object_store.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

class RedisSnapshotCatalogStore final : public SnapshotCatalogStore {
   public:
    RedisSnapshotCatalogStore(SnapshotObjectStore* object_store,
                              std::string connstring,
                              ClusterNamespace cluster_namespace);

    ErrorCode Publish(const SnapshotDescriptor& snapshot) override;

    tl::expected<std::optional<SnapshotDescriptor>, ErrorCode> GetLatest()
        override;

    tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) override;

    ErrorCode Delete(const SnapshotId& snapshot_id) override;

   private:
    static ClusterNamespace ResolveClusterNamespace(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildLatestKey(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildIndexKey(const ClusterNamespace& cluster_namespace);

    SnapshotObjectStore* object_store_;
    std::string connstring_;
    ClusterNamespace cluster_namespace_;
    std::string latest_key_;
    std::string index_key_;
};

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
