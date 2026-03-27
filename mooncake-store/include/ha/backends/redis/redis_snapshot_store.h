#pragma once

#include <string>

#include "ha/snapshot_store.h"
#include "serialize/serializer_backend.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

class RedisSnapshotStore final : public SnapshotStore {
   public:
    RedisSnapshotStore(SerializerBackend* payload_backend,
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

    SerializerBackend* payload_backend_;
    std::string connstring_;
    ClusterNamespace cluster_namespace_;
    std::string latest_key_;
    std::string index_key_;
};

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
