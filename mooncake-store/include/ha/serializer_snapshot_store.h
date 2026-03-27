#pragma once

#include "ha/snapshot_store.h"
#include "serialize/serializer_backend.h"

namespace mooncake {
namespace ha {

class SerializerSnapshotStore : public SnapshotStore {
   public:
    explicit SerializerSnapshotStore(SerializerBackend* backend);

    ErrorCode Publish(const SnapshotDescriptor& snapshot) override;

    tl::expected<std::optional<SnapshotDescriptor>, ErrorCode> GetLatest()
        override;

    tl::expected<std::vector<SnapshotDescriptor>, ErrorCode> List(
        size_t limit) override;

    ErrorCode Delete(const SnapshotId& snapshot_id) override;

   private:
    SerializerBackend* backend_;
};

}  // namespace ha
}  // namespace mooncake
