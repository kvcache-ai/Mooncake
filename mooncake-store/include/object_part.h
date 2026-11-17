#pragma once

// ObjectPart: one byte range of an object's content, written independently.
// An object's content is an ordered sequence of parts; a part's position is its
// index in the owner's container, and the part owns the replicas of that range.
// Each part carries its own length and checksum. Every object has one part
// today.

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include <boost/container/small_vector.hpp>

#include "replica.h"

namespace mooncake {

class ObjectPart {
   public:
    ObjectPart() = delete;

    ObjectPart(const ObjectPart&) = delete;
    ObjectPart& operator=(const ObjectPart&) = delete;
    ObjectPart(ObjectPart&&) noexcept = default;
    ObjectPart& operator=(ObjectPart&&) = delete;

    ObjectPart(size_t length, std::vector<Replica>&& replicas)
        : length_(length),
          replicas_(std::move_iterator(replicas.begin()),
                    std::move_iterator(replicas.end())) {}

    size_t length() const { return length_; }

    std::optional<uint64_t> checksum() const { return checksum_; }

    void SetChecksum(std::optional<uint64_t> checksum) {
        checksum_ = std::move(checksum);
    }

    // Elements may be modified; adding or removing goes through the calls
    // below. The first replica sits in the part itself, so the common one
    // replica case allocates nothing.
    std::span<Replica> replicas() {
        return {replicas_.data(), replicas_.size()};
    }
    std::span<const Replica> replicas() const {
        return {replicas_.data(), replicas_.size()};
    }

    void AddReplicas(std::vector<Replica>&& replicas) {
        replicas_.insert(replicas_.end(), std::move_iterator(replicas.begin()),
                         std::move_iterator(replicas.end()));
    }

    // Inserts a newly registered LOCAL_DISK replica ahead of the existing
    // ones. A restarted client registers under a new UUID while the old
    // owner's replica stays until its lifecycle cleanup; readers take the
    // first LOCAL_DISK replica, so the newest registration must come first.
    void AddLocalDiskReplicaFirst(Replica&& replica) {
        auto pos = std::find_if(replicas_.begin(), replicas_.end(),
                                &Replica::fn_is_local_disk_replica);
        replicas_.insert(pos, std::move(replica));
    }

    // What comes back is out of the part: the caller owns those replicas and
    // decides how to isolate, defer or release them.
    template <typename Pred>
    [[nodiscard]] std::vector<Replica> PopReplicas(Pred&& pred) {
        // Stable, so the remaining replicas keep their order (see
        // AddLocalDiskReplicaFirst).
        auto partition_point = std::stable_partition(
            replicas_.begin(), replicas_.end(),
            [&pred](const Replica& replica) { return !pred(replica); });

        std::vector<Replica> popped_replicas;
        if (partition_point != replicas_.end()) {
            popped_replicas.reserve(
                std::distance(partition_point, replicas_.end()));
            std::move(partition_point, replicas_.end(),
                      std::back_inserter(popped_replicas));
            replicas_.erase(partition_point, replicas_.end());
        }

        return popped_replicas;
    }

    [[nodiscard]] std::vector<Replica> PopReplicas() {
        std::vector<Replica> popped_replicas;
        popped_replicas.reserve(replicas_.size());
        std::move(replicas_.begin(), replicas_.end(),
                  std::back_inserter(popped_replicas));
        replicas_.clear();
        return popped_replicas;
    }

   private:
    const size_t length_;
    std::optional<uint64_t> checksum_;
    boost::container::small_vector<Replica, 1> replicas_;
};

}  // namespace mooncake
