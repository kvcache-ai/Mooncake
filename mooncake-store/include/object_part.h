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
        : length_(length), replicas_(std::move(replicas)) {}

    size_t length() const { return length_; }

    std::optional<uint64_t> checksum() const { return checksum_; }

    void SetChecksum(std::optional<uint64_t> checksum) {
        checksum_ = std::move(checksum);
    }

    // Elements may be modified; adding or removing goes through the calls
    // below.
    std::span<Replica> replicas() { return replicas_; }
    std::span<const Replica> replicas() const { return replicas_; }

    void AddReplicas(std::vector<Replica>&& replicas) {
        replicas_.insert(replicas_.end(), std::move_iterator(replicas.begin()),
                         std::move_iterator(replicas.end()));
    }

    // What comes back is out of the part: the caller owns those replicas and
    // decides how to isolate, defer or release them.
    template <typename Pred>
    [[nodiscard]] std::vector<Replica> PopReplicas(Pred&& pred) {
        auto partition_point = std::partition(
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
        return std::move(replicas_);
    }

   private:
    const size_t length_;
    std::optional<uint64_t> checksum_;
    std::vector<Replica> replicas_;
};

}  // namespace mooncake
