#pragma once

// ObjectPart: one byte range of an object's content, written on its own.
//
// An object's content is an ordered sequence of parts. Each part is a range
// written independently of the others, so it carries its own length and its own
// checksum, and it holds the replicas of that range: which part a replica sits
// in, rather than a flag on the replica, is what says who owns it. A part's
// position in the object is its index in the owner's container.
//
// Every object has exactly one part today.

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

// The part owns its replicas, so the operations that change the set of them are
// the part's own. Everything else reads through replicas().
class ObjectPart {
   public:
    ObjectPart() = delete;

    // A part owns replicas and a replica is not copyable, so a part is not
    // either: the traits say so instead of the copy failing inside the STL
    // when something tries. The length is immutable, so a part moves but is
    // never assigned over.
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

    // The replicas holding this part's range. An element may be modified
    // through the span; a replica is added or removed only by the two calls
    // below, which say that the set is changing.
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
    // Immutable for the part's lifetime, as the object's size was before the
    // content moved here.
    const size_t length_;
    std::optional<uint64_t> checksum_;
    // Read and modify through replicas(), resize through AddReplicas and
    // PopReplicas.
    std::vector<Replica> replicas_;
};

}  // namespace mooncake
