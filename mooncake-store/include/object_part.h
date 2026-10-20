#pragma once

// ObjectPart: one byte range of an object's content, written independently.
// An object's content is an ordered sequence of parts; a part's position is its
// index in the owner's container, and the part owns the replicas of that range.
// Each part carries its own length and checksum.

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

    // True when the part holds replicas and all of them are still PROCESSING.
    bool HasOnlyProcessingReplicas() const {
        return !replicas_.empty() &&
               std::ranges::all_of(replicas_, &Replica::is_processing);
    }

    // True when some replica of the part is still PROCESSING, which is what an
    // open write attempt holds until it is completed or given up. A part can
    // hold replicas of several types, and one type can be finished while
    // another is still being written.
    bool HasProcessingReplica() const {
        return std::ranges::any_of(replicas_, &Replica::is_processing);
    }

    // Elements may be modified; adding or removing goes through the calls
    // below.
    std::span<Replica> replicas() { return replicas_; }
    std::span<const Replica> replicas() const { return replicas_; }

    // Whether one of the part's replicas can be read, by the caller's rule: the
    // part does not own that rule, the service does.
    template <typename IsReadable>
    bool HasReadableReplica(IsReadable&& is_readable) const {
        return std::ranges::any_of(replicas_,
                                   std::forward<IsReadable>(is_readable));
    }

    // The descriptors of the replicas `try_descriptor` accepts, which fills a
    // descriptor as it decides.
    template <typename TryDescriptor>
    std::vector<Replica::Descriptor> ReadableDescriptors(
        TryDescriptor&& try_descriptor) const {
        std::vector<Replica::Descriptor> descriptors;
        descriptors.reserve(replicas_.size());
        for (const auto& replica : replicas_) {
            Replica::Descriptor descriptor;
            if (try_descriptor(replica, descriptor)) {
                descriptors.push_back(std::move(descriptor));
            }
        }
        return descriptors;
    }

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
