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
#include <variant>
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
        : length_(length), replicas_(std::move(replicas)) {
        InlineSingleReplica();
    }

    size_t length() const { return length_; }

    std::optional<uint64_t> checksum() const { return checksum_; }

    void SetChecksum(std::optional<uint64_t> checksum) {
        checksum_ = std::move(checksum);
    }

    // Elements may be modified; adding or removing goes through the calls
    // below. The first replica sits in the part itself, so the common one
    // replica case allocates nothing.
    std::span<Replica> replicas() {
        if (auto* one = std::get_if<Replica>(&replicas_)) return {one, 1};
        return std::get<std::vector<Replica>>(replicas_);
    }
    std::span<const Replica> replicas() const {
        if (auto* one = std::get_if<Replica>(&replicas_)) return {one, 1};
        return std::get<std::vector<Replica>>(replicas_);
    }

    void AddReplicas(std::vector<Replica>&& added) {
        if (added.empty()) return;
        if (auto* one = std::get_if<Replica>(&replicas_)) {
            std::vector<Replica> many;
            many.reserve(1 + added.size());
            many.push_back(std::move(*one));
            replicas_.emplace<std::vector<Replica>>(std::move(many));
        }
        auto& many = std::get<std::vector<Replica>>(replicas_);
        if (many.empty()) {
            many = std::move(added);
            InlineSingleReplica();
        } else {
            many.insert(many.end(), std::make_move_iterator(added.begin()),
                        std::make_move_iterator(added.end()));
        }
    }

    // What comes back is out of the part: the caller owns those replicas and
    // decides how to isolate, defer or release them.
    template <typename Pred>
    [[nodiscard]] std::vector<Replica> PopReplicas(Pred&& pred) {
        if (auto* one = std::get_if<Replica>(&replicas_)) {
            return pred(*one) ? PopReplicas() : std::vector<Replica>{};
        }
        auto& many = std::get<std::vector<Replica>>(replicas_);
        const auto keep_end = std::partition(
            many.begin(), many.end(),
            [&](const Replica& replica) { return !pred(replica); });
        if (keep_end == many.begin()) return PopReplicas();
        std::vector<Replica> popped(std::make_move_iterator(keep_end),
                                    std::make_move_iterator(many.end()));
        many.erase(keep_end, many.end());
        InlineSingleReplica();
        return popped;
    }

    [[nodiscard]] std::vector<Replica> PopReplicas() {
        if (auto* many = std::get_if<std::vector<Replica>>(&replicas_)) {
            return std::exchange(*many, {});
        }
        std::vector<Replica> popped;
        popped.push_back(std::move(std::get<Replica>(replicas_)));
        replicas_.emplace<std::vector<Replica>>();
        return popped;
    }

   private:
    void InlineSingleReplica() {
        auto& many = std::get<std::vector<Replica>>(replicas_);
        if (many.size() == 1) {
            // Move out before emplace destroys the vector holding the element.
            Replica only = std::move(many.front());
            replicas_.emplace<Replica>(std::move(only));
        }
    }
    const size_t length_;
    std::optional<uint64_t> checksum_;
    std::variant<Replica, std::vector<Replica>> replicas_;
};

}  // namespace mooncake
