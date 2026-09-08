#pragma once

// The object replica list: add / pop / erase / visit / query over the set of
// replicas attached to one object. ObjectMetadata owns one and forwards its
// replica API to it.

#include <algorithm>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <vector>

#include "replica.h"

namespace mooncake {

// The object replica list, extracted from ObjectMetadata so the
// container logic (add/pop/erase/visit/query) lives in one focused,
// independently testable place. ObjectMetadata owns one and forwards its
// replica API to it.
class ObjectReplicaList {
   public:
    ObjectReplicaList() = default;
    explicit ObjectReplicaList(std::vector<Replica>&& replicas)
        : replicas_(std::move(replicas)) {}

    void AddReplicas(std::vector<Replica>&& replicas) {
        replicas_.insert(replicas_.end(), std::move_iterator(replicas.begin()),
                         std::move_iterator(replicas.end()));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        auto partition_point = std::partition(
            replicas_.begin(), replicas_.end(),
            [pred_fn](const Replica& replica) { return !pred_fn(replica); });

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

    std::vector<Replica> PopReplicas() { return std::move(replicas_); }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        auto erased_replicas = PopReplicas(pred_fn);
        return erased_replicas.size();
    }

    size_t EraseReplicas() {
        auto erased_replicas = PopReplicas();
        return erased_replicas.size();
    }

    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& replica : replicas_) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        size_t num_visited = 0;

        for (auto& replica : replicas_) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas() const { return replicas_.size(); }

    const std::vector<Replica>& GetAllReplicas() const { return replicas_; }

    std::optional<ReplicaStatus> HasDiffRepStatus(ReplicaStatus status) const {
        for (const auto& replica : replicas_) {
            if (replica.status() != status) {
                return replica.status();
            }
        }
        return {};
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        const auto it =
            std::find_if(replicas_.begin(), replicas_.end(), pred_fn);
        return it != replicas_.end() ? &(*it) : nullptr;
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return GetFirstReplica(
            [&id](const Replica& replica) { return replica.id() == id; });
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return GetFirstReplica([&segment_name](const Replica& replica) {
            auto names = replica.get_segment_names();
            for (auto& name_opt : names) {
                if (name_opt == segment_name) {
                    return true;
                }
            }
            return false;
        });
    }

    std::vector<std::string> GetReplicaSegmentNames() const {
        std::vector<std::string> segment_names;
        for (const auto& replica : replicas_) {
            const auto& segment_name_options = replica.get_segment_names();
            for (const auto& segment_name_opt : segment_name_options) {
                if (segment_name_opt.has_value()) {
                    segment_names.push_back(segment_name_opt.value());
                }
            }
        }
        return segment_names;
    }

   private:
    std::vector<Replica> replicas_;
};

}  // namespace mooncake
