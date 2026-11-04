#pragma once

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <iterator>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "master_metric_manager.h"
#include "mutex.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

struct ObjectMetadata {
    // RAII-style metric management
    ~ObjectMetadata() {
        MasterMetricManager::instance().dec_key_count(1);
        if (soft_pin_timeout) {
            MasterMetricManager::instance().dec_soft_pin_key_count(1);
        }
    }

    ObjectMetadata() = delete;

    ObjectMetadata(
        const UUID& client_id_,
        const std::chrono::steady_clock::time_point put_start_time_,
        size_t value_length, std::vector<Replica>&& reps,
        bool enable_soft_pin)
        : client_id(client_id_),
          put_start_time(put_start_time_),
          size(value_length),
          lease_timeout(),
          soft_pin_timeout(std::nullopt),
          replicas_(std::move(reps)) {
        MasterMetricManager::instance().inc_key_count(1);
        if (enable_soft_pin) {
            soft_pin_timeout.emplace();
            MasterMetricManager::instance().inc_soft_pin_key_count(1);
        }
        MasterMetricManager::instance().observe_value_size(value_length);
    }

    ObjectMetadata(const ObjectMetadata&) = delete;
    ObjectMetadata& operator=(const ObjectMetadata&) = delete;
    ObjectMetadata(ObjectMetadata&&) = delete;
    ObjectMetadata& operator=(ObjectMetadata&&) = delete;

    const UUID client_id;
    const std::chrono::steady_clock::time_point put_start_time;
    const size_t size;

    mutable SpinLock lock;
    // Default constructor, creates a time_point representing
    // the Clock's epoch (i.e., time_since_epoch() is zero).
    mutable std::chrono::steady_clock::time_point lease_timeout
        GUARDED_BY(lock);  // hard lease
    mutable std::optional<std::chrono::steady_clock::time_point>
        soft_pin_timeout GUARDED_BY(lock);  // optional soft pin, only
                                            // set for vip objects

    void AddReplicas(std::vector<Replica>&& replicas) {
        replicas_.insert(replicas_.end(),
                         std::move_iterator(replicas.begin()),
                         std::move_iterator(replicas.end()));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        auto partition_point =
            std::partition(replicas_.begin(), replicas_.end(),
                           [pred_fn](const Replica& replica) {
                               return !pred_fn(replica);
                           });

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

    size_t EraseReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
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

    bool HasReplica(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    bool AllReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
    }

    size_t CountReplicas() const { return replicas_.size(); }

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

    bool EraseReplicaByID(const ReplicaID& id) {
        auto num_erased = EraseReplicas(
            [&id](const Replica& replica) { return replica.id() == id; });
        return num_erased > 0;
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

    // Grant a lease with timeout as now() + ttl, only update if the new
    // timeout is larger
    void GrantLease(const uint64_t ttl, const uint64_t soft_ttl) const {
        SpinLocker locker(&lock);
        std::chrono::steady_clock::time_point now =
            std::chrono::steady_clock::now();
        lease_timeout =
            std::max(lease_timeout, now + std::chrono::milliseconds(ttl));
        if (soft_pin_timeout) {
            soft_pin_timeout =
                std::max(*soft_pin_timeout,
                         now + std::chrono::milliseconds(soft_ttl));
        }
    }

    // Check if the lease has expired
    bool IsLeaseExpired() const {
        SpinLocker locker(&lock);
        return std::chrono::steady_clock::now() >= lease_timeout;
    }

    // Check if the lease has expired
    bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
        SpinLocker locker(&lock);
        return now >= lease_timeout;
    }

    // Check if is in soft pin status
    bool IsSoftPinned() const {
        SpinLocker locker(&lock);
        return soft_pin_timeout &&
               std::chrono::steady_clock::now() < *soft_pin_timeout;
    }

    // Check if is in soft pin status
    bool IsSoftPinned(std::chrono::steady_clock::time_point& now) const {
        SpinLocker locker(&lock);
        return soft_pin_timeout && now < *soft_pin_timeout;
    }

    // Check if the metadata is valid
    // Valid means it has at least one valid replica and size is greater
    // than 0
    bool IsValid() const {
        return size > 0 && HasReplica([](const Replica& replica) {
                   return !replica.is_memory_replica() ||
                          !replica.has_invalid_mem_handle();
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
    // Use the accessors to visit and modify the replicas.
    std::vector<Replica> replicas_;
};

}  // namespace mooncake
