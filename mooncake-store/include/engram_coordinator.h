#pragma once

#include <algorithm>
#include <chrono>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/reflection/user_reflect_macro.hpp>

namespace mooncake {

// Engram control-plane records contain no weight bytes or device addresses
// until an owner explicitly publishes its backend bootstrap metadata.
struct EngramControlRequest {
    std::string action;
    std::string pool;
    std::string node;
    std::string incarnation;
    std::string mode;
    std::string group;
    int shard = -1;
    int group_size = 0;
    int num_callers = 0;
    std::string boot;
    uint64_t generation = 0;
    std::string key;
    std::string value;
};
YLT_REFL(EngramControlRequest, action, pool, node, incarnation, mode, group,
         shard, group_size, num_callers, boot, generation, key, value);

struct EngramControlResponse {
    std::string error;
    bool retry = false;
    bool full = false;
    bool found = false;
    std::string group;
    int shard = -1;
    int group_size = 0;
    std::string boot;
    uint64_t generation = 0;
    std::string value;
};
YLT_REFL(EngramControlResponse, error, retry, full, found, group, shard,
         group_size, boot, generation, value);

// Fixed node membership, rolling admission into independent groups, and
// generation-fenced bootstrap records. State intentionally does not survive a
// Master restart. Rejoining preserves placement but requires all owners to
// rebuild their data-plane communicators before the tables become readable.
class EngramCoordinator {
    using Clock = std::chrono::steady_clock;
    static constexpr auto kNodeLease = std::chrono::seconds(10);
    struct Member {
        std::string node;
        std::string incarnation;
        Clock::time_point seen;
        uint64_t generation = 0;
    };
    struct Group {
        std::string pool;
        std::string mode;
        std::string name;
        int num_callers;
        uint64_t generation = 1;
        bool rebuilding = false;
        std::vector<Member> members;
        std::unordered_map<std::string, std::string> records;
    };
    std::mutex mutex_;
    const std::string boot_ = std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count());
    std::map<std::string, Group> groups_;
    std::unordered_map<std::string, std::string> placements_;
    std::unordered_map<std::string, std::vector<std::string>> admission_;
    uint64_t next_group_ = 0;

    static bool Full(const Group& group, Clock::time_point now) {
        for (const auto& member : group.members) {
            if (member.node.empty() || member.generation != group.generation ||
                now - member.seen >= kNodeLease)
                return false;
        }
        return true;
    }
    EngramControlResponse Describe(const Group& group, int shard,
                                   Clock::time_point now) const {
        EngramControlResponse response;
        response.full = Full(group, now);
        response.group = group.name;
        response.shard = shard;
        response.group_size = static_cast<int>(group.members.size());
        response.boot = boot_;
        response.generation = group.generation;
        return response;
    }
    static EngramControlResponse Error(const std::string& message,
                                       bool retry = false) {
        EngramControlResponse response;
        response.error = message;
        response.retry = retry;
        return response;
    }

   public:
    EngramControlResponse Handle(const EngramControlRequest& request,
                                 Clock::time_point now = Clock::now()) {
        std::lock_guard lock(mutex_);
        if (request.pool.empty() || request.pool.size() > 256 ||
            request.node.empty() || request.node.size() > 512 ||
            request.incarnation.empty() || request.incarnation.size() > 256) {
            return Error("Invalid Engram pool/node/incarnation");
        }
        // pool is a canonical digest; node identities are opaque strings.
        const auto node_key = request.pool + ":" + request.node;
        if (request.action == "join") {
            if ((request.mode != "auto" && request.mode != "manual") ||
                request.group_size < 1 || request.group_size > 64 ||
                request.num_callers < 1 || request.num_callers > 256 ||
                (request.mode == "manual" &&
                 (request.group.empty() || request.group.size() > 128 ||
                  request.shard < 0 || request.shard >= request.group_size))) {
                return Error("Invalid Engram assignment configuration");
            }
            std::string key;
            int shard = request.shard;
            auto previous = placements_.find(node_key);
            if (previous != placements_.end()) {
                key = previous->second;
                auto& group = groups_.at(key);
                if (group.mode != request.mode ||
                    group.members.size() !=
                        static_cast<size_t>(request.group_size) ||
                    group.num_callers != request.num_callers ||
                    (request.mode == "manual" &&
                     (group.name != request.group ||
                      group.members[shard].node != request.node))) {
                    return Error(
                        "Existing Engram placement conflicts with "
                        "configuration");
                }
                for (size_t i = 0; i < group.members.size(); ++i) {
                    if (group.members[i].node == request.node) shard = i;
                }
            } else {
                if (request.mode == "auto") {
                    for (const auto& candidate : admission_[request.pool]) {
                        const auto& group = groups_.at(candidate);
                        if (std::any_of(group.members.begin(),
                                        group.members.end(),
                                        [](const Member& m) {
                                            return m.node.empty();
                                        }) &&
                            group.members.size() ==
                                static_cast<size_t>(request.group_size) &&
                            group.num_callers == request.num_callers) {
                            key = candidate;
                            break;
                        }
                    }
                    if (key.empty()) {
                        key = request.pool +
                              ":auto:" + std::to_string(next_group_++);
                        Group group;
                        group.pool = request.pool;
                        group.mode = "auto";
                        group.name = key.substr(request.pool.size() + 1);
                        group.num_callers = request.num_callers;
                        group.members.resize(request.group_size);
                        groups_.emplace(key, std::move(group));
                        admission_[request.pool].push_back(key);
                    }
                    const auto& group = groups_.at(key);
                    for (size_t i = 0; i < group.members.size(); ++i) {
                        if (group.members[i].node.empty()) {
                            shard = i;
                            break;
                        }
                    }
                } else {
                    key = request.pool + ":manual:" + request.group;
                    if (!groups_.contains(key)) {
                        Group group;
                        group.pool = request.pool;
                        group.mode = "manual";
                        group.name = request.group;
                        group.num_callers = request.num_callers;
                        group.members.resize(request.group_size);
                        groups_.emplace(key, std::move(group));
                    }
                }
            }
            auto& group = groups_.at(key);
            if (group.members.size() !=
                    static_cast<size_t>(request.group_size) ||
                group.num_callers != request.num_callers) {
                return Error("Engram group size/caller count mismatch");
            }
            auto& member = group.members[shard];
            if (!member.node.empty() && member.node != request.node) {
                return Error("Engram shard already belongs to another node");
            }
            if (!member.incarnation.empty() &&
                member.incarnation != request.incarnation) {
                if (now - member.seen < kNodeLease) {
                    return Error(
                        "Previous Engram node incarnation is still leased",
                        true);
                }
                if (!group.rebuilding) {
                    ++group.generation;
                    group.records.clear();
                    group.rebuilding = true;
                }
            }
            member = {request.node, request.incarnation, now, group.generation};
            if (Full(group, now)) group.rebuilding = false;
            placements_[node_key] = key;
            return Describe(group, shard, now);
        }
        auto previous = placements_.find(node_key);
        if (previous == placements_.end()) return Error("Unknown Engram node");
        auto& group = groups_.at(previous->second);
        if (request.boot != boot_ || request.generation != group.generation) {
            return Error("Engram group changed; rebuild every group member");
        }
        int shard = -1;
        for (size_t i = 0; i < group.members.size(); ++i) {
            const auto& member = group.members[i];
            if (member.node == request.node &&
                member.incarnation == request.incarnation)
                shard = i;
        }
        if (shard < 0) return Error("Engram node incarnation was replaced");
        group.members[shard].seen = now;
        auto response = Describe(group, shard, now);
        if (request.action == "heartbeat") return response;
        if (request.key.empty() || request.key.size() > 1024) {
            return Error("Invalid Engram metadata key");
        }
        auto entry = group.records.find(request.key);
        if (request.action == "get") {
            response.found = entry != group.records.end();
            if (response.found) response.value = entry->second;
        } else if (request.action == "put") {
            if (request.value.size() > 1024 * 1024) {
                return Error("Engram metadata record exceeds 1 MiB");
            }
            if (entry != group.records.end() &&
                entry->second != request.value) {
                return Error("Conflicting immutable Engram metadata record");
            }
            group.records.emplace(request.key, request.value);
        } else {
            return Error("Unknown Engram control action");
        }
        return response;
    }
};

}  // namespace mooncake
