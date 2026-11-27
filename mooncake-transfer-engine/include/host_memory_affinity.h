// Copyright 2026 Mooncake contributors
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include "common.h"

#include <sched.h>
#include <algorithm>
#include <memory>

namespace mooncake {

// An operation-local plan: respect the caller's CPU and memory cpusets, and
// spread contiguous partitions across the available NUMA nodes. Affinity is a
// first-touch hint; it does not change a caller's mbind policy or migrate
// pages.
class HostMemoryAffinityPlan {
   public:
    explicit HostMemoryAffinityPlan(bool enabled) {
        cpu_set_t allowed;
        if (!enabled || numa_available() < 0 ||
            sched_getaffinity(0, sizeof(allowed), &allowed) != 0)
            return;
        using Mask = std::unique_ptr<bitmask, decltype(&numa_bitmask_free)>;
        Mask memory_nodes(numa_get_mems_allowed(), numa_bitmask_free);
        if (!memory_nodes) return;
        // Serialize libnuma's lazily initialized topology cache (see common.h).
        std::lock_guard<std::mutex> guard(numaNodeCpuCacheMutex());
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) {
            if (!CPU_ISSET(cpu, &allowed)) continue;
            const int node = numa_node_of_cpu(cpu);
            if (node < 0 || !numa_bitmask_isbitset(memory_nodes.get(), node))
                continue;
            auto it =
                std::find_if(nodes_.begin(), nodes_.end(),
                             [node](const Node &n) { return n.id == node; });
            if (it == nodes_.end()) {
                nodes_.push_back({node, {cpu}});
            } else {
                it->cpus.push_back(cpu);
            }
        }
        std::sort(nodes_.begin(), nodes_.end(),
                  [](const Node &a, const Node &b) { return a.id < b.id; });
    }

    int cpuForPartition(size_t index, size_t count,
                        void *resident_sample = nullptr) const {
        if (nodes_.empty() || count == 0 || index >= count) return -1;
        const size_t group = index * nodes_.size() / count;
        const size_t first =
            (group * count + nodes_.size() - 1) / nodes_.size();
        const Node *node = &nodes_[group];
        if (resident_sample) {
            // A query only: never fault or move memory. Prefer existing
            // placement when a buffer was populated/bound by its caller.
            const long page = sysconf(_SC_PAGESIZE);
            if (page > 0) {
                void *sample = reinterpret_cast<void *>(
                    reinterpret_cast<uintptr_t>(resident_sample) /
                    static_cast<size_t>(page) * static_cast<size_t>(page));
                int status = -1;
                if (numa_move_pages(0, 1, &sample, nullptr, &status, 0) == 0 &&
                    status >= 0) {
                    for (const Node &candidate : nodes_) {
                        if (candidate.id == status) {
                            node = &candidate;
                            break;
                        }
                    }
                }
            }
        }
        return node->cpus[(index - first) % node->cpus.size()];
    }

   private:
    struct Node {
        int id;
        std::vector<int> cpus;
    };
    std::vector<Node> nodes_;
};

// The caller participates in the worker pool too. Always restore its original
// mask, including on failure, so later workers do not inherit one pinned CPU.
class ScopedHostMemoryAffinity {
   public:
    explicit ScopedHostMemoryAffinity(int cpu) {
        if (cpu < 0 || cpu >= CPU_SETSIZE ||
            sched_getaffinity(0, sizeof(saved_), &saved_) != 0)
            return;
        cpu_set_t target;
        CPU_ZERO(&target);
        CPU_SET(cpu, &target);
        changed_ = sched_setaffinity(0, sizeof(target), &target) == 0;
    }
    ~ScopedHostMemoryAffinity() {
        if (changed_ && sched_setaffinity(0, sizeof(saved_), &saved_) != 0)
            PLOG(WARNING) << "Failed to restore host memory worker affinity";
    }
    ScopedHostMemoryAffinity(const ScopedHostMemoryAffinity &) = delete;
    ScopedHostMemoryAffinity &operator=(const ScopedHostMemoryAffinity &) =
        delete;

   private:
    cpu_set_t saved_{};
    bool changed_ = false;
};

}  // namespace mooncake
