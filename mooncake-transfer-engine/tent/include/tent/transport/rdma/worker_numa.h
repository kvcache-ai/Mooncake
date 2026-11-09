// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_RDMA_WORKER_NUMA_H
#define TENT_RDMA_WORKER_NUMA_H

#include <limits>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {

// Parse strictly: Config::get<T>() otherwise silently defaults on bad types.
inline Status parseWorkerNumaConfig(const Config& conf, int worker_count,
                                    std::vector<int>& nodes,
                                    bool& prefer_local) {
    const auto raw_nodes =
        conf.get<json>("transports/rdma/workers/numa_nodes", json::array());
    const auto raw_local =
        conf.get<json>("transports/rdma/workers/prefer_local", false);
    if (!raw_local.is_boolean())
        return Status::InvalidArgument("workers.prefer_local must be boolean");
    if (!raw_nodes.is_array() ||
        (!raw_nodes.empty() && raw_nodes.size() != 1 &&
         raw_nodes.size() != static_cast<size_t>(worker_count)))
        return Status::InvalidArgument(
            "workers.numa_nodes must be empty, one node, or one node per "
            "worker");
    std::vector<int> parsed;
    for (const auto& node : raw_nodes) {
        if (!node.is_number_integer() || node < 0 ||
            node > std::numeric_limits<int>::max())
            return Status::InvalidArgument(
                "workers.numa_nodes entries must be nonnegative integers");
        parsed.push_back(node.get<int>());
    }
    nodes = std::move(parsed);
    prefer_local = raw_local.get<bool>();
    return Status::OK();
}

// Preserve the submitter's round-robin sequence within the local subset.
// An empty subset means locality is unknown/unavailable: keep the old route.
inline int selectSubmissionWorker(size_t sequence, int worker_count,
                                  const std::vector<int>& local_workers) {
    return local_workers.empty()
               ? sequence % worker_count
               : local_workers[sequence % local_workers.size()];
}

// Unlike bindToSocket(), this opt-in path handles sparse node IDs and respects
// the inherited CPU affinity. Never widen a user's taskset/cpuset restrictions.
inline int bindRdmaWorkerToNode(int node) {
    if (numa_available() < 0 || node < 0 || node > numa_max_node() ||
        !numa_bitmask_isbitset(numa_all_nodes_ptr, node))
        return -1;
    auto* cpus = numa_allocate_cpumask();
    auto* allowed = numa_allocate_cpumask();
    if (!cpus || !allowed) {
        if (cpus) numa_free_cpumask(cpus);
        if (allowed) numa_free_cpumask(allowed);
        return -1;
    }
    int result;
    {
        std::lock_guard<std::mutex> guard(numaNodeCpuCacheMutex());
        result = numa_node_to_cpus(node, cpus);
    }
    const auto bytes = numa_bitmask_nbytes(allowed);
    if (result == 0)
        result = pthread_getaffinity_np(
            pthread_self(), bytes,
            reinterpret_cast<cpu_set_t*>(allowed->maskp));
    if (result == 0) {
        for (unsigned int cpu = 0; cpu < cpus->size; ++cpu)
            if (!numa_bitmask_isbitset(allowed, cpu))
                numa_bitmask_clearbit(cpus, cpu);
        result = numa_bitmask_weight(cpus) == 0
                     ? -1
                     : pthread_setaffinity_np(
                           pthread_self(), numa_bitmask_nbytes(cpus),
                           reinterpret_cast<cpu_set_t*>(cpus->maskp));
    }
    numa_free_cpumask(cpus);
    numa_free_cpumask(allowed);
    return result;
}

}  // namespace tent
}  // namespace mooncake
#endif  // TENT_RDMA_WORKER_NUMA_H
