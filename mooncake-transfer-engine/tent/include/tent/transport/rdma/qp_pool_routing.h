// Copyright 2026 KVCache.AI
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

#ifndef TENT_QP_POOL_ROUTING_H
#define TENT_QP_POOL_ROUTING_H

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "params.h"
#include "slice.h"

namespace mooncake {
namespace tent {

struct QpPoolRoute {
    int qp_index = -1;
    int worker_id = -1;
};

struct QpPoolSliceGroup {
    std::string pool;
    std::vector<RdmaSlice*> slices;
};

inline std::string rdmaSliceQpPoolName(const RdmaSlice* slice) {
    return (slice && slice->task) ? slice->task->qp_pool : std::string();
}

inline int ownerWorkerForQpIndex(int qp_index, size_t num_workers,
                                 int fallback_worker) {
    if (qp_index < 0 || num_workers == 0) return fallback_worker;
    return qp_index % static_cast<int>(num_workers);
}

inline QpPoolRoute selectQpPoolRoute(
    const std::vector<QpPoolSegment>& segments, const std::string& qp_pool,
    int candidate, int total_qp, size_t num_workers, int fallback_worker) {
    if (total_qp <= 0) return QpPoolRoute{-1, fallback_worker};
    if (candidate < 0) candidate = 0;
    const int qp_index = selectQpInPool(segments, qp_pool, candidate, total_qp);
    if (num_workers == 0) return QpPoolRoute{qp_index, fallback_worker};

    const auto stable_route = [&](int worker_id) -> QpPoolRoute {
        int routed_qp = selectQpInPool(segments, qp_pool, worker_id, total_qp);
        int owner = ownerWorkerForQpIndex(routed_qp, num_workers,
                                          fallback_worker);
        return QpPoolRoute{routed_qp, owner == worker_id ? worker_id : -1};
    };

    int owner = ownerWorkerForQpIndex(qp_index, num_workers, fallback_worker);
    if (owner >= 0 && owner < static_cast<int>(num_workers)) {
        auto route = stable_route(owner);
        if (route.worker_id == owner) return route;
    }

    for (size_t offset = 0; offset < num_workers; ++offset) {
        int worker_id = static_cast<int>(
            (static_cast<size_t>(candidate) + offset) % num_workers);
        auto route = stable_route(worker_id);
        if (route.worker_id == worker_id) return route;
    }
    return QpPoolRoute{qp_index, fallback_worker};
}

inline std::vector<QpPoolSliceGroup> groupSlicesByQpPool(
    const std::vector<RdmaSlice*>& slices) {
    std::vector<QpPoolSliceGroup> groups;
    std::unordered_map<std::string, size_t> index_by_pool;

    for (auto* slice : slices) {
        auto pool = rdmaSliceQpPoolName(slice);
        auto [it, inserted] = index_by_pool.emplace(pool, groups.size());
        if (inserted) {
            groups.push_back(QpPoolSliceGroup{pool, {}});
        }
        groups[it->second].slices.push_back(slice);
    }
    return groups;
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_QP_POOL_ROUTING_H
