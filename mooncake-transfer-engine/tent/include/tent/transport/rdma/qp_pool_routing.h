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
    const int qp_index = selectQpInPool(segments, qp_pool, candidate, total_qp);
    return QpPoolRoute{
        qp_index, ownerWorkerForQpIndex(qp_index, num_workers, fallback_worker)};
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
