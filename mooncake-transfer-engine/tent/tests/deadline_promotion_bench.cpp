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

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <vector>

#include "tent/runtime/admission_queue.h"

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kNowNs = 1'000'000;

QueueOwnerInput makeOwner(size_t task_id, bool urgent) {
    QueueOwnerInput owner;
    owner.owner_task_id = task_id;
    owner.request.opcode = Request::READ;
    owner.request.target_id = 1;
    owner.request.length = 4096;
    owner.request.deadline_ns = urgent ? kNowNs + 10'000 : kNowNs + 1'000'000;
    return owner;
}

double percentile(std::vector<double> samples, double ratio) {
    std::sort(samples.begin(), samples.end());
    const size_t index =
        static_cast<size_t>(ratio * static_cast<double>(samples.size() - 1));
    return samples[index];
}

void runDepth(size_t depth) {
    const size_t repeats = std::max<size_t>(100, 200'000 / depth);
    std::vector<double> samples;
    samples.reserve(repeats * depth);

    for (size_t repeat = 0; repeat < repeats; ++repeat) {
        QueueLimits limits{depth, depth * 4096, 0, 0};
        limits.deadline_aware = true;
        limits.promotion_slack_ns = 20'000;
        LocalTransferAdmissionQueue queue(limits);
        queue.setDegradationPolicy(nullptr, DegradationHooks{},
                                   [] { return kNowNs; });

        QueueSubmit submit;
        submit.batch_token = repeat + 1;
        submit.batch_slots_left = depth;
        submit.owners.reserve(depth);
        for (size_t i = 0; i < depth; ++i) {
            // Interleave urgent and comfortable requests so every dispatch
            // round exercises the stable partition.
            submit.owners.push_back(makeOwner(i, i % 4 == 3));
        }
        std::vector<QueueOwnerId> admitted;
        if (!queue.tryAdmit(submit, admitted).ok()) std::abort();

        for (size_t i = 0; i < depth; ++i) {
            const auto start = std::chrono::steady_clock::now();
            auto picked = queue.pickForDispatch(1, 4096);
            const auto end = std::chrono::steady_clock::now();
            if (picked.size() != 1 ||
                !queue.complete(picked[0], COMPLETED).ok()) {
                std::abort();
            }
            samples.push_back(
                std::chrono::duration<double, std::micro>(end - start).count());
        }
        if (!queue.retireBatch(submit.batch_token).ok()) std::abort();
    }

    const double mean =
        std::accumulate(samples.begin(), samples.end(), 0.0) / samples.size();
    std::cout << depth << ',' << samples.size() << ',' << std::fixed
              << std::setprecision(3) << mean << ','
              << percentile(samples, 0.50) << ',' << percentile(samples, 0.95)
              << '\n';
}

}  // namespace
}  // namespace tent
}  // namespace mooncake

int main() {
    std::cout << "queue_depth,samples,mean_us,p50_us,p95_us\n";
    for (size_t depth : {32, 64, 128, 256, 512}) {
        mooncake::tent::runDepth(depth);
    }
    return 0;
}
