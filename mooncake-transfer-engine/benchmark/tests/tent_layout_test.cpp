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

#include "tent_backend.h"

#include <gtest/gtest.h>

#include <map>
#include <set>
#include <vector>

namespace mooncake::tent {
namespace {

// --seg_type_mix must spread workers over targets first and types second, so
// that every target sees every type. Both sides pick the type with
// mixTypeIndex(), which is what makes the local and target types match.
TEST(TentLayoutTest, MixedTypesCoverEveryTargetAndType) {
    for (size_t targets : {size_t{1}, size_t{2}, size_t{4}}) {
        for (size_t types : {size_t{1}, size_t{2}, size_t{3}}) {
            std::set<std::pair<size_t, size_t>> pairs;
            for (size_t w = 0; w < targets * types; ++w) {
                pairs.emplace(w % targets,
                              TENTBenchRunner::mixTypeIndex(static_cast<int>(w),
                                                            targets, types));
            }
            EXPECT_EQ(pairs.size(), targets * types)
                << "T=" << targets << " M=" << types;
        }
    }
}

// Workers of the same type must get distinct local slots, so one round uses
// each local buffer exactly once.
TEST(TentLayoutTest, LocalOrdinalsAreUniquePerType) {
    for (size_t targets : {size_t{1}, size_t{2}, size_t{3}}) {
        for (size_t types : {size_t{1}, size_t{2}}) {
            const size_t threads = targets * types * 3;
            std::map<size_t, std::set<size_t>> ordinals;
            for (size_t w = 0; w < threads; ++w) {
                const size_t type = TENTBenchRunner::mixTypeIndex(
                    static_cast<int>(w), targets, types);
                ordinals[type].insert(TENTBenchRunner::mixLocalOrdinal(
                    static_cast<int>(w), targets, types));
            }
            for (auto& [type, seen] : ordinals) {
                // 3 rounds of `targets` workers per type, all distinct.
                EXPECT_EQ(seen.size(), 3 * targets) << "type=" << type;
            }
        }
    }
}

// --target_gpu_id only shifts which target buffer of a type is used.
TEST(TentLayoutTest, TargetGpuIdShiftsTheFirstTargetBuffer) {
    const size_t targets = 2, types = 2;
    const int saved = XferBenchConfig::target_gpu_id;
    for (int gpu_id : {0, 1, 3}) {
        XferBenchConfig::target_gpu_id = gpu_id;
        EXPECT_EQ(TENTBenchRunner::mixTargetOrdinal(0, targets, types),
                  static_cast<size_t>(gpu_id));
        // The second worker of the same type (round 1) uses the next buffer.
        EXPECT_EQ(TENTBenchRunner::mixTargetOrdinal(4, targets, types),
                  static_cast<size_t>(gpu_id) + 1);
    }
    XferBenchConfig::target_gpu_id = saved;
}

}  // namespace
}  // namespace mooncake::tent
