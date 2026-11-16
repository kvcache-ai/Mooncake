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

#ifndef TEBENCH_SPLIT_OUTPUT_H
#define TEBENCH_SPLIT_OUTPUT_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace mooncake {
namespace tent {

struct SplitXferSample {
    uint64_t batch_size;
    uint64_t submit_us;
    uint64_t wait_us;
    uint64_t polls;
};

// False when --split_output_jsonl is unset. Check this in the transfer hot
// path so a disabled log does not call getenv, open a file, or start extra
// timers.
extern bool g_split_output_enabled;

inline bool splitOutputEnabled() noexcept { return g_split_output_enabled; }

bool formatSplitXferLine(char* buf, size_t cap, const SplitXferSample& sample,
                         size_t* written);

bool openSplitOutput(const std::string& path, std::string* error);
void closeSplitOutput();

// One buffered write. Call only after splitOutputEnabled() is true.
void logSplitXfer(const SplitXferSample& sample);

}  // namespace tent
}  // namespace mooncake

#endif
