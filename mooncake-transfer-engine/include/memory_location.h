// Copyright 2024 KVCache.AI
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

#ifndef MEMORY_LOCATION_H
#define MEMORY_LOCATION_H

#include <glog/logging.h>

#include <memory>

#include "common.h"

const int pagesize = 4096;

namespace mooncake {
struct MemoryLocationEntry {
    uint64_t start;
    size_t len;
    std::string location;
};

// Get CPU numa node id
// TODO: support getting cuda device id from unified address.
const std::vector<MemoryLocationEntry> getMemoryLocation(void *start,
                                                         size_t len);

}  // namespace mooncake

#endif  // MEMORY_LOCATION_H
