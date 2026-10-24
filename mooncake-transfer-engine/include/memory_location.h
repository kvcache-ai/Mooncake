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
#include <string>

#include "common.h"

const int pagesize = 4096;

namespace mooncake {
struct MemoryLocationEntry {
    uint64_t start;
    size_t len;
    std::string location;
};

// If only_first_page is true, only the location of the first page will be
// returned. Scan all pages may take a long time, so set only_first_page if only
// the location of the first page is needed.
const std::vector<MemoryLocationEntry> getMemoryLocation(void *start,
                                                         size_t len,
                                                         bool only_first_page);

const static std::string kWildcardLocation = "*";
const static std::string kSegmentsLocationPrefix = "segments:";

/* ------------------------------------------------------------------ */
/* Segments location encoding                                         */
/*                                                                    */
/* Format: "segments:<page_size>:<numa0>,<numa1>,..."                  */
/* Example: "segments:4096:1,3,5,7"                                   */
/*                                                                    */
/* The buffer is divided into N equal regions (N = number of nodes).   */
/* Region i is physically bound to NUMA node numa[i].                 */
/* Given an offset within the buffer:                                  */
/*   region_index = offset / (buffer_length / N)                       */
/*   numa_node    = numa[region_index]                                 */
/* ------------------------------------------------------------------ */

struct SegmentsLocationInfo {
    size_t page_size;
    std::vector<int> numa_nodes;
};

// Build a segments location string for buffer registration.
std::string buildSegmentsLocation(size_t page_size,
                                  const std::vector<int> &numa_nodes);

// Parse a "segments:..." location string. Returns false on bad format.
bool parseSegmentsLocation(const std::string &name, SegmentsLocationInfo &info);

// Resolve the actual "cpu:N" location for a given offset within a
// segments-encoded buffer.
std::string resolveSegmentsLocation(const SegmentsLocationInfo &info,
                                    uint64_t buffer_length,
                                    uint64_t offset);

}  // namespace mooncake

#endif  // MEMORY_LOCATION_H
