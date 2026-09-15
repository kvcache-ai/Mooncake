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

#ifndef MOONCAKE_TESTS_SHM_HUGEPAGE_TEST_UTIL_H
#define MOONCAKE_TESTS_SHM_HUGEPAGE_TEST_UTIL_H

#include <linux/magic.h>
#include <sys/vfs.h>
#include <unistd.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "common.h"

namespace mooncake {

inline const char* HugepageSizeLabel(size_t hugepage_size) {
    if (hugepage_size == SharedMemoryOptions::kHugepage2MB) return "2MB";
    if (hugepage_size == SharedMemoryOptions::kHugepage512MB) return "512MB";
    if (hugepage_size == SharedMemoryOptions::kHugepage1GB) return "1GB";
    return "unknown";
}

inline const char* HugepageSizeTestName(size_t hugepage_size) {
    if (hugepage_size == SharedMemoryOptions::kHugepage2MB) return "Size2MB";
    if (hugepage_size == SharedMemoryOptions::kHugepage512MB) return "Size512MB";
    if (hugepage_size == SharedMemoryOptions::kHugepage1GB) return "Size1GB";
    return "Unknown";
}

inline std::optional<std::string> FindHugetlbfsMount(size_t hugepage_size) {
    std::vector<const char*> candidates;
    if (const char* env = std::getenv("MC_HUGETLBFS_PATH");
        env && hugepage_size == SharedMemoryOptions::kHugepage2MB) {
        candidates.push_back(env);
    }
    if (const char* env = std::getenv("MC_HUGETLBFS_PATH_512M");
        env && hugepage_size == SharedMemoryOptions::kHugepage512MB) {
        candidates.push_back(env);
    }
    if (const char* env = std::getenv("MC_HUGETLBFS_PATH_1G");
        env && hugepage_size == SharedMemoryOptions::kHugepage1GB) {
        candidates.push_back(env);
    }
    if (const char* env = std::getenv("MC_HUGETLBFS_PATH"); env) {
        candidates.push_back(env);
    }
    candidates.push_back(
        SharedMemoryOptions::defaultHugetlbfsPathFor(hugepage_size));
    if (hugepage_size == SharedMemoryOptions::kHugepage2MB) {
        candidates.push_back("/tmp/mooncake_hugepages_2m");
    } else if (hugepage_size == SharedMemoryOptions::kHugepage512MB) {
        candidates.push_back("/tmp/mooncake_hugepages_512m");
    } else if (hugepage_size == SharedMemoryOptions::kHugepage1GB) {
        candidates.push_back("/tmp/mooncake_hugepages_1g");
    }

    for (const char* dir : candidates) {
        if (!dir || !*dir) continue;
        struct statfs sfs;
        if (statfs(dir, &sfs) != 0) continue;
        if (sfs.f_type != HUGETLBFS_MAGIC) continue;
        if (static_cast<size_t>(sfs.f_bsize) != hugepage_size) continue;
        if (access(dir, W_OK) != 0) continue;
        return std::string(dir);
    }
    return std::nullopt;
}

}  // namespace mooncake

#endif  // MOONCAKE_TESTS_SHM_HUGEPAGE_TEST_UTIL_H
