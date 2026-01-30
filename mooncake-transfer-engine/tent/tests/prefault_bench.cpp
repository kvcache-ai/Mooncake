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

#include "tent/common/utils/prefault.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include <sys/mman.h>

namespace {
constexpr size_t kPageSize = 4096;

size_t align_up(size_t size, size_t alignment) {
    if (alignment == 0) return size;
    return ((size + alignment - 1) / alignment) * alignment;
}

size_t parse_size_bytes(const char* str, size_t default_value) {
    if (!str || *str == '\0') return default_value;
    char* end = nullptr;
    unsigned long long base = std::strtoull(str, &end, 10);
    if (end == str) return default_value;
    size_t multiplier = 1;
    if (end && *end) {
        char suffix = static_cast<char>(std::tolower(*end));
        if (suffix == 'k') multiplier = 1024ULL;
        else if (suffix == 'm') multiplier = 1024ULL * 1024ULL;
        else if (suffix == 'g') multiplier = 1024ULL * 1024ULL * 1024ULL;
    }
    return static_cast<size_t>(base) * multiplier;
}

std::vector<unsigned> parse_threads(const char* str,
                                    std::vector<unsigned> defaults) {
    if (!str || *str == '\0') return defaults;
    std::vector<unsigned> out;
    const char* p = str;
    while (*p) {
        while (*p == ',' || *p == ' ') ++p;
        if (!*p) break;
        char* end = nullptr;
        unsigned long val = std::strtoul(p, &end, 10);
        if (end == p) break;
        if (val > 0) out.push_back(static_cast<unsigned>(val));
        p = end;
    }
    if (out.empty()) return defaults;
    return out;
}

void* alloc_buffer(size_t size) {
    size_t aligned = align_up(size, kPageSize);
    void* buf = std::aligned_alloc(kPageSize, aligned);
    return buf;
}

void reset_pages(void* buf, size_t size) {
    if (!buf || size == 0) return;
    madvise(buf, size, MADV_DONTNEED);
}

void run_mode(const char* name, mooncake::tent::PrefaultOptions::Mode mode,
              void** pages, int n, uintptr_t aligned_start, void* start,
              size_t len, const std::vector<unsigned>& threads,
              bool reset_between) {
    for (auto t : threads) {
        mooncake::tent::PrefaultOptions opts;
        opts.page_size = kPageSize;
        opts.madvise_chunk_bytes = 1ULL << 30;
        opts.mode = mode;
        if (mode == mooncake::tent::PrefaultOptions::Mode::kTouch) {
            opts.max_touch_threads = t;
        } else if (mode == mooncake::tent::PrefaultOptions::Mode::kMadvise) {
            opts.max_madvise_threads = t;
        }
        auto result =
            mooncake::tent::prefaultPages(pages, n, aligned_start, opts);
        std::cout << name << " mode=" << result.method
                  << " threads=" << t
                  << " duration_ms=" << result.duration_ms
                  << " err=" << result.err << "\n";
        if (reset_between) reset_pages(start, len);
    }
}
}  // namespace

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;
    size_t size =
        parse_size_bytes(std::getenv("TENT_PREFAULT_SIZE"), 1ULL << 30);
    bool reset_between =
        std::getenv("TENT_PREFAULT_RESET") != nullptr &&
        std::strcmp(std::getenv("TENT_PREFAULT_RESET"), "0") != 0;
    auto threads = parse_threads(std::getenv("TENT_PREFAULT_THREADS"),
                                 {1, 2, 4, 8, 16});

    size_t aligned = align_up(size, kPageSize);
    void* buf = alloc_buffer(aligned);
    if (!buf) {
        std::cerr << "alloc_buffer failed size=" << aligned << "\n";
        return 1;
    }

    uintptr_t aligned_start = reinterpret_cast<uintptr_t>(buf);
    int n = static_cast<int>(aligned / kPageSize);
    std::vector<void*> pages(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        pages[static_cast<size_t>(i)] =
            reinterpret_cast<void*>(aligned_start + i * kPageSize);
    }

    std::cout << "prefault_bench size=" << aligned << " pages=" << n << "\n";

    run_mode("[BENCH][MADVISE]", mooncake::tent::PrefaultOptions::Mode::kMadvise,
             pages.data(), n, aligned_start, buf, aligned, threads,
             reset_between);
    run_mode("[BENCH][MLOCK]", mooncake::tent::PrefaultOptions::Mode::kMlock,
             pages.data(), n, aligned_start, buf, aligned, {1},
             reset_between);
    run_mode("[BENCH][TOUCH]", mooncake::tent::PrefaultOptions::Mode::kTouch,
             pages.data(), n, aligned_start, buf, aligned, threads,
             reset_between);

    std::free(buf);
    return 0;
}
