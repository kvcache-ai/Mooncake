// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace nds_test {
struct State {
    uint64_t npu_base = 0;
    size_t npu_size = 0;
    int opens = 0;
    int closes = 0;
    int pins = 0;
    int unpins = 0;
    int live_contexts = 0;
    int live_files = 0;
    int live_topologies = 0;
    int topology_fd = -1;
    int submits = 0;
    int drains = 0;
    int context_setups = 0;
    int submitted_ios = 0;
    int registered_ios = 0;
    int max_inflight = 0;
    int submit_error = 0;
    bool pause = false;
    bool partial_submit = false;
    bool poll_error = false;
    bool bad_cookie = false;
    bool destroy_error = false;
    bool unpin_error = false;
    bool open_error = false;
    bool handle_error = false;
    std::vector<std::vector<size_t>> submitted_iov_lengths;
};
extern State state;
}  // namespace nds_test
