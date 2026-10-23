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

#pragma once

#include <sycl/sycl.hpp>

#include <cstdint>

namespace mooncake {
namespace xpu_ep {

// Pointers into the current half are local addresses; a peer's copy is
// reached by rebasing onto peer_bases[rank].
struct PeerView {
    void* local_base;
    void* const* peer_bases;  // device table, nullptr for an unmapped rank
};

struct DispatchArgs {
    const void* x;
    const int64_t* topk_idx;
    int32_t* active_ranks;
    void* packed_recv_x;
    float* packed_recv_x_scales;
    int* packed_recv_src_info;
    int64_t* packed_recv_layout_range;
    int* packed_recv_count;
    int* ready;       // this call's half
    int* next_ready;  // the other half, cleaned by this call
    uint8_t* staging;
    int32_t* slot_shadow;  // [num_tokens][num_topk] list slot of each pick
    int* counter_per_expert;
    int* done_counter;
    PeerView peers;
    int num_tokens, hidden, num_topk, num_max_dispatch_tokens_per_rank;
    int num_experts, rank, num_ranks;
    bool use_fp8;
    int64_t timeout_ticks;
    int max_wgs;
};

struct CombineArgs {
    const void* x;  // [num_local][num_ranks * T][hidden] expert outputs
    const int64_t* topk_idx;     // the dispatch-time snapshot
    const int32_t* slot_shadow;  // the dispatch-time list slots
    const float* topk_weights;
    const int64_t* layout_range;
    int32_t* active_ranks;
    void* combined_x;
    int* ready;
    int* next_ready;
    uint8_t* staging;
    int* done_counter;
    PeerView peers;
    int num_combined_tokens, hidden, num_topk, num_max_dispatch_tokens_per_rank;
    int num_experts, rank, num_ranks;
    int64_t timeout_ticks;
    int max_wgs;
};

// Each collective is a send kernel, which stages this rank's data and then,
// from its last work-group, flags every peer; and a receive kernel, which
// waits for the peers' flags and pulls their staged data. The receive kernel
// is what a return_recv_hook defers.
sycl::event dispatch_send(sycl::queue& q, const DispatchArgs& args);
sycl::event dispatch_recv(sycl::queue& q, const DispatchArgs& args);
sycl::event combine_send(sycl::queue& q, const CombineArgs& args);
sycl::event combine_recv(sycl::queue& q, const CombineArgs& args);

}  // namespace xpu_ep
}  // namespace mooncake
