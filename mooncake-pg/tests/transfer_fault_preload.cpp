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

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <dlfcn.h>
#include <mutex>
#include <optional>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "control_plane/link_manager.h"
#include "transfer_engine.h"

namespace {

std::atomic<bool> fault_enabled{false};
std::atomic<uint64_t> injected_failure_count{0};

std::mutex fault_config_mutex;
std::set<mooncake::GlobalRank> failed_targets;

// Fault rules use global ranks, while transfer requests identify targets by
// segment ID. Capture both mappings so we can apply rank-level rules to
// individual transfer tasks.
std::mutex mapping_mutex;
std::unordered_map<mooncake::SegmentID, mooncake::GlobalRank>
    segment_id_to_rank;
std::unordered_map<mooncake::BatchID, std::vector<mooncake::GlobalRank>>
    batch_id_to_target_ranks;

constexpr char kGetTransferStatusSymbol[] =
    "_ZN8mooncake14TransferEngine17getTransferStatusEmmRNS_"
    "9Transport14TransferStatusE";
constexpr char kSubmitTransferSymbol[] =
    "_ZN8mooncake14TransferEngine14submitTransferEmRKSt6vectorINS_"
    "9Transport15TransferRequestESaIS3_EE";
constexpr char kResolvePeerSymbol[] =
    "_ZNK8mooncake11LinkManager11resolvePeerEi";

using GetTransferStatus = mooncake::Status (*)(mooncake::TransferEngine*,
                                               mooncake::BatchID, size_t,
                                               mooncake::TransferStatus&);
using SubmitTransfer =
    mooncake::Status (*)(mooncake::TransferEngine*, mooncake::BatchID,
                         const std::vector<mooncake::TransferRequest>&);
using ResolvePeer = std::optional<mooncake::SegmentID> (*)(
    const mooncake::LinkManager*, mooncake::GlobalRank);

void* findRealSymbol(const char* name) {
    if (auto symbol = dlsym(RTLD_NEXT, name)) return symbol;

    // Python loads extension dependencies in a local ELF scope, where
    // RTLD_NEXT cannot see libmooncake_pg even though interposition still
    // applies to its PLT calls. Look up the original definition directly
    // from the already-loaded library in that case.
    static auto handle = dlopen("libmooncake_pg.so", RTLD_LAZY | RTLD_NOLOAD);
    return handle ? dlsym(handle, name) : nullptr;
}

GetTransferStatus findRealGetTransferStatus() {
    static auto real = reinterpret_cast<GetTransferStatus>(
        findRealSymbol(kGetTransferStatusSymbol));
    return real;
}

SubmitTransfer findRealSubmitTransfer() {
    static auto real =
        reinterpret_cast<SubmitTransfer>(findRealSymbol(kSubmitTransferSymbol));
    return real;
}

ResolvePeer findRealResolvePeer() {
    static auto real =
        reinterpret_cast<ResolvePeer>(findRealSymbol(kResolvePeerSymbol));
    return real;
}

bool shouldInjectFailure(mooncake::GlobalRank target_rank) {
    if (!fault_enabled.load(std::memory_order_acquire)) return false;

    std::lock_guard<std::mutex> lock(fault_config_mutex);
    return failed_targets.contains(target_rank);
}

}  // namespace

namespace mooncake {

std::optional<TransferMetadata::SegmentID> LinkManager::resolvePeer(
    GlobalRank peer) const {
    auto real = findRealResolvePeer();
    if (!real) std::abort();
    auto target_id = real(this, peer);
    if (target_id.has_value()) {
        std::lock_guard<std::mutex> lock(mapping_mutex);
        segment_id_to_rank[*target_id] = peer;
    }
    return target_id;
}

Status TransferEngine::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest>& entries) {
    auto real = findRealSubmitTransfer();
    if (!real) std::abort();
    auto result = real(this, batch_id, entries);
    if (result.ok()) {
        std::vector<GlobalRank> target_ranks(entries.size(),
                                             kInvalidGlobalRank);
        std::lock_guard<std::mutex> lock(mapping_mutex);
        for (size_t task_id = 0; task_id < entries.size(); ++task_id) {
            auto rank_it = segment_id_to_rank.find(entries[task_id].target_id);
            if (rank_it != segment_id_to_rank.end()) {
                target_ranks[task_id] = rank_it->second;
            }
        }
        batch_id_to_target_ranks.insert_or_assign(batch_id,
                                                  std::move(target_ranks));
    }
    return result;
}

Status TransferEngine::getTransferStatus(BatchID batch_id, size_t task_id,
                                         TransferStatus& status) {
    auto real = findRealGetTransferStatus();
    if (!real) std::abort();
    auto result = real(this, batch_id, task_id, status);
    if (result.ok() && status.s == TransferStatusEnum::COMPLETED &&
        fault_enabled.load(std::memory_order_acquire)) {
        GlobalRank target_rank = kInvalidGlobalRank;
        {
            std::lock_guard<std::mutex> lock(mapping_mutex);
            auto batch_it = batch_id_to_target_ranks.find(batch_id);
            if (batch_it != batch_id_to_target_ranks.end() &&
                task_id < batch_it->second.size()) {
                target_rank = batch_it->second[task_id];
            }
        }
        if (shouldInjectFailure(target_rank)) {
            status.s = TransferStatusEnum::FAILED;
            injected_failure_count.fetch_add(1, std::memory_order_relaxed);
        }
    }
    return result;
}

}  // namespace mooncake

extern "C" int mooncakePgTestFaultAvailable() {
    return findRealGetTransferStatus() != nullptr &&
           findRealSubmitTransfer() != nullptr &&
           findRealResolvePeer() != nullptr;
}

extern "C" void mooncakePgTestClearFailedTargets() {
    std::lock_guard<std::mutex> lock(fault_config_mutex);
    failed_targets.clear();
}

extern "C" void mooncakePgTestAddFailedTarget(int target_rank) {
    std::lock_guard<std::mutex> lock(fault_config_mutex);
    failed_targets.insert(target_rank);
}

extern "C" void mooncakePgTestSetFaultEnabled(int enabled) {
    fault_enabled.store(enabled != 0, std::memory_order_release);
}

extern "C" void mooncakePgTestResetFailureCount() {
    injected_failure_count.store(0, std::memory_order_release);
}

extern "C" uint64_t mooncakePgTestGetFailureCount() {
    return injected_failure_count.load(std::memory_order_acquire);
}
