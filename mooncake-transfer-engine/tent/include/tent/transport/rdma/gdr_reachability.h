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

#ifndef TENT_GDR_REACHABILITY_H
#define TENT_GDR_REACHABILITY_H

#include <atomic>
#include <chrono>
#include <cstdint>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace mooncake {
namespace tent {

class Config;

// GdrReachability tracks whether a NIC can actually GPUDirect-DMA (P2P) to a
// GPU. ibv_reg_mr (nvidia-peermem) and ibv_reg_dmabuf_mr (DMA-BUF) both
// succeed on a GPU buffer for every NIC, so registration cannot tell whether
// a given NIC's PCIe path can really reach the GPU -- that depends
// on PCIe topology / ACS and only surfaces on the data plane as
// IBV_WC_LOC_PROT_ERR (local NIC -> local GPU) or IBV_WC_REM_ACCESS_ERR
// (remote NIC -> remote GPU).
//
// This component learns unreachable (NIC, GPU) pairs at runtime (and, when the
// registration probe is enabled, up front) and lets device selection skip
// them. A permissive fabric never records anything and keeps full multi-rail
// aggregation; a restrictive fabric converges onto the reachable NIC(s) instead
// of exhausting retries and failing the whole transfer.
//
// Exclusion uses the same threshold + exponential-cooldown + re-admit scheme as
// RailMonitor, so a transient error self-heals after one cooldown and a
// genuinely-dead path is only re-probed occasionally.
//
// It is a process-wide singleton keyed by stable identifiers (RDMA device name
// + GPU ordinal, plus peer machine id for the remote side), so a single
// instance is correct even across multiple TransferEngine instances in one
// process. All methods are thread-safe.
class GdrReachability {
   public:
    static GdrReachability &instance();

    // Cheap global fast path: true only after at least one pair has ever been
    // excluded. Hot selection paths skip all work while this is false.
    static bool hasAnyExclusion() {
        return any_exclusion_.load(std::memory_order_relaxed);
    }

    // Override error_threshold / error_window / cooldown from config. Config
    // keys mirror RailMonitor: transports/rdma/gdr_error_threshold,
    // transports/rdma/gdr_error_window_secs, transports/rdma/gdr_cooldown_secs.
    void configure(const Config *conf);

    // --- Local side: this host's NIC -> this host's GPU ---
    void reportLocalFailure(const std::string &nic_name, int gpu_ordinal);
    void reportLocalSuccess(const std::string &nic_name, int gpu_ordinal);
    bool localReachable(const std::string &nic_name, int gpu_ordinal);

    // --- Remote side: peer `machine_id`'s NIC -> that peer's GPU ---
    void reportRemoteFailure(const std::string &machine_id,
                             const std::string &nic_name, int gpu_ordinal);
    void reportRemoteSuccess(const std::string &machine_id,
                             const std::string &nic_name, int gpu_ordinal);
    bool remoteReachable(const std::string &machine_id,
                         const std::string &nic_name, int gpu_ordinal);

   private:
    GdrReachability() = default;

    struct State {
        uint32_t error_count = 0;
        std::chrono::steady_clock::time_point last_error{};
        std::chrono::steady_clock::duration cooldown{std::chrono::seconds(0)};
        std::chrono::steady_clock::time_point resume_time{};  // paused until
    };

    bool reachable(const std::string &key);
    void markFailed(const std::string &key);
    void markRecovered(const std::string &key);

    static std::string localKey(const std::string &nic_name, int gpu_ordinal);
    static std::string remoteKey(const std::string &machine_id,
                                 const std::string &nic_name, int gpu_ordinal);

    static std::atomic<bool> any_exclusion_;

    std::shared_mutex mutex_;
    std::unordered_map<std::string, State> state_;

    int error_threshold_ = 2;
    std::chrono::seconds error_window_{10};
    std::chrono::seconds cooldown_{30};
    static constexpr std::chrono::seconds kMaxCooldown{300};
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_GDR_REACHABILITY_H
