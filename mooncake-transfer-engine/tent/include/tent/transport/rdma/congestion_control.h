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

#ifndef TENT_RDMA_CONGESTION_CONTROL_H
#define TENT_RDMA_CONGESTION_CONTROL_H

#ifdef MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL

#include <infiniband/verbs.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "adaptive_congestion_control.h"

namespace mooncake::tent {

class RdmaEndPoint;
struct RdmaSlice;

// TENT adapter for the shared adaptive congestion-control core. The adapter
// owns one device domain per local NIC, plus one route domain per resolved
// local-NIC/peer-NIC path. These domains are shared by every TENT worker that
// posts to the same physical resource. The adapter does not add a second queue,
// split requests, or change TENT's existing path-health policy.
class TentRdmaCongestionControl {
   public:
    explicit TentRdmaCongestionControl(size_t device_count);
    TentRdmaCongestionControl(adaptive_congestion_control::Config config,
                              size_t device_count);
    ~TentRdmaCongestionControl();

    TentRdmaCongestionControl(const TentRdmaCongestionControl&) = delete;
    TentRdmaCongestionControl& operator=(const TentRdmaCongestionControl&) =
        delete;

    bool enabled() const {
        return mode_ != adaptive_congestion_control::Mode::kOff;
    }

    // Admit a FIFO prefix for one already-resolved path. The caller must
    // return permits for entries that do not reach the QP. A stale permit is
    // returned and reacquired against the current endpoint/device generation.
    size_t admitPrefix(int local_device, const std::string& peer_machine,
                       int remote_device, RdmaEndPoint* endpoint,
                       const std::vector<RdmaSlice*>& slices);

    // A physical CQ completion settles the permit for this posting attempt.
    void complete(RdmaSlice* slice, ibv_wc_status status,
                  uint32_t vendor_error);

    // Settle a permit when an admitted slice did not reach the hardware.
    void releaseUnposted(RdmaSlice* slice);

    // Run the shared controller's feedback update without taking ownership of
    // TENT's RailMonitor recovery decisions.
    void tick(uint64_t now_ns);

    // Fence permits created before a local device was refreshed/recovered.
    void resetDevice(int local_device);

   private:
    struct RouteKey {
        int local_device;
        int remote_device;
        std::string peer_machine;

        bool operator==(const RouteKey& other) const {
            return local_device == other.local_device &&
                   remote_device == other.remote_device &&
                   peer_machine == other.peer_machine;
        }
    };

    struct RouteKeyHash {
        size_t operator()(const RouteKey& key) const;
    };

    struct RouteState;
    struct EndpointBinding {
        uint32_t route_generation;
        uint64_t endpoint_generation;
    };

    RouteState* route(const RouteKey& key);
    static EndpointBinding bindEndpoint(RouteState& route,
                                        RdmaEndPoint* endpoint);

    adaptive_congestion_control::Config config_;
    adaptive_congestion_control::Mode mode_;
    size_t device_count_;
    std::vector<std::unique_ptr<adaptive_congestion_control::DomainState>>
        devices_;
    std::mutex routes_mutex_;
    std::unordered_map<RouteKey, std::unique_ptr<RouteState>, RouteKeyHash>
        routes_;
};

}  // namespace mooncake::tent

#endif  // MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL
#endif  // TENT_RDMA_CONGESTION_CONTROL_H
