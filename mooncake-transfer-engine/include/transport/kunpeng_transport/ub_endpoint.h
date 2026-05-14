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

#ifndef UB_ENDPOINT_H
#define UB_ENDPOINT_H
#include "common.h"
#include "config.h"
#include "transfer_metadata.h"
#include "transport/transport.h"

namespace mooncake {
// define the UbEndpoint class
class UbEndPoint {
   public:
    enum Status {
        INITIALIZING,
        UNCONNECTED,
        CONNECTED,
    };

    using HandShakeDesc = TransferMetadata::HandShakeDesc;

    UbEndPoint() : inactive_time_(0), active_(true), status_(INITIALIZING) {}

    virtual int construct(GlobalConfig& config) = 0;

    virtual int deconstruct() = 0;

    virtual void setPeerNicPath(const std::string& peer_nic_path) = 0;

    virtual int setupConnectionsByActive() = 0;

    virtual int setupConnectionsByPassive(const HandShakeDesc& peer_desc,
                                          HandShakeDesc& local_desc) = 0;

    virtual bool hasOutstandingSlice() const = 0;

    virtual int submitPostSend(
        std::vector<Transport::Slice*>& slice_list,
        std::vector<Transport::Slice*>& failed_slice_list) = 0;

    virtual const std::string toString() const = 0;

    int setupConnectionsByActive(const std::string& peer_nic_path) {
        setPeerNicPath(peer_nic_path);
        return setupConnectionsByActive();
    }

    bool active() const { return active_; }

    void set_active(bool flag) {
        RWSpinlock::WriteGuard guard(lock_);
        active_ = flag;
        if (!flag) inactive_time_ = getCurrentTimeInNano();
    }

    double inactiveTime() {
        if (active_) return 0.0;
        return (getCurrentTimeInNano() - inactive_time_) / 1000000000.0;
    }

    bool connected() const {
        return status_.load(std::memory_order_relaxed) == CONNECTED;
    }

    void disconnect() {
        RWSpinlock::WriteGuard guard(lock_);
        disconnectUnlocked();
    }

   private:
    virtual void disconnectUnlocked() = 0;

   protected:
    volatile uint64_t inactive_time_{};
    volatile bool active_{};
    std::atomic<Status> status_;
    RWSpinlock lock_;
    std::string peer_nic_path_;
};
}  // namespace mooncake

#endif  // UB_ENDPOINT_H
