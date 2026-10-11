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

#ifndef TENT_REGISTRATION_GUARD_H
#define TENT_REGISTRATION_GUARD_H

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

namespace mooncake {
namespace tent {

// Serializes registration lifecycle operations sharing a base address. A batch
// reserves all of its addresses together, so waiting never holds a partial
// batch. Operations on disjoint addresses can run concurrently without holding
// the coordinator mutex. The coordinator must outlive all guards and waiters;
// acquiring an address already held by the same thread is not reentrant.
class RegistrationCoordinator {
   public:
    class Guard {
       public:
        Guard() = default;
        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;

        Guard(Guard&& other) noexcept
            : owner_(std::exchange(other.owner_, nullptr)),
              addresses_(std::move(other.addresses_)) {}

        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                reset();
                owner_ = std::exchange(other.owner_, nullptr);
                addresses_ = std::move(other.addresses_);
            }
            return *this;
        }

        ~Guard() { reset(); }

        void reset() noexcept {
            if (owner_) {
                owner_->release(addresses_);
                owner_ = nullptr;
                addresses_.clear();
            }
        }

       private:
        friend class RegistrationCoordinator;
        Guard(RegistrationCoordinator& owner,
              std::vector<uint64_t> addresses) noexcept
            : owner_(&owner), addresses_(std::move(addresses)) {}

        RegistrationCoordinator* owner_{nullptr};
        std::vector<uint64_t> addresses_;
    };

    RegistrationCoordinator() = default;
    RegistrationCoordinator(const RegistrationCoordinator&) = delete;
    RegistrationCoordinator& operator=(const RegistrationCoordinator&) = delete;

    Guard acquire(std::vector<uint64_t> addresses) {
        std::sort(addresses.begin(), addresses.end());
        addresses.erase(std::unique(addresses.begin(), addresses.end()),
                        addresses.end());
        if (addresses.empty()) return {};

        std::unique_lock<std::mutex> lock(mutex_);
        changed_.wait(lock, [&] {
            return std::none_of(
                addresses.begin(), addresses.end(),
                [&](uint64_t address) { return active_.count(address) != 0; });
        });
        size_t inserted = 0;
        try {
            for (auto address : addresses) {
                active_.insert(address);
                ++inserted;
            }
        } catch (...) {
            // No waiter can observe this partial reservation while mutex_ is
            // held. Restore the prior state if allocating a set node fails.
            for (size_t i = 0; i < inserted; ++i) active_.erase(addresses[i]);
            throw;
        }
        return Guard(*this, std::move(addresses));
    }

   private:
    void release(const std::vector<uint64_t>& addresses) noexcept {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto address : addresses) active_.erase(address);
        }
        changed_.notify_all();
    }

    std::mutex mutex_;
    std::condition_variable changed_;
    // Only active reservations are retained; completed addresses are erased.
    std::set<uint64_t> active_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_REGISTRATION_GUARD_H
