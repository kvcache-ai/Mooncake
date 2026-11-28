#pragma once

// OffloadLedger: what async offload-on-evict (--offload_on_evict_async) has
// queued and not yet seen end, so eviction can tell memory already on its way
// out from memory it still has to free, and a store with a deep queue from
// one with room.
//
// Each queued offload takes a Credit, which its OffloadingTask holds: the
// bytes of its source replica, charged to its tenant, and one object charged
// to each client the copy goes to. The credit gives all of it back when its
// last copy goes, and every path that ends an offload already resets the
// task (landing, NACK, expiry, cancel, erase, a disabled heartbeat, an
// unmount sweep), so each credit is given back exactly once with no call per
// site. Credits may outlive the ledger.
//
// Memory in flight only goes once its copy lands, and a store that took its
// copies and hangs never lands them. So the ledger also tells, per owner,
// when it last made progress: ended a copy, or got its first one since it
// last had none in flight. LiveBytes counts only the owners that did within
// a window; eviction discounts and waits on those alone.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/functional/hash.hpp>

#include "tenant_id.h"
#include "types.h"

namespace mooncake {
namespace eviction {

class OffloadLedger {
   public:
    using Credit = std::shared_ptr<void>;
    using SteadyClock = std::chrono::steady_clock;

    // Charges an offload of `bytes` from `tenant_id` to each of `owners`.
    [[nodiscard]] Credit Take(
        const TenantId& tenant_id, std::vector<UUID> owners, uint64_t bytes,
        SteadyClock::time_point now = SteadyClock::now()) {
        state_->Charge(tenant_id.value(), owners, bytes, +1, now);
        return std::make_shared<Charge>(state_, tenant_id.value(),
                                        std::move(owners), bytes);
    }

    // Bytes and objects charged and not given back, in all and per tenant or
    // owner.
    [[nodiscard]] uint64_t bytes() const { return state_->bytes.load(); }
    [[nodiscard]] uint64_t objects() const { return state_->objects.load(); }
    [[nodiscard]] uint64_t bytes(const TenantId& tenant_id) const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        const auto it = state_->tenant_bytes.find(tenant_id.value());
        return it == state_->tenant_bytes.end() ? 0 : it->second;
    }
    [[nodiscard]] uint64_t objects(const UUID& owner) const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        const auto it = state_->owners.find(owner);
        return it == state_->owners.end() ? 0 : it->second.objects;
    }
    // Bumped each time a credit is given back.
    [[nodiscard]] uint64_t release_seq() const {
        return state_->release_seq.load();
    }

    // Bytes charged to owners that made progress within `window` before
    // `now`, in all or of one tenant. A credit charged to several owners
    // counts once per owner.
    [[nodiscard]] uint64_t LiveBytes(SteadyClock::time_point now,
                                     SteadyClock::duration window) const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        uint64_t live = 0;
        for (const auto& [_, owner] : state_->owners) {
            if (now - owner.progress <= window) {
                live += owner.bytes;
            }
        }
        return live;
    }
    [[nodiscard]] uint64_t LiveBytes(const TenantId& tenant_id,
                                     SteadyClock::time_point now,
                                     SteadyClock::duration window) const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        uint64_t live = 0;
        for (const auto& [_, owner] : state_->owners) {
            if (now - owner.progress > window) {
                continue;
            }
            const auto it = owner.tenant_bytes.find(tenant_id.value());
            if (it != owner.tenant_bytes.end()) {
                live += it->second;
            }
        }
        return live;
    }

    // Whether `owner` has copies in flight and made no progress within
    // `window` before `now`.
    [[nodiscard]] bool Stalled(const UUID& owner, SteadyClock::time_point now,
                               SteadyClock::duration window) const {
        std::lock_guard<std::mutex> lock(state_->mutex);
        const auto it = state_->owners.find(owner);
        return it != state_->owners.end() && now - it->second.progress > window;
    }

   private:
    struct Owner {
        uint64_t objects = 0;
        uint64_t bytes = 0;
        SteadyClock::time_point progress;
        std::unordered_map<std::string, uint64_t> tenant_bytes;
    };

    struct State {
        void Charge(const std::string& tenant, const std::vector<UUID>& owners,
                    uint64_t amount, int sign, SteadyClock::time_point now) {
            std::lock_guard<std::mutex> lock(mutex);
            Apply(tenant_bytes, tenant, amount, sign);
            for (const UUID& id : owners) {
                Owner& owner = this->owners[id];
                if (sign > 0) {
                    if (owner.objects == 0) {
                        owner.progress = now;
                    }
                    ++owner.objects;
                    owner.bytes += amount;
                } else {
                    --owner.objects;
                    owner.bytes -= amount;
                    owner.progress = now;
                }
                Apply(owner.tenant_bytes, tenant, amount, sign);
                if (owner.objects == 0) {
                    this->owners.erase(id);
                }
            }
            if (sign > 0) {
                bytes += amount;
                ++objects;
            } else {
                bytes -= amount;
                --objects;
                ++release_seq;
            }
        }

        template <typename Map, typename Key>
        static void Apply(Map& map, const Key& key, uint64_t amount, int sign) {
            if (sign > 0) {
                map[key] += amount;
            } else if (const auto it = map.find(key);
                       it != map.end() && (it->second -= amount) == 0) {
                map.erase(it);
            }
        }

        std::atomic<uint64_t> bytes{0};
        std::atomic<uint64_t> objects{0};
        std::atomic<uint64_t> release_seq{0};
        mutable std::mutex mutex;
        std::unordered_map<std::string, uint64_t> tenant_bytes;
        std::unordered_map<UUID, Owner, boost::hash<UUID>> owners;
    };

    // One offload's charge, given back when its last copy goes.
    struct Charge {
        Charge(std::shared_ptr<State> state, std::string tenant,
               std::vector<UUID> owners, uint64_t bytes)
            : state(std::move(state)),
              tenant(std::move(tenant)),
              owners(std::move(owners)),
              bytes(bytes) {}
        Charge(const Charge&) = delete;
        Charge& operator=(const Charge&) = delete;
        ~Charge() {
            state->Charge(tenant, owners, bytes, -1, SteadyClock::now());
        }

        const std::shared_ptr<State> state;
        const std::string tenant;
        const std::vector<UUID> owners;
        const uint64_t bytes;
    };

    const std::shared_ptr<State> state_ = std::make_shared<State>();
};

}  // namespace eviction
}  // namespace mooncake
