#pragma once

#include <array>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "piece_put_types.h"

namespace mooncake {

// Bounded, producer-scoped replay fencing; independent of mounted owners and
// their liveness. Registry locks never span key access or wait for a gate.
class PiecePutAdmissionRegistry {
   public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    static constexpr uint32_t kCapacity = 4096;
    static constexpr size_t kMaxProducers = 8192;
    static constexpr size_t kMaxHostIdBytes = 256;
    static constexpr auto kIdleTimeout = std::chrono::seconds(60);

   private:
    struct Session {
        Session(TimePoint now, const std::string& host)
            : last_used(now.time_since_epoch().count()), host_id(host) {}
        uint64_t id{0};
        std::shared_mutex gate;
        bool closed{false};  // guarded by gate
        std::atomic<Clock::duration::rep> last_used;
        std::array<std::atomic<uint64_t>, kCapacity / 64> spent{};
        // Immutable placement hint; bounded and reclaimed with this session.
        const std::string host_id;
    };

   public:
    // A shared gate remains held through key access and grant publication.
    // An empty guard proves the requested, previously issued window is closed.
    class Guard {
       public:
        Guard() = default;
        explicit Guard(std::shared_ptr<Session> session)
            : session_(std::move(session)), lock_(session_->gate) {}
        Guard(Guard&&) noexcept = default;
        Guard& operator=(Guard&& other) noexcept {
            // Unlock before releasing the old session's owning reference.
            lock_ = std::move(other.lock_);
            session_ = std::move(other.session_);
            return *this;
        }
        explicit operator bool() const { return session_ != nullptr; }
        const std::string& HostId() const noexcept { return session_->host_id; }

        // A granting request holds the object key guard through publication.
        // A non-granting request may consume after a namespace miss, but must
        // look up the namespace again and fence its key before settling: a
        // concurrent grant that consumed first already published its namespace.
        // Different keys may race, but at most one can create a new claim.
        bool Consume(uint64_t sequence) {
            const auto bit = sequence - 1;
            const uint64_t mask = uint64_t{1} << (bit % 64);
            return (session_->spent[bit / 64].fetch_or(
                        mask, std::memory_order_relaxed) &
                    mask) == 0;
        }

       private:
        friend class PiecePutAdmissionRegistry;
        std::shared_ptr<Session> session_;
        std::shared_lock<std::shared_mutex> lock_;
    };

    // A retry carries the previously observed id. It rotates only while that
    // exact id is current; a lost reply therefore cannot rotate twice.
    tl::expected<uint64_t, ErrorCode> Get(const UUID& client_id,
                                          uint64_t previous_id, TimePoint now,
                                          const std::string& host_id = {}) {
        if (host_id.size() > kMaxHostIdBytes)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        for (;;) {
            auto current = Find(client_id);
            // Allocate outside the registry lock, before closing anything.
            auto next = std::make_shared<Session>(now, host_id);
            std::unique_lock<std::shared_mutex> gate;
            if (current) gate = std::unique_lock(current->gate);
            std::unique_lock registry(mutex_);
            auto it = sessions_.find(client_id);
            if ((it == sessions_.end() && current) ||
                (it != sessions_.end() && it->second != current)) {
                continue;
            }
            if (current && current->id != previous_id) {
                current->last_used.store(now.time_since_epoch().count(),
                                         std::memory_order_relaxed);
                return current->id;
            }
            if ((!current && sessions_.size() >= kMaxProducers) ||
                next_id_ == std::numeric_limits<uint64_t>::max()) {
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
            next->id = next_id_;
            if (current) {
                current->closed = true;
                it->second = next;
            } else {
                // An allocation failure must not consume an id or close an
                // existing window. All session state is prepared above.
                sessions_.emplace(client_id, next);
            }
            ++next_id_;
            return next->id;
        }
    }

    tl::expected<Guard, ErrorCode> Acquire(const UUID& client_id,
                                           uint64_t session_id, TimePoint now) {
        std::shared_ptr<Session> current;
        {
            std::shared_lock registry(mutex_);
            if (session_id == 0 || session_id >= next_id_) {
                return tl::make_unexpected(ErrorCode::INVALID_WRITE);
            }
            const auto it = sessions_.find(client_id);
            if (it != sessions_.end() && it->second->id == session_id) {
                current = it->second;
            }
        }
        if (!current) return Guard{};
        Guard guard(std::move(current));
        if (guard.session_->closed) return Guard{};
        guard.session_->last_used.store(now.time_since_epoch().count(),
                                        std::memory_order_relaxed);
        return guard;
    }

    // Never wait for an active window. Closing admission does not touch any
    // fragment, hold, allocation or physical drain state.
    void Sweep(TimePoint now) {
        std::vector<std::pair<UUID, std::shared_ptr<Session>>> candidates;
        {
            std::shared_lock registry(mutex_);
            candidates.reserve(sessions_.size());
            for (const auto& entry : sessions_) candidates.push_back(entry);
        }
        for (const auto& [client_id, session] : candidates) {
            std::unique_lock gate(session->gate, std::try_to_lock);
            if (!gate ||
                now - TimePoint(Clock::duration(
                          session->last_used.load(std::memory_order_relaxed))) <
                    kIdleTimeout) {
                continue;
            }
            std::unique_lock registry(mutex_);
            const auto it = sessions_.find(client_id);
            if (it != sessions_.end() && it->second == session) {
                session->closed = true;
                sessions_.erase(it);
            }
        }
    }

   private:
    std::shared_ptr<Session> Find(const UUID& client_id) {
        std::shared_lock registry(mutex_);
        const auto it = sessions_.find(client_id);
        return it == sessions_.end() ? nullptr : it->second;
    }

    std::shared_mutex mutex_;
    std::unordered_map<UUID, std::shared_ptr<Session>, boost::hash<UUID>>
        sessions_;
    uint64_t next_id_{1};  // guarded by mutex_; never reused within this Master
};

}  // namespace mooncake
