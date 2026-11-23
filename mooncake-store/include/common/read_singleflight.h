#pragma once

#include <future>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace mooncake {

// Registration and publication only: callers retain their own I/O batching,
// result ownership and validation. Completed results are never cached here.
template <typename Key, typename Value, typename Hash = std::hash<Key>>
class ReadSingleFlight {
    struct State {
        std::shared_future<Value> result;
        bool joined = false;
    };

   public:
    class Ticket {
       public:
        Ticket(Ticket&&) = default;
        Ticket& operator=(Ticket&&) = delete;
        Ticket(const Ticket&) = delete;
        Ticket& operator=(const Ticket&) = delete;
        ~Ticket() {
            // Only the producer owns the promise. On unwind, remove its entry
            // before promise destruction wakes readers with broken_promise.
            if (producer_ && state_) Retire();
        }
        bool IsLeader() const { return producer_ != nullptr; }
        const std::shared_future<Value>& Result() const {
            return state_->result;
        }

        // Retire before inspecting joined or snapshotting a borrowed buffer:
        // no later reader can join once its source owner starts returning.
        bool Retire() {
            std::lock_guard lock(owner_->mutex_);
            auto it = owner_->flights_.find(key_);
            if (it != owner_->flights_.end() && it->second == state_)
                owner_->flights_.erase(it);
            return state_->joined;
        }
        void Publish(Value result) {
            Retire();
            producer_->set_value(std::move(result));
            producer_.reset();
        }

       private:
        friend class ReadSingleFlight;
        Ticket(ReadSingleFlight* owner, const Key& key)
            : owner_(owner), key_(key) {}
        ReadSingleFlight* owner_;
        Key key_;
        std::shared_ptr<State> state_;
        std::unique_ptr<std::promise<Value>> producer_;
    };

    Ticket Acquire(const Key& key) {
        Ticket ticket(this, key);
        std::lock_guard lock(mutex_);
        auto it = flights_.find(key);
        if (it != flights_.end()) {
            ticket.state_ = it->second;
            ticket.state_->joined = true;
        } else {
            ticket.producer_ = std::make_unique<std::promise<Value>>();
            ticket.state_ = std::make_shared<State>();
            ticket.state_->result = ticket.producer_->get_future().share();
            flights_.emplace(key, ticket.state_);
        }
        return ticket;
    }

    template <typename Predicate>
    void InvalidateIf(Predicate matches) {
        std::lock_guard lock(mutex_);
        for (auto it = flights_.begin(); it != flights_.end();) {
            if (matches(it->first))
                it = flights_.erase(it);
            else
                ++it;
        }
    }
    void Clear() {
        InvalidateIf([](const Key&) { return true; });
    }

   private:
    std::mutex mutex_;
    std::unordered_map<Key, std::shared_ptr<State>, Hash> flights_;
};

}  // namespace mooncake
