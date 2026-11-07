#include "client_session_event_dispatcher.h"

#include <glog/logging.h>

#include <utility>

namespace mooncake {

ClientSessionEventDispatcher::~ClientSessionEventDispatcher() { Stop(); }

bool ClientSessionEventDispatcher::AddListener(Listener listener) {
    std::lock_guard lock(mutex_);
    if (!listener || listeners_frozen_) {
        return false;
    }
    listeners_.push_back(std::move(listener));
    return true;
}

void ClientSessionEventDispatcher::Publish(const ClientSessionEvent& event) {
    {
        std::lock_guard lock(mutex_);
        if (stopped_) {
            return;
        }
        events_.push_back(event);
    }
    cv_.notify_one();
}

void ClientSessionEventDispatcher::Start() {
    CHECK(!thread_.joinable());
    {
        std::lock_guard lock(mutex_);
        listeners_frozen_ = true;
        stopped_ = false;
    }
    try {
        thread_ = std::thread(&ClientSessionEventDispatcher::ThreadFunc, this);
    } catch (...) {
        Stop();
        throw;
    }
}

void ClientSessionEventDispatcher::Stop() {
    {
        // Whatever was published before this point is still delivered below;
        // later transitions are rejected.
        std::lock_guard lock(mutex_);
        stopped_ = true;
    }
    cv_.notify_one();
    if (thread_.joinable()) {
        thread_.join();
    } else {
        Drain();
    }
}

void ClientSessionEventDispatcher::Drain() {
    for (;;) {
        std::deque<ClientSessionEvent> events;
        {
            std::lock_guard lock(mutex_);
            listeners_frozen_ = true;
            events.swap(events_);
        }
        if (events.empty()) {
            return;
        }
        for (const auto& event : events) {
            LOG(INFO) << "client_id=" << event.client_id
                      << ", action=client_session_transition, previous="
                      << toString(event.previous)
                      << ", current=" << toString(event.current);
            for (const auto& listener : listeners_) {
                listener(event);
            }
        }
    }
}

void ClientSessionEventDispatcher::ThreadFunc() {
    for (bool stopped = false; !stopped;) {
        {
            std::unique_lock lock(mutex_);
            cv_.wait(lock, [this] { return stopped_ || !events_.empty(); });
            stopped = stopped_;
        }
        Drain();
    }
}

}  // namespace mooncake
