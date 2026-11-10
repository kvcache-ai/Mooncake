#pragma once

#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "client_session_event.h"

namespace mooncake {

// Fans liveness transitions out to the components that own the resources of a
// client session. Publication happens on the producer's thread, under the
// record's transition lock; delivery happens on this dispatcher's own thread,
// so a listener may take segment, metadata or snapshot locks.
class ClientSessionEventDispatcher {
   public:
    using Listener = std::function<void(const ClientSessionEvent&)>;

    ClientSessionEventDispatcher() = default;
    ~ClientSessionEventDispatcher();
    ClientSessionEventDispatcher(const ClientSessionEventDispatcher&) = delete;
    ClientSessionEventDispatcher& operator=(
        const ClientSessionEventDispatcher&) = delete;

    // Register before the first Start/Drain/Stop. Returns false for an empty
    // listener or once delivery has been enabled; Stop does not reopen
    // registration. Multiple components may register independently.
    [[nodiscard]] bool AddListener(Listener listener);

    // Queues one transition for delivery. Callers publish in state-change
    // order, holding the record's transition lock, so this only enqueues: it
    // must not run resource cleanup. A transition published after Stop is
    // dropped.
    void Publish(const ClientSessionEvent& event);

    // Listeners run in registration order, serially, and must not throw. Stop
    // drains queued events and joins; listener owners must outlive Stop. Do
    // not call Stop from a listener. Stop rejects subsequent transitions until
    // Start; stop transition producers first so no production transition loses
    // its side effects.
    void Start();
    void Stop();
    // Deliver everything queued on the caller's thread. Used for deterministic
    // ticks when no delivery thread is running.
    void Drain();

   private:
    void ThreadFunc();

    std::vector<Listener> listeners_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<ClientSessionEvent> events_;
    // Set by Stop: publication is rejected and the delivery thread exits after
    // a final drain.
    bool stopped_{false};
    bool listeners_frozen_{false};
    std::thread thread_;
};

}  // namespace mooncake
