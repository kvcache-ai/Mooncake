/*
 * Copyright (c) 2024, Alibaba Group Holding Limited;
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ASYNC_SIMPLE_SIGNAL_H
#define ASYNC_SIMPLE_SIGNAL_H

#ifndef ASYNC_SIMPLE_USE_MODULES

#include <assert.h>
#include <any>
#include <atomic>
#include <bit>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "async_simple/Common.h"
#include "util/move_only_function.h"
#endif  // ASYNC_SIMPLE_USE_MODULES

namespace async_simple {

enum SignalType : uint64_t {
    None = 0,
    // low 32 bit signal only trigger once.
    // regist those signal's handler after triggered will failed and return
    // false.
    Terminate = 1,  // signal type: terminate
    // 1-16 bits reserve for async-simple
    // 17-32 bits could used for user-defined signal

    // high 32 bit signal, could trigger multiple times, emplace an handler
    // 33-48 bits reserve for async-simple
    // 49-64 bits could used for user-defined signal
    // after signal triggered will always success and return true.
    All = UINT64_MAX,
};

class Slot;

namespace detail {
struct SignalSlotSharedState;
constexpr uint64_t high32bits_mask = uint64_t{UINT32_MAX} << 32;
constexpr uint64_t MinMultiTriggerSignal = 1ull << 32;
}  // namespace detail

// The Signal type is used to emit a signal, whereas a Slot is used to
// receive signals. We can create a signal using factory methods and bind
// multiple Slots to the same Signal. When the Signal emits a signal, all
// bound `Slot`s will receive the corresponding signal.
// The Signal's life-time is not shorter than the bound Slots. Because those
// slots owns a shared_ptr of signal.
class Signal : public std::enable_shared_from_this<Signal> {
    friend class Slot;
    friend struct detail::SignalSlotSharedState;

private:
    // A list to manage different type of signal's handler.
    detail::SignalSlotSharedState& registSlot(SignalType filter);
    SignalType UpdateState(std::atomic<uint64_t>& state, SignalType type) {
        uint64_t expected = state.load(std::memory_order_acquire);
        uint64_t validSignal;
        do {
            validSignal = (expected | type) ^ expected;
        } while (validSignal &&
                 !state.compare_exchange_weak(expected, expected | type,
                                              std::memory_order_release));
        // high 32 bits signal is always valid, they can be trigger multiple
        // times. low 32 bits signal only trigger once.
        return static_cast<SignalType>(validSignal |
                                       (type & detail::high32bits_mask));
    }

    struct PrivateConstructTag {};

public:
    Signal(PrivateConstructTag){};
    // Same type Signal can only trigger once. This function emit a signal to
    // binding slots, then execute the slot callback functions. It will return
    // the signal which success triggered. If no signal success triggger, return
    // SignalType::none.
    SignalType emits(SignalType state) noexcept;

    // Return now signal type.
    SignalType state() const noexcept {
        return static_cast<SignalType>(_state.load(std::memory_order_acquire));
    }
    // Create Signal by Factory function.
    //
    template <typename T = Signal>
    static std::shared_ptr<T> create() {
        static_assert(std::is_base_of_v<Signal, T>,
                      "T should be signal or derived from Signal");
        return std::make_shared<T>(PrivateConstructTag{});
    }
    virtual ~Signal();

private:
    // Default _state is zero. It's value means what signal type has triggered.
    // It was updated by UpdateState(). If a signal is low 32bits, it can't emit
    // multiple times.
    std::atomic<uint64_t> _state;
    std::atomic<detail::SignalSlotSharedState*> _slotsHead;
};
namespace detail {
// Each Slot has a reference to SignalSlotSharedState.
// SignalSlotSharedState lifetime is managed by Signal.
// It will destructed when Signal destructed.
struct SignalSlotSharedState {
    // The type of Signal Handler
    using Handler = util::move_only_function<void(SignalType, Signal*)>;
    // The type of Transfer Signal Handler
    using ChainedSignalHandler = util::move_only_function<void(SignalType)>;
    struct HandlerManager {
        uint8_t _index;
        std::atomic<Handler*> _handler;
        HandlerManager* _next;
        HandlerManager(uint8_t index, HandlerManager* next)
            : _index(index), _next(next) {}
        void releaseNodes() {
            for (HandlerManager* tmp = _next; tmp != nullptr; tmp = _next) {
                _next = _next->_next;
                delete tmp;
            }
            delete this;
        }
        ~HandlerManager();
        static inline Handler emittedTag;
    };
    static bool isMultiTriggerSignal(SignalType type) {
        return type >= MinMultiTriggerSignal;
    }
    SignalSlotSharedState(SignalType filter)
        : _handlerManager(), _filter(filter), _next(nullptr) {}
    ~SignalSlotSharedState();
    void releaseNodes() {
        for (SignalSlotSharedState *node = this->_next, *next_node;
             node != nullptr; node = next_node) {
            next_node = node->_next;
            delete node;
        }
        delete this;
    }
    bool triggered(SignalType state) const noexcept {
        return static_cast<uint64_t>(state) &
               static_cast<uint64_t>(_filter.load(std::memory_order_acquire));
    }
    void setFilter(SignalType filter) noexcept {
        _filter.store(filter, std::memory_order_release);
    }
    SignalType getFilter() const noexcept {
        return _filter.load(std::memory_order_acquire);
    }
    void invoke(SignalType type, SignalType signal, Signal* self,
                std::atomic<Handler*>& handlerPtr);
    void operator()(SignalType type, Signal* self);
    std::atomic<HandlerManager*> _handlerManager;
    std::atomic<ChainedSignalHandler*> _chainedSignalHandler;
    std::atomic<SignalType> _filter;
    SignalSlotSharedState* _next;
};
}  // namespace detail

// `Slot` is used to receive signals for an asynchronous task. We can create a
// signal through factory methods and bind multiple `Slot`s to the same
// `Signal`. When a `Signal` is triggered, all `Slot`s will receive the
// corresponding signal. When construct Slot, we should bing it to a signal and
// extend the signal's life-time by own a shared_ptr of signal. Since each
// asynchronous task should own its own `Slot`, slot objects are not
// thread-safe. We prohibit the concurrent invocation of the public interface of
// the same `Slot`.

class Slot {
    friend class Signal;

public:
    // regist a slot to signal
    Slot(Signal* signal, SignalType filter = SignalType::All)
        : _signal(signal->shared_from_this()),
          _node(signal->registSlot(filter)){};
    Slot(const Slot&) = delete;

public:
    // Register a signal handler. Returns false if the signal has
    // already been triggered(and signal can't trigger twice).
    template <typename... Args>
    [[nodiscard]] bool emplace(SignalType type, Args&&... args) {
        static_assert(sizeof...(args) >= 1,
                      "we dont allow emplace an empty signal handler");
        logicAssert(std::popcount(static_cast<uint64_t>(type)) == 1,
                    "It's not allow to emplace for multiple signals");
        auto handler = std::make_unique<detail::SignalSlotSharedState::Handler>(
            std::forward<Args>(args)...);
        auto oldHandlerPtr = loadHandler<true>(type);
        // check trigger-once signal has already been triggered
        // if signal has already been triggered, return false
        if (!detail::SignalSlotSharedState::isMultiTriggerSignal(type) &&
            (signal()->state() & type)) {
            return false;
        }
        // if signal triggered later, we will found it by atomic handler CAS
        // failed.
        auto oldHandler = oldHandlerPtr->load(std::memory_order_acquire);
        if (oldHandler ==
            &detail::SignalSlotSharedState::HandlerManager::emittedTag) {
            return false;
        }
        auto new_handler = handler.release();
        if (!oldHandlerPtr->compare_exchange_strong(
                oldHandler, new_handler, std::memory_order_release)) {
            // if slot is triggered and new emplace handler doesn't exec, return
            // false
            assert(oldHandler ==
                   &detail::SignalSlotSharedState::HandlerManager::emittedTag);
            delete new_handler;
            return false;
        }
        delete oldHandler;
        return true;
    }
    // Clear the signal handler. If return false, it indicates
    // that the signal handler has been executed or handler is empty
    bool clear(SignalType type) {
        logicAssert(std::popcount(static_cast<uint64_t>(type)) == 1,
                    "It's not allow to emplace for multiple signals");
        auto oldHandlerPtr = loadHandler<false>(type);
        if (oldHandlerPtr == nullptr) {
            return false;
        }
        auto oldHandler = oldHandlerPtr->load(std::memory_order_acquire);
        if (oldHandler ==
                &detail::SignalSlotSharedState::HandlerManager::emittedTag ||
            oldHandler == nullptr) {
            return false;
        }
        if (!oldHandlerPtr->compare_exchange_strong(
                oldHandler, nullptr, std::memory_order_release)) {
            assert(oldHandler ==
                   &detail::SignalSlotSharedState::HandlerManager::emittedTag);
            return false;
        }
        delete oldHandler;
        return true;
    }

    class FilterGuard {
        friend class Slot;

    public:
        ~FilterGuard() noexcept { _slot->_node.setFilter(_oldFilter); }

    private:
        FilterGuard(Slot* slot, SignalType newFilter) noexcept
            : _slot(slot), _oldFilter(_slot->getFilter()) {
            auto filter = static_cast<uint64_t>(_oldFilter) &
                          static_cast<uint64_t>(newFilter);
            _slot->_node.setFilter(static_cast<SignalType>(filter));
        }
        Slot* _slot;
        SignalType _oldFilter;
    };

    // Filter signals within the specified scope. If signal type & filter is 0,
    // then the signal type will not be triggered within this scope. Nested
    // filters are allowed.
    // The returned guard object can be used to restore the previous filter.
    // It's life-time must shorter than Slot.
    [[nodiscard]] FilterGuard setScopedFilter(SignalType filter) noexcept {
        return FilterGuard{this, filter};
    }
    // Set the current scope's filter.
    void setFilter(SignalType filter) noexcept { _node.setFilter(filter); }

    // Get the current scope's filter.
    SignalType getFilter() const noexcept { return _node.getFilter(); }

    // Check whether the filtered signal has emitted.
    // if return true, the signal has emitted(but handler may hasn't execute
    // now)
    bool hasTriggered(SignalType type) const noexcept {
        return _node.triggered(
            static_cast<SignalType>(signal()->state() & type));
    }
    bool canceled() const noexcept {
        return hasTriggered(SignalType::Terminate);
    }

    // The slot holds ownership of the corresponding signal, so the signal's
    // lifetime is always no shorter than the slot's. To extend the signal's
    // lifetime, you can call signal()->shared_from_this(), or start a new
    // coroutine with the signal.
    Signal* signal() const noexcept { return _signal.get(); }

    // bind a chained signal to this slot. all triggered signal will transfer to
    // this chainedSignal.
    void chainedSignal(Signal* chainedSignal) {
        if (chainedSignal == nullptr) {
            _node._chainedSignalHandler = nullptr;
            return;
        }

        _node._chainedSignalHandler.store(
            new detail::SignalSlotSharedState::ChainedSignalHandler(
                [chainedSignal =
                     chainedSignal->weak_from_this()](SignalType type) {
                    if (auto signal = chainedSignal.lock(); signal != nullptr) {
                        signal->emits(type);
                    }
                }),
            std::memory_order_release);
    }
    ~Slot() {
        delete _node._chainedSignalHandler.exchange(nullptr,
                                                    std::memory_order_release);
    }

private:
    template <bool should_emplace>
    std::atomic<detail::SignalSlotSharedState::Handler*>* loadHandler(
        SignalType type) {
        uint8_t index = std::countr_zero(static_cast<uint64_t>(type));
        if (auto iter = _handlerTables.find(index);
            iter != _handlerTables.end()) {
            return iter->second;
        }
        if constexpr (!should_emplace) {
            return nullptr;
        } else {
            auto handlerManager =
                _node._handlerManager.load(std::memory_order_acquire);
            auto newManager = new detail::SignalSlotSharedState::HandlerManager(
                index, handlerManager);
            // It's ok here, write only has once thread
            _node._handlerManager.store(newManager, std::memory_order_release);
            _handlerTables.emplace(index, &newManager->_handler);
            return &newManager->_handler;
        }
    }

    // TODO: may be flatmap better?
    std::unordered_map<uint8_t,
                       std::atomic<detail::SignalSlotSharedState::Handler*>*>
        _handlerTables;
    std::shared_ptr<Signal> _signal;
    detail::SignalSlotSharedState& _node;
};

class SignalException : public std::runtime_error {
private:
    SignalType _signal;

public:
    explicit SignalException(SignalType signal, std::string msg = "")
        : std::runtime_error(std::move(msg)), _signal(signal) {}
    SignalType value() const { return _signal; }
};

// those helper cancel functions are used for coroutine await_resume,
// await_suspend & await_ready

struct signalHelper {
    signalHelper(SignalType sign) : _sign(sign) {}
    template <typename... Args>
    [[nodiscard]] bool tryEmplace(Slot* slot, Args&&... args) noexcept {
        if (slot &&
            !slot->emplace(_sign,
                           std::forward<Args>(args)...)) {  // has canceled
            return false;
        }
        return true;
    }

    void checkHasCanceled(Slot* slot, std::string error_msg = "") {
        if (slot && !slot->clear(_sign) && slot->canceled()) {
            if (error_msg.empty()) {
                error_msg =
                    "async-simple signal triggered and throw exception, signal "
                    "no: " +
                    std::to_string(std::countr_zero(uint64_t{_sign}));
            }
            // TODO: may add option for user to wait signal handler finished?
            throw SignalException{_sign, std::move(error_msg)};
        }
    }

    bool hasCanceled(const Slot* slot) noexcept {
        return slot && slot->hasTriggered(_sign);
    }
    SignalType _sign;
};

inline void detail::SignalSlotSharedState::invoke(
    SignalType type, SignalType signal, Signal* self,
    std::atomic<SignalSlotSharedState::Handler*>& handlerPtr) {
    if (!isMultiTriggerSignal(type)) {
        if (auto* handler = handlerPtr.exchange(
                &SignalSlotSharedState::HandlerManager::emittedTag,
                std::memory_order_acq_rel);
            handler != nullptr) {
            (*handler)(signal, self);
            delete handler;
        }
    } else {
        if (auto* handler = handlerPtr.load(std::memory_order_acquire);
            handler != nullptr) {
            (*handler)(signal, self);
        }
    }
}

inline void detail::SignalSlotSharedState::operator()(SignalType type,
                                                      Signal* self) {
    uint64_t state = type & _filter.load(std::memory_order_acquire);
    auto handlerManager = _handlerManager.load(std::memory_order_acquire);
    if (auto* handler = _chainedSignalHandler.load(std::memory_order_acquire);
        handler != nullptr && state) {
        (*handler)(static_cast<SignalType>(state));
    }
    while (state && handlerManager) {
        SignalType nowSignal =
            static_cast<SignalType>(uint64_t{1} << handlerManager->_index);
        if (nowSignal & state) {
            invoke(nowSignal, static_cast<SignalType>(state), self,
                   handlerManager->_handler);
            state ^= nowSignal;
        }
        handlerManager = handlerManager->_next;
    }
}

inline detail::SignalSlotSharedState::~SignalSlotSharedState() {
    if (auto manager = _handlerManager.load(std::memory_order_acquire);
        manager) {
        manager->releaseNodes();
    }
}

inline SignalType Signal::emits(SignalType state) noexcept {
    if (state != SignalType::None) {
        SignalType vaildSignal = UpdateState(_state, state);
        if (vaildSignal) {
            for (detail::SignalSlotSharedState* node =
                     _slotsHead.load(std::memory_order_acquire);
                 node != nullptr; node = node->_next) {
                (*node)(vaildSignal, this);
            }
        }
        return vaildSignal;
    }
    return SignalType::None;
}

inline detail::SignalSlotSharedState& Signal::registSlot(SignalType filter) {
    auto next_node = _slotsHead.load(std::memory_order_acquire);
    auto* node = new detail::SignalSlotSharedState{filter};
    node->_next = next_node;
    while (!_slotsHead.compare_exchange_weak(node->_next, node,
                                             std::memory_order_release,
                                             std::memory_order_relaxed))
        ;
    return *node;
}

inline Signal::~Signal() {
    if (auto node = _slotsHead.load(std::memory_order_acquire);
        node != nullptr) {
        node->releaseNodes();
    }
}

inline detail::SignalSlotSharedState::HandlerManager::~HandlerManager() {
    auto handler = _handler.load(std::memory_order_acquire);
    if (handler != &HandlerManager::emittedTag) {
        delete _handler;
    }
}

}  // namespace async_simple

#endif  // ASYNC_SIMPLE_SIGNAL_H