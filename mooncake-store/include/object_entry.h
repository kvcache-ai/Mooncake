#pragma once

// ObjectEntry: the per-object runtime shell. The ObjectMetadata envelope is
// the single source of identity (user_key, group_id) and group-lease wiring;
// the entry adds the per-key runtime state, the flag marking the one
// publication it stands for, and the lock guarding all of it.
//
// The lock never leaves the class. Once the entry is published, everything
// below the identity is reachable only through a Hold, which only a Tenant
// makes after confirming under the lock that the route still publishes the
// entry, or the cursor of the route itself; before then, through
// WithUnpublished. A caller cannot act on half of a compound operation, nor
// change a published entry behind the tenant's back. The one exception is the
// in-flight hook, which belongs to the tenant's list of entries with work in
// flight and is guarded by that list's lock instead.

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <type_traits>
#include <utility>

#include "common/intrusive_list.h"
#include "object_metadata.h"
#include "object_runtime_state.h"

namespace mooncake {

namespace metadata {
class Tenant;
}  // namespace metadata

namespace test {
struct ObjectEntryTestPeer;
}  // namespace test

// How an entry is locked: shared to read it, exclusive to write it.
enum class LockMode { kRead, kWrite };

// Tags the hook that links an entry into its tenant's in-flight list.
struct InFlightListTag;

class ObjectEntry : private IntrusiveListHook<InFlightListTag> {
   public:
    // What the entry adds to the envelope: the per-key task state, at most one
    // in-flight task per entry, and the lifecycle claims over it.
    struct State {
        // A primary write or a background task is in flight for this key.
        bool is_processing{false};
        std::optional<ReplicationTask> replication_task;
        std::optional<OffloadingTask> offloading_task;
        std::optional<PromotionTask> promotion_task;
        std::optional<PromotionCandidate> promotion_candidate;
        std::optional<DynamicReplicaPending> dynamic_replication_pending;
        std::chrono::steady_clock::time_point dynamic_replication_cooldown{};

        // True while a primary write or a replication, offloading or
        // promotion task is in flight: the work an expiry sweep reclaims.
        [[nodiscard]] bool HasInFlightWork() const noexcept {
            return is_processing || replication_task.has_value() ||
                   offloading_task.has_value() || promotion_task.has_value();
        }

       private:
        friend class metadata::Tenant;  // claims it, and lists it in flight

        // Teardown-once claim, so a second eraser of the same entry does not
        // release its refcounts, quota charges and KV removal events again.
        bool is_torn_down{false};
        // Whether the tenant's in-flight list holds the entry, kept in step
        // with the list under this lock so a write hold that changed nothing
        // there settles without taking the list's lock.
        bool in_flight_listed{false};
    };

    // Takes ownership of a non-null metadata envelope; the envelope carries the
    // object's identity (user_key, group_id).
    explicit ObjectEntry(std::unique_ptr<ObjectMetadata> metadata)
        : metadata_(std::move(metadata)) {}

    // Not copyable/movable: it owns per-object state and a per-object lock.
    ObjectEntry(const ObjectEntry&) = delete;
    ObjectEntry& operator=(const ObjectEntry&) = delete;
    ObjectEntry(ObjectEntry&&) = delete;
    ObjectEntry& operator=(ObjectEntry&&) = delete;

    // Identity lives in the envelope, which holds both as const members, so
    // these read without taking `mutex_`.
    const std::string& key() const noexcept { return metadata_->user_key; }
    const std::string& group_id() const noexcept { return metadata_->group_id; }

    // True once the route has published this entry. An entry instance stands
    // for exactly one publication — `ObjectIndex::Insert` asserts that it is
    // published once — so a handle names one publication and no more.
    [[nodiscard]] bool IsPublished() const noexcept {
        return published_.load(std::memory_order_relaxed);
    }

    // Runs `fn(envelope, state)` on an entry no route has published yet, held
    // exclusively, and returns whatever `fn` returns: filling it in before
    // InsertObject publishes it, or giving back what it owns when the insert
    // finds the key taken. Once published, the entry is the tenant's, and only
    // its holds reach it.
    template <typename Fn>
    decltype(auto) WithUnpublished(Fn&& fn) {
        assert(!IsPublished());
        return WithExclusiveAccess(std::forward<Fn>(fn));
    }

    // What every locked view of an entry offers, a Hold or the object a cursor
    // stands on, given the view's `handle()`. The view is what keeps the
    // entry's lock, read or write, while these are used.
    template <typename View, LockMode kMode>
    class LockedAccess {
        static constexpr bool kWrite = kMode == LockMode::kWrite;

       public:
        using Metadata =
            std::conditional_t<kWrite, ObjectMetadata, const ObjectMetadata>;
        using State = std::conditional_t<kWrite, ObjectEntry::State,
                                         const ObjectEntry::State>;

        const std::string& key() const { return entry().key(); }
        Metadata& metadata() const NO_THREAD_SAFETY_ANALYSIS {
            return *entry().metadata_;
        }
        State& state() const NO_THREAD_SAFETY_ANALYSIS {
            return entry().state_;
        }

       private:
        ObjectEntry& entry() const {
            return *static_cast<const View&>(*this).handle();
        }
    };

    // The scoped form of the two above, for a caller whose critical section
    // is the rest of its own scope rather than one callback: the entry stays
    // held, read or write, until the hold is destroyed, under the same lock
    // order. Only a Tenant makes one (see Tenant::ReadHold and WriteHold),
    // after it has confirmed under the lock that the route still publishes the
    // entry, so a hold always names the current publication of its key.
    //
    // A hold owns a strong handle, so the entry outlives its lock. It can be
    // moved into place but not assigned, since assigning would replace the
    // handle before releasing the old entry's lock.
    //
    // The tenant may hang a hook on a hold, which runs as the hold ends, with
    // the lock still held: that is where a write hold settles what the tenant
    // keeps about the entry, so the code that changed the entry need not.
    template <LockMode kMode>
    class Hold : public LockedAccess<Hold<kMode>, kMode> {
        static constexpr bool kWrite = kMode == LockMode::kWrite;
        using Lock =
            std::conditional_t<kWrite, std::unique_lock<std::shared_mutex>,
                               std::shared_lock<std::shared_mutex>>;

       public:
        using ReleaseHook = void (*)(void* context, const Hold& hold);

        Hold(Hold&& other) noexcept
            : entry_(std::move(other.entry_)),
              lock_(std::move(other.lock_)),
              on_release_(std::exchange(other.on_release_, nullptr)),
              hook_context_(std::exchange(other.hook_context_, nullptr)) {}
        ~Hold() {
            if (on_release_ != nullptr) {
                on_release_(hook_context_, *this);
            }
        }
        Hold& operator=(Hold&&) = delete;
        Hold(const Hold&) = delete;
        Hold& operator=(const Hold&) = delete;

        const std::shared_ptr<ObjectEntry>& handle() const { return entry_; }

       private:
        friend class metadata::Tenant;

        explicit Hold(std::shared_ptr<ObjectEntry> entry,
                      ReleaseHook on_release = nullptr,
                      void* hook_context = nullptr) NO_THREAD_SAFETY_ANALYSIS
            : entry_(std::move(entry)),
              lock_(entry_->mutex_),
              on_release_(on_release),
              hook_context_(hook_context) {}

        // Declared first so it is destroyed last, after the lock.
        std::shared_ptr<ObjectEntry> entry_;
        Lock lock_;
        ReleaseHook on_release_;
        void* hook_context_;
    };
    using ReadHold = Hold<LockMode::kRead>;
    using WriteHold = Hold<LockMode::kWrite>;

   private:
    friend class ObjectIndex;  // claims the entry at route publication
    friend class IntrusiveList<ObjectEntry, InFlightListTag>;  // in-flight hook
    friend struct test::ObjectEntryTestPeer;

    // Runs `fn(envelope, state)` with the entry held exclusively and returns
    // whatever `fn` returns. Lock order: entry → route → metadata spin lock,
    // never the reverse for any pair. Both references last only for the call.
    template <typename Fn>
    decltype(auto) WithExclusiveAccess(Fn&& fn) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return std::forward<Fn>(fn)(*metadata_, state_);
    }

    // The same for readers: `fn` sees both halves but may not mutate them.
    template <typename Fn>
    decltype(auto) WithSharedAccess(Fn&& fn) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return std::forward<Fn>(fn)(std::as_const(*metadata_),
                                    std::as_const(state_));
    }

    std::unique_ptr<ObjectMetadata> metadata_;
    // Atomic because publication claims it under the route lock while a holder
    // reads it without any lock. Relaxed: the flag carries no other state.
    std::atomic<bool> published_{false};
    // Mutable so a const entry can still be read under the shared lock.
    mutable std::shared_mutex mutex_;
    State state_ GUARDED_BY(mutex_);
};

}  // namespace mooncake
