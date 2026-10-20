#pragma once

// ObjectRoute: one keyspace's key -> object route, its group index, and the
// lock that guards each key.
//
// The lock belongs to the key, not to an object. Every key the route knows has
// one KeySlot: a lock and, while the key holds an object, that object's record.
// A slot outlives the objects published in it, so a writer can lock a key that
// holds no object yet, replace an object without a window in which the key is
// absent, and tear one down by clearing the record. While a slot exists it is
// the only one for its key, so holding its lock excludes every other access to
// the key, the record and its group membership.
//
// Each publication gets a Generation, unique within the route, so code that
// lets go of a key and comes back later (a scan that evicts afterwards, a
// durable callback) names the object it judged with an ObjectRef and acts only
// while that same publication is still there.
//
// Locking: a stripe lock is held only to find, create or drop a slot, and
// across a cursor, which only tries slot locks while it holds one. Nothing
// waits for a slot lock while holding a stripe lock, so a slot holder may
// still look up other keys. A thread holds at most one slot lock at a time (a
// cursor holds the one it stands on), since two writers each holding one key
// and waiting for the other's would deadlock. The group index is a leaf under
// a slot lock.
//
// Layout: every key costs one allocation. Its slot lives in its stripe's map
// node, next to the key it is filed under, and holds the object inline. Guards
// and cursors keep a slot in the map with a SlotHandle. The slot counts its
// handles and whether it holds an object in one word, so the release of the
// last handle on a slot with no object sees that it is the last one and drops
// the slot from the map; nothing else ever does.
//
// Lifetime: guards and cursors point into the route, so its owner keeps it
// alive for as long as any of them exists; a route destroyed under one aborts.
//
// Owner state: every slot also carries a few words and two list hooks for the
// route's owner (see SlotOwnerState), which the route stores and never reads.
// The owner keeps them in step with each write through the RouteObserver, so
// they describe the key as of the last write guard released on it.

#include <array>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/intrusive/list.hpp>
#include <glog/logging.h>

#include "common/transparent_string_hash.h"
#include "metadata/group_index.h"
#include "metadata/object_state.h"
#include "mutex.h"
#include "object_metadata.h"

namespace mooncake {

// How a key is locked: shared to read it, exclusive to write it.
enum class LockMode { kRead, kWrite };

namespace test {
struct ObjectRouteTestPeer;
}  // namespace test

namespace route {

// Names one publication of a key within a route. Never reused, never
// persisted: a restore assigns fresh ones.
using Generation = uint64_t;

// A publication, named so it can be found again after its lock is let go.
struct ObjectRef {
    std::string key;
    Generation generation{0};
};

// What the route's owner keeps on every slot, guarded by the slot lock. The
// route stores it and never reads it; it survives the objects published in the
// slot, so the owner returns it to its initial value once the key holds none,
// before the slot can be dropped.
struct SlotOwnerState {
    // Whether the owner's in-flight list holds the slot.
    bool in_flight_listed{false};
    // Whether the owner's eviction index files the slot, at what rank, and
    // under which of the index's own entries for a group, if any.
    bool eviction_filed{false};
    int64_t eviction_rank{0};
    void* eviction_group{nullptr};
    // Two words for the owner's namespace policy.
    std::array<uint64_t, 2> policy_words{};
};

// A slot's lock as a guard or cursor holds it in `kMode`.
template <LockMode kMode>
using SlotLock = std::conditional_t<kMode == LockMode::kWrite,
                                    std::unique_lock<std::shared_mutex>,
                                    std::shared_lock<std::shared_mutex>>;

class ObjectRoute;
template <LockMode kMode>
class Guard;
using ReadGuard = Guard<LockMode::kRead>;
using WriteGuard = Guard<LockMode::kWrite>;

// One key's lock and, while the key holds an object, its record. A slot lives
// in its stripe's map node and never moves. Only the route and its guards
// reach inside.
class KeySlot {
   public:
    KeySlot() = default;
    KeySlot(const KeySlot&) = delete;
    KeySlot& operator=(const KeySlot&) = delete;

    const std::string& key() const noexcept { return *key_; }

   private:
    friend class ObjectRoute;
    friend class SlotHandle;
    template <LockMode>
    friend class Guard;
    friend struct test::ObjectRouteTestPeer;

    // Set in state_ while the slot holds an object.
    static constexpr uint32_t kOccupied = uint32_t{1} << 31;

    // Links the slot into one of its owner's lists. Neither the hook nor the
    // list synchronizes: whatever guards a list guards every hook in it. A
    // safe-link hook asserts it is unlinked when destroyed, and a list unlinks
    // whatever it still holds when it is cleared or destroyed.
    using ListHook = boost::intrusive::list_member_hook<
        boost::intrusive::link_mode<boost::intrusive::safe_link>>;

    // One publication. The metadata is built in place, since it can be
    // neither copied nor moved.
    struct Record {
        template <typename... Args>
        explicit Record(Generation generation, Args&&... metadata_args)
            : generation(generation),
              metadata(std::forward<Args>(metadata_args)...) {}

        Generation generation;
        ObjectMetadata metadata;
        ObjectState state;
    };

    // The key the map files this slot under, in the same node.
    const std::string* key_ = nullptr;
    // How many SlotHandles hold the slot, with kOccupied set while record_
    // holds an object; the map's own entry is not a holder. Exactly 1 means a
    // sole holder and no object, so that holder's release drops the slot.
    std::atomic<uint32_t> state_{0};
    mutable std::shared_mutex mutex_;
    std::optional<Record> record_;
    SlotOwnerState owner_;
    // Hooks into the owner's in-flight list and its eviction index.
    ListHook in_flight_hook_;
    ListHook eviction_hook_;

   public:
    // The owner's lists of slots. Each names its hook here, where the hook is
    // accessible, so the slot needs no friend for them. The owner links a slot
    // only while it holds an object, so a slot is never dropped linked.
    using InFlightList = boost::intrusive::list<
        KeySlot, boost::intrusive::member_hook<KeySlot, ListHook,
                                               &KeySlot::in_flight_hook_>>;
    using EvictionList = boost::intrusive::list<
        KeySlot, boost::intrusive::member_hook<KeySlot, ListHook,
                                               &KeySlot::eviction_hook_>>;
};

// Each slot is built in its map node and stays there while the map grows, so a
// slot's address and its key's are stable.
using SlotMap = std::unordered_map<std::string, KeySlot, TransparentStringHash,
                                   std::equal_to<>>;

// One of a route's stripes: a share of its slots under one lock.
struct Stripe {
    mutable std::shared_mutex lock;
    SlotMap slots;
};

// A counted hold on a slot. A slot that holds no object stays in the route's
// map while any handle holds it, so a holder can still lock it; the release of
// the last such handle drops it. A handle is only made under the slot's stripe
// lock, so a release holding that lock exclusively knows no new holder can
// appear. The route owns its slots, so a handle must not outlive it.
class SlotHandle {
   public:
    SlotHandle() = default;
    SlotHandle(Stripe& stripe, KeySlot& slot) noexcept
        : stripe_(&stripe), slot_(&slot) {
        slot.state_.fetch_add(1, std::memory_order_relaxed);
    }
    SlotHandle(SlotHandle&& other) noexcept
        : stripe_(std::exchange(other.stripe_, nullptr)),
          slot_(std::exchange(other.slot_, nullptr)) {}
    SlotHandle(const SlotHandle&) = delete;
    SlotHandle& operator=(const SlotHandle&) = delete;
    SlotHandle& operator=(SlotHandle&&) = delete;
    ~SlotHandle() { reset(); }

    KeySlot* get() const noexcept { return slot_; }
    KeySlot& operator*() const noexcept { return *slot_; }
    KeySlot* operator->() const noexcept { return slot_; }
    explicit operator bool() const noexcept { return slot_ != nullptr; }

    // Lets go of the slot, and drops it from the map when this was its last
    // handle and it holds no object. That takes the stripe lock and destroys
    // the slot, so the holder lets go only once it holds neither the slot's
    // lock nor a stripe lock.
    void reset() noexcept;

   private:
    Stripe* stripe_ = nullptr;
    KeySlot* slot_ = nullptr;
};

inline void SlotHandle::reset() noexcept {
    if (slot_ == nullptr) {
        return;
    }
    Stripe* const stripe = std::exchange(stripe_, nullptr);
    KeySlot* const slot = std::exchange(slot_, nullptr);
    // Anything but a sole holder of an empty slot only counts down. The
    // compare-exchange decides on the value it replaces, so an object
    // published or torn down by another holder meanwhile is not missed.
    uint32_t state = slot->state_.load(std::memory_order_relaxed);
    while (state != 1) {
        if (slot->state_.compare_exchange_weak(state, state - 1,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)) {
            return;
        }
    }
    // This handle is the last and the slot is empty. Under the exclusive
    // stripe lock no new holder can appear, and one that joined while this
    // waited is seen in the count, so the slot goes only if it is still the
    // last. Until then this handle keeps it alive. The acquire orders the
    // other holders' accesses before the slot is destroyed.
    std::unique_lock<std::shared_mutex> lock(stripe->lock);
    if (slot->state_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        stripe->slots.erase(stripe->slots.find(slot->key()));
    }
}

// Called by the route as a guard it handed out is released, with the slot
// locked: what the owner keeps about the key is settled there, so the code
// that changed the key need not do it. Read releases are reported only to a
// route built to report them, since every read pays for the call.
class RouteObserver {
   public:
    virtual void OnWriteRelease(const WriteGuard& guard) = 0;
    virtual void OnReadRelease(const ReadGuard& /*guard*/) {}

   protected:
    ~RouteObserver() = default;
};

// Holds one key, read or write, for the caller's scope. Only the route makes
// one. A write guard may hold a key with no object (see
// ObjectRoute::WriteKey); a read guard always holds an object.
//
// A guard holds its slot with a SlotHandle, so the slot outlives its lock. It
// moves but does not assign, since assigning would drop one key's lock while
// taking another's.
template <LockMode kMode>
class Guard {
    static constexpr bool kWrite = kMode == LockMode::kWrite;
    using Lock = SlotLock<kMode>;
    // A read guard reaches its route only through const members.
    using Route = std::conditional_t<kWrite, ObjectRoute, const ObjectRoute>;

   public:
    using Metadata =
        std::conditional_t<kWrite, ObjectMetadata, const ObjectMetadata>;
    using State = std::conditional_t<kWrite, ObjectState, const ObjectState>;

    Guard(Guard&& other) noexcept
        : route_(std::exchange(other.route_, nullptr)),
          slot_(std::move(other.slot_)),
          lock_(std::move(other.lock_)),
          handed_out_(other.handed_out_) {}
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&) = delete;
    Guard& operator=(Guard&&) = delete;
    ~Guard();

    const std::string& key() const { return slot_->key(); }

    // Whether the key holds an object.
    bool has_object() const { return slot_->record_.has_value(); }

    // The rest require has_object().
    Generation generation() const { return record().generation; }
    ObjectRef ref() const { return {key(), generation()}; }
    Metadata& metadata() const { return record().metadata; }
    State& state() const { return record().state; }

    // Publishes an object under this key, which must hold none: builds its
    // metadata in place from `metadata_args`, ObjectMetadata's constructor
    // arguments, and wires its group membership and the group's shared lease;
    // the state starts empty. Returns the new publication's generation.
    //
    // Two caller bugs abort in every build rather than corrupt the route: a
    // key that already holds an object, whose group membership and count the
    // new one would strand, and metadata whose user_key is not this key.
    //
    // Nobody else can reach the object until the guard is released, so the
    // caller finishes setting it up through metadata() and state().
    template <typename... Args>
    Generation Publish(Args&&... metadata_args) const
        requires kWrite;

    // Ends the publication this guard holds: drops its group membership and
    // the record. The caller gives back whatever hangs off the object first,
    // under this same guard. A later Publish on the guard starts a new
    // publication of the key with nothing in between. A key that holds no
    // object aborts in every build.
    void TearDown() const
        requires kWrite;

    // The owner's per-slot state and the slot its lists link, for the
    // route's owner only.
    SlotOwnerState& owner_state() const
        requires kWrite
    {
        return slot_->owner_;
    }
    KeySlot& slot() const { return *slot_; }

   private:
    friend class ObjectRoute;

    Guard(Route* route, SlotHandle slot) NO_THREAD_SAFETY_ANALYSIS
        : route_(route),
          slot_(std::move(slot)),
          lock_(slot_->mutex_) {}

    // Marks the guard as handed to a caller, so its release is reported to
    // the observer; one the route takes and drops itself, finding the key
    // empty or on another publication, is not.
    Guard&& HandOut() && {
        handed_out_ = true;
        return std::move(*this);
    }

    KeySlot::Record& record() const {
        assert(slot_->record_.has_value());
        return *slot_->record_;
    }

    Route* route_;
    // Declared before the lock so it is destroyed after it.
    SlotHandle slot_;
    Lock lock_;
    bool handed_out_ = false;
};

class ObjectRoute {
   public:
    // A stripe's map rehashes under its exclusive lock, which stalls every
    // reader of the stripe, and a batch read touches every stripe. Many
    // small stripes keep each stall short.
    static constexpr size_t kStripeCount = 1024;

    // `observer`, when set, outlives the route; it hears of read releases
    // only when `report_reads` is set.
    explicit ObjectRoute(RouteObserver* observer = nullptr,
                         bool report_reads = false)
        : observer_(observer),
          report_reads_(observer != nullptr && report_reads) {}
    ObjectRoute(const ObjectRoute&) = delete;
    ObjectRoute& operator=(const ObjectRoute&) = delete;

    // Guards and cursors point into the route, so none may outlive it: the
    // owner keeps the route alive for as long as it hands them out (a tenant's
    // route through the registry's shared_ptr to the tenant). Debug builds
    // abort when a route is destroyed under a guard or a cursor.
    ~ObjectRoute();

    // The object under `key`, held shared; nullopt when the key holds none.
    [[nodiscard]] std::optional<ReadGuard> Read(std::string_view key) const {
        return Acquire<LockMode::kRead>(*this, key, std::nullopt);
    }
    // The same, only while `ref` is still the key's current publication.
    [[nodiscard]] std::optional<ReadGuard> Read(const ObjectRef& ref) const {
        return Acquire<LockMode::kRead>(*this, ref.key, ref.generation);
    }

    // The object under `key`, held exclusively; nullopt when it holds none.
    [[nodiscard]] std::optional<WriteGuard> Write(std::string_view key) {
        return Acquire<LockMode::kWrite>(*this, key, std::nullopt);
    }
    // The same, only while `ref` is still the key's current publication.
    [[nodiscard]] std::optional<WriteGuard> Write(const ObjectRef& ref) {
        return Acquire<LockMode::kWrite>(*this, ref.key, ref.generation);
    }
    // The key held exclusively whether or not it holds an object, so the
    // caller can publish one.
    [[nodiscard]] WriteGuard WriteKey(std::string_view key);

    // Whether `key` holds an object right now. It takes the key's lock, so a
    // caller already holding that key must not ask.
    [[nodiscard]] bool Contains(std::string_view key) const {
        return Read(key).has_value();
    }

    [[nodiscard]] size_t ObjectCount() const {
        return object_count_.load(std::memory_order_relaxed);
    }

    // True when the route holds no object and no group membership.
    [[nodiscard]] bool Empty() const {
        return ObjectCount() == 0 && groups_.Empty();
    }

    template <LockMode kMode>
    class Cursor;

    // A cursor over every object, each visited under its own key's read or
    // write lock:
    //
    //     for (auto object : route.ReadCursor()) { ... object.metadata() ... }
    //
    // Each object is visited at most once, and the cursor moves one stripe at
    // a time rather than reading one point in time: an object published or
    // torn down meanwhile may or may not be seen. A write cursor may change an
    // object's metadata and state but not publish or tear down, and it does
    // not run the observer, so it must not start or finish in-flight work,
    // and the owner hears of nothing else it changes either.
    //
    // The cursor holds a stripe lock across the loop body, so the body must
    // not reach this route or another one (debug builds assert it), nor block
    // on a lock whose holder may be waiting on a route. One that acts after
    // the cursor keeps `ref()` and goes through Write(ref).
    [[nodiscard]] Cursor<LockMode::kRead> ReadCursor() const;
    [[nodiscard]] Cursor<LockMode::kWrite> WriteCursor();

    // The member keys of one group, as a snapshot to resolve again: a key in
    // it may hold a different object by the time the caller reads it, or none.
    [[nodiscard]] std::vector<std::string> GroupMembers(
        std::string_view group_id) const {
        return groups_.Members(group_id);
    }

    // Rebuilds group membership and the group leases from object metadata,
    // for the snapshot and standby restore paths. The first pass takes the
    // latest restored deadline per group, so a grouped object is not left on a
    // zero-deadline lease that post-restore cleanup would drop; the second
    // registers membership and points every grouped object at its group's
    // shared lease.
    void RebuildGroupState();

   private:
    template <LockMode>
    friend class Guard;
    friend struct test::ObjectRouteTestPeer;

    [[nodiscard]] static size_t StripeIndex(std::string_view key) {
        return TransparentStringHash{}(key) % kStripeCount;
    }
    [[nodiscard]] Stripe& StripeOf(std::string_view key) const {
        return stripes_[StripeIndex(key)];
    }

    // The key locked in `kMode` on `route`, handed out only while it holds an
    // object and, given a `generation`, only while that is the current
    // publication. A read takes the route const, a write takes it mutable.
    template <LockMode kMode>
    [[nodiscard]] static std::optional<Guard<kMode>> Acquire(
        typename Guard<kMode>::Route& route, std::string_view key,
        std::optional<Generation> generation);

    // A handle on `key`'s slot; empty when the route has none.
    [[nodiscard]] SlotHandle FindSlot(std::string_view key) const;
    [[nodiscard]] SlotHandle FindOrCreateSlot(std::string_view key);

    template <typename... Args>
    Generation PublishLocked(KeySlot& slot, Args&&... metadata_args);
    void TearDownLocked(KeySlot& slot);

    // How many stripe locks this thread holds for a cursor. A route access
    // made while one is held is the cursor's loop body reaching a route, which
    // can deadlock against a writer that holds a slot and waits on a stripe.
    static inline thread_local int cursor_stripes_ = 0;
    static void AssertNotInCursor() {
        assert(cursor_stripes_ == 0 &&
               "route access from inside an object cursor");
    }

    RouteObserver* const observer_;
    const bool report_reads_;
    std::atomic<Generation> next_generation_{1};
    std::atomic<size_t> object_count_{0};
    // How many cursors are open on the route, counted in debug builds only
    // so that the destructor can catch one. It exists in every build, so the
    // route's layout does not depend on NDEBUG.
    mutable std::atomic<uint32_t> live_cursors_{0};
    mutable std::array<Stripe, kStripeCount> stripes_;
    // Group membership and the one shared Lease per group.
    GroupIndex groups_;
};

// --- Guard ------------------------------------------------------------------

template <LockMode kMode>
Guard<kMode>::~Guard() {
    if (route_ == nullptr) {
        return;  // moved from
    }
    if constexpr (kWrite) {
        if (handed_out_ && route_->observer_ != nullptr) {
            route_->observer_->OnWriteRelease(*this);
        }
    } else {
        if (handed_out_ && route_->report_reads_) {
            route_->observer_->OnReadRelease(*this);
        }
    }
    // The lock goes first, then the handle, whose release drops the slot if
    // it is the last and the key holds no object.
}

template <LockMode kMode>
template <typename... Args>
Generation Guard<kMode>::Publish(Args&&... metadata_args) const
    requires kWrite
{
    if (has_object()) {
        LOG(FATAL) << "Publish on key " << key()
                   << ", which already holds an object";
    }
    return route_->PublishLocked(*slot_, std::forward<Args>(metadata_args)...);
}

template <LockMode kMode>
void Guard<kMode>::TearDown() const
    requires kWrite
{
    if (!has_object()) {
        LOG(FATAL) << "TearDown on key " << key() << ", which holds no object";
    }
    route_->TearDownLocked(*slot_);
}

// --- ObjectRoute::Cursor ----------------------------------------------------

// The single-pass cursor behind ReadCursor() and WriteCursor(). It owns
// the locks of the position it stands on, so it neither copies nor moves,
// and leaving the loop early releases them.
//
// While it holds a stripe the cursor only tries a slot's lock: a slot
// busy elsewhere is set aside and visited after the stripes, under a lock
// it waits for.
template <LockMode kMode>
class ObjectRoute::Cursor {
    static constexpr bool kWrite = kMode == LockMode::kWrite;
    using Lock = SlotLock<kMode>;

   public:
    // The object a position stands on, valid until the cursor advances.
    class Object {
       public:
        using Metadata = typename Guard<kMode>::Metadata;
        using State = typename Guard<kMode>::State;

        const std::string& key() const { return slot_.key(); }
        Generation generation() const { return slot_.record_->generation; }
        ObjectRef ref() const { return {key(), generation()}; }
        Metadata& metadata() const { return slot_.record_->metadata; }
        State& state() const { return slot_.record_->state; }
        // The owner's per-slot state, for the route's owner only.
        SlotOwnerState& owner_state() const
            requires kWrite
        {
            return slot_.owner_;
        }

       private:
        friend class Cursor;
        explicit Object(KeySlot& slot) : slot_(slot) {}
        KeySlot& slot_;
    };

    struct Sentinel {};

    class Iterator {
       public:
        Object operator*() const { return Object(*cursor_->current_); }
        Iterator& operator++() {
            cursor_->Advance();
            return *this;
        }
        bool operator!=(Sentinel) const { return cursor_->current_ != nullptr; }

       private:
        friend class Cursor;
        explicit Iterator(Cursor* cursor) : cursor_(cursor) {}
        Cursor* cursor_;
    };

    Cursor(const Cursor&) = delete;
    Cursor& operator=(const Cursor&) = delete;
    ~Cursor() {
        AssertInFlightUnchanged();
        UnlockSlot();
        UnlockStripe();
        // Done with the route before the count says so.
        deferred_.clear();
#ifndef NDEBUG
        route_.live_cursors_.fetch_sub(1, std::memory_order_relaxed);
#endif
    }

    Iterator begin() { return Iterator(this); }
    Sentinel end() const { return {}; }

   private:
    friend class ObjectRoute;

    explicit Cursor(const ObjectRoute& route) : route_(route) {
#ifndef NDEBUG
        route_.live_cursors_.fetch_add(1, std::memory_order_relaxed);
#endif
        Settle();
    }

    void Advance() {
        AssertInFlightUnchanged();
        UnlockSlot();
        Settle();
    }

    // Moves to the next object not yet passed whose lock it obtains, or to
    // the end. The position is always past the slot stood on, so this
    // resumes from it whichever phase the cursor is in.
    void Settle() NO_THREAD_SAFETY_ANALYSIS {
        for (; stripe_ < kStripeCount; ++stripe_) {
            Stripe& stripe = route_.stripes_[stripe_];
            if (!stripe_lock_.owns_lock()) {
                AssertNotInCursor();
                stripe_lock_ = std::shared_lock<std::shared_mutex>(stripe.lock);
                ++cursor_stripes_;
                slot_it_ = stripe.slots.begin();
            }
            while (slot_it_ != stripe.slots.end()) {
                KeySlot& slot = (slot_it_++)->second;
                Lock lock(slot.mutex_, std::try_to_lock);
                if (!lock.owns_lock()) {
                    deferred_.emplace_back(stripe, slot);
                    continue;
                }
                if (slot.record_.has_value()) {
                    Stand(slot, std::move(lock));
                    return;
                }
            }
            UnlockStripe();
        }
        while (deferred_pos_ < deferred_.size()) {
            KeySlot& slot = *deferred_[deferred_pos_++];
            Lock lock(slot.mutex_);
            if (slot.record_.has_value()) {
                Stand(slot, std::move(lock));
                return;
            }
        }
        // No lock is held any more, so these releases may drop empty slots.
        deferred_.clear();
        current_ = nullptr;
    }

    // A write cursor does not run the observer, so the loop body must leave
    // the in-flight work of the object it stands on as it found it, whether
    // the cursor then advances or the loop is left early.
    void AssertInFlightUnchanged() const {
        assert((!kWrite || current_ == nullptr ||
                current_->record_->state.HasWorkInFlight() ==
                    visited_in_flight_) &&
               "a write cursor's loop body changed in-flight work");
    }

    void Stand(KeySlot& slot, Lock lock) {
        slot_lock_ = std::move(lock);
        current_ = &slot;
        visited_in_flight_ = slot.record_->state.HasWorkInFlight();
    }

    void UnlockSlot() {
        if (slot_lock_.owns_lock()) {
            slot_lock_.unlock();
        }
    }

    void UnlockStripe() {
        if (stripe_lock_.owns_lock()) {
            stripe_lock_.unlock();
            --cursor_stripes_;
        }
    }

    const ObjectRoute& route_;
    size_t stripe_ = 0;
    std::shared_lock<std::shared_mutex> stripe_lock_;
    // The next slot to try in the stripe held.
    typename SlotMap::iterator slot_it_;
    // Slots that were busy when the cursor passed their stripe; the handles
    // keep them in the map until the cursor has visited them all.
    std::vector<SlotHandle> deferred_;
    size_t deferred_pos_ = 0;
    Lock slot_lock_;
    KeySlot* current_ = nullptr;
    // Whether the object stood on carried in-flight work when reached.
    bool visited_in_flight_ = false;
};

// --- ObjectRoute ------------------------------------------------------------

inline ObjectRoute::~ObjectRoute() {
#ifndef NDEBUG
    // A route goes only with its owner, once nothing reaches it any more, so a
    // holder left here is a caller bug, which debug builds catch: an open
    // cursor by the count, and a guard by its slot. A slot with no holder and
    // no object is already gone, so the walk covers the objects plus the slots
    // someone still holds. A holder lets go before its owner lets go of the
    // route, so relaxed loads see every release.
    if (live_cursors_.load(std::memory_order_relaxed) != 0) {
        LOG(FATAL) << "ObjectRoute destroyed under an open cursor";
    }
    for (const Stripe& stripe : stripes_) {
        for (const auto& [key, slot] : stripe.slots) {
            if ((slot.state_.load(std::memory_order_relaxed) &
                 ~KeySlot::kOccupied) != 0) {
                LOG(FATAL) << "ObjectRoute destroyed while key " << key
                           << " is still held";
            }
        }
    }
#endif
}

inline SlotHandle ObjectRoute::FindSlot(std::string_view key) const {
    AssertNotInCursor();
    Stripe& stripe = StripeOf(key);
    std::shared_lock<std::shared_mutex> lock(stripe.lock);
    const auto it = stripe.slots.find(key);
    return it == stripe.slots.end() ? SlotHandle()
                                    : SlotHandle(stripe, it->second);
}

inline SlotHandle ObjectRoute::FindOrCreateSlot(std::string_view key) {
    if (SlotHandle slot = FindSlot(key)) {
        return slot;
    }
    Stripe& stripe = StripeOf(key);
    std::unique_lock<std::shared_mutex> lock(stripe.lock);
    const auto [it, created] = stripe.slots.try_emplace(std::string(key));
    if (created) {
        it->second.key_ = &it->first;
    }
    return SlotHandle(stripe, it->second);
}

template <LockMode kMode>
std::optional<Guard<kMode>> ObjectRoute::Acquire(
    typename Guard<kMode>::Route& route, std::string_view key,
    std::optional<Generation> generation) {
    SlotHandle slot = route.FindSlot(key);
    if (!slot) {
        return std::nullopt;
    }
    Guard<kMode> guard(&route, std::move(slot));
    if (!guard.has_object() ||
        (generation.has_value() && guard.generation() != *generation)) {
        return std::nullopt;
    }
    return std::optional<Guard<kMode>>(std::move(guard).HandOut());
}

inline WriteGuard ObjectRoute::WriteKey(std::string_view key) {
    return WriteGuard(this, FindOrCreateSlot(key)).HandOut();
}

template <typename... Args>
Generation ObjectRoute::PublishLocked(KeySlot& slot, Args&&... metadata_args) {
    const Generation generation =
        next_generation_.fetch_add(1, std::memory_order_relaxed);
    ObjectMetadata& metadata =
        slot.record_.emplace(generation, std::forward<Args>(metadata_args)...)
            .metadata;
    slot.state_.fetch_or(KeySlot::kOccupied, std::memory_order_relaxed);
    if (metadata.user_key != slot.key()) {
        LOG(FATAL) << "Publish under key " << slot.key()
                   << " of an object keyed " << metadata.user_key;
    }
    if (!metadata.group_id.empty()) {
        // AddMember returns null only for an empty group_id.
        auto lease = groups_.AddMember(metadata.group_id, slot.key());
        assert(lease != nullptr);
        SpinLocker locker(&metadata.lock);
        metadata.lease_ = std::move(lease);
    }
    object_count_.fetch_add(1, std::memory_order_relaxed);
    return generation;
}

inline void ObjectRoute::TearDownLocked(KeySlot& slot) {
    const std::string& group_id = slot.record_->metadata.group_id;
    if (!group_id.empty()) {
        (void)groups_.RemoveMember(group_id, slot.key());
    }
    slot.record_.reset();
    slot.state_.fetch_and(~KeySlot::kOccupied, std::memory_order_relaxed);
    object_count_.fetch_sub(1, std::memory_order_relaxed);
}

inline ObjectRoute::Cursor<LockMode::kRead> ObjectRoute::ReadCursor() const {
    return Cursor<LockMode::kRead>(*this);
}

inline ObjectRoute::Cursor<LockMode::kWrite> ObjectRoute::WriteCursor() {
    return Cursor<LockMode::kWrite>(*this);
}

inline void ObjectRoute::RebuildGroupState() {
    std::unordered_map<std::string, std::chrono::system_clock::time_point>
        max_deadline_by_group;
    for (auto object : ReadCursor()) {
        const ObjectMetadata& metadata = object.metadata();
        if (!metadata.IsGrouped()) {
            continue;
        }
        const auto deadline = metadata.EvictionDeadline();
        auto [it, inserted] =
            max_deadline_by_group.try_emplace(metadata.group_id, deadline);
        if (!inserted) {
            it->second = std::max(it->second, deadline);
        }
    }
    // The group index is a leaf under a slot lock, so the loop body registers
    // membership itself.
    for (auto object : WriteCursor()) {
        const std::string& group_id = object.metadata().group_id;
        if (group_id.empty()) {
            continue;
        }
        auto lease = groups_.AddMember(group_id, object.key());
        assert(lease != nullptr);
        const auto it = max_deadline_by_group.find(group_id);
        if (it != max_deadline_by_group.end()) {
            lease->ExtendTo(it->second);
        }
        SpinLocker locker(&object.metadata().lock);
        object.metadata().lease_ = std::move(lease);
    }
}

}  // namespace route
}  // namespace mooncake
