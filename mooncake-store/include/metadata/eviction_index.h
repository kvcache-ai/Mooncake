#pragma once

// EvictionIndex: the order in which eviction visits one namespace's objects.
// The namespace owns one and tells it about every key the route hands out: a
// read as its read guard is released, a write as its write guard is. Eviction
// reads it through a cursor.
//
// An entry is what eviction takes together: one key, or the members of a
// group, whose shared lease makes them due together. The index files a group
// as one entry, so a walk meets each group once.
//
// An index may rank keys however it likes (by lease deadline, by access
// frequency, by size) as long as it keeps one rule: the rank a key is filed at
// is never later than the rank it really has. A read may raise a key's real
// rank without telling the index, which then catches up when the cursor's
// StillAt is asked about the key under its write lock: that release files the
// key again at its real rank. So an index only has to watch reads when a read
// can lower a rank, and says so at construction; the route skips the read hook
// for every other index.
//
// Lock order: whatever lock an index takes is a leaf under a key lock, and
// nothing takes a key lock while holding it.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string_view>

#include "metadata/object_route.h"

namespace mooncake {
namespace metadata {

class EvictionIndex {
   public:
    using Clock = std::chrono::system_clock;
    // Lower ranks are visited first.
    using Rank = int64_t;

    // What the index filed at one rank: one key, or a group's members. The
    // keys view the cursor that handed the entry out, until it is asked for
    // the next entry or destroyed.
    struct Entry {
        std::span<const std::string_view> keys;
        Rank rank;
        // Publication or group incarnation identity, checked by StillAt.
        uint64_t identity = 0;
    };

    // One walk over the filed entries, in ascending rank order. What it hands
    // out is a snapshot: a key may be refiled or gone by the time it is acted
    // on.
    class Cursor {
       public:
        virtual ~Cursor() = default;
        // The next filed entry; nullopt once the walk is done.
        virtual std::optional<Entry> Next() = 0;
        // Whether the key `guard` holds still belongs to this entry and its
        // rank. Publication or group identity must still match. Asked under the
        // key's write lock, so a key whose rank grew since it was filed is
        // refiled when the lock is let go. Exact lease expiry is separately
        // revalidated by the evictor.
        [[nodiscard]] virtual bool StillAt(const route::WriteGuard& guard,
                                           const Entry& entry) const = 0;
        // How many keys were filed when the walk began.
        [[nodiscard]] virtual size_t Total() const = 0;
    };

    // Optional parallel range scan. A wave's schedule is immutable while
    // workers claim disjoint ranges; Widen runs only after all workers join.
    class RangeCursor : public Cursor {
       public:
        using Visitor = std::function<bool(const Entry&)>;
        virtual uint64_t Limit() const = 0;
        virtual uint64_t GrainSize() const { return 1; }
        virtual void VisitRange(uint64_t begin, uint64_t end,
                                const Visitor& visitor) const = 0;
        virtual bool Widen() = 0;
        virtual uint64_t Examined() const = 0;
    };

    // Maps the snapshot's filed key count (`Cursor::Total()`) to a key-count
    // cutoff hint for the initial wave (0 means all due ranks).
    using TargetForTotal = std::function<size_t(size_t total)>;

    explicit EvictionIndex(bool observes_reads)
        : observes_reads_(observes_reads) {}
    EvictionIndex(const EvictionIndex&) = delete;
    EvictionIndex& operator=(const EvictionIndex&) = delete;
    virtual ~EvictionIndex() = default;

    // Called as a read guard on the key is released, with the key's lock held
    // shared, possibly by several readers at once; only when the index
    // observes reads. It must not touch the slot's owner state, which the
    // shared lock does not guard.
    virtual void OnRead(const route::ReadGuard& guard) = 0;
    // Called as a write guard on the key is released, with the key's lock
    // held exclusively: files, refiles or drops the key to match the object
    // it holds now.
    virtual void OnWrite(const route::WriteGuard& guard) = 0;
    // A walk over the keys that may be due by `now`.
    [[nodiscard]] virtual std::unique_ptr<Cursor> Scan(
        Clock::time_point now) const = 0;

    // Null for indexes that only support an ordered cursor. `target_for_total`
    // counts keys (groups remain one entry); it is a cutoff hint, not a scan
    // budget.
    [[nodiscard]] virtual std::unique_ptr<RangeCursor> ScanRanges(
        Clock::time_point /*now*/,
        const TargetForTotal& /*target_for_total*/) const {
        return nullptr;
    }

    [[nodiscard]] std::unique_ptr<RangeCursor> ScanRanges(Clock::time_point now,
                                                          size_t target) const {
        return ScanRanges(now, [target](size_t) { return target; });
    }

    // Whether OnRead must be called.
    [[nodiscard]] bool observes_reads() const { return observes_reads_; }

   private:
    const bool observes_reads_;
};

}  // namespace metadata
}  // namespace mooncake
