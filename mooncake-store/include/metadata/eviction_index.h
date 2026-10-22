#pragma once

// EvictionIndex: the order in which eviction visits one namespace's objects.
// The namespace owns one and tells it about every key the route hands out: a
// read as its read guard is released, a write as its write guard is. Eviction
// reads it through a cursor, oldest-ranked key first.
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
// Ranks are comparable across the namespaces of one service, which all use
// the same kind of index, so eviction can merge their cursors.
//
// Lock order: whatever lock an index takes is a leaf under a key lock, and
// nothing takes a key lock while holding it.

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

#include "metadata/object_route.h"

namespace mooncake {
namespace metadata {

class EvictionIndex {
   public:
    using Clock = std::chrono::system_clock;
    // Lower ranks are evicted first.
    using Rank = int64_t;

    // A key as the index filed it.
    struct Entry {
        std::string key;
        Rank rank;
    };

    // One walk over the filed keys, lowest rank first. What it hands out is a
    // snapshot: a key may be refiled or gone by the time it is acted on.
    class Cursor {
       public:
        virtual ~Cursor() = default;
        // The next filed key; nullopt once the walk is done.
        virtual std::optional<Entry> Next() = 0;
        // Whether `entry`, which this cursor handed out, still stands at its
        // rank. Asked under the key's write lock, so a key that moved on is
        // refiled at its real rank as that lock is let go.
        [[nodiscard]] virtual bool StillAt(const route::WriteGuard& guard,
                                           const Entry& entry) const = 0;
        // How many keys were filed when the walk began.
        [[nodiscard]] virtual size_t Total() const = 0;
    };

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

    // Whether OnRead must be called.
    [[nodiscard]] bool observes_reads() const { return observes_reads_; }

   private:
    const bool observes_reads_;
};

}  // namespace metadata
}  // namespace mooncake
