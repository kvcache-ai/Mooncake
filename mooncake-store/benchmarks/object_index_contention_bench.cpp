// Measures the object route's cost against its stripe count: how many stripes a
// tenant's route needs before its writes stop serializing, what a lookup costs
// beside that, and what the count costs in per-tenant memory.
//
// The route is a map under a lock per stripe. One stripe puts every key behind
// one lock, so writes to distinct keys serialize; more stripes spread them at
// about 120 bytes each. A stripe also bounds the map's own cost, because a map
// that outgrows its buckets rehashes every node it holds under its exclusive
// lock: the insert-stall table reports how far the worst single insert grows as
// that walk gets longer.
//
// Every point builds its own route with the count under test, so the printed
// size is the per-tenant cost of that count. Keys are deterministic strings and
// each thread publishes and reads only its own, so a point shows what the
// stripe count buys rather than what two threads writing one key cost. A point
// fills every worker's keys before its window opens and starts every worker at
// a barrier, and the rate is over the window the workers actually ran in, so
// the count under test is not charged for a slower fill.
//
// Output is CSV on stdout. The throughput table is
//   stripes,threads,mode,ops_per_s,route_bytes
// with one of three modes: `get` (a lookup), `publish` (build an entry, publish
// it, drop the slot the key held before, as a put does) and `mixed` (eight
// lookups per publish). `ops_per_s` counts a lookup as one operation and a
// publish, including the drop it does first, as one. The insert-stall table is
//   stripes,keys,p50_us,p99_us,max_us
// from a single thread publishing distinct keys into a growing route, where the
// entries stay alive so the map keeps every node it rehashes.

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "object_entry.h"
#include "object_index.h"

namespace mooncake {
namespace {

constexpr int kThreadCounts[] = {1, 4, 16, 32};
constexpr size_t kStripeCounts[] = {1, 4, 16, 64, 256};
// Keys one thread owns. A publisher keeps all of them published and cycles
// over them, dropping each key's previous slot before it publishes the new one.
constexpr size_t kKeysPerThread = 4096;
// Lookups the mixed mode runs per publish.
constexpr size_t kLookupsPerPublish = 8;
constexpr auto kMeasureFor = std::chrono::seconds(2);
// Keys the single-threaded insert-stall pass publishes into a growing route.
constexpr size_t kStallKeys = 1 << 18;

// One stripe is a lock plus the map of the keys that land in it, and the
// stripes are the index's own allocation, so sizeof(ObjectIndex) alone does not
// carry their cost. A map's own size does not depend on what it maps to, so the
// mapped type only has to be a string-keyed map with the same hasher and
// comparator as the route's.
using RouteMap = std::unordered_map<std::string, std::shared_ptr<ObjectEntry>,
                                    TransparentStringHash, std::equal_to<>>;
constexpr size_t kStripeBytes = sizeof(std::shared_mutex) + sizeof(RouteMap);

size_t RouteBytes(size_t stripe_count) {
    return sizeof(ObjectIndex) + stripe_count * kStripeBytes;
}

std::string KeyOf(size_t thread, size_t index) {
    return "t" + std::to_string(thread) + "_k" + std::to_string(index);
}

// The minimal 128 B, replica-less envelope the object suites build: the route
// stores the shell, and a benchmark needs no replica validity.
std::shared_ptr<ObjectEntry> MakeEntry(const std::string& key) {
    constexpr auto kWriteTime =
        std::chrono::system_clock::time_point(std::chrono::seconds(1));
    return std::make_shared<ObjectEntry>(std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, 128, std::vector<Replica>{}, std::nullopt,
        false, ObjectDataType::UNKNOWN, std::string{}, TenantId(), key));
}

// Every thread's keys, published before anything is timed, and the strong
// handle of the entry each key currently holds. Keeping the handles is what
// lets a publisher drop the slot it published before, and what keeps the
// entries alive so the map holds every node it has rehashed.
std::vector<std::shared_ptr<ObjectEntry>> Fill(ObjectIndex& route,
                                               size_t thread) {
    std::vector<std::shared_ptr<ObjectEntry>> entries;
    entries.reserve(kKeysPerThread);
    for (size_t i = 0; i < kKeysPerThread; ++i) {
        auto entry = MakeEntry(KeyOf(thread, i));
        const bool published = route.Insert(entry);
        assert(published);
        (void)published;
        entries.push_back(std::move(entry));
    }
    return entries;
}

enum class Mode { kGet, kPublish, kMixed };

const char* ModeName(Mode mode) {
    switch (mode) {
        case Mode::kGet:
            return "get";
        case Mode::kPublish:
            return "publish";
        case Mode::kMixed:
            return "mixed";
    }
    return "unknown";
}

// What one thread does until it is told to stop: read its own keys, or cycle
// over them publishing each one, with eight lookups before each publish in the
// mixed mode. The worker fills the route and then waits at the start barrier,
// so the window opens for every worker at once and the fill is not charged to
// it: a fill that serializes more (fewer stripes) would otherwise be paid for
// out of the measured window while its inserts are not counted.
void RunWorker(ObjectIndex& route, size_t thread, Mode mode,
               std::atomic<uint64_t>& total_ops, std::atomic<bool>& stop,
               std::barrier<>& start_barrier) {
    std::vector<std::string> keys;
    keys.reserve(kKeysPerThread);
    for (size_t i = 0; i < kKeysPerThread; ++i) {
        keys.push_back(KeyOf(thread, i));
    }
    std::vector<std::shared_ptr<ObjectEntry>> entries = Fill(route, thread);
    start_barrier.arrive_and_wait();
    uint64_t ops = 0;
    size_t at = 0;
    while (!stop.load(std::memory_order_relaxed)) {
        if (mode == Mode::kGet) {
            for (const auto& key : keys) {
                const auto entry = route.Get(key);
                assert(entry != nullptr);
                (void)entry;
            }
            ops += keys.size();
            continue;
        }
        const size_t slot = at % keys.size();
        if (mode == Mode::kMixed) {
            for (size_t i = 0; i < kLookupsPerPublish; ++i) {
                const auto entry = route.Get(keys[(slot + i) % keys.size()]);
                assert(entry != nullptr);
                (void)entry;
            }
            ops += kLookupsPerPublish;
        }
        // A publish drops the slot this key held and publishes a fresh entry,
        // counting as one operation either way.
        const bool dropped = route.EraseIf(keys[slot], entries[slot]);
        assert(dropped);
        (void)dropped;
        auto fresh = MakeEntry(keys[slot]);
        const bool published = route.Insert(fresh);
        assert(published);
        (void)published;
        entries[slot] = std::move(fresh);
        ++ops;
        ++at;
    }
    total_ops.fetch_add(ops, std::memory_order_relaxed);
}

void RunPoint(size_t stripe_count, int threads, Mode mode) {
    ObjectIndex route{stripe_count};
    std::atomic<uint64_t> total_ops{0};
    std::atomic<bool> stop{false};
    // Every worker fills the route and reaches the barrier before the window
    // opens, so the fill is neither counted nor charged to the measurement.
    std::barrier start_barrier(threads + 1);
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            RunWorker(route, static_cast<size_t>(t), mode, total_ops, stop,
                      start_barrier);
        });
    }
    start_barrier.arrive_and_wait();
    const auto start = std::chrono::steady_clock::now();
    while (std::chrono::steady_clock::now() - start < kMeasureFor) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    stop.store(true, std::memory_order_relaxed);
    for (auto& worker : workers) {
        worker.join();
    }
    // The rate is over the window the workers actually ran in, which is the
    // sleeping window plus the time the last worker took to notice the stop.
    const double seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    std::cout << stripe_count << "," << threads << "," << ModeName(mode) << ","
              << static_cast<uint64_t>(total_ops.load() / seconds) << ","
              << RouteBytes(stripe_count) << std::endl;
}

// The same publish path without the erase in front of it, one thread, into a
// route that keeps growing: the tail of this table is one stripe's rehash walk.
void RunInsertStall(size_t stripe_count) {
    ObjectIndex route{stripe_count};
    std::vector<std::shared_ptr<ObjectEntry>> entries;
    entries.reserve(kStallKeys);
    std::vector<uint64_t> latencies_ns;
    latencies_ns.reserve(kStallKeys);
    for (size_t i = 0; i < kStallKeys; ++i) {
        auto entry = MakeEntry(KeyOf(0, i));
        const auto before = std::chrono::steady_clock::now();
        const bool published = route.Insert(entry);
        const auto after = std::chrono::steady_clock::now();
        assert(published);
        (void)published;
        latencies_ns.push_back(static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(after - before)
                .count()));
        entries.push_back(std::move(entry));
    }
    std::sort(latencies_ns.begin(), latencies_ns.end());
    const auto percentile = [&latencies_ns](double share) {
        const double rank =
            share * static_cast<double>(latencies_ns.size() - 1);
        return static_cast<double>(latencies_ns[static_cast<size_t>(rank)]) /
               1000.0;
    };
    std::cout << stripe_count << "," << kStallKeys << "," << std::fixed
              << std::setprecision(1) << percentile(0.50) << ","
              << percentile(0.99) << ","
              << static_cast<double>(latencies_ns.back()) / 1000.0 << std::endl;
}

}  // namespace
}  // namespace mooncake

int main() {
    std::cout << "stripes,threads,mode,ops_per_s,route_bytes" << std::endl;
    for (size_t stripe_count : mooncake::kStripeCounts) {
        for (int threads : mooncake::kThreadCounts) {
            for (mooncake::Mode mode :
                 {mooncake::Mode::kGet, mooncake::Mode::kPublish,
                  mooncake::Mode::kMixed}) {
                mooncake::RunPoint(stripe_count, threads, mode);
            }
        }
    }
    std::cout << std::endl;
    std::cout << "stripes,keys,p50_us,p99_us,max_us" << std::endl;
    for (size_t stripe_count : mooncake::kStripeCounts) {
        mooncake::RunInsertStall(stripe_count);
    }
    return 0;
}
