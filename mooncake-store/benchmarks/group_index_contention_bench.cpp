// Measures the group table's write path: several threads register members in
// distinct groups, for several stripe counts. Striping is a compile time
// parameter of StripedGroupIndex, so each point instantiates its own table and
// the printed size is the per-tenant cost of that count.
//
// A round walks every group once with the same member name, so an iteration
// registers one member and (once the first rounds have filled the window) drops
// one, keeping membership bounded instead of growing a set per group for the
// whole run. The names are built once per thread, so the rate is the table's
// own cost rather than string construction.

#include <atomic>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "group_index.h"

namespace mooncake {
namespace {

constexpr int kThreadCounts[] = {1, 4, 16, 32};
constexpr size_t kGroupsPerThread = 4096;
// A group holds a handful of members in practice.
constexpr size_t kMembersPerGroup = 8;
constexpr auto kMeasureFor = std::chrono::seconds(2);

template <size_t StripeCount>
void RunPoint(int threads) {
    StripedGroupIndex<StripeCount> index;
    std::atomic<uint64_t> total_writes{0};
    std::atomic<bool> stop{false};
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (int worker = 0; worker < threads; ++worker) {
        workers.emplace_back([&, worker] {
            const std::string prefix = "group_" + std::to_string(worker) + "_";
            std::vector<std::string> groups;
            groups.reserve(kGroupsPerThread);
            for (size_t i = 0; i < kGroupsPerThread; ++i) {
                groups.push_back(prefix + std::to_string(i));
            }
            std::vector<std::string> members;
            members.reserve(kMembersPerGroup);
            for (size_t i = 0; i < kMembersPerGroup; ++i) {
                members.push_back("member_" + std::to_string(i));
            }

            uint64_t writes = 0;
            uint64_t round = 0;
            while (!stop.load(std::memory_order_relaxed)) {
                const std::string& member = members[round % kMembersPerGroup];
                for (size_t group = 0; group < kGroupsPerThread; ++group) {
                    // One publication generation per member: the benchmark
                    // measures the write path of the table, not replacement.
                    const uint64_t generation = round + 1;
                    (void)index.AddMember(groups[group], member, generation);
                    if (round >= kMembersPerGroup) {
                        (void)index.RemoveMember(groups[group], member,
                                                 generation);
                    }
                    ++writes;
                    if (stop.load(std::memory_order_relaxed)) {
                        break;
                    }
                }
                ++round;
            }
            total_writes.fetch_add(writes, std::memory_order_relaxed);
        });
    }
    std::this_thread::sleep_for(kMeasureFor);
    stop.store(true, std::memory_order_relaxed);
    for (auto& worker : workers) {
        worker.join();
    }
    const double seconds = std::chrono::duration<double>(kMeasureFor).count();
    std::cout << StripeCount << "," << threads << ","
              << static_cast<uint64_t>(total_writes.load() / seconds) << ","
              << sizeof(StripedGroupIndex<StripeCount>) << std::endl;
}

template <size_t StripeCount>
void RunStripeCount() {
    for (int threads : kThreadCounts) {
        RunPoint<StripeCount>(threads);
    }
}

}  // namespace
}  // namespace mooncake

int main() {
    std::cout << "stripes,threads,member_writes_per_s,table_bytes" << std::endl;
    mooncake::RunStripeCount<1>();
    mooncake::RunStripeCount<16>();
    mooncake::RunStripeCount<32>();
    mooncake::RunStripeCount<64>();
    mooncake::RunStripeCount<128>();
    mooncake::RunStripeCount<256>();
    return 0;
}
