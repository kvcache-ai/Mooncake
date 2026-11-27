// Linux component benchmark for committed Master metadata. The offset
// allocator manages synthetic addresses and never maps the object payload.
#include <gflags/gflags.h>

#include <barrier>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "metadata/namespace.h"

DEFINE_uint64(objects, 500000, "Number of live keys");
DEFINE_uint32(key_bytes, 64, "Bytes in each key");
DEFINE_uint32(replicas, 1, "MEMORY replicas per key on distinct segments");
DEFINE_uint32(group_width, 0, "Objects per group (0 for ungrouped keys)");
DEFINE_uint32(read_threads, 0, "Concurrent readers (0 for footprint only)");
DEFINE_uint32(read_rounds, 3, "Full keyspace passes per read workload");

DEFINE_uint64(read_working_set, 0,
              "Distinct keys to read (0 for all keys, 1 for a hot key)");

namespace {

size_t ResidentBytes() {
    std::ifstream status("/proc/self/status");
    std::string line;
    size_t kib = 0;
    while (std::getline(status, line)) {
        if (std::sscanf(line.c_str(), "VmRSS: %zu kB", &kib) == 1) {
            return kib * 1024;
        }
    }
    throw std::runtime_error("unable to read VmRSS from /proc/self/status");
}

std::string MakeKey(size_t i, size_t length = FLAGS_key_bytes) {
    const auto suffix = std::to_string(i);
    return std::string(length - suffix.size(), 'k') + suffix;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    constexpr size_t kValueBytes = 4096;
    constexpr size_t kSegmentSize = 8ULL << 30;
    if (!FLAGS_replicas || FLAGS_read_working_set > FLAGS_objects ||
        !FLAGS_objects || FLAGS_objects > kSegmentSize / kValueBytes ||
        FLAGS_key_bytes < std::to_string(FLAGS_objects - 1).size()) {
        std::cerr << "Invalid replica count, key length, working set or object "
                     "count for 8 "
                     "GiB segment\n";
        return 1;
    }

    using namespace mooncake;
    MasterMetricManager::instance();
    metadata::Namespace ns;
    std::vector<std::shared_ptr<OffsetBufferAllocator>> segments;
    for (size_t i = 0; i < FLAGS_replicas; ++i) {
        segments.push_back(std::make_shared<OffsetBufferAllocator>(
            "segment-" + std::to_string(i), (i + 1) * kSegmentSize,
            kSegmentSize, "host:9111", "rdma"));
    }
    const size_t baseline = ResidentBytes();
    for (size_t i = 0; i < FLAGS_objects; ++i) {
        const auto key = MakeKey(i);
        std::vector<Replica> replicas;
        replicas.reserve(segments.size());
        for (const auto& segment : segments) {
            auto buffer = segment->allocate(kValueBytes);
            if (!buffer) return 1;
            replicas.emplace_back(std::move(buffer), ReplicaStatus::COMPLETE);
        }
        const auto group = FLAGS_group_width
                               ? MakeKey(i / FLAGS_group_width, 32)
                               : std::string{};
        auto guard = ns.objects.WriteKey(key);
        guard.Publish(UUID{1, 2}, std::chrono::system_clock::now(), kValueBytes,
                      std::move(replicas), std::nullopt, false,
                      ObjectDataType::KVCACHE, group, TenantId::Default(), key);
    }
    const auto delta =
        static_cast<int64_t>(ResidentBytes()) - static_cast<int64_t>(baseline);
    std::cout << "keys=" << ns.objects.ObjectCount()
              << " replicas=" << FLAGS_replicas
              << " group_width=" << FLAGS_group_width << " rss_bytes_per_key="
              << static_cast<double>(delta) / FLAGS_objects << '\n';
    if (!FLAGS_read_threads) return 0;

    const size_t working_set =
        FLAGS_read_working_set ? FLAGS_read_working_set : FLAGS_objects;
    std::barrier ready(FLAGS_read_threads + 1);
    std::vector<std::thread> readers;
    for (size_t worker = 0; worker < FLAGS_read_threads; ++worker) {
        readers.emplace_back([&, worker] {
            ready.arrive_and_wait();
            for (size_t pass = 0; pass < FLAGS_read_rounds; ++pass) {
                for (size_t i = worker; i < FLAGS_objects;
                     i += FLAGS_read_threads) {
                    auto guard = ns.objects.Read(MakeKey(i % working_set));
                    if (!guard) std::abort();
                    const auto& metadata = guard->metadata();
                    const auto descriptor =
                        metadata.GetAllReplicas()[0].get_descriptor();
                    if (descriptor.get_memory_descriptor()
                            .buffer_descriptor.size_ != kValueBytes) {
                        std::abort();
                    }
                    metadata.GrantReadLease(std::chrono::seconds(1));
                }
            }
        });
    }
    const auto start = std::chrono::steady_clock::now();
    ready.arrive_and_wait();
    for (auto& reader : readers) reader.join();
    const std::chrono::duration<double> elapsed =
        std::chrono::steady_clock::now() - start;
    std::cout << "read_threads=" << FLAGS_read_threads << " reads_per_second="
              << FLAGS_read_rounds * static_cast<double>(FLAGS_objects) /
                     elapsed.count()
              << '\n';
}
