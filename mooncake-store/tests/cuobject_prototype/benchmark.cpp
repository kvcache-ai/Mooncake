#include "adapter.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <time.h>
#include <vector>

using namespace mooncake;
using namespace mooncake::cuobject_prototype;
using Clock = std::chrono::steady_clock;

namespace {
void Check(bool ok, const char* message) {
    if (!ok) throw std::runtime_error(message);
}

struct Buffer {
    explicit Buffer(size_t size) : size(size) {
        Check(posix_memalign(&data, 65536, size) == 0, "allocation failed");
    }
    ~Buffer() { std::free(data); }
    void* data = nullptr;
    size_t size;
};

double CpuSeconds() {
    timespec ts{};
    clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

template <class Operation, class Prepare>
void Measure(const char* mode, const char* operation, size_t bytes,
             size_t iterations, Operation run, Prepare prepare) {
    for (int i = 0; i < 2; ++i) run();
    // Reset data after warmup so a no-op measured transfer cannot pass the
    // final byte check using bytes left by setup. This work is not timed.
    prepare();
    std::vector<double> latency;
    latency.reserve(iterations);
    double cpu_start = CpuSeconds();
    auto start = Clock::now();
    for (size_t i = 0; i < iterations; ++i) {
        auto before = Clock::now();
        run();
        latency.push_back(
            std::chrono::duration<double, std::milli>(Clock::now() - before)
                .count());
    }
    double wall = std::chrono::duration<double>(Clock::now() - start).count();
    double cpu = CpuSeconds() - cpu_start;
    std::sort(latency.begin(), latency.end());
    const double gib = static_cast<double>(bytes) * iterations / (1ull << 30);
    std::cout << "{\"mode\":\"" << mode << "\",\"operation\":\"" << operation
              << "\",\"bytes\":" << bytes << ",\"iterations\":" << iterations
              << ",\"wall_seconds\":" << wall << ",\"cpu_seconds\":" << cpu
              << ",\"GiB_per_second\":" << gib / wall
              << ",\"cpu_seconds_per_GiB\":" << cpu / gib
              << ",\"p50_ms\":" << latency[iterations / 2]
              << ",\"p95_ms\":" << latency[(iterations - 1) * 95 / 100]
              << "}\n";
}
}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    try {
        Check(argc >= 2 && argc <= 5,
              "usage: cuobject_benchmark --sdk-smoke | --benchmark [bytes] "
              "[iterations]");
        const std::string mode = argv[1];
        Check(mode == "--sdk-smoke" || mode == "--benchmark", "unknown mode");
        const size_t bytes = argc > 2 ? std::stoull(argv[2]) : 4 * 1024 * 1024;
        const size_t iterations = argc > 3 ? std::stoull(argv[3]) : 32;
        const bool reverse = argc > 4 && std::string(argv[4]) == "--reverse";
        Check(argc <= 4 || reverse, "unknown ordering option");
        Check(bytes > 0 && bytes <= 256 * 1024 * 1024 && iterations > 0 &&
                  iterations <= 10000,
              "invalid benchmark size/count");
        Buffer source(bytes), destination(bytes);
        for (size_t i = 0; i < bytes; ++i)
            static_cast<unsigned char*>(source.data)[i] = (i * 31 + 17) & 255;
        std::memset(destination.data, 0, bytes);
        // Client is destroyed before either allocation, including exceptions.
        auto client = CreateSdkClient();
        const auto register_start = Clock::now();
        Check(client->Register(source.data, bytes),
              "source registration failed");
        Check(client->Register(destination.data, bytes),
              "destination registration failed");
        const double registration_ms =
            std::chrono::duration<double, std::milli>(Clock::now() -
                                                      register_start)
                .count();
        if (mode == "--sdk-smoke") {
            for (bool upload : {false, true}) {
                auto token = client->Token(source.data, bytes, upload);
                Check(!token.empty(), "token creation failed");
                client->Release(token);
            }
            client->Unregister(source.data);
            client->Unregister(destination.data);
            std::cout << "SDK_SMOKE_PASS bytes=" << bytes
                      << " registration_ms=" << registration_ms << '\n';
            return 0;
        }
        const std::string prefix =
            "cuobject-prototype/" + std::to_string(getpid()) + "-" +
            std::to_string(Clock::now().time_since_epoch().count());
        S3ObjectStorageAdapter tcp(prefix);
        Adapter rdma(prefix, client);
        Check(tcp.Init().has_value() && rdma.Init().has_value(),
              "S3 configuration invalid");
        struct Cleanup {
            S3ObjectStorageAdapter& tcp;
            ~Cleanup() {
                auto result = tcp.Delete("payload");
                if (!result) std::cerr << "Failed to delete prototype object\n";
            }
        } cleanup{tcp};
        const auto payload =
            std::span(static_cast<const char*>(source.data), bytes);
        // Ordinary TCP first seeds a known object; RDMA GET is verified before
        // allowing a body-less PUT against the selected endpoint.
        Check(tcp.Put("payload", payload).has_value(), "TCP seed PUT failed");
        auto get = rdma.Get("payload", destination.data, bytes);
        Check(get && *get == bytes &&
                  !std::memcmp(source.data, destination.data, bytes),
              "RDMA GET correctness check failed");
        // Write different bytes so an acknowledged but skipped RDMA PUT
        // cannot pass by reading back the TCP seed.
        for (size_t i = 0; i < bytes; ++i)
            static_cast<unsigned char*>(source.data)[i] ^= 0xff;
        Check(rdma.Put("payload", payload).has_value(), "RDMA PUT failed");
        std::memset(destination.data, 0, bytes);
        get = tcp.Get("payload", destination.data, bytes);
        Check(get && *get == bytes &&
                  !std::memcmp(source.data, destination.data, bytes),
              "TCP verification of RDMA PUT failed");
        std::cout << "{\"registration_ms\":" << registration_ms
                  << ",\"concurrency\":1,\"registered_buffers\":2}\n";
        // Match the serial request shape; use multiple independent runs in
        // alternating order before drawing throughput conclusions.
        std::vector<ObjectStorageAdapter*> adapters{&tcp, &rdma};
        if (reverse) std::reverse(adapters.begin(), adapters.end());
        for (auto* adapter : adapters) {
            const char* label = adapter == &tcp ? "tcp" : "cuobject";
            Measure(
                label, "GET", bytes, iterations,
                [&] {
                    auto r = adapter->Get("payload", destination.data, bytes);
                    Check(r && *r == bytes, "measured GET failed");
                },
                [&] { std::memset(destination.data, 0, bytes); });
            Check(!std::memcmp(source.data, destination.data, bytes),
                  "GET data mismatch");
            Measure(
                label, "PUT", bytes, iterations,
                [&] {
                    Check(adapter->Put("payload", payload).has_value(),
                          "measured PUT failed");
                },
                [&] {
                    std::memset(destination.data, 0, bytes);
                    Check(tcp.Put("payload",
                                  {static_cast<const char*>(destination.data),
                                   bytes})
                              .has_value(),
                          "TCP reset before timed PUT failed");
                });
            std::memset(destination.data, 0, bytes);
            auto r = tcp.Get("payload", destination.data, bytes);
            Check(r && *r == bytes &&
                      !std::memcmp(source.data, destination.data, bytes),
                  "PUT data mismatch");
        }
        client->Unregister(source.data);
        client->Unregister(destination.data);
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "prototype: " << e.what() << '\n';
        return 1;
    }
}
