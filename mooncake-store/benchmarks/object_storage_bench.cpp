// Compare the same bounded object I/O component against full synchronous Store
// calls. Reads explicitly select the object descriptor even while memory
// exists.
#include <algorithm>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "client_service.h"
#include "master_client.h"
#include "real_client.h"
#include "storage/distributed/object_replica_io.h"

DEFINE_string(mode, "object", "object or memory replicas");
DEFINE_string(master, "127.0.0.1:50051", "Serving Master address");
DEFINE_uint64(bytes, 1048576, "Bytes per object");
DEFINE_uint64(concurrency, 1, "Concurrent callers");
DEFINE_uint64(iterations, 4, "Measured operations per caller");
DEFINE_string(metrics_output, "", "Write SDK Prometheus metrics to this file");

namespace {
using Clock = std::chrono::steady_clock;
struct Samples {
    std::vector<double> latency_us;
    double seconds;
};
template <class Operation>
Samples Measure(Operation operation) {
    std::vector<std::vector<double>> latencies(FLAGS_concurrency);
    std::vector<std::thread> workers;
    std::mutex error_mutex;
    std::exception_ptr error;
    auto begin = Clock::now();
    for (size_t worker = 0; worker < FLAGS_concurrency; ++worker) {
        workers.emplace_back([&, worker] {
            try {
                for (size_t iteration = 0; iteration < FLAGS_iterations;
                     ++iteration) {
                    auto start = Clock::now();
                    operation(worker, iteration);
                    latencies[worker].push_back(
                        std::chrono::duration<double, std::micro>(Clock::now() -
                                                                  start)
                            .count());
                }
            } catch (...) {
                std::lock_guard lock(error_mutex);
                if (!error) error = std::current_exception();
            }
        });
    }
    for (auto& worker : workers) worker.join();
    if (error) std::rethrow_exception(error);
    Samples samples{
        {}, std::chrono::duration<double>(Clock::now() - begin).count()};
    for (auto& values : latencies)
        samples.latency_us.insert(samples.latency_us.end(), values.begin(),
                                  values.end());
    std::sort(samples.latency_us.begin(), samples.latency_us.end());
    return samples;
}
void Print(const char* mode, const char* operation, const Samples& samples) {
    auto percentile = [&](double p) {
        return samples.latency_us[static_cast<size_t>(
            (samples.latency_us.size() - 1) * p)];
    };
    double total_us = 0;
    for (auto sample : samples.latency_us) total_us += sample;
    std::cout << mode << ',' << operation << ',' << FLAGS_bytes << ','
              << FLAGS_concurrency << ',' << FLAGS_iterations << ','
              << samples.seconds << ',' << percentile(.50) << ','
              << percentile(.95) << ',' << percentile(.99) << ','
              << FLAGS_bytes * samples.latency_us.size() /
                     (1024.0 * 1024 * samples.seconds)
              << ',' << total_us << '\n';
}
void Require(bool condition, const char* message) {
    if (!condition) throw std::runtime_error(message);
}
}  // namespace
int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    try {
        Require(FLAGS_bytes && FLAGS_concurrency && FLAGS_iterations,
                "Nonzero workload required");
        Require(FLAGS_concurrency <= 32 && FLAGS_iterations <= 128 &&
                    FLAGS_bytes <= 16ULL * 1024 * 1024,
                "Workload exceeds benchmark bounds");
        const auto run = mooncake::UuidToString(mooncake::generate_uuid());
        mooncake::MasterClient control(mooncake::generate_uuid());
        Require(control.Connect(FLAGS_master) == mooncake::ErrorCode::OK,
                "Master connection failed");
        auto config = control.GetObjectStorageConfig();
        Require(bool(config), "Cannot fetch cluster configuration");
        Require(FLAGS_mode == "object" || FLAGS_mode == "memory",
                "Unknown benchmark mode");
        std::shared_ptr<mooncake::ObjectStorageAdapter> adapter;
        std::unique_ptr<mooncake::ObjectStorageBackend> backend;
        if (FLAGS_mode == "object") {
            Require(config->enabled, "Object replicas must be enabled");
            auto created_adapter =
                mooncake::CreateObjectReplicaAdapter(*config);
            Require(bool(created_adapter), "Adapter initialization failed");
            adapter = *created_adapter;
            backend = std::make_unique<mooncake::ObjectStorageBackend>(
                mooncake::ObjectStorageBackendConfig{config->max_object_bytes,
                                                     config->max_staging_bytes},
                adapter);
        }
        auto owner = mooncake::RealClient::create();
        auto memory = std::max<uint64_t>(
            256ULL * 1024 * 1024,
            FLAGS_bytes * FLAGS_concurrency * FLAGS_iterations * 2);
        Require(memory <= 16ULL * 1024 * 1024 * 1024,
                "Memory budget exceeds 16 GiB");
        Require(owner->setup_real("127.0.0.1:0", "P2PHANDSHAKE", memory,
                                  32ULL << 20, "tcp", "", FLAGS_master) == 0,
                "Owner setup failed");
        auto created = mooncake::Client::Create(
            "127.0.0.1:0", "P2PHANDSHAKE", "tcp", std::nullopt, FLAGS_master);
        Require(bool(created), "Writer setup failed");
        auto client = *created;
        std::vector<std::vector<char>> source(FLAGS_concurrency),
            destination(FLAGS_concurrency);
        std::vector<std::vector<mooncake::Slice>> input(FLAGS_concurrency),
            output(FLAGS_concurrency);
        for (size_t i = 0; i < FLAGS_concurrency; ++i) {
            source[i].assign(FLAGS_bytes, static_cast<char>('a' + i % 20));
            destination[i].resize(FLAGS_bytes);
            input[i] = {{source[i].data(), source[i].size()}};
            output[i] = {{destination[i].data(), destination[i].size()}};
        }
        auto key = [&](size_t worker, size_t iteration, bool direct) {
            return run + (direct ? "-direct-" : "-store-") +
                   std::to_string(worker) + "-" + std::to_string(iteration);
        };
        const auto host = [](const mooncake::Slice&) { return true; };
        std::cout << "mode,operation,bytes,concurrency,iterations,seconds,p50_"
                     "us,p95_us,p99_us,mib_s,total_latency_us\n";
        if (backend) {
            // Warm signing, HTTP and metadata paths outside measured samples.
            Require(backend->WriteV(run + "-warm", FLAGS_bytes, input[0], host,
                                    {}) == mooncake::ErrorCode::OK,
                    "Warm upload failed");
            Require(backend->Read(
                        run + "-warm", FLAGS_bytes, output[0], 0,
                        [](void* dst, const void* src, size_t size) {
                            std::memcpy(dst, src, size);
                            return true;
                        },
                        host) == mooncake::ErrorCode::OK,
                    "Warm read failed");
            Require(bool(adapter->Delete(run + "-warm")),
                    "Warm cleanup failed");
            Print("direct", "put",
                  Measure([&](size_t worker, size_t iteration) {
                      Require(backend->WriteV(key(worker, iteration, true),
                                              FLAGS_bytes, input[worker], host,
                                              {}) == mooncake::ErrorCode::OK,
                              "Direct PUT failed");
                  }));
            Print("direct", "get",
                  Measure([&](size_t worker, size_t iteration) {
                      Require(backend->Read(
                                  key(worker, iteration, true), FLAGS_bytes,
                                  output[worker], 0,
                                  [](void* dst, const void* src, size_t size) {
                                      std::memcpy(dst, src, size);
                                      return true;
                                  },
                                  host) == mooncake::ErrorCode::OK,
                              "Direct GET failed");
                  }));
            for (size_t i = 0; i < FLAGS_concurrency; ++i)
                Require(source[i] == destination[i],
                        "Direct read corrupted data");
        }
        mooncake::ReplicateConfig replicas;
        replicas.os_replica_num = FLAGS_mode == "object" ? 1 : 0;
        auto warm = run + "-store-warm";
        Require(bool(client->Put(warm, input[0], replicas)),
                "Warm Store PUT failed");
        Require(bool(client->Remove(warm, true)), "Warm Store cleanup failed");
        if (!FLAGS_metrics_output.empty()) {
            auto metrics = client->SerializeMetrics();
            Require(bool(metrics), "SDK metrics unavailable");
            std::ofstream(FLAGS_metrics_output + ".before") << *metrics;
        }
        Print("store", "put", Measure([&](size_t worker, size_t iteration) {
                  Require(bool(client->Put(key(worker, iteration, false),
                                           input[worker], replicas)),
                          "Store PUT failed");
              }));
        Print("store", "get", Measure([&](size_t worker, size_t iteration) {
                  auto name = key(worker, iteration, false);
                  auto query = client->Query(name);
                  Require(bool(query), "Store Query failed");
                  auto object = std::find_if(
                      query->replicas.begin(), query->replicas.end(),
                      [](const auto& replica) {
                          return FLAGS_mode == "object"
                                     ? replica.is_object_storage_replica()
                                     : replica.is_memory_replica();
                      });
                  Require(object != query->replicas.end(),
                          "Object descriptor missing");
                  mooncake::QueryResult selected(
                      std::vector<mooncake::Replica::Descriptor>{*object},
                      query->lease_timeout, query->object_checksum,
                      query->resolved_tenant_id);
                  Require(bool(client->Get(name, selected, output[worker])),
                          "Store GET failed");
              }));
        for (size_t i = 0; i < FLAGS_concurrency; ++i)
            Require(source[i] == destination[i], "Store read corrupted data");
        if (!FLAGS_metrics_output.empty()) {
            auto metrics = client->SerializeMetrics();
            Require(bool(metrics), "SDK metrics unavailable");
            std::ofstream(FLAGS_metrics_output) << *metrics;
        }
        for (size_t i = 0; i < FLAGS_concurrency; ++i) {
            for (size_t j = 0; j < FLAGS_iterations; ++j) {
                if (adapter)
                    Require(bool(adapter->Delete(key(i, j, true))),
                            "Direct cleanup failed");
                Require(bool(client->Remove(key(i, j, false), true)),
                        "Store cleanup failed");
            }
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
