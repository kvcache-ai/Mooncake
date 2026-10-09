// Query microbenchmark; replica counts describe directory entries, not GPUs.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "conductor/prefixindex/hash_strategy.h"
#include "conductor/prefixindex/prefix_indexer.h"

namespace pi = mooncake::conductor::prefixindex;
using Clock = std::chrono::steady_clock;

int main(int argc, char** argv) {
    if (argc < 5) {
        std::cerr << "usage: query_benchmark replicas tokens iterations "
                     "common|disjoint|miss|filtered [token_file|-] [readers] "
                     "[writer] [block_size]\n";
        return 2;
    }
    try {
        const int replicas = std::stoi(argv[1]);
        const int token_count = std::stoi(argv[2]);
        const int iterations = std::stoi(argv[3]);
        const std::string pattern = argv[4];
        const int readers = argc > 6 ? std::stoi(argv[6]) : 1;
        const bool writer_enabled = argc > 7 && std::stoi(argv[7]) != 0;
        const int block_size = argc > 8 ? std::stoi(argv[8]) : 16;
        if (replicas <= 0 || token_count <= 0 || iterations <= 0 ||
            readers <= 0 || block_size <= 0 ||
            (pattern != "common" && pattern != "disjoint" &&
             pattern != "miss" && pattern != "filtered")) {
            throw std::invalid_argument("invalid benchmark arguments");
        }
        std::vector<int32_t> tokens(token_count);
        if (argc > 5 && std::string(argv[5]) != "-") {
            std::ifstream input(argv[5]);
            for (auto& token : tokens) {
                if (!(input >> token))
                    throw std::runtime_error("short token file");
            }
        } else {
            for (int i = 0; i < token_count; ++i) tokens[i] = 100 + i % 30000;
        }
        const pi::ContextKey context{"benchmark", "qwen3", "", block_size};
        const pi::HashProfile profile{"sglang", "sha256_raw", "0",
                                      std::string(64, '0'), "first64_be"};
        std::string error;
        auto strategy = pi::CreateHashStrategy(profile, &error);
        if (!strategy) throw std::runtime_error(error);
        // Isolate queries from directory-capacity eviction during fixture
        // setup.
        pi::PrefixCacheTable table(0);
        auto require = [](const std::string& result) {
            if (!result.empty()) throw std::runtime_error(result);
        };
        for (int i = 0; i < replicas; ++i) {
            const std::string id = "replica-" + std::to_string(i);
            require(
                table.Register({context, profile, id, 0, block_size, 0}).error);
            auto owned_tokens = tokens;
            if (pattern == "disjoint" && i != 0) owned_tokens[0] += i;
            std::vector<pi::HashBlock> blocks;
            require(strategy->Compute(context, owned_tokens, std::nullopt,
                                      &blocks));
            pi::EngineMutation mutation{.context = context,
                                        .owner = {"publisher-" + id, id, 0},
                                        .effective_block_size = block_size,
                                        .cache_group = 0};
            for (const auto& block : blocks)
                mutation.prefixes.push_back(block.projected);
            require(table.StoreEngine(mutation));
        }
        if (pattern == "miss") tokens[0] += 32000;
        const std::optional<std::string> filter =
            pattern == "filtered" ? std::optional<std::string>("replica-0")
                                  : std::nullopt;
        const auto expected =
            table.Query(context, tokens, std::nullopt, filter);
        if (expected.size() != static_cast<size_t>(filter ? 1 : replicas)) {
            throw std::runtime_error("fixture lost a registered candidate");
        }
        for (const auto& [instance, hit] : expected) {
            const int64_t wanted =
                pattern != "miss" &&
                        (pattern != "disjoint" || instance == "replica-0")
                    ? token_count
                    : 0;
            if (hit.npu != wanted) {
                throw std::runtime_error(
                    "fixture prefix coverage is incorrect");
            }
        }
        for (int i = 0; i < 10; ++i) {
            if (table.Query(context, tokens, std::nullopt, filter) !=
                expected) {
                throw std::runtime_error("unstable warmup result");
            }
        }
        std::atomic<bool> start{false}, stop{false}, failed{false};
        std::vector<std::vector<double>> timings(readers);
        std::vector<double> writer_timings;
        std::thread writer;
        if (writer_enabled) {
            writer = std::thread([&] {
                const pi::EngineMutation mutation{
                    .context = context,
                    .prefixes = {{0xfedcba9876543210ULL}},
                    .owner = {"writer", "replica-0", 0},
                    .effective_block_size = block_size,
                    .cache_group = 0};
                while (!start.load()) std::this_thread::yield();
                while (!stop.load()) {
                    const auto begin = Clock::now();
                    if (!table.StoreEngine(mutation).empty() ||
                        !table.RemoveEngine(mutation).empty())
                        failed.store(true);
                    writer_timings.push_back(
                        std::chrono::duration<double, std::micro>(Clock::now() -
                                                                  begin)
                            .count());
                }
            });
        }
        std::vector<std::thread> threads;
        for (int reader = 0; reader < readers; ++reader) {
            threads.emplace_back([&, reader] {
                auto& samples = timings[reader];
                samples.reserve(iterations);
                while (!start.load()) std::this_thread::yield();
                for (int i = 0; i < iterations; ++i) {
                    const auto begin = Clock::now();
                    auto result =
                        table.Query(context, tokens, std::nullopt, filter);
                    samples.push_back(std::chrono::duration<double, std::micro>(
                                          Clock::now() - begin)
                                          .count());
                    if (result != expected) failed.store(true);
                }
            });
        }
        start.store(true);
        for (auto& thread : threads) thread.join();
        stop.store(true);
        if (writer.joinable()) writer.join();
        if (failed.load())
            throw std::runtime_error("query result or writer failure");
        std::cout << "operation,reader,sample,latency_us\n";
        for (int reader = 0; reader < readers; ++reader) {
            for (size_t i = 0; i < timings[reader].size(); ++i) {
                std::cout << "query," << reader << ',' << i << ','
                          << timings[reader][i] << '\n';
            }
        }
        for (size_t i = 0; i < writer_timings.size(); ++i) {
            std::cout << "store_remove,-1," << i << ',' << writer_timings[i]
                      << '\n';
        }
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
