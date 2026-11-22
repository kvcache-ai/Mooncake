#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <glog/logging.h>

#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "types.h"

namespace mooncake::bench {
namespace {

struct Options {
    std::string master = "127.0.0.1:15800";
    std::string protocol = "tcp";
    std::string host = "127.0.0.1";
    std::string key_prefix;
    std::size_t segment_bytes = 0;
    std::size_t block_bytes = 512 * 1024;
    int blocks_per_request = 8;
    int prefix_blocks = 4;
    int prefixes = 32;
    int requests_per_prefix = 32;
    int rate = 2000;
    int connections = 16;
    int scale = 1;
    int duration_s = 30;
    int warmup_s = 5;
    int provider_port = 19101;
    int consumer_port = 19102;
};

void PrintUsage(const char* program) {
    std::fprintf(
        stderr,
        "usage: %s [options]\n"
        "  --master <host:port>          master address (default "
        "127.0.0.1:15800)\n"
        "  --protocol <tcp|rdma>         transfer protocol (default tcp)\n"
        "  --host <address>              local address for the clients "
        "(default 127.0.0.1)\n"
        "  --key-prefix <text>           namespace for every object key, so "
        "several processes can share a master\n"
        "  --block-bytes <n>             size of one KV block (default "
        "524288)\n"
        "  --blocks-per-request <n>      blocks a request reads (default 8)\n"
        "  --prefix-blocks <n>           of those, blocks shared with other "
        "requests (default 4)\n"
        "  --prefixes <n>                distinct shared prefixes (default "
        "32)\n"
        "  --requests-per-prefix <n>     distinct requests on one prefix "
        "(default 32)\n"
        "  --rate <n>                    target requests per second (default "
        "2000)\n"
        "  --connections <n>             concurrent client threads (default "
        "16)\n"
        "  --scale <k>                   multiplies rate, connections and "
        "distinct requests by k\n"
        "  --duration <seconds>          measurement window (default 30)\n"
        "  --warmup <seconds>            discarded prefix of the window "
        "(default 5)\n"
        "  --segment-bytes <n>           provider segment size; defaults to "
        "the working set plus one GiB\n"
        "  --provider-port <n>           port the provider client listens on "
        "(default 19101)\n"
        "  --consumer-port <n>           port the consumer client listens on "
        "(default 19102)\n",
        program);
}

bool ParseU64(const std::string& text, std::uint64_t* value) {
    if (text.empty() || text[0] == '-' || text[0] == '+') {
        return false;
    }
    char* end = nullptr;
    errno = 0;
    const unsigned long long parsed = std::strtoull(text.c_str(), &end, 10);
    if (errno != 0 || end == text.c_str() || *end != '\0') {
        return false;
    }
    *value = parsed;
    return true;
}

bool ParseOptions(int argc, char** argv, Options* options) {
    for (int i = 1; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--help" || argument == "-h") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (i + 1 >= argc) {
            std::fprintf(stderr, "%s needs a value\n", argument.c_str());
            return false;
        }
        const std::string value = argv[++i];
        std::uint64_t number = 0;
        if (argument == "--master") {
            options->master = value;
        } else if (argument == "--protocol") {
            options->protocol = value;
        } else if (argument == "--host") {
            options->host = value;
        } else if (argument == "--key-prefix") {
            options->key_prefix = value;
        } else if (argument == "--block-bytes") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --block-bytes\n");
                return false;
            }
            options->block_bytes = number;
        } else if (argument == "--blocks-per-request") {
            if (!ParseU64(value, &number) || number == 0 || number > 1024) {
                std::fprintf(stderr, "bad --blocks-per-request\n");
                return false;
            }
            options->blocks_per_request = static_cast<int>(number);
        } else if (argument == "--prefix-blocks") {
            if (!ParseU64(value, &number) || number > 1024) {
                std::fprintf(stderr, "bad --prefix-blocks\n");
                return false;
            }
            options->prefix_blocks = static_cast<int>(number);
        } else if (argument == "--prefixes") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --prefixes\n");
                return false;
            }
            options->prefixes = static_cast<int>(number);
        } else if (argument == "--requests-per-prefix") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --requests-per-prefix\n");
                return false;
            }
            options->requests_per_prefix = static_cast<int>(number);
        } else if (argument == "--rate") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --rate\n");
                return false;
            }
            options->rate = static_cast<int>(number);
        } else if (argument == "--connections") {
            if (!ParseU64(value, &number) || number == 0 || number > 4096) {
                std::fprintf(stderr, "bad --connections\n");
                return false;
            }
            options->connections = static_cast<int>(number);
        } else if (argument == "--scale") {
            if (!ParseU64(value, &number) || number == 0 || number > 1024) {
                std::fprintf(stderr, "bad --scale\n");
                return false;
            }
            options->scale = static_cast<int>(number);
        } else if (argument == "--duration") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --duration\n");
                return false;
            }
            options->duration_s = static_cast<int>(number);
        } else if (argument == "--warmup") {
            if (!ParseU64(value, &number)) {
                std::fprintf(stderr, "bad --warmup\n");
                return false;
            }
            options->warmup_s = static_cast<int>(number);
        } else if (argument == "--segment-bytes") {
            if (!ParseU64(value, &number) || number == 0) {
                std::fprintf(stderr, "bad --segment-bytes\n");
                return false;
            }
            options->segment_bytes = number;
        } else if (argument == "--provider-port" ||
                   argument == "--consumer-port") {
            if (!ParseU64(value, &number) || number == 0 || number > 65535) {
                std::fprintf(stderr, "bad %s\n", argument.c_str());
                return false;
            }
            if (argument == "--provider-port") {
                options->provider_port = static_cast<int>(number);
            } else {
                options->consumer_port = static_cast<int>(number);
            }
        } else {
            std::fprintf(stderr, "unknown option: %s\n", argument.c_str());
            return false;
        }
    }
    if (options->warmup_s >= options->duration_s) {
        std::fprintf(stderr, "--warmup must be shorter than --duration\n");
        return false;
    }
    if (options->prefix_blocks > options->blocks_per_request) {
        std::fprintf(stderr,
                     "--prefix-blocks cannot exceed --blocks-per-request\n");
        return false;
    }
    return true;
}

std::string PrefixKey(const std::string& prefix, int shared, int block) {
    std::string key = prefix;
    key += "kv/";
    key += std::to_string(shared);
    key += "/p/";
    key += std::to_string(block);
    return key;
}

std::string UniqueKey(const std::string& prefix, int shared, int request,
                      int block) {
    std::string key = prefix;
    key += "kv/";
    key += std::to_string(shared);
    key += "/r/";
    key += std::to_string(request);
    key += '/';
    key += std::to_string(block);
    return key;
}

// Escapes the characters JSON does not allow inside a string literal, so a key
// prefix taken from the command line cannot break the report.
std::string JsonEscape(const std::string& text) {
    std::string out;
    out.reserve(text.size());
    for (const char c : text) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buffer[7];
                    std::snprintf(buffer, sizeof(buffer), "\\u%04x",
                                  static_cast<unsigned char>(c));
                    out += buffer;
                } else {
                    out += c;
                }
                break;
        }
    }
    return out;
}

std::uint64_t NowNs() {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

double Percentile(std::vector<std::uint64_t>& values, double percentile) {
    if (values.empty()) {
        return 0.0;
    }
    const std::size_t index = static_cast<std::size_t>(
        std::ceil(percentile / 100.0 * static_cast<double>(values.size())));
    const std::size_t clamped =
        std::min(values.size() - 1, index > 0 ? index - 1 : 0);
    std::nth_element(values.begin(), values.begin() + clamped, values.end());
    return static_cast<double>(values[clamped]);
}

struct WorkingSet {
    std::size_t requests = 0;
    std::size_t blocks = 0;
    std::uint64_t bytes = 0;
    std::vector<std::pair<int, int>> request_owner;  // (prefix, request)
};

std::size_t UniqueBlocks(const Options& options) {
    return static_cast<std::size_t>(options.blocks_per_request -
                                    options.prefix_blocks);
}

WorkingSet BuildWorkingSet(const Options& options) {
    WorkingSet set;
    const std::size_t unique = UniqueBlocks(options);
    for (int prefix = 0; prefix < options.prefixes; ++prefix) {
        set.blocks += static_cast<std::size_t>(options.prefix_blocks);
        for (int request = 0; request < options.requests_per_prefix;
             ++request) {
            set.request_owner.emplace_back(prefix, request);
            set.blocks += unique;
        }
    }
    set.requests = set.request_owner.size();
    set.bytes = set.blocks * options.block_bytes;
    return set;
}

std::shared_ptr<Client> CreateClient(const Options& options, int port) {
    const std::string hostname = options.host + ":" + std::to_string(port);
    auto client = Client::Create(hostname, "P2PHANDSHAKE", options.protocol,
                                 std::nullopt, options.master);
    return client.has_value() ? client.value() : nullptr;
}

bool Prefill(Client& provider, const Options& options, const WorkingSet& set,
             void* source) {
    ReplicateConfig config;
    config.replica_num = 1;
    std::vector<Slice> slices{Slice{source, options.block_bytes}};
    std::size_t written = 0;
    for (int prefix = 0; prefix < options.prefixes; ++prefix) {
        for (int block = 0; block < options.prefix_blocks; ++block) {
            const auto result = provider.Put(
                PrefixKey(options.key_prefix, prefix, block), slices, config);
            if (!result.has_value()) {
                std::fprintf(
                    stderr, "put %s failed: %s\n",
                    PrefixKey(options.key_prefix, prefix, block).c_str(),
                    toString(result.error()).c_str());
                return false;
            }
            ++written;
        }
        for (int request = 0; request < options.requests_per_prefix;
             ++request) {
            for (int block = 0;
                 block < options.blocks_per_request - options.prefix_blocks;
                 ++block) {
                const auto result = provider.Put(
                    UniqueKey(options.key_prefix, prefix, request, block),
                    slices, config);
                if (!result.has_value()) {
                    std::fprintf(
                        stderr, "put %s failed: %s\n",
                        UniqueKey(options.key_prefix, prefix, request, block)
                            .c_str(),
                        toString(result.error()).c_str());
                    return false;
                }
                ++written;
            }
        }
    }
    std::fprintf(stderr, "prefilled %zu objects (%zu blocks)\n", written,
                 set.blocks);
    return true;
}

struct Result {
    std::uint64_t completed = 0;
    std::uint64_t errors = 0;
    std::uint64_t warmup_errors = 0;
    std::uint64_t bytes = 0;
    std::vector<std::uint64_t> fetch_us;
};

void RunWorker(Client& consumer, const Options& options, const WorkingSet& set,
               char* read_base, std::size_t stride, int index,
               std::atomic<bool>& stop, std::uint64_t measure_from_ns,
               std::uint64_t finish_ns, Result* result) {
    const std::size_t per_request =
        static_cast<std::size_t>(options.blocks_per_request);
    char* buffer = read_base + static_cast<std::size_t>(index) * stride;
    const std::uint64_t interval_ns =
        static_cast<std::uint64_t>(1e9) *
        static_cast<std::uint64_t>(options.connections) /
        static_cast<std::uint64_t>(options.rate);
    std::uint64_t planned = NowNs();
    std::size_t next = static_cast<std::size_t>(index);
    std::unordered_map<std::string, std::vector<Slice>> destinations;
    destinations.reserve(per_request);

    while (!stop.load(std::memory_order_relaxed)) {
        const std::uint64_t now = NowNs();
        if (now >= finish_ns) {
            break;
        }
        if (now < planned) {
            std::this_thread::sleep_for(
                std::chrono::nanoseconds(planned - now));
            if (NowNs() >= finish_ns) {
                break;
            }
        }
        planned += interval_ns;
        if (planned < NowNs()) {
            planned = NowNs();
        }

        const auto& owner = set.request_owner[next % set.request_owner.size()];
        ++next;

        std::vector<std::string> keys;
        keys.reserve(per_request);
        destinations.clear();
        for (int block = 0; block < options.prefix_blocks; ++block) {
            keys.push_back(PrefixKey(options.key_prefix, owner.first, block));
        }
        for (int block = 0;
             block < options.blocks_per_request - options.prefix_blocks;
             ++block) {
            keys.push_back(UniqueKey(options.key_prefix, owner.first,
                                     owner.second, block));
        }
        for (std::size_t slot = 0; slot < keys.size(); ++slot) {
            auto& slices = destinations[keys[slot]];
            slices.clear();
            slices.push_back(Slice{buffer + slot * options.block_bytes,
                                   options.block_bytes});
        }

        const std::uint64_t begin = NowNs();
        const auto results = consumer.BatchGet(keys, destinations);
        const std::uint64_t end = NowNs();
        bool ok = results.size() == keys.size();
        for (const auto& item : results) {
            ok = ok && item.has_value();
        }
        if (!ok) {
            for (const auto& item : results) {
                if (!item.has_value()) {
                    std::fprintf(stderr, "get failed: %s\n",
                                 toString(item.error()).c_str());
                    break;
                }
            }
            if (begin >= measure_from_ns) {
                ++result->errors;
            } else {
                ++result->warmup_errors;
            }
            continue;
        }
        if (begin >= measure_from_ns && end <= finish_ns) {
            ++result->completed;
            result->bytes += per_request * options.block_bytes;
            result->fetch_us.push_back((end - begin) / 1000);
        }
    }
}

}  // namespace
}  // namespace mooncake::bench

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    FLAGS_minloglevel = google::ERROR;

    mooncake::bench::Options options;
    if (!mooncake::bench::ParseOptions(argc, argv, &options)) {
        mooncake::bench::PrintUsage(argv[0]);
        return 2;
    }

    options.prefixes *= options.scale;
    options.rate *= options.scale;
    options.connections *= options.scale;

    auto set = mooncake::bench::BuildWorkingSet(options);
    const std::uint64_t needed = set.bytes + (1ull << 30);
    if (options.segment_bytes == 0) {
        options.segment_bytes = static_cast<std::size_t>(needed);
    }

    auto provider =
        mooncake::bench::CreateClient(options, options.provider_port);
    if (provider == nullptr) {
        std::fprintf(stderr, "cannot create the provider client\n");
        return 1;
    }
    void* segment = mooncake::allocate_buffer_allocator_memory(
        options.segment_bytes, options.protocol);
    if (segment == nullptr) {
        std::fprintf(stderr, "cannot allocate a %zu byte segment\n",
                     options.segment_bytes);
        return 1;
    }
    const auto mounted = provider->MountSegment(segment, options.segment_bytes,
                                                options.protocol);
    if (!mounted.has_value()) {
        std::fprintf(stderr, "cannot mount the segment: %s\n",
                     mooncake::toString(mounted.error()).c_str());
        return 1;
    }

    // The allocator refuses buffers smaller than 16 MiB, which is larger than
    // a block at the sizes a KV cache uses.
    const std::size_t source_bytes =
        std::max<std::size_t>(options.block_bytes, 16u * 1024u * 1024u);
    void* source = mooncake::allocate_buffer_allocator_memory(source_bytes,
                                                              options.protocol);
    if (source == nullptr) {
        std::fprintf(stderr, "cannot allocate a source buffer\n");
        return 1;
    }
    std::memset(source, 0x5a, options.block_bytes);
    const std::uint64_t prefill_begin = mooncake::bench::NowNs();
    if (!mooncake::bench::Prefill(*provider, options, set, source)) {
        return 1;
    }
    const std::uint64_t prefill_end = mooncake::bench::NowNs();

    auto consumer =
        mooncake::bench::CreateClient(options, options.consumer_port);
    if (consumer == nullptr) {
        std::fprintf(stderr, "cannot create the consumer client\n");
        return 1;
    }
    const std::size_t read_stride =
        static_cast<std::size_t>(options.blocks_per_request) *
        options.block_bytes;
    const std::size_t read_bytes = std::max<std::size_t>(
        read_stride * static_cast<std::size_t>(options.connections),
        16u * 1024u * 1024u);
    void* reads = mooncake::allocate_buffer_allocator_memory(read_bytes,
                                                             options.protocol);
    if (reads == nullptr) {
        std::fprintf(stderr, "cannot allocate %zu read bytes\n", read_bytes);
        return 1;
    }
    const auto reads_mounted =
        consumer->MountSegment(reads, read_bytes, options.protocol);
    if (!reads_mounted.has_value()) {
        std::fprintf(stderr, "cannot mount the read segment: %s\n",
                     mooncake::toString(reads_mounted.error()).c_str());
        return 1;
    }

    std::vector<mooncake::bench::Result> results(
        static_cast<std::size_t>(options.connections));
    std::atomic<bool> stop{false};
    const std::uint64_t start_ns = mooncake::bench::NowNs();
    const std::uint64_t measure_from_ns =
        start_ns + static_cast<std::uint64_t>(options.warmup_s) * 1000000000ull;
    const std::uint64_t finish_ns =
        start_ns +
        static_cast<std::uint64_t>(options.duration_s) * 1000000000ull;

    std::vector<std::thread> workers;
    workers.reserve(results.size());
    for (std::size_t i = 0; i < results.size(); ++i) {
        workers.emplace_back([&, i] {
            mooncake::bench::RunWorker(*consumer, options, set,
                                       static_cast<char*>(reads), read_stride,
                                       static_cast<int>(i), stop,
                                       measure_from_ns, finish_ns, &results[i]);
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    std::uint64_t completed = 0;
    std::uint64_t errors = 0;
    std::uint64_t warmup_errors = 0;
    std::uint64_t bytes = 0;
    std::vector<std::uint64_t> fetch_us;
    for (auto& result : results) {
        completed += result.completed;
        errors += result.errors;
        warmup_errors += result.warmup_errors;
        bytes += result.bytes;
        fetch_us.insert(fetch_us.end(), result.fetch_us.begin(),
                        result.fetch_us.end());
    }
    const double window_s = std::max(
        1e-9, static_cast<double>(options.duration_s - options.warmup_s));
    const double rps = static_cast<double>(completed) / window_s;
    const double gib = static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0);
    const bool client_bound = rps < 0.99 * static_cast<double>(options.rate);

    std::printf(
        "{\"tool\":\"traffic_kv\",\"master\":\"%s\",\"protocol\":\"%s\","
        "\"block_bytes\":%zu,\"blocks_per_request\":%d,\"prefix_blocks\":%d,"
        "\"requests\":%zu,\"scale\":%d,\"rate_target\":%d,"
        "\"connections\":%d,\"duration_s\":%d,\"warmup_s\":%d,"
        "\"achieved_rps\":%.1f,\"client_bound\":%s,\"gib_per_s\":%.3f,"
        "\"errors\":%llu,\"warmup_errors\":%llu,\"key_prefix\":\"%s\","
        "\"kv_bytes_per_request\":%llu,\"working_set_bytes\":%llu,"
        "\"prefill_ms\":%.1f,\"fetch_us\":{\"p50\":%.0f,\"p90\":%.0f,"
        "\"p99\":%.0f,\"p999\":%.0f}}\n",
        options.master.c_str(), options.protocol.c_str(), options.block_bytes,
        options.blocks_per_request, options.prefix_blocks, set.requests,
        options.scale, options.rate, options.connections, options.duration_s,
        options.warmup_s, rps, client_bound ? "true" : "false", gib / window_s,
        static_cast<unsigned long long>(errors),
        static_cast<unsigned long long>(warmup_errors),
        mooncake::bench::JsonEscape(options.key_prefix).c_str(),
        static_cast<unsigned long long>(
            static_cast<std::uint64_t>(options.blocks_per_request) *
            options.block_bytes),
        static_cast<unsigned long long>(set.bytes),
        static_cast<double>(prefill_end - prefill_begin) / 1e6,
        mooncake::bench::Percentile(fetch_us, 50.0),
        mooncake::bench::Percentile(fetch_us, 90.0),
        mooncake::bench::Percentile(fetch_us, 99.0),
        mooncake::bench::Percentile(fetch_us, 99.9));

    mooncake::free_memory(options.protocol, source);
    consumer->UnmountSegment(reads, read_bytes);
    mooncake::free_memory(options.protocol, reads);
    provider->UnmountSegment(segment, options.segment_bytes);
    mooncake::free_memory(options.protocol, segment);
    google::ShutdownGoogleLogging();
    return errors == 0 ? 0 : 1;
}
