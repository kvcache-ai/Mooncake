#include "offload_read_scheduler.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

namespace mooncake {
namespace {

using ReadResult = tl::expected<void, ErrorCode>;
constexpr auto kWaitLimit = std::chrono::seconds(10);

std::string Key(const std::string &prefix, int i) {
    char buffer[16];
    std::snprintf(buffer, sizeof(buffer), "%02d", i);
    return prefix + buffer;
}

// key -> one slice of `bytes` (the pointer is never dereferenced); keys are
// zero-padded, so their sorted order is their index order.
OffloadReadObjects MakeObjects(const std::string &prefix, int count,
                               uint64_t bytes) {
    OffloadReadObjects objects;
    for (int i = 0; i < count; ++i) {
        objects.emplace(Key(prefix, i),
                        std::vector<Slice>{Slice{nullptr, bytes}});
    }
    return objects;
}

std::set<std::string> KeysOf(const OffloadReadObjects &objects) {
    std::set<std::string> keys;
    for (const auto &object : objects) keys.insert(object.first);
    return keys;
}

std::vector<std::set<std::string>> Membership(
    const std::vector<OffloadReadChunk> &chunks) {
    std::vector<std::set<std::string>> membership;
    for (const auto &chunk : chunks)
        membership.push_back(KeysOf(chunk.objects));
    return membership;
}

// Lets `count` calls meet: Arrive() returns once that many have arrived (or
// after kWaitLimit, failing the test), so they are all in flight together.
class Rendezvous {
   public:
    explicit Rendezvous(size_t count) : count_(count) {}

    void Arrive() {
        std::unique_lock<std::mutex> lock(mu_);
        ++arrived_;
        cv_.notify_all();
        EXPECT_TRUE(cv_.wait_for(lock, kWaitLimit,
                                 [this] { return arrived_ >= count_; }))
            << arrived_ << " of " << count_ << " arrived";
    }

   private:
    std::mutex mu_;
    std::condition_variable cv_;
    size_t count_;
    size_t arrived_ = 0;
};

// A flag one thread raises and others wait for.
class Signal {
   public:
    void Raise() {
        std::lock_guard<std::mutex> lock(mu_);
        raised_ = true;
        cv_.notify_all();
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mu_);
        EXPECT_TRUE(cv_.wait_for(lock, kWaitLimit, [this] { return raised_; }));
    }

   private:
    std::mutex mu_;
    std::condition_variable cv_;
    bool raised_ = false;
};

// Time for a failing chunk's thread to record the failure after its reader
// returned, before a sibling chunk finishes. The failing thread's own next
// pick does not depend on it.
void LetTheFailureLand() {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
}

// ---- SplitOffloadReadChunks ----

TEST(SplitOffloadReadChunksTest, GreedySplitRespectsCap) {
    // 11 keys of 6 MiB with a 64 MiB cap: 10 + 1 (the 11th would make 66).
    const auto chunks =
        SplitOffloadReadChunks(MakeObjects("k", 11, 6ULL << 20), 64ULL << 20);
    ASSERT_EQ(chunks.size(), 2u);
    EXPECT_EQ(chunks[0].objects.size(), 10u);
    EXPECT_EQ(chunks[0].bytes, 60ULL << 20);
    EXPECT_EQ(chunks[1].objects.size(), 1u);
    EXPECT_EQ(chunks[1].objects.begin()->first, "k10");
}

TEST(SplitOffloadReadChunksTest, OversizedObjectGetsOwnChunk) {
    OffloadReadObjects objects;
    objects.emplace("big", std::vector<Slice>{Slice{nullptr, 100}});
    objects.emplace("small", std::vector<Slice>{Slice{nullptr, 1}});
    const auto chunks = SplitOffloadReadChunks(objects, 10);
    ASSERT_EQ(chunks.size(), 2u);
    EXPECT_EQ(Membership(chunks),
              (std::vector<std::set<std::string>>{{"big"}, {"small"}}));
    EXPECT_EQ(chunks[0].bytes, 100u);
    EXPECT_EQ(chunks[1].bytes, 1u);
}

TEST(SplitOffloadReadChunksTest, ZeroCapKeepsOneChunk) {
    const auto chunks =
        SplitOffloadReadChunks(MakeObjects("k", 50, 1 << 20), 0);
    ASSERT_EQ(chunks.size(), 1u);
    EXPECT_EQ(chunks[0].objects.size(), 50u);
}

TEST(SplitOffloadReadChunksTest, EveryKeyInExactlyOneChunkWithItsSlices) {
    OffloadReadObjects objects;
    std::vector<char> buffer(64);
    for (int i = 0; i < 16; ++i) {
        // Two slices per object; bytes are summed across slices.
        objects.emplace(Key("k", i),
                        std::vector<Slice>{Slice{buffer.data() + i, 3},
                                           Slice{buffer.data() + 32 + i, 4}});
    }
    const auto chunks = SplitOffloadReadChunks(objects, 15);
    std::map<std::string, int> seen;
    for (const auto &chunk : chunks) {
        EXPECT_LE(chunk.bytes, 15u);
        uint64_t bytes = 0;
        for (const auto &[key, slices] : chunk.objects) {
            ++seen[key];
            EXPECT_EQ(slices.size(), 2u);
            EXPECT_EQ(slices[0].ptr, objects.at(key)[0].ptr);
            EXPECT_EQ(slices[1].ptr, objects.at(key)[1].ptr);
            bytes += OffloadReadObjectBytes(slices);
        }
        EXPECT_EQ(bytes, chunk.bytes);
    }
    EXPECT_EQ(chunks.size(), 8u);  // 7-byte objects, two per 15-byte chunk
    ASSERT_EQ(seen.size(), objects.size());
    for (const auto &[key, count] : seen) EXPECT_EQ(count, 1) << key;
}

// Concurrent readers of one batch must send the same chunks, whatever order
// their maps iterate in, so that the owner's cold-read singleflight (keyed by
// a batch's keys and sizes) can share each chunk's SSD read between them.
TEST(SplitOffloadReadChunksTest, ChunksDependOnlyOnKeysAndSizes) {
    // Uneven sizes, so a different order would close chunks elsewhere.
    const std::vector<uint64_t> sizes = {5, 1, 4, 4, 2, 7, 3, 3, 6, 1, 2, 5};
    OffloadReadObjects forward;
    OffloadReadObjects backward;
    backward.reserve(1024);  // other bucket count, other iteration order
    for (size_t i = 0; i < sizes.size(); ++i) {
        forward.emplace(Key("k", i), std::vector<Slice>{{nullptr, sizes[i]}});
    }
    for (size_t i = sizes.size(); i-- > 0;) {
        backward.emplace(Key("k", i), std::vector<Slice>{{nullptr, sizes[i]}});
    }
    const auto a = SplitOffloadReadChunks(forward, 10);
    const auto b = SplitOffloadReadChunks(backward, 10);
    // Greedy in key order: 5+1+4 | 4+2 | 7+3 | 3+6+1 | 2+5.
    const std::vector<std::set<std::string>> expected = {{"k00", "k01", "k02"},
                                                         {"k03", "k04"},
                                                         {"k05", "k06"},
                                                         {"k07", "k08", "k09"},
                                                         {"k10", "k11"}};
    EXPECT_EQ(Membership(a), expected);
    EXPECT_EQ(Membership(b), expected);
    for (size_t i = 0; i < a.size() && i < b.size(); ++i) {
        EXPECT_EQ(a[i].bytes, b[i].bytes) << i;
    }
}

TEST(SplitOffloadReadChunksTest, IsSplitOnlyWhenMoreThanOneChunk) {
    EXPECT_FALSE(OffloadReadIsSplit(MakeObjects("k", 8, 1 << 20), 0));
    EXPECT_FALSE(OffloadReadIsSplit(MakeObjects("k", 8, 1 << 20), 8 << 20));
    EXPECT_TRUE(OffloadReadIsSplit(MakeObjects("k", 9, 1 << 20), 8 << 20));
    EXPECT_FALSE(OffloadReadIsSplit(MakeObjects("k", 1, 512 << 20), 1 << 20));
    EXPECT_FALSE(OffloadReadIsSplit(OffloadReadObjects{}, 1));
}

// ---- OffloadReadErrorIsKeyLocal ----

TEST(OffloadReadErrorTest, OnlyKeyDataErrorsAreKeyLocal) {
    for (ErrorCode error :
         {ErrorCode::INVALID_KEY, ErrorCode::OBJECT_NOT_FOUND,
          ErrorCode::BUCKET_NOT_FOUND, ErrorCode::FILE_NOT_FOUND,
          ErrorCode::FILE_OPEN_FAIL, ErrorCode::FILE_READ_FAIL}) {
        EXPECT_TRUE(OffloadReadErrorIsKeyLocal(error)) << error;
    }
    for (ErrorCode error :
         {ErrorCode::RPC_FAIL, ErrorCode::TRANSFER_FAIL,
          ErrorCode::BUFFER_OVERFLOW, ErrorCode::OBJECT_HAS_LEASE,
          ErrorCode::LEASE_EXPIRED, ErrorCode::INTERNAL_ERROR,
          ErrorCode::INVALID_PARAMS}) {
        EXPECT_FALSE(OffloadReadErrorIsKeyLocal(error)) << error;
    }
}

// ---- RunOffloadReadPipeline ----

// Source bytes of every key and a reader that copies them into a chunk's
// destination slices, the way the owner's transfer fills them.
struct CopyingSource {
    std::map<std::string, std::string> values;
    std::mutex mu;
    std::map<std::string, int> reads;

    // Each object's destination is split into two slices of `dest`.
    OffloadReadObjects Destinations(std::map<std::string, std::string> &dest) {
        OffloadReadObjects objects;
        for (const auto &[key, value] : values) {
            auto &buffer = dest[key];
            buffer.assign(value.size(), '\0');
            const size_t head = value.size() / 3;
            objects.emplace(
                key, std::vector<Slice>{
                         Slice{buffer.data(), head},
                         Slice{buffer.data() + head, value.size() - head}});
        }
        return objects;
    }

    OffloadChunkReader reader() {
        return [this](const std::string &,
                      OffloadReadObjects &objects) -> ReadResult {
            for (auto &[key, slices] : objects) {
                const std::string &value = values.at(key);
                size_t offset = 0;
                for (auto &slice : slices) {
                    std::memcpy(slice.ptr, value.data() + offset, slice.size);
                    offset += slice.size;
                }
                std::lock_guard<std::mutex> lock(mu);
                ++reads[key];
            }
            return {};
        };
    }
};

// Single-key chunks k00..k{count-1}, in that order.
std::vector<OffloadReadChunk> SingleKeyChunks(int count) {
    std::vector<OffloadReadChunk> chunks;
    for (int i = 0; i < count; ++i) {
        chunks.push_back(OffloadReadChunk{
            {{Key("k", i), std::vector<Slice>{Slice{nullptr, 1}}}}, 1});
    }
    return chunks;
}

std::string ChunkKey(const OffloadReadObjects &objects) {
    return objects.begin()->first;
}

int ChunkIndex(const OffloadReadObjects &objects) {
    return std::stoi(ChunkKey(objects).substr(1));
}

std::vector<ErrorCode> Errors(const std::vector<OffloadChunkResult> &results) {
    std::vector<ErrorCode> errors;
    for (const auto &result : results) errors.push_back(result.error);
    return errors;
}

std::vector<bool> Started(const std::vector<OffloadChunkResult> &results) {
    std::vector<bool> started;
    for (const auto &result : results) started.push_back(result.started);
    return started;
}

std::vector<ErrorCode> Repeat(ErrorCode error, size_t count) {
    return std::vector<ErrorCode>(count, error);
}

std::vector<bool> FirstN(size_t n, size_t count) {
    std::vector<bool> started(count, false);
    for (size_t i = 0; i < n && i < count; ++i) started[i] = true;
    return started;
}

TEST(OffloadReadPipelineTest, MultiChunkReadFillsEveryDestination) {
    CopyingSource source;
    for (int i = 0; i < 40; ++i) {
        std::string value(1000 + 37 * i, '\0');
        for (size_t j = 0; j < value.size(); ++j) {
            value[j] = static_cast<char>((i * 31 + j * 7) % 251);
        }
        source.values.emplace(Key("k", i), std::move(value));
    }
    std::map<std::string, std::string> dest;
    const auto objects = source.Destinations(dest);
    auto chunks = SplitOffloadReadChunks(objects, 8 << 10);
    ASSERT_GT(chunks.size(), 4u);
    OffloadReadPipelineStats stats;
    const auto results =
        RunOffloadReadPipeline("owner", chunks, 4, source.reader(), {}, &stats);
    EXPECT_EQ(Errors(results), Repeat(ErrorCode::OK, chunks.size()));
    EXPECT_EQ(stats.chunks, chunks.size());
    EXPECT_EQ(stats.started, chunks.size());
    EXPECT_EQ(stats.failed, 0u);
    EXPECT_EQ(stats.threads, 4u);
    ASSERT_EQ(source.reads.size(), source.values.size());
    for (const auto &[key, value] : source.values) {
        EXPECT_EQ(source.reads[key], 1) << key;
        EXPECT_EQ(dest[key], value) << key;
    }
}

TEST(OffloadReadPipelineTest, PassesTheOwnerEndpointToEveryChunk) {
    auto chunks = SingleKeyChunks(6);
    std::mutex mu;
    std::set<std::string> endpoints;
    RunOffloadReadPipeline(
        "owner-a:1234", chunks, 3,
        [&](const std::string &endpoint, OffloadReadObjects &) -> ReadResult {
            std::lock_guard<std::mutex> lock(mu);
            endpoints.insert(endpoint);
            return {};
        });
    EXPECT_EQ(endpoints, std::set<std::string>{"owner-a:1234"});
}

// K chunks in flight on K threads: K - 1 helpers and the calling thread. The
// first K reads only return once all K are in flight, so they must be on K
// different threads.
TEST(OffloadReadPipelineTest, ReadsKChunksAtOnceOnKThreads) {
    constexpr size_t kConcurrency = 3;
    auto chunks = SingleKeyChunks(32);
    Rendezvous first_window(kConcurrency);
    std::atomic<int> in_flight{0};
    std::atomic<int> peak{0};
    std::mutex mu;
    std::set<std::thread::id> threads;
    int spawns = 0;
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, kConcurrency,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            const int now = ++in_flight;
            int prev = peak.load();
            while (now > prev && !peak.compare_exchange_weak(prev, now)) {
            }
            {
                std::lock_guard<std::mutex> lock(mu);
                threads.insert(std::this_thread::get_id());
            }
            if (ChunkIndex(objects) < static_cast<int>(kConcurrency)) {
                first_window.Arrive();
            }
            --in_flight;
            return {};
        },
        {}, &stats,
        [&](std::function<void()> work) {
            ++spawns;
            return std::thread(std::move(work));
        });
    EXPECT_EQ(Errors(results), Repeat(ErrorCode::OK, 32));
    EXPECT_EQ(peak.load(), static_cast<int>(kConcurrency));
    EXPECT_EQ(spawns, static_cast<int>(kConcurrency) - 1);
    EXPECT_EQ(stats.threads, kConcurrency);
    EXPECT_EQ(threads.size(), kConcurrency);
    EXPECT_TRUE(threads.count(std::this_thread::get_id()))
        << "the calling thread did not read any chunk";
}

TEST(OffloadReadPipelineTest, NoMoreThreadsThanChunks) {
    auto chunks = SingleKeyChunks(2);
    int spawns = 0;
    OffloadReadPipelineStats stats;
    RunOffloadReadPipeline(
        "owner", chunks, 8,
        [](const std::string &, OffloadReadObjects &) -> ReadResult {
            return {};
        },
        {}, &stats,
        [&](std::function<void()> work) {
            ++spawns;
            return std::thread(std::move(work));
        });
    EXPECT_EQ(spawns, 1);
    EXPECT_EQ(stats.threads, 2u);
}

// A thread takes the next chunk as soon as its own is done: a slow chunk
// does not hold up the chunks behind it.
TEST(OffloadReadPipelineTest, SlowChunkDoesNotHoldUpTheOthers) {
    auto chunks = SingleKeyChunks(10);
    Signal others_done;
    std::atomic<int> others{0};
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 2,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            if (ChunkIndex(objects) == 0) {
                others_done.Wait();  // k01..k09, all on the other thread
            } else if (++others == 9) {
                others_done.Raise();
            }
            return {};
        });
    EXPECT_EQ(others.load(), 9);
    EXPECT_EQ(Errors(results), Repeat(ErrorCode::OK, 10));
}

// After a failure the chunks not yet started are not read; the chunks read
// before keep OK.
TEST(OffloadReadPipelineTest, SerialReadStopsAtTheFirstFailure) {
    auto chunks = SingleKeyChunks(10);
    std::vector<std::string> order;
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 1,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            order.push_back(ChunkKey(objects));
            if (order.back() == "k03") {
                return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
            }
            return {};
        },
        {}, &stats);
    EXPECT_EQ(order, (std::vector<std::string>{"k00", "k01", "k02", "k03"}));
    EXPECT_EQ(Started(results), FirstN(4, 10));
    EXPECT_EQ(results[3].error, ErrorCode::BUFFER_OVERFLOW);
    for (size_t i = 0; i < 3; ++i) EXPECT_EQ(results[i].error, ErrorCode::OK);
    EXPECT_EQ(stats.started, 4u);
    EXPECT_EQ(stats.failed, 1u);
    EXPECT_EQ(stats.threads, 1u);
}

// After a failure no chunk is started, but the chunks already in flight are
// waited for: each holds owner staging until its own read returns, and they
// refer to the caller's chunks and destinations. Their own results are kept.
TEST(OffloadReadPipelineTest, FailureWaitsForInFlightChunksAndStartsNoMore) {
    auto chunks = SingleKeyChunks(12);
    Rendezvous first_window(4);
    Signal k0_failed;
    std::atomic<int> held{0};  // reads in progress ("staging leases")
    std::atomic<int> reads{0};
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 4,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            ++held;
            ++reads;
            first_window.Arrive();
            ReadResult r{};
            if (ChunkIndex(objects) == 0) {
                r = tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
                k0_failed.Raise();
            } else {
                k0_failed.Wait();
                LetTheFailureLand();
            }
            --held;
            return r;
        },
        {}, &stats);
    EXPECT_EQ(held.load(), 0) << "returned before an in-flight chunk ended";
    EXPECT_EQ(reads.load(), 4);  // the first window only
    EXPECT_EQ(stats.started, 4u);
    EXPECT_EQ(stats.failed, 1u);
    EXPECT_EQ(Started(results), FirstN(4, 12));
    EXPECT_EQ(results[0].error, ErrorCode::OBJECT_HAS_LEASE);
    for (size_t i = 1; i < 4; ++i) EXPECT_EQ(results[i].error, ErrorCode::OK);
}

// A failure of a chunk that is not the lowest in flight also stops the read:
// the thread freed by k00 takes no further chunk.
TEST(OffloadReadPipelineTest, FailureOfAnyInFlightChunkStopsTheRead) {
    auto chunks = SingleKeyChunks(10);
    Rendezvous both(2);
    Signal k1_failed;
    std::mutex mu;
    std::vector<std::string> started;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 2,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            {
                std::lock_guard<std::mutex> lock(mu);
                started.push_back(ChunkKey(objects));
            }
            both.Arrive();
            if (ChunkIndex(objects) == 1) {
                k1_failed.Raise();
                return tl::make_unexpected(ErrorCode::TRANSFER_FAIL);
            }
            k1_failed.Wait();
            LetTheFailureLand();
            return {};
        });
    EXPECT_EQ(started.size(), 2u) << "a chunk was started after k01 failed";
    EXPECT_EQ(Started(results), FirstN(2, 10));
    EXPECT_EQ(results[0].error, ErrorCode::OK);
    EXPECT_EQ(results[1].error, ErrorCode::TRANSFER_FAIL);
}

// The owner could not find or read the failed chunk's keys: the other
// chunks, with other keys, are still read. Errors about the owner or the
// whole read stop it.
TEST(OffloadReadPipelineTest, OnlyErrorsThatAreNotKeyLocalStopTheRead) {
    for (ErrorCode error :
         {ErrorCode::INVALID_KEY, ErrorCode::BUCKET_NOT_FOUND,
          ErrorCode::FILE_READ_FAIL, ErrorCode::RPC_FAIL,
          ErrorCode::TRANSFER_FAIL, ErrorCode::BUFFER_OVERFLOW,
          ErrorCode::OBJECT_HAS_LEASE, ErrorCode::LEASE_EXPIRED}) {
        SCOPED_TRACE(toString(error));
        auto chunks = SingleKeyChunks(6);
        int reads = 0;
        const auto results = RunOffloadReadPipeline(
            "owner", chunks, 1,
            [&](const std::string &,
                OffloadReadObjects &objects) -> ReadResult {
                ++reads;
                if (ChunkIndex(objects) == 2) return tl::make_unexpected(error);
                return {};
            });
        const bool key_local = OffloadReadErrorIsKeyLocal(error);
        EXPECT_EQ(reads, key_local ? 6 : 3);
        EXPECT_EQ(Started(results), FirstN(key_local ? 6 : 3, 6));
        EXPECT_EQ(results[2].error, error);
        EXPECT_EQ(results[2].stop_error(), key_local ? ErrorCode::OK : error);
    }
}

TEST(OffloadReadPipelineTest, ThrowingReaderFailsItsChunkWithInternalError) {
    auto chunks = SingleKeyChunks(6);
    Rendezvous both(2);
    Signal k0_threw;
    std::atomic<int> reads{0};
    std::vector<OffloadChunkResult> results;
    EXPECT_NO_THROW(results = RunOffloadReadPipeline(
                        "owner", chunks, 2,
                        [&](const std::string &,
                            OffloadReadObjects &objects) -> ReadResult {
                            ++reads;
                            both.Arrive();
                            if (ChunkIndex(objects) == 0) {
                                k0_threw.Raise();
                                throw std::runtime_error("boom");
                            }
                            k0_threw.Wait();
                            LetTheFailureLand();
                            return {};
                        }));
    EXPECT_EQ(reads.load(), 2);  // k00 and k01, nothing after the throw
    EXPECT_EQ(Started(results), FirstN(2, 6));
    EXPECT_EQ(results[0].error, ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(results[1].error, ErrorCode::OK);
}

std::system_error ThreadLimit() {
    return std::system_error(
        std::make_error_code(std::errc::resource_unavailable_try_again),
        "injected");
}

// std::thread throws std::system_error when no thread can be started. The
// read then goes on with the threads that did start: same result, fewer
// chunks in flight.
TEST(OffloadReadPipelineTest, ThreadStartFailureReadsWithTheThreadsStarted) {
    auto chunks = SingleKeyChunks(8);
    std::mutex mu;
    std::map<std::string, int> reads;
    std::set<std::thread::id> threads;
    int spawns = 0;
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 4,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            std::lock_guard<std::mutex> lock(mu);
            ++reads[ChunkKey(objects)];
            threads.insert(std::this_thread::get_id());
            return {};
        },
        {}, &stats,
        [&](std::function<void()> work) -> std::thread {
            if (++spawns == 2) throw ThreadLimit();
            return std::thread(std::move(work));
        });
    EXPECT_EQ(Errors(results), Repeat(ErrorCode::OK, 8));
    EXPECT_EQ(spawns, 2) << "kept trying to start threads after a failure";
    EXPECT_EQ(stats.started, 8u);
    EXPECT_EQ(stats.threads, 2u);
    EXPECT_LE(threads.size(), 2u);
    ASSERT_EQ(reads.size(), 8u);
    for (const auto &[key, count] : reads) EXPECT_EQ(count, 1) << key;
}

TEST(OffloadReadPipelineTest, NoThreadStartsReadsOnTheCallingThread) {
    auto chunks = SingleKeyChunks(8);
    const auto caller = std::this_thread::get_id();
    int reads = 0;
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 4,
        [&](const std::string &, OffloadReadObjects &) -> ReadResult {
            EXPECT_EQ(std::this_thread::get_id(), caller);
            ++reads;
            return {};
        },
        {}, &stats,
        [](std::function<void()>) -> std::thread { throw ThreadLimit(); });
    EXPECT_EQ(Errors(results), Repeat(ErrorCode::OK, 8));
    EXPECT_EQ(reads, 8);
    EXPECT_EQ(stats.threads, 1u);
}

TEST(OffloadReadPipelineTest, NoThreadStartsThenReadFailureStops) {
    auto chunks = SingleKeyChunks(8);
    int reads = 0;
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 4,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            ++reads;
            if (ChunkIndex(objects) == 1) {
                return tl::make_unexpected(ErrorCode::RPC_FAIL);
            }
            return {};
        },
        {}, &stats,
        [](std::function<void()>) -> std::thread { throw ThreadLimit(); });
    EXPECT_EQ(reads, 2);  // k00 and k01, both on the calling thread
    EXPECT_EQ(stats.started, 2u);
    EXPECT_EQ(Started(results), FirstN(2, 8));
    EXPECT_EQ(results[1].error, ErrorCode::RPC_FAIL);
}

TEST(OffloadReadPipelineTest, SingleSlotNeverStartsThreads) {
    auto chunks = SingleKeyChunks(5);
    const auto caller = std::this_thread::get_id();
    int reads = 0;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 1,
        [&](const std::string &, OffloadReadObjects &) -> ReadResult {
            EXPECT_EQ(std::this_thread::get_id(), caller);
            ++reads;
            return {};
        },
        {}, nullptr,
        [](std::function<void()>) -> std::thread {
            ADD_FAILURE() << "a thread was started for a single slot";
            return {};
        });
    EXPECT_EQ(Errors(results), Repeat(ErrorCode::OK, 5));
    EXPECT_EQ(reads, 5);
}

TEST(OffloadReadPipelineTest, EmptyIsNoop) {
    std::vector<OffloadReadChunk> chunks;
    OffloadReadPipelineStats stats;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 4,
        [](const std::string &, OffloadReadObjects &) -> ReadResult {
            ADD_FAILURE() << "reader called with no chunks";
            return {};
        },
        {}, &stats);
    EXPECT_TRUE(results.empty());
    EXPECT_EQ(stats.chunks, 0u);
    EXPECT_EQ(stats.started, 0u);
}

// The key check runs right after each chunk's read, on the thread that read
// it, and only for chunks that were read.
TEST(OffloadReadPipelineTest, KeyCheckRunsAfterEachReadOnItsThread) {
    auto chunks = SplitOffloadReadChunks(MakeObjects("k", 12, 1), 3);
    ASSERT_EQ(chunks.size(), 4u);
    std::mutex mu;
    std::map<std::string, std::thread::id> read_on;
    std::map<std::string, int> checks;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 2,
        [&](const std::string &, OffloadReadObjects &objects) -> ReadResult {
            std::lock_guard<std::mutex> lock(mu);
            for (const auto &object : objects) {
                read_on[object.first] = std::this_thread::get_id();
            }
            // k06..k08 are not read, so they must not be checked.
            if (objects.count("k06")) {
                return tl::make_unexpected(ErrorCode::FILE_READ_FAIL);
            }
            return {};
        },
        [&](const std::string &key) {
            std::lock_guard<std::mutex> lock(mu);
            EXPECT_TRUE(read_on.count(key)) << key << " checked before read";
            EXPECT_EQ(read_on[key], std::this_thread::get_id()) << key;
            ++checks[key];
            return ErrorCode::OK;
        });
    EXPECT_EQ(checks.size(), 9u);
    for (const auto &[key, count] : checks) EXPECT_EQ(count, 1) << key;
    EXPECT_FALSE(checks.count("k06"));
    EXPECT_EQ(results[2].error, ErrorCode::FILE_READ_FAIL);
    EXPECT_TRUE(results[3].started) << "a key-local error stopped the read";
}

TEST(OffloadReadPipelineTest, ThrowingKeyCheckFailsItsChunk) {
    auto chunks = SingleKeyChunks(4);
    int reads = 0;
    const auto results = RunOffloadReadPipeline(
        "owner", chunks, 1,
        [&](const std::string &, OffloadReadObjects &) -> ReadResult {
            ++reads;
            return {};
        },
        [](const std::string &key) -> ErrorCode {
            if (key == "k01") throw std::out_of_range("no such key");
            return ErrorCode::OK;
        });
    EXPECT_EQ(reads, 2);
    EXPECT_EQ(results[1].error, ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(results[1].failed_keys.empty());
    EXPECT_EQ(Started(results), FirstN(2, 4));
}

// ---- ReadOffloadObjectsInChunks ----

// Records every reader call (on any thread) and fails the calls whose keys
// include one of `fail_keys`. With wait_for_calls > 0, a call returns only
// once that many calls have arrived, so they are all in flight together.
struct RecordingReader {
    std::mutex mu;
    std::condition_variable cv;
    std::vector<std::set<std::string>> calls;
    std::vector<std::thread::id> threads;
    std::map<std::string, ErrorCode> fail_keys;
    size_t wait_for_calls = 0;

    OffloadChunkReader reader() {
        return [this](const std::string &,
                      OffloadReadObjects &objects) -> ReadResult {
            std::unique_lock<std::mutex> lock(mu);
            calls.push_back(KeysOf(objects));
            threads.push_back(std::this_thread::get_id());
            cv.notify_all();
            EXPECT_TRUE(cv.wait_for(lock, kWaitLimit, [this] {
                return calls.size() >= wait_for_calls;
            }));
            for (const auto &object : objects) {
                auto it = fail_keys.find(object.first);
                if (it != fail_keys.end()) {
                    return tl::make_unexpected(it->second);
                }
            }
            return {};
        };
    }
};

std::set<std::string> KeyRange(int first, int last) {
    std::set<std::string> keys;
    for (int i = first; i <= last; ++i) keys.insert(Key("k", i));
    return keys;
}

// Chunk bytes 0 (the default): one read of all the owner's keys on the
// calling thread, and a failure fails every key, as before chunking. The key
// check is left to the caller.
TEST(ReadOffloadObjectsInChunksTest, ZeroChunkBytesReadsEverythingOnce) {
    auto objects = MakeObjects("k", 20, 1 << 20);
    RecordingReader recorder;
    int checks = 0;
    const OffloadKeyCheck check = [&checks](const std::string &) {
        ++checks;
        return ErrorCode::OK;
    };
    OffloadReadPipelineStats stats;
    auto read = ReadOffloadObjectsInChunks("owner", objects, 0, 4,
                                           recorder.reader(), check, &stats);
    EXPECT_TRUE(read.failed_keys.empty());
    EXPECT_FALSE(read.keys_checked);
    ASSERT_EQ(recorder.calls.size(), 1u);
    EXPECT_EQ(recorder.calls[0], KeysOf(objects));
    EXPECT_EQ(recorder.threads[0], std::this_thread::get_id());
    EXPECT_EQ(stats.chunks, 1u);
    EXPECT_EQ(checks, 0);

    recorder.calls.clear();
    recorder.fail_keys["k07"] = ErrorCode::BUFFER_OVERFLOW;
    read = ReadOffloadObjectsInChunks("owner", objects, 0, 4, recorder.reader(),
                                      check);
    ASSERT_EQ(recorder.calls.size(), 1u);
    ASSERT_EQ(read.failed_keys.size(), objects.size());
    for (const auto &[key, error] : read.failed_keys) {
        EXPECT_EQ(error, ErrorCode::BUFFER_OVERFLOW) << key;
    }
    EXPECT_EQ(checks, 0);
}

// A read that fits in one chunk, or a single object, is one read as before.
TEST(ReadOffloadObjectsInChunksTest, SmallReadOrSingleObjectIsOneRead) {
    RecordingReader recorder;
    auto small = MakeObjects("k", 8, 1 << 20);
    EXPECT_TRUE(ReadOffloadObjectsInChunks("owner", small, 8 << 20, 4,
                                           recorder.reader())
                    .failed_keys.empty());
    auto big = MakeObjects("big", 1, 512 << 20);
    EXPECT_TRUE(
        ReadOffloadObjectsInChunks("owner", big, 64 << 20, 4, recorder.reader())
            .failed_keys.empty());
    ASSERT_EQ(recorder.calls.size(), 2u);
    EXPECT_EQ(recorder.calls[0], KeysOf(small));
    EXPECT_EQ(recorder.calls[1], KeysOf(big));
    for (const auto &thread : recorder.threads) {
        EXPECT_EQ(thread, std::this_thread::get_id());
    }
}

TEST(ReadOffloadObjectsInChunksTest, LargeReadIsSplitIntoBoundedChunks) {
    auto objects = MakeObjects("k", 64, 1 << 20);
    RecordingReader recorder;
    std::atomic<int> checks{0};
    OffloadReadPipelineStats stats;
    const auto read = ReadOffloadObjectsInChunks(
        "owner", objects, 8 << 20, 4, recorder.reader(),
        [&checks](const std::string &) {
            ++checks;
            return ErrorCode::OK;
        },
        &stats);
    EXPECT_TRUE(read.failed_keys.empty());
    EXPECT_TRUE(read.keys_checked);
    EXPECT_EQ(checks.load(), 64);
    EXPECT_EQ(stats.chunks, 8u);
    EXPECT_EQ(stats.started, 8u);
    ASSERT_EQ(recorder.calls.size(), 8u);
    std::set<std::set<std::string>> calls(recorder.calls.begin(),
                                          recorder.calls.end());
    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(calls.count(KeyRange(8 * i, 8 * i + 7))) << i;
    }
}

// A failed chunk fails only its own keys (and the keys of the chunks it kept
// from starting): the chunks read before it keep their data.
TEST(ReadOffloadObjectsInChunksTest, FailedChunkFailsOnlyItsKeys) {
    auto objects = MakeObjects("k", 12, 1 << 20);
    // Serial: chunks of 3 keys in key order; the 3rd chunk fails.
    RecordingReader serial;
    serial.fail_keys["k07"] = ErrorCode::BUFFER_OVERFLOW;
    auto read = ReadOffloadObjectsInChunks("owner", objects, 3 << 20, 1,
                                           serial.reader());
    ASSERT_EQ(serial.calls.size(), 3u) << "a chunk was read after the failure";
    EXPECT_EQ(serial.calls[2], KeyRange(6, 8));
    // 6 keys read, 3 in the failed chunk, 3 in the chunk never sent.
    EXPECT_EQ(read.failed_keys.size(), 6u);
    for (const auto &key : KeyRange(6, 11)) {
        ASSERT_TRUE(read.failed_keys.count(key)) << key;
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::BUFFER_OVERFLOW) << key;
    }

    // Concurrent: every chunk is in flight at once and only one fails, so
    // only that chunk's keys fail.
    RecordingReader concurrent;
    concurrent.fail_keys["k05"] = ErrorCode::BUFFER_OVERFLOW;
    concurrent.wait_for_calls = 4;
    read = ReadOffloadObjectsInChunks("owner", objects, 3 << 20, 4,
                                      concurrent.reader());
    ASSERT_EQ(concurrent.calls.size(), 4u);
    ASSERT_EQ(read.failed_keys.size(), 3u);
    for (const auto &key : KeyRange(3, 5)) {
        ASSERT_TRUE(read.failed_keys.count(key)) << key;
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::BUFFER_OVERFLOW);
    }
}

// A key-local error fails its chunk's keys only, and the read goes on.
TEST(ReadOffloadObjectsInChunksTest, KeyLocalFailureDoesNotStopTheRead) {
    auto objects = MakeObjects("k", 12, 1 << 20);
    RecordingReader recorder;
    recorder.fail_keys["k04"] = ErrorCode::FILE_READ_FAIL;
    const auto read = ReadOffloadObjectsInChunks("owner", objects, 3 << 20, 1,
                                                 recorder.reader());
    EXPECT_EQ(recorder.calls.size(), 4u);
    ASSERT_EQ(read.failed_keys.size(), 3u);
    for (const auto &key : KeyRange(3, 5)) {
        ASSERT_TRUE(read.failed_keys.count(key)) << key;
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::FILE_READ_FAIL);
    }
}

// The unsent keys fail with the error of the failed chunk with the lowest
// index among those that stopped the read.
TEST(ReadOffloadObjectsInChunksTest, UnsentKeysGetTheLowestIndexStopError) {
    auto objects = MakeObjects("k", 12, 1 << 20);
    // Both of the first two chunks fail while in flight together.
    RecordingReader both;
    both.wait_for_calls = 2;
    both.fail_keys["k00"] = ErrorCode::RPC_FAIL;
    both.fail_keys["k03"] = ErrorCode::BUFFER_OVERFLOW;
    auto read =
        ReadOffloadObjectsInChunks("owner", objects, 3 << 20, 2, both.reader());
    ASSERT_EQ(both.calls.size(), 2u);
    ASSERT_EQ(read.failed_keys.size(), 12u);
    for (const auto &key : KeyRange(0, 2)) {
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::RPC_FAIL) << key;
    }
    for (const auto &key : KeyRange(3, 5)) {
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::BUFFER_OVERFLOW) << key;
    }
    for (const auto &key : KeyRange(6, 11)) {
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::RPC_FAIL) << key;
    }

    // A key-local failure before the stop is not the unsent keys' error.
    RecordingReader serial;
    serial.fail_keys["k00"] = ErrorCode::INVALID_KEY;
    serial.fail_keys["k03"] = ErrorCode::TRANSFER_FAIL;
    read = ReadOffloadObjectsInChunks("owner", objects, 3 << 20, 1,
                                      serial.reader());
    ASSERT_EQ(serial.calls.size(), 2u);
    for (const auto &key : KeyRange(0, 2)) {
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::INVALID_KEY) << key;
    }
    for (const auto &key : KeyRange(3, 11)) {
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::TRANSFER_FAIL) << key;
    }
}

// The master lease is rechecked right after each chunk: keys whose lease ran
// out fail with LEASE_EXPIRED, keys read within their lease keep their data,
// and no further chunk is sent.
TEST(ReadOffloadObjectsInChunksTest, ExpiredLeaseFailsItsKeysAndStopsTheRead) {
    auto objects = MakeObjects("k", 12, 1 << 20);
    RecordingReader recorder;
    const auto read = ReadOffloadObjectsInChunks(
        "owner", objects, 3 << 20, 1, recorder.reader(),
        [](const std::string &key) {
            return key == "k07" ? ErrorCode::LEASE_EXPIRED : ErrorCode::OK;
        });
    EXPECT_TRUE(read.keys_checked);
    ASSERT_EQ(recorder.calls.size(), 3u);
    ASSERT_EQ(read.failed_keys.size(), 4u);
    for (const auto &key : {"k07", "k09", "k10", "k11"}) {
        ASSERT_TRUE(read.failed_keys.count(key)) << key;
        EXPECT_EQ(read.failed_keys.at(key), ErrorCode::LEASE_EXPIRED) << key;
    }
}

TEST(ReadOffloadObjectsInChunksTest, LogsOneSummaryPerSplitRead) {
    FLAGS_logtostderr = true;
    auto objects = MakeObjects("k", 12, 1 << 20);
    RecordingReader recorder;
    recorder.fail_keys["k04"] = ErrorCode::FILE_READ_FAIL;
    recorder.fail_keys["k07"] = ErrorCode::BUFFER_OVERFLOW;
    testing::internal::CaptureStderr();
    const auto read = ReadOffloadObjectsInChunks("owner-x", objects, 3 << 20, 1,
                                                 recorder.reader());
    const std::string log = testing::internal::GetCapturedStderr();
    EXPECT_EQ(read.failed_keys.size(), 9u);
    size_t summaries = 0;
    for (size_t at = log.find("Offload read from owner-x:");
         at != std::string::npos;
         at = log.find("Offload read from owner-x:", at + 1)) {
        ++summaries;
    }
    EXPECT_EQ(summaries, 1u) << log;
    EXPECT_NE(log.find("12 keys, 12582912 bytes in 4 chunks"),
              std::string::npos)
        << log;
    EXPECT_NE(log.find("2 of 3 sent chunks failed, 1 not sent; 9 of 12 keys "
                       "failed (BUFFER_OVERFLOW: 6, FILE_READ_FAIL: 3)"),
              std::string::npos)
        << log;
}

TEST(ReadOffloadObjectsInChunksTest, FormatsErrorCountsByCode) {
    EXPECT_EQ(FormatOffloadReadErrorCounts({}), "");
    EXPECT_EQ(FormatOffloadReadErrorCounts({{"a", ErrorCode::LEASE_EXPIRED},
                                            {"b", ErrorCode::BUFFER_OVERFLOW},
                                            {"c", ErrorCode::LEASE_EXPIRED}}),
              "BUFFER_OVERFLOW: 1, LEASE_EXPIRED: 2");
}

}  // namespace
}  // namespace mooncake
