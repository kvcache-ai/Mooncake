// Bounded, chunked reads of one owner's LOCAL_DISK (SSD offload) objects.
//
// A batch get sends every LOCAL_DISK key that lives on one owner as a single
// batch_get_offload_object RPC, so the owner has to stage the whole request in
// its offload staging buffer (MOONCAKE_OFFLOAD_LOCAL_BUFFER_SIZE_BYTES) at
// once. A request larger than the free part of that buffer fails with
// BUFFER_OVERFLOW for every key on the owner, however long it waits.
//
// ReadOffloadObjectsInChunks splits one owner's objects into chunks of at most
// chunk_bytes (an object larger than that gets a chunk of its own) and reads
// them with at most `concurrency` chunks in flight. Each chunk is one owner
// RPC + transfer + release, so a read holds at most about concurrency x
// chunk_bytes of the owner's staging buffer (ignoring per-object alignment
// padding), whatever the size of the batch. chunk_bytes == 0 (the default), a
// single object, or a read that fits in one chunk keeps the single read of all
// the owner's objects.
//
// Chunks are formed from the keys in sorted order, so concurrent readers of
// the same batch send the same chunks, and the owner's cold-read singleflight
// (keyed by a batch's keys and sizes, see BuildColdReadFlightKey in
// file_storage.cpp) still serves each chunk with one SSD read.
//
// Failures: a chunk that fails with an error about the owner or the read as a
// whole (see OffloadReadErrorIsKeyLocal) stops the read: no further chunk is
// started, and the chunks already in flight are waited for. A chunk that
// fails because the owner could not find or read its keys does not stop the
// read. The keys of the chunks that were read keep their data; a failed
// chunk's keys fail with its error, and the keys of the chunks that were not
// started fail with the error of the failed chunk with the lowest index among
// those that stopped the read.
//
// The splitter and the pipeline are independent of RealClient so they can be
// tested with a fake chunk reader.

#pragma once

#include <glog/logging.h>
#include <ylt/util/tl/expected.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "types.h"

namespace mooncake {

// key -> destination slices of one owner's objects.
using OffloadReadObjects = std::unordered_map<std::string, std::vector<Slice>>;

// One owner RPC's worth of keys.
struct OffloadReadChunk {
    OffloadReadObjects objects;
    uint64_t bytes = 0;
};

inline uint64_t OffloadReadObjectBytes(const std::vector<Slice> &slices) {
    uint64_t total = 0;
    for (const auto &slice : slices) total += slice.size;
    return total;
}

// Whether a read of `objects` is split into more than one chunk.
inline bool OffloadReadIsSplit(const OffloadReadObjects &objects,
                               uint64_t chunk_bytes) {
    if (chunk_bytes == 0 || objects.size() <= 1) return false;
    uint64_t total = 0;
    for (const auto &object : objects) {
        total += OffloadReadObjectBytes(object.second);
        if (total > chunk_bytes) return true;
    }
    return false;
}

// Greedy split in key order: a chunk closes before an object that would take
// it past chunk_bytes, and an object larger than chunk_bytes gets a chunk of
// its own. chunk_bytes == 0 keeps every object in one chunk. The keys of one
// owner's map are unique, so the key order is total and the chunks depend
// only on the keys, their sizes and chunk_bytes, not on the map's iteration
// order. Slices point into the caller's destinations, so the chunks only copy
// (pointer, size) descriptors.
inline std::vector<OffloadReadChunk> SplitOffloadReadChunks(
    const OffloadReadObjects &objects, uint64_t chunk_bytes) {
    std::vector<OffloadReadObjects::const_pointer> sorted;
    sorted.reserve(objects.size());
    for (const auto &object : objects) sorted.push_back(&object);
    std::sort(sorted.begin(), sorted.end(),
              [](OffloadReadObjects::const_pointer lhs,
                 OffloadReadObjects::const_pointer rhs) {
                  return lhs->first < rhs->first;
              });

    std::vector<OffloadReadChunk> chunks;
    for (const auto *object : sorted) {
        const uint64_t bytes = OffloadReadObjectBytes(object->second);
        if (chunks.empty() || (chunk_bytes != 0 && chunks.back().bytes > 0 &&
                               chunks.back().bytes + bytes > chunk_bytes)) {
            chunks.emplace_back();
        }
        chunks.back().objects.emplace(object->first, object->second);
        chunks.back().bytes += bytes;
    }
    return chunks;
}

// Whether a chunk's error is about that chunk's keys only: the owner could
// not find or read their data. The next chunk, with other keys, may well
// succeed, so the read goes on. Every other error stops the read: the RPC or
// the transfer failing (RPC_FAIL, TRANSFER_FAIL, ...), the owner out of
// staging space (BUFFER_OVERFLOW), the owner's staging lease ending before
// the transfer did (OBJECT_HAS_LEASE), the master lease expiring
// (LEASE_EXPIRED), and any error not listed here. Further chunks would most
// likely fail the same way, and they would add load to an owner that is
// already failing.
inline bool OffloadReadErrorIsKeyLocal(ErrorCode error) {
    switch (error) {
        case ErrorCode::INVALID_KEY:  // not in the owner's storage index
        case ErrorCode::OBJECT_NOT_FOUND:
        case ErrorCode::BUCKET_NOT_FOUND:
        case ErrorCode::FILE_NOT_FOUND:
        case ErrorCode::FILE_OPEN_FAIL:
        case ErrorCode::FILE_READ_FAIL:
            return true;
        default:
            return false;
    }
}

// Reads one chunk: (owner endpoint, the chunk's objects) -> ok or error.
using OffloadChunkReader = std::function<tl::expected<void, ErrorCode>(
    const std::string &, OffloadReadObjects &)>;

// Called for each key of a chunk right after the chunk was read, on the
// thread that read it (so it must be thread-safe): OK if the key's data can be
// used, or the error to fail the key with. RealClient rechecks the key's
// master lease here and returns LEASE_EXPIRED once it has run out.
using OffloadKeyCheck = std::function<ErrorCode(const std::string &)>;

// Starts a helper thread running its argument. The default is std::thread,
// which throws std::system_error when no thread can be started (e.g. EAGAIN
// at the process's thread limit). Injectable so tests can make it fail.
using OffloadThreadSpawner = std::function<std::thread(std::function<void()>)>;

// The outcome of one chunk of a pipelined read.
struct OffloadChunkResult {
    // False for a chunk that was not sent because the read had stopped.
    bool started = false;
    // The read's error; OK if the chunk was read.
    ErrorCode error = ErrorCode::OK;
    // Keys of a chunk that was read but failed the key check, with the
    // check's error.
    std::vector<std::pair<std::string, ErrorCode>> failed_keys;

    bool failed() const {
        return error != ErrorCode::OK || !failed_keys.empty();
    }

    // The error that stops the read, OK if none: the read's error unless it
    // is key-local, else the first key-check error that is not key-local.
    ErrorCode stop_error() const {
        if (error != ErrorCode::OK) {
            return OffloadReadErrorIsKeyLocal(error) ? ErrorCode::OK : error;
        }
        for (const auto &failed_key : failed_keys) {
            if (!OffloadReadErrorIsKeyLocal(failed_key.second)) {
                return failed_key.second;
            }
        }
        return ErrorCode::OK;
    }
};

struct OffloadReadPipelineStats {
    size_t chunks = 0;
    size_t started = 0;  // chunks whose read began; the rest were not sent
    size_t failed = 0;   // started chunks that failed (read or key check)
    size_t threads = 0;  // threads that read chunks, the calling thread too
};

// Reads one chunk, then checks its keys; a reader or a check that throws
// fails the chunk with INTERNAL_ERROR.
inline OffloadChunkResult ReadOffloadChunk(const OffloadChunkReader &read_chunk,
                                           const OffloadKeyCheck &check_key,
                                           const std::string &endpoint,
                                           OffloadReadChunk &chunk) {
    OffloadChunkResult result;
    result.started = true;
    try {
        auto read = read_chunk(endpoint, chunk.objects);
        if (!read) {
            result.error = read.error();
            return result;
        }
        if (check_key) {
            for (const auto &object : chunk.objects) {
                const ErrorCode error = check_key(object.first);
                if (error != ErrorCode::OK) {
                    result.failed_keys.emplace_back(object.first, error);
                }
            }
        }
        return result;
    } catch (const std::exception &e) {
        LOG(ERROR) << "Offload read chunk from " << endpoint
                   << " threw: " << e.what();
    } catch (...) {
        LOG(ERROR) << "Offload read chunk from " << endpoint
                   << " threw a non-std exception";
    }
    result.error = ErrorCode::INTERNAL_ERROR;
    result.failed_keys.clear();
    return result;
}

// Reads `chunks` of one owner on up to `concurrency` threads: the calling
// thread and at most concurrency - 1 helper threads, each taking the next
// unread chunk in index order as soon as its previous one is done, so a slow
// chunk holds up only its own thread. Returns one result per chunk.
//
// Once a chunk has failed with an error that stops the read
// (OffloadChunkResult::stop_error), no further chunk is started; the chunks
// already in flight are still read and waited for (each releases its owner
// staging when its own read returns, and none may outlive `chunks`).
// check_key, if set, runs on every key of each chunk that was read.
//
// If a helper thread cannot be started, the read goes on with the threads
// that did start (at least the calling thread): fewer chunks in flight, same
// result. Failing the read would turn a transient thread shortage into a
// failed get, although a chunk read needs no thread of its own.
inline std::vector<OffloadChunkResult> RunOffloadReadPipeline(
    const std::string &endpoint, std::vector<OffloadReadChunk> &chunks,
    size_t concurrency, const OffloadChunkReader &read_chunk,
    const OffloadKeyCheck &check_key = nullptr,
    OffloadReadPipelineStats *stats = nullptr,
    const OffloadThreadSpawner &spawn = nullptr) {
    std::vector<OffloadChunkResult> results(chunks.size());
    if (stats) *stats = OffloadReadPipelineStats{.chunks = chunks.size()};
    if (chunks.empty()) return results;
    const size_t workers = std::clamp<size_t>(concurrency, 1, chunks.size());

    // Each chunk index is taken by exactly one thread, which alone writes
    // that chunk's result; the caller reads the results after joining every
    // helper.
    std::atomic<size_t> next{0};
    std::atomic<bool> stopped{false};
    auto work = [&]() {
        for (;;) {
            const size_t index = next.fetch_add(1, std::memory_order_relaxed);
            if (index >= chunks.size() ||
                stopped.load(std::memory_order_relaxed)) {
                return;
            }
            auto &result = results[index];
            result = ReadOffloadChunk(read_chunk, check_key, endpoint,
                                      chunks[index]);
            VLOG(1) << "Offload read chunk " << index << " of " << chunks.size()
                    << " from " << endpoint << ": "
                    << chunks[index].objects.size() << " keys, "
                    << chunks[index].bytes << " bytes, " << result.error << ", "
                    << result.failed_keys.size() << " keys failed the check";
            if (result.stop_error() != ErrorCode::OK) {
                stopped.store(true, std::memory_order_relaxed);
            }
        }
    };

    std::vector<std::thread> helpers;
    helpers.reserve(workers - 1);  // push_back below never reallocates
    auto join_helpers = [&helpers]() {
        for (auto &helper : helpers) {
            if (helper.joinable()) helper.join();
        }
    };
    try {
        for (size_t i = 1; i < workers; ++i) {
            try {
                helpers.push_back(spawn ? spawn(work) : std::thread(work));
            } catch (const std::exception &e) {
                LOG(WARNING)
                    << "Offload read from " << endpoint
                    << ": could not start a helper thread (" << e.what()
                    << "); reading with " << helpers.size() + 1 << " of "
                    << workers << " threads";
                break;
            }
        }
        work();
    } catch (...) {
        // Nothing above is expected to throw past the handlers, but the
        // helpers refer to this frame and must not outlive it.
        join_helpers();
        throw;
    }
    join_helpers();

    if (stats) {
        stats->threads = helpers.size() + 1;
        for (const auto &result : results) {
            stats->started += result.started ? 1 : 0;
            stats->failed += result.started && result.failed() ? 1 : 0;
        }
    }
    return results;
}

// "BUFFER_OVERFLOW: 3, LEASE_EXPIRED: 2": how many keys failed with each
// error, by error name.
inline std::string FormatOffloadReadErrorCounts(
    const std::unordered_map<std::string, ErrorCode> &failed_keys) {
    std::map<std::string, size_t> counts;
    for (const auto &failed_key : failed_keys) {
        ++counts[toString(failed_key.second)];
    }
    std::ostringstream out;
    bool first = true;
    for (const auto &[error, count] : counts) {
        out << (first ? "" : ", ") << error << ": " << count;
        first = false;
    }
    return out.str();
}

struct OffloadReadResult {
    // Keys that were not read, or were read but failed the key check, with
    // their error; empty when every key was read.
    std::unordered_map<std::string, ErrorCode> failed_keys;
    // The read was split into chunks and check_key ran on every key that was
    // read, right after its chunk.
    bool keys_checked = false;
};

// Reads one owner's `objects`, split into chunks of at most chunk_bytes with
// at most `concurrency` in flight (see the top of this file). check_key runs
// on each key of a chunk right after the chunk was read.
//
// chunk_bytes == 0, a single object, or a read that fits in one chunk calls
// read_chunk once with all of `objects` on the calling thread, exactly as
// before chunking: on error every key fails with it, and check_key is not
// called (keys_checked stays false).
inline OffloadReadResult ReadOffloadObjectsInChunks(
    const std::string &endpoint, OffloadReadObjects &objects,
    uint64_t chunk_bytes, size_t concurrency,
    const OffloadChunkReader &read_chunk,
    const OffloadKeyCheck &check_key = nullptr,
    OffloadReadPipelineStats *stats = nullptr,
    const OffloadThreadSpawner &spawn = nullptr) {
    OffloadReadResult read;
    auto fail_all = [&read](const OffloadReadObjects &keys, ErrorCode error) {
        for (const auto &object : keys) {
            read.failed_keys.emplace(object.first, error);
        }
    };

    if (!OffloadReadIsSplit(objects, chunk_bytes)) {
        auto result = read_chunk(endpoint, objects);
        if (!result) fail_all(objects, result.error());
        if (stats) {
            *stats = OffloadReadPipelineStats{.chunks = 1,
                                              .started = 1,
                                              .failed = result ? 0u : 1u,
                                              .threads = 1};
        }
        return read;
    }

    const auto start = std::chrono::steady_clock::now();
    auto chunks = SplitOffloadReadChunks(objects, chunk_bytes);
    OffloadReadPipelineStats local_stats;
    const auto results =
        RunOffloadReadPipeline(endpoint, chunks, concurrency, read_chunk,
                               check_key, &local_stats, spawn);
    read.keys_checked = static_cast<bool>(check_key);

    // Unsent chunks fail with the stop error of the lowest-index chunk that
    // had one. Only such an error stops the read, so one exists whenever a
    // chunk was not started.
    ErrorCode stop_error = ErrorCode::INTERNAL_ERROR;
    for (const auto &result : results) {
        if (result.started && result.stop_error() != ErrorCode::OK) {
            stop_error = result.stop_error();
            break;
        }
    }
    uint64_t total_bytes = 0;
    for (size_t i = 0; i < chunks.size(); ++i) {
        total_bytes += chunks[i].bytes;
        const auto &result = results[i];
        if (!result.started) {
            fail_all(chunks[i].objects, stop_error);
        } else if (result.error != ErrorCode::OK) {
            fail_all(chunks[i].objects, result.error);
        } else {
            for (const auto &[key, error] : result.failed_keys) {
                read.failed_keys.emplace(key, error);
            }
        }
    }

    // One line per owner read; the chunks log at VLOG(1).
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - start)
            .count();
    std::ostringstream summary;
    summary << "Offload read from " << endpoint << ": " << objects.size()
            << " keys, " << total_bytes << " bytes in " << chunks.size()
            << " chunks (cap " << chunk_bytes << " bytes) on "
            << local_stats.threads << " threads, " << elapsed_ms << " ms";
    if (read.failed_keys.empty()) {
        LOG(INFO) << summary.str();
    } else {
        LOG(WARNING) << summary.str() << "; " << local_stats.failed << " of "
                     << local_stats.started << " sent chunks failed, "
                     << local_stats.chunks - local_stats.started
                     << " not sent; " << read.failed_keys.size() << " of "
                     << objects.size() << " keys failed ("
                     << FormatOffloadReadErrorCounts(read.failed_keys) << ")";
    }
    if (stats) *stats = local_stats;
    return read;
}

}  // namespace mooncake
