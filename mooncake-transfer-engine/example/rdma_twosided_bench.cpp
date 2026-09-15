// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Sustained-load benchmark for the two-sided RDMA data path (SEND/RECV over
// TE-managed bounce buffers). Two TransferEngine instances run in one process
// over the same NIC, so a single command reproduces a concurrent workload whose
// in-flight chunk count far exceeds the bounce pool, which is what exercises
// slot backpressure and mid-transfer resume.
//
// Each thread keeps --queue_depth batches in flight instead of submitting one
// batch and waiting for it, and the run is time-bounded so throughput is
// reported as a steady-state rate with per-interval samples rather than a
// single burst. Every batch carries (thread, slot, iteration) stamps every 4
// KiB that are checked on completion, so a chunk that is dropped, reordered or
// replayed is caught while the load is running.
//
// With the defaults (16 threads x 4 depth x 4 requests x 1 MiB, 64 KiB slots)
// about 4096 chunks are in flight against a pool of 64..256 slots.
//
// Note the two engines talk over one local NIC, so the reported bandwidth is a
// NIC loopback figure and is not a cross-node line-rate measurement.
//
// Usage:
//   ./rdma_twosided_bench [--threads=16] [--queue_depth=4] [--batch_size=4]
//                         [--block_size=1048576] [--duration_s=30]
//                         [--warmup_s=3] [--report_interval_s=5] [--iters=0]
//                         [--opcode=write|read] [--verify] [--device=mlx5_0]

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "config.h"
#include "transfer_engine.h"
#include "transport/rdma_twosided/rdma_twosided_transport.h"

DEFINE_uint32(threads, 16, "Concurrent submitting threads");
DEFINE_uint32(queue_depth, 4, "Batches kept in flight per thread");
DEFINE_uint32(batch_size, 1, "Requests per batch");
// The defaults keep threads*queue_depth*batch_size chunks in flight, which is
// the peer's bounce pool size (MC_RDMA_MSG_POOL_BASE). Past that the transfers
// queue for bounce slots and throughput drops by an order of magnitude, so
// raise the pool along with the load when driving more.
DEFINE_uint64(block_size, 64 * 1024, "Bytes per request");
DEFINE_uint32(duration_s, 30, "Steady-state duration, after warmup");
DEFINE_uint32(warmup_s, 3, "Warmup seconds excluded from the statistics");
DEFINE_uint32(report_interval_s, 5, "Progress report period, 0 disables");
DEFINE_uint32(iters, 0, "Batches per thread instead of a timed run");
DEFINE_string(opcode, "write", "write | read");
DEFINE_bool(verify, true, "Check payload stamps on every batch");
DEFINE_string(device, "", "RDMA device name (default: first usable)");
DEFINE_uint32(timeout_ms, 60000, "Per-batch completion timeout");
DEFINE_uint32(drain_timeout_ms, 5000,
              "Completion budget for batches still in flight when the "
              "measured window ends");

using namespace mooncake;
using Clock = std::chrono::steady_clock;

namespace {

bool deviceUsable(ibv_device *device) {
    ibv_context *ctx = ibv_open_device(device);
    if (!ctx) return false;
    ibv_device_attr attr{};
    if (ibv_query_device(ctx, &attr) != 0) {
        ibv_close_device(ctx);
        return false;
    }
    bool ok = false;
    for (uint8_t port = 1; port <= attr.phys_port_cnt; ++port) {
        ibv_port_attr port_attr{};
        if (ibv_query_port(ctx, port, &port_attr) != 0) continue;
        if (port_attr.gid_tbl_len > 0 && port_attr.state == IBV_PORT_ACTIVE) {
            ok = true;
            break;
        }
    }
    ibv_close_device(ctx);
    return ok;
}

std::string pickDevice() {
    if (!FLAGS_device.empty()) return FLAGS_device;
    int num_devices = 0;
    ibv_device **list = ibv_get_device_list(&num_devices);
    if (!list || num_devices == 0) {
        if (list) ibv_free_device_list(list);
        return "";
    }
    std::string name;
    for (int i = 0; i < num_devices; ++i) {
        if (deviceUsable(list[i])) {
            name = ibv_get_device_name(list[i]);
            break;
        }
    }
    ibv_free_device_list(list);
    return name;
}

// Written every kStampStride bytes of a request so a completed batch can be
// attributed to the exact submission that produced it. The bytes between two
// stamps stay at kFiller, which lets a completion be checked end to end without
// keeping a shadow copy of the payload.
struct Stamp {
    uint32_t tid;
    uint32_t slot;
    uint64_t iter;
    uint64_t off;
};
constexpr size_t kStampStride = 4096;
constexpr char kFiller = 0x5a;

void stampRegion(char *base, size_t len, uint32_t tid, uint32_t slot,
                 uint64_t iter) {
    for (size_t off = 0; off + sizeof(Stamp) <= len; off += kStampStride) {
        Stamp s{tid, slot, iter, off};
        std::memcpy(base + off, &s, sizeof(s));
    }
}

bool checkRegion(const char *base, size_t len, uint32_t tid, uint32_t slot,
                 uint64_t iter) {
    static const std::vector<char> filler(kStampStride, kFiller);
    for (size_t off = 0; off < len; off += kStampStride) {
        const size_t span = std::min(kStampStride, len - off);
        size_t body = 0;
        if (span >= sizeof(Stamp)) {
            Stamp s{};
            std::memcpy(&s, base + off, sizeof(s));
            if (s.tid != tid || s.slot != slot || s.iter != iter ||
                s.off != off)
                return false;
            body = sizeof(Stamp);
        }
        if (std::memcmp(base + off + body, filler.data(), span - body) != 0)
            return false;
    }
    return true;
}

struct ThreadStats {
    std::vector<double> lat_us;
    uint64_t batches = 0;
    uint64_t failures = 0;
    uint64_t mismatches = 0;
    uint64_t drain_failures = 0;
    std::atomic<uint64_t> live_bytes{0};
    std::atomic<uint64_t> live_batches{0};
    std::atomic<uint64_t> live_failures{0};
};

double percentile(const std::vector<double> &sorted, double p) {
    if (sorted.empty()) return 0.0;
    size_t idx = static_cast<size_t>(p * (sorted.size() - 1));
    return sorted[idx];
}

size_t rssBytes() {
    FILE *f = std::fopen("/proc/self/statm", "r");
    if (!f) return 0;
    long total = 0, resident = 0;
    if (std::fscanf(f, "%ld %ld", &total, &resident) != 2) resident = 0;
    std::fclose(f);
    return static_cast<size_t>(resident) *
           static_cast<size_t>(sysconf(_SC_PAGESIZE));
}

}  // namespace

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    const bool is_read = FLAGS_opcode == "read";
    if (!is_read && FLAGS_opcode != "write") {
        LOG(ERROR) << "--opcode must be write or read";
        return 1;
    }
    if (!FLAGS_threads || !FLAGS_queue_depth || !FLAGS_batch_size ||
        FLAGS_block_size < sizeof(Stamp)) {
        LOG(ERROR) << "threads/queue_depth/batch_size must be non-zero and "
                      "block_size must hold a stamp";
        return 1;
    }
    const bool timed = FLAGS_iters == 0;
    if (timed && !FLAGS_duration_s) {
        LOG(ERROR) << "--duration_s must be non-zero unless --iters is set";
        return 1;
    }

    std::string device = pickDevice();
    if (device.empty()) {
        LOG(ERROR) << "no usable RDMA device";
        return 1;
    }
    setenv("MC_TE_FILTERS", device.c_str(), 1);
    setenv("MC_USE_RDMA_TWOSIDED", "1", 1);
    setenv("MC_RDMA_MSG_ENABLED", "1", 1);
    loadGlobalConfig(globalConfig());

    std::vector<std::string> filter{device};
    auto sender = std::make_unique<TransferEngine>(true, filter);
    auto receiver = std::make_unique<TransferEngine>(true, filter);
    if (sender->init("P2PHANDSHAKE", "127.0.0.1:0") ||
        receiver->init("P2PHANDSHAKE", "127.0.0.1:0")) {
        LOG(ERROR) << "TransferEngine init failed";
        return 1;
    }

    auto *sender_ts = dynamic_cast<RdmaTwoSidedTransport *>(
        sender->getTransport("rdma_twosided"));
    auto *receiver_ts = dynamic_cast<RdmaTwoSidedTransport *>(
        receiver->getTransport("rdma_twosided"));
    if (!sender_ts || !receiver_ts) {
        LOG(ERROR) << "rdma_twosided transport not installed";
        return 1;
    }

    // One contiguous managed buffer per side, sliced per (thread, queue slot)
    // so in-flight batches never share a destination range.
    const size_t slice = FLAGS_block_size * FLAGS_batch_size;
    const size_t slices = static_cast<size_t>(FLAGS_threads) *
                          static_cast<size_t>(FLAGS_queue_depth);
    const size_t total = slice * slices;
    char *local = static_cast<char *>(sender_ts->allocateManagedBuffer(total));
    char *remote =
        static_cast<char *>(receiver_ts->allocateManagedBuffer(total));
    if (!local || !remote) {
        LOG(ERROR) << "managed buffer allocation failed, total=" << total;
        return 1;
    }
    char *src_side = is_read ? remote : local;
    char *dst_side = is_read ? local : remote;
    std::memset(src_side, kFiller, total);
    std::memset(dst_side, 0, total);

    auto segment = sender->openSegment(receiver->getLocalIpAndPort());
    if (!segment) {
        LOG(ERROR) << "openSegment failed";
        return 1;
    }

    std::vector<std::unique_ptr<ThreadStats>> stats;
    for (uint32_t i = 0; i < FLAGS_threads; ++i)
        stats.emplace_back(std::make_unique<ThreadStats>());
    std::atomic<bool> running{true};
    std::atomic<bool> measuring{!timed || FLAGS_warmup_s == 0};

    auto worker = [&](uint32_t tid) {
        auto &st = *stats[tid];
        st.lat_us.reserve(1 << 16);
        struct Slot {
            BatchID batch = 0;
            bool active = false;
            uint64_t iter = 0;
            Clock::time_point start;
            Clock::time_point deadline;
        };
        std::vector<Slot> slots(FLAGS_queue_depth);
        uint64_t next_iter = 0;
        uint64_t done = 0;
        Clock::time_point drain_start{};
        const auto drain_budget =
            std::chrono::milliseconds(FLAGS_drain_timeout_ms);

        auto submit = [&](uint32_t s) -> bool {
            const size_t base =
                slice * (static_cast<size_t>(tid) * FLAGS_queue_depth + s);
            auto &slot = slots[s];
            slot.iter = next_iter++;
            for (uint32_t i = 0; i < FLAGS_batch_size; ++i)
                stampRegion(src_side + base + FLAGS_block_size * i,
                            FLAGS_block_size, tid, s, slot.iter);
            std::vector<TransferRequest> requests(FLAGS_batch_size);
            for (uint32_t i = 0; i < FLAGS_batch_size; ++i) {
                const size_t off = base + FLAGS_block_size * i;
                requests[i].opcode =
                    is_read ? TransferRequest::READ : TransferRequest::WRITE;
                requests[i].source = local + off;
                requests[i].target_id = segment;
                requests[i].target_offset =
                    reinterpret_cast<uint64_t>(remote + off);
                requests[i].length = FLAGS_block_size;
            }
            slot.batch = sender->allocateBatchID(FLAGS_batch_size);
            slot.start = Clock::now();
            slot.deadline =
                slot.start + std::chrono::milliseconds(FLAGS_timeout_ms);
            if (!sender->submitTransfer(slot.batch, requests).ok()) {
                sender->freeBatchID(slot.batch);
                slot.active = false;
                return false;
            }
            slot.active = true;
            return true;
        };

        auto retire = [&](uint32_t s, bool ok) {
            auto &slot = slots[s];
            const size_t base =
                slice * (static_cast<size_t>(tid) * FLAGS_queue_depth + s);
            double us = std::chrono::duration<double, std::micro>(Clock::now() -
                                                                  slot.start)
                            .count();
            sender->freeBatchID(slot.batch);
            slot.active = false;
            const bool record = measuring.load(std::memory_order_relaxed);
            if (!ok) {
                if (record)
                    ++st.failures;
                else if (!running.load(std::memory_order_relaxed))
                    ++st.drain_failures;
                st.live_failures.fetch_add(1, std::memory_order_relaxed);
                return;
            }
            if (FLAGS_verify) {
                for (uint32_t i = 0; i < FLAGS_batch_size; ++i) {
                    if (!checkRegion(dst_side + base + FLAGS_block_size * i,
                                     FLAGS_block_size, tid, s, slot.iter)) {
                        if (record) ++st.mismatches;
                        break;
                    }
                }
            }
            if (record) {
                st.lat_us.push_back(us);
                ++st.batches;
            }
            st.live_bytes.fetch_add(slice, std::memory_order_relaxed);
            st.live_batches.fetch_add(1, std::memory_order_relaxed);
            ++done;
        };

        auto want_more = [&]() {
            if (!running.load(std::memory_order_relaxed)) return false;
            return timed || next_iter < FLAGS_iters;
        };

        for (uint32_t s = 0; s < FLAGS_queue_depth && want_more(); ++s) {
            if (!submit(s)) {
                st.live_failures.fetch_add(1, std::memory_order_relaxed);
            }
        }

        while (true) {
            const bool live = running.load(std::memory_order_relaxed);
            if (!live && drain_start.time_since_epoch().count() == 0)
                drain_start = Clock::now();
            bool progressed = false;
            bool any_active = false;
            for (uint32_t s = 0; s < FLAGS_queue_depth; ++s) {
                auto &slot = slots[s];
                if (!slot.active) {
                    if (want_more() && submit(s)) progressed = true;
                    if (slot.active) any_active = true;
                    continue;
                }
                any_active = true;
                TransferStatus status;
                if (!sender->getBatchTransferStatus(slot.batch, status).ok()) {
                    retire(s, false);
                    progressed = true;
                    continue;
                }
                auto now = Clock::now();
                if (status.s == TransferStatusEnum::COMPLETED) {
                    retire(s, true);
                    progressed = true;
                } else if (status.s == TransferStatusEnum::FAILED ||
                           now > slot.deadline ||
                           (!live && now > drain_start + drain_budget)) {
                    retire(s, false);
                    progressed = true;
                }
            }
            if (!any_active && !want_more()) break;
            if (!progressed) std::this_thread::yield();
        }
        (void)done;
    };

    std::vector<std::thread> workers;
    auto wall_start = Clock::now();
    for (uint32_t tid = 0; tid < FLAGS_threads; ++tid)
        workers.emplace_back(worker, tid);

    auto snapshot = [&](uint64_t &bytes, uint64_t &batches, uint64_t &fails) {
        bytes = batches = fails = 0;
        for (auto &st : stats) {
            bytes += st->live_bytes.load(std::memory_order_relaxed);
            batches += st->live_batches.load(std::memory_order_relaxed);
            fails += st->live_failures.load(std::memory_order_relaxed);
        }
    };

    size_t rss_peak = rssBytes();
    uint64_t last_bytes = 0, last_batches = 0, last_fails = 0;
    uint64_t last_resumes = sender_ts->twoSidedResumeCount();
    auto tick = Clock::now();
    if (timed && FLAGS_warmup_s) {
        std::this_thread::sleep_for(std::chrono::seconds(FLAGS_warmup_s));
        snapshot(last_bytes, last_batches, last_fails);
        last_resumes = sender_ts->twoSidedResumeCount();
        tick = Clock::now();
        measuring.store(true, std::memory_order_relaxed);
        LOG(INFO) << "warmup done after " << FLAGS_warmup_s
                  << "s, measuring for " << FLAGS_duration_s << "s";
    }

    auto measure_start = Clock::now();
    auto measure_end = measure_start + std::chrono::seconds(FLAGS_duration_s);
    while (timed && Clock::now() < measure_end) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        rss_peak = std::max(rss_peak, rssBytes());
        if (!FLAGS_report_interval_s) continue;
        auto now = Clock::now();
        double dt = std::chrono::duration<double>(now - tick).count();
        if (dt < FLAGS_report_interval_s) continue;
        uint64_t bytes = 0, batches = 0, fails = 0;
        snapshot(bytes, batches, fails);
        uint64_t resumes = sender_ts->twoSidedResumeCount();
        LOG(INFO) << "interval t=+"
                  << std::chrono::duration<double>(now - measure_start).count()
                  << "s GB/s=" << (bytes - last_bytes) / 1e9 / dt
                  << " batch_IOPS=" << (batches - last_batches) / dt
                  << " failures=" << (fails - last_fails)
                  << " resumes=" << (resumes - last_resumes)
                  << " rss_MB=" << rssBytes() / (1024 * 1024);
        last_bytes = bytes;
        last_batches = batches;
        last_fails = fails;
        last_resumes = resumes;
        tick = now;
    }
    // The measured window closes before the in-flight batches are drained, so
    // the drain does not dilute the reported rate.
    Clock::time_point measure_stop;
    if (timed) {
        measure_stop = Clock::now();
        measuring.store(false, std::memory_order_relaxed);
        running.store(false, std::memory_order_relaxed);
    }
    for (auto &t : workers) t.join();
    if (!timed) measure_stop = Clock::now();
    double measured_s =
        std::chrono::duration<double>(measure_stop - measure_start).count();
    double wall_s =
        std::chrono::duration<double>(Clock::now() - wall_start).count();
    rss_peak = std::max(rss_peak, rssBytes());

    std::vector<double> all_us;
    uint64_t batches = 0, failures = 0, mismatches = 0, drain_failures = 0;
    for (auto &st : stats) {
        all_us.insert(all_us.end(), st->lat_us.begin(), st->lat_us.end());
        batches += st->batches;
        failures += st->failures;
        mismatches += st->mismatches;
        drain_failures += st->drain_failures;
    }
    std::sort(all_us.begin(), all_us.end());
    const uint64_t measured_bytes = batches * slice;

    const size_t chunk_payload =
        globalConfig().rdma_msg_slot_size - kMsgHeaderSize;
    const uint64_t chunks_per_request =
        (FLAGS_block_size + chunk_payload - 1) / chunk_payload;
    LOG(INFO) << "two-sided " << FLAGS_opcode << " device=" << device
              << " threads=" << FLAGS_threads
              << " queue_depth=" << FLAGS_queue_depth
              << " batch_size=" << FLAGS_batch_size
              << " block_size=" << FLAGS_block_size << " inflight_requests="
              << FLAGS_threads * FLAGS_queue_depth * FLAGS_batch_size
              << " inflight_chunks="
              << FLAGS_threads * FLAGS_queue_depth * FLAGS_batch_size *
                     chunks_per_request
              << " pool_base=" << globalConfig().rdma_msg_pool_base
              << " pool_max=" << globalConfig().rdma_msg_pool_max
              << " buffer_MB_per_side=" << total / (1024 * 1024);
    LOG(INFO) << "steady state batches=" << batches << " failures=" << failures
              << " stamp_mismatches=" << mismatches
              << " GB=" << measured_bytes / 1e9 << " seconds=" << measured_s
              << " GB/s="
              << (measured_s > 0 ? measured_bytes / 1e9 / measured_s : 0.0)
              << " batch_IOPS=" << (measured_s > 0 ? batches / measured_s : 0.0)
              << " wall_s=" << wall_s;
    LOG(INFO) << "batch latency us p50=" << percentile(all_us, 0.50)
              << " p99=" << percentile(all_us, 0.99)
              << " p999=" << percentile(all_us, 0.999)
              << " max=" << (all_us.empty() ? 0.0 : all_us.back());
    LOG(INFO) << "mid-transfer resumes after bounce-slot backpressure="
              << sender_ts->twoSidedResumeCount()
              << " rss_peak_MB=" << rss_peak / (1024 * 1024)
              << " drain_failures=" << drain_failures;
    LOG(INFO) << "note: both engines share one local NIC, so this is a "
                 "loopback rate, not a cross-node line rate";

    if (FLAGS_verify && !mismatches)
        LOG(INFO) << "payload stamps verified on every completed batch, "
                  << measured_bytes / 1e9 << " GB";

    sender_ts->releaseManagedBuffer(local);
    receiver_ts->releaseManagedBuffer(remote);
    return (failures || mismatches) ? 1 : 0;
}
