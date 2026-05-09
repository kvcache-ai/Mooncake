// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// First-submit latency probe for EFA transport.
//
// transfer_engine_bench reports a 10-second throughput average, which
// hides the two costs that matter for first-request latency: the
// handshake / fi_av_insert that fires on the first send to each
// (local_NIC, peer_NIC) pair, and the optional warmupSegment() that
// pre-pays that cost outside the critical path.  This probe times
// those two things separately so the cost of each is directly visible.
//
// Reports:
//   - warmup:             time for EfaTransport::warmupSegment() (if enabled)
//   - submit #0..#N-1:    time for a single 1 MB submitTransfer + poll
//
// The first submit carries the handshake cost when warmup is off; the
// rest are steady-state.  It's a two-host tool (not hooked into ctest)
// used to:
//   (a) decide whether to call warmupSegment() in your application —
//       does it actually remove the stall?
//   (b) compare this PR's shared-endpoint cost against upstream
//       per-peer-endpoint cost (see the numbers in docs/.../efa_transport.md
//       under "First-request latency").
//
// Usage:
//   Target:     ./efa_first_submit_probe --mode=target ...
//   Initiator:  ./efa_first_submit_probe --mode=initiator \
//                   --segment_id=<target_host>:<port> \
//                   --warmup=1 --iters=5

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <string>

#include "transfer_engine.h"
#include "transport/efa_transport/efa_transport.h"
#include "transport/transport.h"

using namespace mooncake;

DEFINE_string(mode, "initiator", "initiator or target");
DEFINE_string(metadata_server, "P2PHANDSHAKE", "metadata backend");
DEFINE_string(local_server_name, "",
              "local server name (host:port); default = hostname:12345");
DEFINE_string(
    segment_id, "",
    "target <host>:<port> from target's startup log (initiator only)");
DEFINE_bool(warmup, true, "call warmupSegment before first submit");
DEFINE_int32(iters, 5, "number of timed submits after warmup");
DEFINE_uint64(xfer_size, 1 << 20, "bytes per submit (default 1 MB)");
DEFINE_uint64(buffer_size, 1ULL << 30, "registered buffer size (default 1 GB)");

static std::string defaultLocalServerName() {
    char host[256];
    if (gethostname(host, sizeof(host)) != 0) return "127.0.0.1:12345";
    return std::string(host) + ":12345";
}

static double toMs(std::chrono::steady_clock::duration d) {
    return std::chrono::duration<double, std::milli>(d).count();
}

static int runTarget() {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->getLocalTopology()->discover({});

    std::string name = FLAGS_local_server_name.empty()
                           ? defaultLocalServerName()
                           : FLAGS_local_server_name;
    auto hp = parseHostNameWithPort(name);
    int rc =
        engine->init(FLAGS_metadata_server, name, hp.first.c_str(), hp.second);
    LOG_ASSERT(!rc) << "engine init failed";

    Transport *xport = engine->installTransport("efa", nullptr);
    LOG_ASSERT(xport) << "installTransport(efa) failed";

    void *buf = numa_alloc_onnode(FLAGS_buffer_size, 0);
    LOG_ASSERT(buf) << "numa_alloc_onnode failed";

    rc = engine->registerLocalMemory(buf, FLAGS_buffer_size, "cpu:0");
    LOG_ASSERT(!rc) << "registerLocalMemory failed";

    LOG(INFO) << "[target] ready, addr=" << engine->getLocalIpAndPort();
    LOG(INFO) << "[target] Ctrl-C to stop";

    pause();  // block until SIGINT

    engine->unregisterLocalMemory(buf);
    numa_free(buf, FLAGS_buffer_size);
    return 0;
}

static int runInitiator() {
    if (FLAGS_segment_id.empty()) {
        LOG(ERROR) << "--segment_id is required for initiator";
        return 1;
    }

    auto engine = std::make_unique<TransferEngine>(false);
    engine->getLocalTopology()->discover({});

    std::string name = FLAGS_local_server_name.empty()
                           ? defaultLocalServerName()
                           : FLAGS_local_server_name;
    auto hp = parseHostNameWithPort(name);
    int rc =
        engine->init(FLAGS_metadata_server, name, hp.first.c_str(), hp.second);
    LOG_ASSERT(!rc) << "engine init failed";

    Transport *xport = engine->installTransport("efa", nullptr);
    LOG_ASSERT(xport) << "installTransport(efa) failed";

    void *buf = numa_alloc_onnode(FLAGS_buffer_size, 0);
    LOG_ASSERT(buf) << "numa_alloc_onnode failed";
    memset(buf, 0xAB, FLAGS_xfer_size);

    rc = engine->registerLocalMemory(buf, FLAGS_buffer_size, "cpu:0");
    LOG_ASSERT(!rc) << "registerLocalMemory failed";

    auto segment_id = engine->openSegment(FLAGS_segment_id);
    LOG_ASSERT(segment_id != (SegmentID)-1) << "openSegment failed";

    auto seg_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    LOG_ASSERT(seg_desc) << "getSegmentDescByID failed";
    uint64_t remote_base = (uint64_t)seg_desc->buffers[0].addr;

    LOG(INFO) << "[initiator] peer=" << FLAGS_segment_id
              << " warmup=" << (FLAGS_warmup ? "ON" : "OFF")
              << " iters=" << FLAGS_iters << " xfer_size=" << FLAGS_xfer_size;

    if (FLAGS_warmup) {
        auto *efa = dynamic_cast<EfaTransport *>(xport);
        LOG_ASSERT(efa) << "transport is not EfaTransport";
        auto t0 = std::chrono::steady_clock::now();
        int wrc = efa->warmupSegment(FLAGS_segment_id);
        auto elapsed = std::chrono::steady_clock::now() - t0;
        LOG(INFO) << "warmup: " << toMs(elapsed) << " ms (rc=" << wrc << ")";
    }

    for (int i = 0; i < FLAGS_iters; ++i) {
        auto t0 = std::chrono::steady_clock::now();

        auto batch_id = engine->allocateBatchID(1);
        TransferRequest req;
        req.opcode = TransferRequest::WRITE;
        req.length = FLAGS_xfer_size;
        req.source = (uint8_t *)buf;
        req.target_id = segment_id;
        req.target_offset = remote_base;

        Status s = engine->submitTransfer(batch_id, {req});
        LOG_ASSERT(s.ok()) << "submitTransfer #" << i
                           << " failed: " << s.ToString();

        TransferStatus status;
        for (int poll = 0; poll < 20'000'000; ++poll) {
            s = engine->getTransferStatus(batch_id, 0, status);
            LOG_ASSERT(s.ok());
            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED)
                break;
        }
        LOG_ASSERT(status.s == TransferStatusEnum::COMPLETED)
            << "submit #" << i << " did not complete";

        engine->freeBatchID(batch_id);

        auto elapsed = std::chrono::steady_clock::now() - t0;
        LOG(INFO) << "submit #" << i << ": " << toMs(elapsed) << " ms";
    }

    engine->unregisterLocalMemory(buf);
    numa_free(buf, FLAGS_buffer_size);
    return 0;
}

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    if (FLAGS_mode == "target") return runTarget();
    if (FLAGS_mode == "initiator") return runInitiator();

    LOG(ERROR) << "--mode must be 'target' or 'initiator'";
    return 1;
}
