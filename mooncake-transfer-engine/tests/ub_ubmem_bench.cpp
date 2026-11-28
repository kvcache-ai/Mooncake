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

// Cross-node benchmark for Mooncake TE transports on the UB fabric:
//   ub    (kunpeng_transport, URMA DMA engine)     — DRAM pool, remote absolute
//   VA ubmem (ubmem_transport, nc mirror mmap memcpy) — shmdev window, window
//   offset
// Both sides are plain DRAM behind the same UB fabric; sync
// one-request-at-a-time submission on both paths so the numbers are comparable.
//
// target:    ./ub_ubmem_bench --mode=target --protocol=ub --device_name=udma1
//            ./ub_ubmem_bench --mode=target --protocol=ubmem
//            --ubmem_device=/dev/obmm_shmdev9
// initiator: ./ub_ubmem_bench --mode=initiator --protocol=ub
// --target_server=hw01 ...
//            (ubmem initiator sets MC_UBMEM_PEER_DEV_MAP in env)

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <numa.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <vector>

#include "transfer_engine.h"
#include "transport/transport.h"
#ifdef USE_UBMEM
#include "transport/ubmem_transport/ubmem_transport.h"
#endif
#ifdef USE_CXL
#include "transport/cxl_transport/cxl_transport.h"
#endif
#include "common.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "127.0.0.1:2379", "Metadata server connstring");
DEFINE_string(mode, "initiator", "target or initiator");
DEFINE_string(protocol, "ub", "ub (URMA DMA) or ubmem (nc mirror memcpy)");
DEFINE_string(device_name, "udma1", "[ub] urma device for nic priority matrix");
DEFINE_string(ubmem_device, "/dev/obmm_shmdev9", "[ubmem] pool shmdev path");
DEFINE_string(cxl_device, "/dev/obmm_shmdev9", "[cxl] shared window path");
DEFINE_string(target_server, "", "[initiator] peer segment name");
DEFINE_int64(rpc_port, 12001, "RPC port base for handshake daemon");
DEFINE_int64(verify_seconds, 600, "[target] hold time before pattern check");
DEFINE_string(sizes, "4096,65536,1048576,4194304,16777216",
              "Comma separated transfer sizes in bytes");
DEFINE_int64(max_iters, 200, "Iteration cap per (op, size)");
DEFINE_double(budget_secs, 2.0,
              "Stop a (op, size) run after this many seconds");
DEFINE_int64(pool_mb, 256, "[ub] DRAM pool MB");

// ubmem registered window sub-range: [R_OFF, R_OFF + R_LEN) inside the device
// window; bench transfers use target_offset = R_OFF.
static const uint64_t kUbmemOff = 4ull << 20;
static const uint64_t kUbmemLen = 48ull << 20;

struct Bench {
    std::unique_ptr<TransferEngine> engine;
    Transport* xport = nullptr;
    void* pool = nullptr;  // ub: numa DRAM; ubmem: malloc'd src/dst DRAM
    size_t pool_size = 0;
    uint8_t* window = nullptr;  // ubmem: local device window base
    uint64_t remote_base = 0;   // ub: peer absolute VA; ubmem: kUbmemOff
    Transport::SegmentID segment_id = 0;
};

static double nowSecs() {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

static void fillPattern(uint8_t* p, size_t len, uint64_t seed) {
    for (size_t i = 0; i < len; i++)
        p[i] = (uint8_t)((seed * 131 + i * 7) >> 3);
}

static int runOne(Bench& b, TransferRequest::OpCode op, uint64_t target_offset,
                  void* local, size_t len, double* out_secs) {
    // t0 before submit: ubmem completes the copy inline inside submitTransfer,
    // ub returns immediately and completes asynchronously — both paths must be
    // timed end to end or the sync transport measures only poll overhead.
    double t0 = nowSecs();
    TransferRequest entry;
    entry.opcode = op;
    entry.length = len;
    entry.source = (uint8_t*)local;
    entry.target_id = b.segment_id;
    entry.target_offset = target_offset;
    auto batch_id = b.engine->allocateBatchID(1);
    Status s = b.engine->submitTransfer(batch_id, {entry});
    if (!s.ok()) {
        LOG(ERROR) << "submit failed: " << s;
        b.engine->freeBatchID(batch_id);
        return -1;
    }
    TransferStatus st;
    for (;;) {
        Status gs = b.xport->getTransferStatus(batch_id, 0, st);
        if (!gs.ok()) {
            LOG(ERROR) << "getTransferStatus failed: " << gs;
            b.engine->freeBatchID(batch_id);
            return -1;
        }
        if (st.s == TransferStatusEnum::COMPLETED) break;
        if (st.s == TransferStatusEnum::FAILED) {
            LOG(ERROR) << "transfer FAILED";
            b.engine->freeBatchID(batch_id);
            return -1;
        }
    }
    *out_secs = nowSecs() - t0;
    b.engine->freeBatchID(batch_id);
    return 0;
}

static int benchLoop(Bench& b, const char* opname, TransferRequest::OpCode op) {
    std::vector<size_t> sizes;
    {
        std::string spec = FLAGS_sizes;
        size_t pos = 0;
        while (pos < spec.size()) {
            size_t comma = spec.find(',', pos);
            if (comma == std::string::npos) comma = spec.size();
            if (comma > pos)
                sizes.push_back(std::stoull(spec.substr(pos, comma - pos)));
            pos = comma + 1;
        }
    }
    // first half = working buffer, second half = verify scratch (both
    // registered for ub, plain DRAM for ubmem)
    uint8_t* work = (uint8_t*)b.pool;
    uint8_t* chk = (uint8_t*)b.pool + b.pool_size / 2;
    const size_t maxLen = b.pool_size / 4;
    for (size_t len : sizes) {
        if (len > maxLen) {
            printf("SKIP op=%s size=%zu (pool too small)\n", opname, len);
            continue;
        }
        if (op == TransferRequest::READ) {
            // verify round-trip before timing reads
            fillPattern(work, len, 42);
            double t;
            if (runOne(b, TransferRequest::WRITE, b.remote_base, work, len,
                       &t) ||
                runOne(b, TransferRequest::READ, b.remote_base, chk, len, &t) ||
                memcmp(chk, work, len) != 0) {
                printf("VERIFY FAIL at size=%zu\n", len);
                return -1;
            }
        }
        int iters = 0;
        double total = 0;
        while (iters < FLAGS_max_iters &&
               (total < FLAGS_budget_secs || iters == 0)) {
            double t;
            if (runOne(b, op, b.remote_base, work, len, &t)) return -1;
            total += t;
            iters++;
        }
        double gb = (double)len * iters / 1e9;
        printf(
            "RES PROT=%s op=%s size=%zu iters=%d secs=%.4f GBps=%.2f "
            "lat_us=%.1f\n",
            FLAGS_protocol.c_str(), opname, len, iters, total, gb / total,
            total * 1e6 / iters);
        fflush(stdout);
    }
    return 0;
}

static int runTarget(Bench& b) {
    printf("target ready: protocol=%s\n", FLAGS_protocol.c_str());
    fflush(stdout);
    sleep(FLAGS_verify_seconds);
    // initiator always writes the registered slot head; check 'M' arrived
    bool ok;
    if (FLAGS_protocol == "ub")
        ok = ((uint8_t*)b.pool)[0] == 'M';
    else
        ok = b.window[kUbmemOff] == 'M';
    printf("TARGET VERIFY %s\n", ok ? "PASS" : "FAIL");
    fflush(stdout);
    return ok ? 0 : 1;
}

static int runInitiator(Bench& b) {
    b.segment_id = b.engine->openSegment(FLAGS_target_server.c_str());
    if (b.segment_id == 0) {
        LOG(ERROR) << "cannot open target segment " << FLAGS_target_server;
        return 1;
    }
    auto desc = b.engine->getMetadata()->getSegmentDescByID(b.segment_id);
    if (!desc) {
        LOG(ERROR) << "cannot fetch target segment desc";
        return 1;
    }
    if (FLAGS_protocol == "ub") {
        b.remote_base = desc->buffers[0].addr;
        printf("remote_base=0x%lx\n", (unsigned long)b.remote_base);
    } else {
        b.remote_base = kUbmemOff;
    }
    double t;
    fillPattern((uint8_t*)b.pool, 4096, 7);
    ((uint8_t*)b.pool)[0] = 'M';
    if (runOne(b, TransferRequest::WRITE, b.remote_base, b.pool, 4096, &t))
        return 1;

    int rc = benchLoop(b, "WRITE", TransferRequest::WRITE);
    if (rc == 0) rc = benchLoop(b, "READ", TransferRequest::READ);
    printf("INITIATOR DONE rc=%d\n", rc);
    fflush(stdout);
    return rc;
}

}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    FLAGS_logtostderr = 1;

    Bench b;
    b.engine = std::make_unique<TransferEngine>(false);
    auto hp = parseHostNameWithPort(FLAGS_local_server_name);
    b.engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                   hp.first.c_str(), FLAGS_rpc_port);

    if (FLAGS_protocol == "ub") {
        b.pool_size = (size_t)FLAGS_pool_mb << 20;
        std::string matrix = "{\"cpu:0\": [[\"" + FLAGS_device_name +
                             "\"], []], \"cpu:1\": [[\"" + FLAGS_device_name +
                             "\"], []]}";
        void* args[2] = {(void*)matrix.c_str(), nullptr};
        b.xport = b.engine->installTransport("ub", args);
        if (!b.xport) {
            LOG(ERROR) << "install ub transport failed";
            return 1;
        }
        b.pool = numa_alloc_onnode(b.pool_size, 0);
        if (b.engine->registerLocalMemory(b.pool, b.pool_size, "cpu:0")) {
            LOG(ERROR) << "register ub pool failed";
            return 1;
        }
    } else if (FLAGS_protocol == "ubmem") {
        setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
        setenv("MC_UBMEM_DEV_SIZE", std::to_string(64ull << 20).c_str(), 1);
        void* args[2] = {nullptr, nullptr};
        b.xport = b.engine->installTransport("ubmem", args);
        if (!b.xport) {
            LOG(ERROR) << "install ubmem transport failed";
            return 1;
        }
#ifdef USE_UBMEM
        auto* ut = dynamic_cast<UbmemTransport*>(b.xport);
        b.window = (uint8_t*)ut->getUbmemBaseAddr();
        if (!b.window) {
            LOG(ERROR) << "no ubmem window base";
            return 1;
        }
#endif
        // registered target slot inside the window; local src/dst is plain DRAM
        if (b.engine->registerLocalMemory(b.window + kUbmemOff, kUbmemLen)) {
            LOG(ERROR) << "register ubmem slot failed";
            return 1;
        }
        b.pool_size = 128ull
                      << 20;  // malloc src/dst DRAM: 64MB work + 64MB chk
        b.pool = malloc(b.pool_size);
    } else if (FLAGS_protocol == "cxl") {
        // CxlTransport assumes both ends mmap the same physical window (real
        // CXL coherent model). On this fabric that model only holds within a
        // node: the peer mirror windows are nc-only and reject the plain
        // O_RDWR mapping cxlDevInit uses, so cross-node cxl cannot run here.
        setenv("MC_CXL_DEV_PATH", FLAGS_cxl_device.c_str(), 1);
        setenv("MC_CXL_DEV_SIZE", std::to_string(64ull << 20).c_str(), 1);
        void* args[2] = {nullptr, nullptr};
        b.xport = b.engine->installTransport("cxl", args);
        if (!b.xport) {
            LOG(ERROR) << "install cxl transport failed";
            return 1;
        }
#ifdef USE_CXL
        auto* ct = dynamic_cast<CxlTransport*>(b.xport);
        b.window = (uint8_t*)ct->getCxlBaseAddr();
        if (!b.window) {
            LOG(ERROR) << "no cxl window base";
            return 1;
        }
#endif
        if (b.engine->registerLocalMemory(b.window + kUbmemOff, kUbmemLen)) {
            LOG(ERROR) << "register cxl slot failed";
            return 1;
        }
        b.pool_size = 128ull << 20;
        b.pool = malloc(b.pool_size);
    } else {
        LOG(ERROR) << "unknown protocol " << FLAGS_protocol;
        return 1;
    }

    int rc = (FLAGS_mode == "target") ? runTarget(b) : runInitiator(b);

    if (FLAGS_protocol == "ub" && b.pool) {
        b.engine->unregisterLocalMemory(b.pool);
        numa_free(b.pool, b.pool_size);
    }
    return rc;
}
