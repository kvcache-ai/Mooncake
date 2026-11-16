// Same-host RDMA probe for #5482.
// Posts WRITEs at base+1024 inside a 4MiB registration, then unregisters the
// base. Fixed builds must return ERR_BATCH_BUSY (-4) while a batch is live.
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "error.h"
#include "transfer_engine.h"
#include "transfer_metadata.h"

using namespace mooncake;

namespace {
std::string hostname() {
    char buf[256] = {};
    if (gethostname(buf, sizeof(buf) - 1) != 0) return "127.0.0.1";
    return buf;
}

int engine_up(TransferEngine& engine, const std::string& name, uint16_t port,
              const std::string& dev, std::string& matrix_keep) {
    int rc = engine.init("P2PHANDSHAKE", name, hostname(), port);
    if (rc != 0) return rc;
    matrix_keep = std::string("{\"cpu:0\": [[\"") + dev +
                  "\"], []], \"cpu:1\": [[\"" + dev + "\"], []]}";
    void* args[2] = {const_cast<char*>(matrix_keep.c_str()), nullptr};
    if (!engine.installTransport("rdma", args)) return -1;
    return 0;
}
}  // namespace

int main(int argc, char** argv) {
    const std::string dev = argc > 1 ? argv[1] : "mlx5_0";
    const std::string host = hostname();
    const std::string target_name = host + ":19011";
    const std::string init_name = host + ":19012";
    constexpr size_t kBytes = 4 << 20;

    std::string target_matrix, init_matrix;
    TransferEngine target(false);
    TransferEngine initiator(false);
    if (int rc = engine_up(target, target_name, 19011, dev, target_matrix)) {
        std::cerr << "target init/install failed " << rc << "\n";
        return 2;
    }
    std::vector<char> remote(kBytes, 0);
    if (target.registerLocalMemory(remote.data(), kBytes) != 0) {
        std::cerr << "target register failed\n";
        return 2;
    }
    if (int rc = engine_up(initiator, init_name, 19012, dev, init_matrix)) {
        std::cerr << "initiator init/install failed " << rc << "\n";
        return 2;
    }
    std::vector<char> local(kBytes, 0x5a);
    if (initiator.registerLocalMemory(local.data(), kBytes) != 0) {
        std::cerr << "initiator register failed\n";
        return 2;
    }

    SegmentID sid = initiator.openSegment(target_name);
    auto desc = initiator.getMetadata()->getSegmentDescByID(sid);
    if (!desc || desc->buffers.empty()) {
        std::cerr << "openSegment failed for " << target_name << "\n";
        return 2;
    }
    const uint64_t remote_base =
        reinterpret_cast<uint64_t>(desc->buffers[0].addr);

    constexpr int kPosts = 64;
    constexpr size_t kChunk = 64 << 10;
    std::vector<BatchID> batches;
    batches.reserve(kPosts);
    for (int i = 0; i < kPosts; ++i) {
        BatchID batch = initiator.allocateBatchID(1);
        TransferRequest req;
        req.opcode = TransferRequest::WRITE;
        req.source = local.data() + 1024;
        req.length = kChunk;
        req.target_id = sid;
        req.target_offset = remote_base + 1024;
        auto st = initiator.submitTransfer(batch, {req});
        if (!st.ok()) {
            std::cerr << "submit failed: " << st.ToString() << "\n";
            return 2;
        }
        batches.push_back(batch);
    }

    const int unreg = initiator.unregisterLocalMemory(local.data());
    std::cout << "device=" << dev << " posts=" << batches.size()
              << " unreg=" << unreg << " (want " << ERR_BATCH_BUSY << ")\n";
    if (unreg == ERR_BATCH_BUSY) {
        std::cout << "PASS mid-flight unregister refused\n";
        return 0;
    }
    if (unreg == 0) {
        std::cout << "FAIL unregister returned 0 while batches were still "
                     "allocated\n";
        return 1;
    }
    std::cout << "FAIL unexpected unregister code\n";
    return 1;
}
