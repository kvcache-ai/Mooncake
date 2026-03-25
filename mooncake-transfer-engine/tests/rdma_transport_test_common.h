#pragma once

#include <glog/logging.h>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "common.h"
#include "memory_location.h"
#include "rdma_mem_ops.h"
#include "transfer_engine.h"
#include "transport/transport.h"

namespace mooncake {
namespace rdma_test {

struct TestOpts {
    std::string local_server_name = mooncake::getHostname();
    std::string metadata_server = "127.0.0.1:2379";
    std::string mode = "initiator";
    std::string device_name = "mlx5_0";
    std::string nic_priority_matrix;
    std::string segment_id = "127.0.0.1";
    std::string expect_remote_location;
    int device_id = 0;
    bool use_wildcard_location = false;
    size_t buffer_size = 64ull << 20;
    size_t data_length = 4ull << 20;
};

struct TestCtx {
    TestOpts opts;
    std::shared_ptr<MemOps> mem;
};

inline std::string fmtDevs(const std::string &dev_names) {
    std::stringstream ss(dev_names);
    std::string item;
    std::vector<std::string> items;
    while (getline(ss, item, ',')) {
        items.push_back(item);
    }

    std::string out;
    for (size_t i = 0; i < items.size(); ++i) {
        out += "\"" + items[i] + "\"";
        if (i + 1 < items.size()) {
            out += ",";
        }
    }
    return out;
}

inline std::string expLoc(const TestCtx &ctx) {
    return ctx.mem->loc(ctx.opts.device_id);
}

inline std::string reqLoc(const TestCtx &ctx) {
    return ctx.opts.use_wildcard_location ? kWildcardLocation : expLoc(ctx);
}

inline std::string loadNicMat(const TestCtx &ctx) {
    if (!ctx.opts.nic_priority_matrix.empty()) {
        std::ifstream file(ctx.opts.nic_priority_matrix);
        if (file.is_open()) {
            std::string content((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
            file.close();
            return content;
        }
    }

    auto dev_names = fmtDevs(ctx.opts.device_name);
    std::string mat = "{\"cpu:0\": [[" + dev_names + "], []]";
    mat += ", \"cpu:1\": [[" + dev_names + "], []]";
    if (ctx.mem->name() != "cpu") {
        mat += ", \"" + expLoc(ctx) + "\": [[" + dev_names + "], []]";
    }
    mat += "}";
    return mat;
}

inline void fillBuf(char *buf, size_t size) {
    for (size_t i = 0; i < size; ++i) {
        buf[i] = 'a' + lrand48() % 26;
    }
}

inline void logLoc(void *addr, size_t data_len) {
    auto entries = getMemoryLocation(addr, std::min(data_len, size_t(4096)),
                                     true);
    if (entries.empty()) {
        LOG(WARNING) << "getMemoryLocation returned empty result";
        return;
    }
    LOG(INFO) << "Detected local memory location: " << entries[0].location;
}

inline void waitDone(TransferEngine *engine, BatchID batch_id,
                     const char *stage) {
    TransferStatus status;
    bool done = false;
    while (!done) {
        auto s = engine->getTransferStatus(batch_id, 0, status);
        LOG_ASSERT(s.ok());
        if (status.s == TransferStatusEnum::COMPLETED) {
            done = true;
        } else if (status.s == TransferStatusEnum::FAILED) {
            LOG(FATAL) << stage << " transfer failed";
        }
    }
}

inline int runXfer(TransferEngine *engine, SegmentID seg_id, void *addr,
                   const TestCtx &ctx) {
    bindToSocket(0);
    auto seg_desc = engine->getMetadata()->getSegmentDescByID(seg_id);
    LOG_ASSERT(seg_desc);
    LOG_ASSERT(!seg_desc->buffers.empty());
    LOG(INFO) << "Remote segment protocol: " << seg_desc->protocol;
    LOG(INFO) << "Remote buffer location: " << seg_desc->buffers[0].name;
    if (!ctx.opts.expect_remote_location.empty()) {
        LOG_ASSERT(seg_desc->buffers[0].name ==
                   ctx.opts.expect_remote_location);
    }

    uint64_t remote_base = seg_desc->buffers[0].addr;
    auto wr = std::make_unique<char[]>(ctx.opts.data_length);
    auto rd = std::make_unique<char[]>(ctx.opts.data_length);
    fillBuf(wr.get(), ctx.opts.data_length);
    ctx.mem->copyIn(addr, wr.get(), ctx.opts.data_length);

    {
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest req;
        req.opcode = TransferRequest::WRITE;
        req.length = ctx.opts.data_length;
        req.source = reinterpret_cast<uint8_t *>(addr);
        req.target_id = seg_id;
        req.target_offset = remote_base;
        auto s = engine->submitTransfer(batch_id, {req});
        LOG_ASSERT(s.ok());
        waitDone(engine, batch_id, "WRITE");
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    {
        auto batch_id = engine->allocateBatchID(1);
        TransferRequest req;
        req.opcode = TransferRequest::READ;
        req.length = ctx.opts.data_length;
        req.source = reinterpret_cast<uint8_t *>(addr) + ctx.opts.data_length;
        req.target_id = seg_id;
        req.target_offset = remote_base;
        auto s = engine->submitTransfer(batch_id, {req});
        LOG_ASSERT(s.ok());
        waitDone(engine, batch_id, "READ");
        s = engine->freeBatchID(batch_id);
        LOG_ASSERT(s.ok());
    }

    ctx.mem->copyOut(rd.get(),
                     reinterpret_cast<uint8_t *>(addr) + ctx.opts.data_length,
                     ctx.opts.data_length);
    int ret = memcmp(wr.get(), rd.get(), ctx.opts.data_length);
    LOG(INFO) << "RDMA compare: " << (ret == 0 ? "OK" : "FAILED");
    return ret == 0 ? 0 : -1;
}

inline std::unique_ptr<TransferEngine> mkEngine(const TestCtx &ctx) {
    LOG_ASSERT(ctx.opts.buffer_size >= ctx.opts.data_length * 2);
    auto engine = std::make_unique<TransferEngine>(false);
    auto host_port = parseHostNameWithPort(ctx.opts.local_server_name);
    engine->init(ctx.opts.metadata_server, ctx.opts.local_server_name.c_str(),
                 host_port.first.c_str(), host_port.second);

    auto nic_mat = loadNicMat(ctx);
    void *args[2] = {const_cast<char *>(nic_mat.c_str()), nullptr};
    auto *xport = engine->installTransport("rdma", args);
    LOG_ASSERT(xport);
    LOG(INFO) << "Local topology: " << engine->getLocalTopology()->toString();
    return engine;
}

inline int runInit(const TestCtx &ctx) {
    auto engine = mkEngine(ctx);
    ctx.mem->setDev(ctx.opts.device_id);
    void *addr = ctx.mem->alloc(ctx.opts.buffer_size, 0);
    LOG_ASSERT(addr);
    auto loc = reqLoc(ctx);
    LOG(INFO) << "Registering local memory with location: " << loc;
    int rc = engine->registerLocalMemory(addr, ctx.opts.buffer_size, loc);
    LOG_ASSERT(!rc);
    logLoc(addr, ctx.opts.data_length);

    auto seg_id = engine->openSegment(ctx.opts.segment_id.c_str());
    int ret = runXfer(engine.get(), seg_id, addr, ctx);
    engine->unregisterLocalMemory(addr);
    ctx.mem->freeBuf(addr, ctx.opts.buffer_size);
    return ret;
}

inline int runTgt(const TestCtx &ctx) {
    auto engine = mkEngine(ctx);
    ctx.mem->setDev(ctx.opts.device_id);
    void *addr = ctx.mem->alloc(ctx.opts.buffer_size, 0);
    LOG_ASSERT(addr);
    auto loc = reqLoc(ctx);
    LOG(INFO) << "Registering local memory with location: " << loc;
    int rc = engine->registerLocalMemory(addr, ctx.opts.buffer_size, loc);
    LOG_ASSERT(!rc);
    logLoc(addr, ctx.opts.data_length);

    while (true) sleep(1);
    return 0;
}

}  // namespace rdma_test
}  // namespace mooncake
