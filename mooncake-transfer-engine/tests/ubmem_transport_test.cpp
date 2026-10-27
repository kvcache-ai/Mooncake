// Copyright 2024 KVCache.AI
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

// UbmemTransport tests. gtest mode (default): WRITE -> READ byte-exact
// verification plus the submit/resolve error contract, on a local device
// window (--ubmem_device). When the path does not exist the fixture creates
// a sparse backing file, so the suite runs on any Linux box without a
// shmdev (same scheme as the cxl test's tmp_dax_sim). Cross-node mode
// (main args outside gtest) still wants a real mirror pair, e.g.
//   initiator: --mode=initiator --local_server_name=hw02 \
//              --ubmem_device=/dev/obmm_shmdev14 \
//              --target_server=hw01 --target_device=/dev/obmm_shmdev13 \
//              --peer_dev_map=hw01:/dev/obmm_shmdev13=/dev/obmm_shmdev5

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "common.h"
#include "transfer_engine.h"
#include "transport/transport.h"
#include "transport/ubmem_transport/ubmem_transport.h"

using namespace mooncake;

namespace mooncake {

DEFINE_string(local_server_name, getHostname(),
              "Local server name for segment discovery");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server (http metadata server or etcd)");
DEFINE_string(mode, "gtest",
              "gtest | initiator | target (cross-node manual mode)");
DEFINE_string(ubmem_device, "tmp_ubmem_sim",
              "Local UB shmdev path for this process (created as a sparse "
              "file when missing)");
DEFINE_uint64(ubmem_size, 64ull << 20, "Device window bytes to map");
DEFINE_string(target_server, "", "[initiator] peer server name");
DEFINE_string(target_device, "", "[initiator] peer device path");
DEFINE_string(peer_dev_map, "", "[initiator] MC_UBMEM_PEER_DEV_MAP content");
DEFINE_int64(verify_seconds, 60, "[target] wait before verify+exit");

static void* allocateMemoryPool(size_t size) { return malloc(size); }

class UbmemTransportTest : public ::testing::Test {
   public:
    std::shared_ptr<mooncake::TransferMetadata> metadata_client;
    uint8_t* addr = nullptr;
    uint8_t* base_addr;
    std::pair<std::string, uint16_t> hostname_port;
    std::unique_ptr<mooncake::TransferEngine> engine;
    const size_t offset_1 = 2 * 1024 * 1024;
    const size_t len = 2 * 1024 * 1024;
    Transport* xport;
    void** args;
    mooncake::Transport::SegmentID segment_id;
    std::shared_ptr<TransferMetadata::SegmentDesc> segment_desc;
    const size_t kDataLength = 4 * 1024;

   protected:
    void SetUp() override {
        static int offset = 0;
        google::InitGoogleLogging("UbmemTransportTest");
        FLAGS_logtostderr = 1;

        // A missing device path becomes a sparse backing file; a real
        // shmdev passed via --ubmem_device is used untouched
        struct stat st;
        if (stat(FLAGS_ubmem_device.c_str(), &st) != 0) {
            int fd = open(FLAGS_ubmem_device.c_str(), O_RDWR | O_CREAT, 0666);
            ASSERT_GE(fd, 0);
            ASSERT_EQ(ftruncate(fd, FLAGS_ubmem_size), 0);
            close(fd);
            created_backing_file_ = true;
        }
        setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
        setenv("MC_UBMEM_DEV_SIZE", std::to_string(FLAGS_ubmem_size).c_str(),
               1);

        engine = std::make_unique<TransferEngine>(false);
        hostname_port = parseHostNameWithPort(FLAGS_local_server_name);
        engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                     hostname_port.first.c_str(),
                     hostname_port.second + offset++);
        xport = nullptr;

        args = (void**)malloc(2 * sizeof(void*));
        args[0] = nullptr;
        xport = engine->installTransport("ubmem", args);
        ASSERT_NE(xport, nullptr);

        UbmemTransport* ubmem_xport = dynamic_cast<UbmemTransport*>(xport);
        ASSERT_NE(ubmem_xport, nullptr);
        base_addr = (uint8_t*)ubmem_xport->getUbmemBaseAddr();
        ASSERT_NE(base_addr, nullptr);
        addr = (uint8_t*)allocateMemoryPool(kDataLength);

        int rc = engine->registerLocalMemory(base_addr + offset_1, len);
        ASSERT_EQ(rc, 0);

        segment_id = engine->openSegment(FLAGS_local_server_name.c_str());
        segment_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
        ASSERT_NE(segment_desc, nullptr);
        ASSERT_EQ(segment_desc->protocol, "ubmem");
        ASSERT_EQ(segment_desc->ubmem_name, FLAGS_ubmem_device);
    }

    void TearDown() override {
        free(args);
        free(addr);
        google::ShutdownGoogleLogging();
        if (created_backing_file_) {
            unlink(FLAGS_ubmem_device.c_str());
            created_backing_file_ = false;
        }
    }

   private:
    bool created_backing_file_ = false;

   public:
    // Submit one request, poll it to a terminal state and return the status
    TransferStatus runToCompletion(BatchID batch_id, size_t task_id) {
        TransferStatus status;
        while (true) {
            Status s = xport->getTransferStatus(batch_id, task_id, status);
            EXPECT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED)
                return status;
        }
    }
};

TEST_F(UbmemTransportTest, WriteReadBack) {
    int times = 10;
    while (times--) {
        for (size_t offset = 0; offset < kDataLength; ++offset)
            *((char*)(addr) + offset) = 'a' + lrand48() % 26;

        auto batch_id = xport->allocateBatchID(1);
        TransferRequest entry;
        entry.opcode = TransferRequest::WRITE;
        entry.length = kDataLength;
        entry.source = (uint8_t*)(addr);
        entry.target_id = segment_id;
        entry.target_offset = (uint64_t)(base_addr + offset_1);
        Status s = engine->submitTransfer(batch_id, {entry});
        LOG_ASSERT(s.ok());

        bool completed = false;
        TransferStatus status;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED) {
                ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
                completed = true;
            }
        }
        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());

        // READ back into a fresh buffer, byte-exact verification
        void* verify = allocateMemoryPool(kDataLength);
        memset(verify, 0, kDataLength);
        batch_id = xport->allocateBatchID(1);
        TransferRequest read_entry;
        read_entry.opcode = TransferRequest::READ;
        read_entry.length = kDataLength;
        read_entry.source = (uint8_t*)verify;
        read_entry.target_id = segment_id;
        read_entry.target_offset = (uint64_t)(base_addr + offset_1);
        s = engine->submitTransfer(batch_id, {read_entry});
        ASSERT_EQ(s, Status::OK());

        completed = false;
        while (!completed) {
            Status s = xport->getTransferStatus(batch_id, 0, status);
            ASSERT_EQ(s, Status::OK());
            if (status.s == TransferStatusEnum::COMPLETED ||
                status.s == TransferStatusEnum::FAILED) {
                ASSERT_EQ(status.s, TransferStatusEnum::COMPLETED);
                completed = true;
            }
        }
        s = xport->freeBatchID(batch_id);
        ASSERT_EQ(s, Status::OK());
        ASSERT_EQ(memcmp(verify, addr, kDataLength), 0);
        free(verify);
    }
    engine->unregisterLocalMemory(base_addr + offset_1);
}

TEST_F(UbmemTransportTest, OutOfBoundsRejected) {
    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t*)(addr);
    entry.target_id = segment_id;
    entry.target_offset = (uint64_t)(base_addr + FLAGS_ubmem_size) -
                          1024;  // out of bounds (partially past the window)
    Status s = engine->submitTransfer(batch_id, {entry});
    LOG_ASSERT(s.ok());

    TransferStatus status;
    bool completed = false;
    while (!completed) {
        Status s = xport->getTransferStatus(batch_id, 0, status);
        ASSERT_EQ(s, Status::OK());
        if (status.s == TransferStatusEnum::COMPLETED ||
            status.s == TransferStatusEnum::FAILED)
            completed = true;
    }
    ASSERT_EQ(status.s, TransferStatusEnum::FAILED);  // out-of-bounds must fail
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

// The store-side grouped-batch path gates on the segment protocol resolved
// from the descriptor; this pins the query the gate relies on.
TEST_F(UbmemTransportTest, SegmentProtocolQuery) {
    EXPECT_EQ(engine->getSegmentProtocol(FLAGS_local_server_name), "ubmem");
    EXPECT_EQ(engine->getSegmentProtocol("no_such_segment_name"), "");
}

TEST_F(UbmemTransportTest, ZeroLengthTransferCompletes) {
    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = 0;
    entry.source = (uint8_t*)(addr);
    entry.target_id = segment_id;
    entry.target_offset = (uint64_t)(base_addr + offset_1);
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    TransferStatus status = runToCompletion(batch_id, 0);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 0u);
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

TEST_F(UbmemTransportTest, InvalidTargetIdRejectedAtSubmit) {
    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t*)(addr);
    entry.target_id = 4095;  // never allocated
    entry.target_offset = (uint64_t)(base_addr + offset_1);
    // submission routing cannot resolve an unknown segment id, so the
    // request is rejected up front instead of reaching the transport
    Status s = engine->submitTransfer(batch_id, {entry});
    EXPECT_FALSE(s.ok());
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

TEST_F(UbmemTransportTest, OffsetBelowOwnerBaseRejected) {
    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t*)(addr);
    entry.target_id = segment_id;
    entry.target_offset = 0;  // below the owner device base address
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    TransferStatus status = runToCompletion(batch_id, 0);
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

TEST_F(UbmemTransportTest, BatchCapacityRejected) {
    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t*)(addr);
    entry.target_id = segment_id;
    entry.target_offset = (uint64_t)(base_addr + offset_1);
    Status s = engine->submitTransfer(batch_id, {entry, entry});
    EXPECT_FALSE(s.ok());  // capacity 1, two entries
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

TEST_F(UbmemTransportTest, StatusInvalidTaskIdRejected) {
    auto batch_id = xport->allocateBatchID(1);
    TransferStatus status;
    Status s = xport->getTransferStatus(batch_id, 5, status);
    EXPECT_FALSE(s.ok());
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
}

TEST_F(UbmemTransportTest, RegisterOutsideDeviceRejected) {
    void* foreign = malloc(kDataLength);
    ASSERT_NE(foreign, nullptr);
    // addresses outside the mapped device window must not register
    int rc =
        engine->registerLocalMemory(foreign, kDataLength, "test", false, false);
    EXPECT_EQ(rc, -1);
    free(foreign);
}

// A cross-node ubmem segment with no mirror configured must fail the
// transfer (documented misconfiguration), exercised without hardware by
// publishing a foreign-host segment descriptor into the metadata.
TEST_F(UbmemTransportTest, CrossNodeTargetWithoutMirrorFails) {
    const std::string peer_name = "ubmem_peer_test_host:19998";
    auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
    desc->name = peer_name;
    desc->protocol = "ubmem";
    desc->ubmem_name = "/dev/obmm_shmdev_missing";
    desc->ubmem_base_addr = 0x100000;
    constexpr Transport::SegmentID kPeerSegmentID = 4090;
    ASSERT_EQ(engine->getMetadata()->addLocalSegment(kPeerSegmentID, peer_name,
                                                     std::move(desc)),
              0);

    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = kDataLength;
    entry.source = (uint8_t*)(addr);
    entry.target_id = kPeerSegmentID;
    entry.target_offset = 0x100000 + 4096;
    Status s = engine->submitTransfer(batch_id, {entry});
    ASSERT_TRUE(s.ok());

    TransferStatus status = runToCompletion(batch_id, 0);
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    s = xport->freeBatchID(batch_id);
    ASSERT_EQ(s, Status::OK());
    engine->getMetadata()->removeSegmentDesc(peer_name);
}

// Without MC_UBMEM_DEV_PATH the deferred constructor error must surface as
// a failed install, not a broken transport object.
TEST(UbmemInstallTest, InstallFailsWithoutDevicePath) {
    const char* saved = std::getenv("MC_UBMEM_DEV_PATH");
    ASSERT_EQ(unsetenv("MC_UBMEM_DEV_PATH"), 0);

    TransferEngine engine2(false);
    auto hp = parseHostNameWithPort("ubmem_install_test:19997");
    engine2.init(FLAGS_metadata_server, "ubmem_install_test", hp.first.c_str(),
                 hp.second);
    void* args[1] = {nullptr};
    EXPECT_EQ(engine2.installTransport("ubmem", args), nullptr);

    if (saved) setenv("MC_UBMEM_DEV_PATH", saved, 1);
}

// ---- Cross-node manual modes (outside gtest) ----

static int runTargetMode() {
    setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
    setenv("MC_UBMEM_DEV_SIZE", std::to_string(FLAGS_ubmem_size).c_str(), 1);
    auto engine = std::make_unique<TransferEngine>(false);
    auto hp = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hp.first.c_str(), hp.second);
    void* args[1] = {nullptr};
    Transport* xport = engine->installTransport("ubmem", args);
    if (!xport) {
        LOG(ERROR) << "target: installTransport failed";
        return 1;
    }
    UbmemTransport* ub = dynamic_cast<UbmemTransport*>(xport);
    uint8_t* base = (uint8_t*)ub->getUbmemBaseAddr();
    engine->registerLocalMemory(base + (2 << 20), 2 << 20);
    LOG(INFO) << "target ready: device=" << FLAGS_ubmem_device
              << " base=" << (void*)base << ", waiting " << FLAGS_verify_seconds
              << "s";
    sleep(FLAGS_verify_seconds);

    // Target-side self check: the initiator's pattern starts with 'M',
    // the rest is offset-dependent
    const size_t len = 2 << 20, off = 2 << 20;
    uint8_t* window = base + off;
    bool ok = window[0] == 'M';
    for (size_t i = 1; ok && i < len; i += 4096)
        ok = (window[i] == (uint8_t)(i ^ (i >> 8)));
    LOG(INFO) << (ok ? "TARGET VERIFY PASS" : "TARGET VERIFY FAIL");
    return ok ? 0 : 2;
}

static int runInitiatorMode() {
    setenv("MC_UBMEM_DEV_PATH", FLAGS_ubmem_device.c_str(), 1);
    setenv("MC_UBMEM_DEV_SIZE", std::to_string(FLAGS_ubmem_size).c_str(), 1);
    if (!FLAGS_peer_dev_map.empty())
        setenv("MC_UBMEM_PEER_DEV_MAP", FLAGS_peer_dev_map.c_str(), 1);
    auto engine = std::make_unique<TransferEngine>(false);
    auto hp = parseHostNameWithPort(FLAGS_local_server_name);
    engine->init(FLAGS_metadata_server, FLAGS_local_server_name.c_str(),
                 hp.first.c_str(), hp.second);
    void* args[1] = {nullptr};
    Transport* xport = engine->installTransport("ubmem", args);
    if (!xport) {
        LOG(ERROR) << "initiator: installTransport failed";
        return 1;
    }
    // No official INVALID sentinel for SegmentID; treat 0 as open failure
    auto segment_id = engine->openSegment(FLAGS_target_server.c_str());
    if (segment_id == 0) {
        LOG(ERROR) << "initiator: cannot open target segment "
                   << FLAGS_target_server;
        return 1;
    }

    const size_t len = 2 << 20;
    auto target_desc = engine->getMetadata()->getSegmentDescByID(segment_id);
    const uint64_t target_offset = target_desc->ubmem_base_addr + (2 << 20);
    uint8_t* src = (uint8_t*)malloc(len);
    src[0] = 'M';
    for (size_t i = 1; i < len; ++i) src[i] = (uint8_t)(i ^ (i >> 8));

    auto batch_id = xport->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::WRITE;
    entry.length = len;
    entry.source = src;
    entry.target_id = segment_id;
    entry.target_offset = target_offset;
    Status s = engine->submitTransfer(batch_id, {entry});
    if (!s.ok()) {
        LOG(ERROR) << "initiator: submit WRITE failed: " << s;
        return 1;
    }
    TransferStatus status;
    while (xport->getTransferStatus(batch_id, 0, status).ok() &&
           status.s == TransferStatusEnum::WAITING)
        usleep(1000);
    LOG_IF(ERROR, status.s != TransferStatusEnum::COMPLETED)
        << "initiator: cross-node WRITE failed";
    xport->freeBatchID(batch_id);
    if (status.s != TransferStatusEnum::COMPLETED) return 1;

    // READ-back verification through the mirror
    uint8_t* verify = (uint8_t*)malloc(len);
    batch_id = xport->allocateBatchID(1);
    TransferRequest read_entry;
    read_entry.opcode = TransferRequest::READ;
    read_entry.length = len;
    read_entry.source = verify;
    read_entry.target_id = segment_id;
    read_entry.target_offset = target_offset;
    s = engine->submitTransfer(batch_id, {read_entry});
    if (!s.ok()) {
        LOG(ERROR) << "initiator: submit READ failed: " << s;
        return 1;
    }
    while (xport->getTransferStatus(batch_id, 0, status).ok() &&
           status.s == TransferStatusEnum::WAITING)
        usleep(1000);
    xport->freeBatchID(batch_id);
    if (status.s != TransferStatusEnum::COMPLETED) {
        LOG(ERROR) << "initiator: cross-node READ failed";
        return 1;
    }
    bool ok = memcmp(verify, src, len) == 0;
    LOG(INFO) << (ok ? "INITIATOR VERIFY PASS (write+read-back via mirror)"
                     : "INITIATOR VERIFY FAIL");
    free(src);
    free(verify);
    return ok ? 0 : 2;
}

}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    // gtest-path logging init lives in SetUp/TearDown (aligned with the
    // cxl test); manual modes init at their own entry points to avoid
    // double InitGoogleLogging
    if (mooncake::FLAGS_mode == "target") {
        google::InitGoogleLogging("ubmem_transport_test");
        FLAGS_logtostderr = 1;
        return mooncake::runTargetMode();
    }
    if (mooncake::FLAGS_mode == "initiator") {
        google::InitGoogleLogging("ubmem_transport_test");
        FLAGS_logtostderr = 1;
        return mooncake::runInitiatorMode();
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
