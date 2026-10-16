#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <sys/time.h>

#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <memory>
#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>

#include "transfer_engine.h"
#include "transport/transport.h"
#include "transport/cxi_transport/cxi_transport.h"
#include "common.h"

using namespace mooncake;

namespace mooncake {

class CxiTransportUnitTest : public ::testing::Test {
   protected:
    void SetUp() override {
        metadata_server_ = "P2PHANDSHAKE";
        LOG(INFO) << "metadata_server: " << metadata_server_;

        local_server_name_ = "127.0.0.1:12345";
        LOG(INFO) << "local_server_name: " << local_server_name_;
    }

    void TearDown() override {}

    std::string metadata_server_;
    std::string local_server_name_;
};

TEST_F(CxiTransportUnitTest, TestInit) {
    std::unique_ptr<TransferEngine> engine;
    std::string device_filter = "cxi0";
    engine = std::make_unique<TransferEngine>(
        false, std::vector<std::string>{device_filter});
    EXPECT_NE(engine, nullptr) << "failed to allocate TransferEngine";

    engine->getLocalTopology()->discover({});
    auto hp = parseHostNameWithPort(local_server_name_);
    int rc = engine->init(metadata_server_, local_server_name_,
                          hp.first.c_str(), hp.second);
    EXPECT_EQ(rc, 0) << "engine->init failed";

    Transport* xport = engine->installTransport("cxi", nullptr);
    EXPECT_NE(xport, nullptr) << "installTransport(\"cxi\") failed";
    CxiTransport* cxi_xport = dynamic_cast<CxiTransport*>(xport);
    EXPECT_NE(cxi_xport, nullptr) << "initialization for cxi failed";

    engine.reset();

    LOG(INFO) << "default init ok";

    device_filter = "invalid_device";  // dummy device name
    engine = std::make_unique<TransferEngine>(
        false, std::vector<std::string>{device_filter});
    EXPECT_NE(engine, nullptr) << "failed to allocate TransferEngine";
    engine->getLocalTopology()->discover({device_filter});
    hp = parseHostNameWithPort(local_server_name_);
    rc = engine->init(metadata_server_, local_server_name_, hp.first.c_str(),
                      hp.second);
    EXPECT_EQ(rc, 0) << "engine->init failed";
    xport = engine->installTransport("cxi", nullptr);
    EXPECT_EQ(xport, nullptr)
        << "installTransport(\"cxi\") should fail for invalid_device";
}

TEST_F(CxiTransportUnitTest, TestAllocate) {
    std::unique_ptr<TransferEngine> engine;
    engine = std::make_unique<TransferEngine>(false);
    EXPECT_NE(engine, nullptr) << "failed to allocate TransferEngine";

    engine->getLocalTopology()->discover({});
    auto hp = parseHostNameWithPort(local_server_name_);
    int rc = engine->init(metadata_server_, local_server_name_,
                          hp.first.c_str(), hp.second);
    EXPECT_EQ(rc, 0) << "engine->init failed";

    Transport* xport = engine->installTransport("cxi", nullptr);
    EXPECT_NE(xport, nullptr) << "installTransport(\"cxi\") failed";
    CxiTransport* cxi_xport = dynamic_cast<CxiTransport*>(xport);
    EXPECT_NE(cxi_xport, nullptr) << "initialization for cxi failed";

    size_t buffer_size = 128 * 1024;
    auto buffer = std::vector<uint8_t>(buffer_size);
    size_t dummy_buffer_size = 128;
    auto dummy_buffer = std::vector<uint8_t>(dummy_buffer_size);

    // check invalid device
    int retcode = engine->registerLocalMemory(
        (void*)dummy_buffer.data(), dummy_buffer_size, "invalid_device:0");
    EXPECT_EQ(retcode, ERR_DEVICE_NOT_FOUND)
        << "invalid device memory should not be registered!";

    retcode =
        engine->registerLocalMemory((void*)buffer.data(), buffer_size, "cpu:0");
    EXPECT_EQ(retcode, 0) << "unable to register memory";

    retcode = engine->registerLocalMemory(
        (void*)(buffer.data() + (buffer_size >> 1)), buffer_size >> 1, "cpu:1");
    EXPECT_EQ(retcode, ERR_ADDRESS_OVERLAPPED)
        << "should not be able to register overlapping MRs!";

    engine->unregisterLocalMemory(buffer.data());
}

};  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    ::testing::InitGoogleTest(&argc, argv);
    google::InitGoogleLogging("CxiTransportUnitTest");
    FLAGS_logtostderr = 1;
    int rc = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return rc;
}
