#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "nof/linux_device.h"

namespace mooncake {
namespace {
TEST(LinuxNofEndpointTest, ParsesEndpointFields) {
    detail::NofEndpoint endpoint;
    std::string error;
    ASSERT_TRUE(detail::ParseNofEndpoint(
        "trtype:tcp traddr:127.0.0.1 trsvcid:4420 subnqn:nqn.test:pool ns:7",
        endpoint, error))
        << error;
    EXPECT_EQ(endpoint.transport, "tcp");
    EXPECT_EQ(endpoint.address, "127.0.0.1");
    EXPECT_EQ(endpoint.service, "4420");
    EXPECT_EQ(endpoint.nqn, "nqn.test:pool");
    EXPECT_EQ(endpoint.nsid, 7u);
}

TEST(LinuxNofEndpointTest, RejectsInvalidOrAmbiguousFields) {
    const std::string base =
        "trtype:TCP traddr:127.0.0.1 trsvcid:4420 subnqn:nqn.test:pool";
    for (const auto& text : {std::string{}, base + " ns:0", base + " ns:1junk",
                             base + " ns:1 ns:2"}) {
        detail::NofEndpoint endpoint;
        std::string error;
        EXPECT_FALSE(detail::ParseNofEndpoint(text, endpoint, error)) << text;
        EXPECT_FALSE(error.empty());
    }
}

class LinuxNofDiscoveryTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char directory[] = "/tmp/mooncake-nof-sysfs.XXXXXX";
        const char* created = mkdtemp(directory);
        ASSERT_NE(created, nullptr);
        root = created;
        endpoint = {"tcp", "127.0.0.1", "4420", "nqn.test:pool", 7};
        Write("subsystem/subsysnqn", endpoint.nqn);
        Controller("nvme19", "127.0.0.1");
    }

    void TearDown() override { std::filesystem::remove_all(root); }

    void Write(const std::string& name, const std::string& value) {
        auto path = root / name;
        std::filesystem::create_directories(path.parent_path());
        std::ofstream(path) << value << '\n';
    }

    void Controller(const std::string& name, const std::string& address) {
        Write("subsystem/" + name + "/transport", "tcp");
        Write("subsystem/" + name + "/address",
              "traddr=" + address + ",trsvcid=4420,src_addr=127.0.0.1");
    }

    std::string Find() {
        return detail::FindLinuxNvmeDevice(endpoint, error, root.string());
    }

    std::filesystem::path root;
    detail::NofEndpoint endpoint;
    std::string error;
};

TEST_F(LinuxNofDiscoveryTest, FindsNamespaceByIdNotDeviceSuffix) {
    Write("subsystem/nvme19/nvme19n2/nsid", "7");
    Write("subsystem/nvme19/nvme19n2/dev", "259:1");
    EXPECT_EQ(Find(), "/dev/nvme19n2");
    endpoint.nsid = 2;
    EXPECT_TRUE(Find().empty());
}

TEST_F(LinuxNofDiscoveryTest, UsesVisibleHeadWithOneController) {
    // Select the visible namespace head rather than the hidden path.
    Write("subsystem/nvme19c19n2/nsid", "7");
    Write("subsystem/nvme19c19n2/hidden", "1");
    Write("subsystem/nvme19n2/nsid", "7");
    Write("subsystem/nvme19n2/dev", "259:1");
    EXPECT_EQ(Find(), "/dev/nvme19n2");

    // Reject a second controller even if it does not match the endpoint.
    Controller("nvme20", "127.0.0.2");
    EXPECT_TRUE(Find().empty());
    EXPECT_NE(error.find("one connected controller"), std::string::npos);
}

TEST_F(LinuxNofDiscoveryTest, RejectsMissingAndAmbiguousDevices) {
    EXPECT_TRUE(Find().empty());
    Write("subsystem/nvme19n1/nsid", "7");
    Write("subsystem/nvme19n1/dev", "259:1");
    Write("subsystem/nvme19n2/nsid", "7");
    Write("subsystem/nvme19n2/dev", "259:2");
    EXPECT_TRUE(Find().empty());
    endpoint.address = "127.0.0.2";
    EXPECT_TRUE(Find().empty());
}

}  // namespace
}  // namespace mooncake
