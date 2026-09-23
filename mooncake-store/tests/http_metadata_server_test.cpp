#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_http/coro_http_client.hpp>

#include "http_metadata_server.h"
#include "../src/config/admin_http_bootstrap_config_loader.h"
#include "default_config.h"
#include "common/network.h"

namespace mooncake::testing {

class AdminHttpBootstrapConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::string pattern = (std::filesystem::temp_directory_path() /
                               "admin_http_bootstrap_config_test_XXXXXX")
                                  .string();
        char* directory = mkdtemp(pattern.data());
        ASSERT_NE(directory, nullptr);
        temp_dir_ = directory;
    }

    void TearDown() override {
        if (!temp_dir_.empty()) {
            std::filesystem::remove_all(temp_dir_);
        }
    }

    std::unique_ptr<DefaultConfig> LoadConfig(const std::string& extension,
                                              const std::string& contents) {
        const auto path = temp_dir_ / ("config" + extension);
        {
            std::ofstream file(path);
            EXPECT_TRUE(file.is_open());
            file << contents;
        }
        auto config = std::make_unique<DefaultConfig>();
        config->SetPath(path.string());
        config->Load();
        return config;
    }

    std::filesystem::path temp_dir_;
};

TEST_F(AdminHttpBootstrapConfigTest, UsesOwnerDefaultsWithoutSources) {
    const auto config = ResolveAdminHttpBootstrapConfig(nullptr, {});
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.port, 8080u);
    EXPECT_EQ(config.host, "0.0.0.0");
}

TEST_F(AdminHttpBootstrapConfigTest, LoadsExistingFlatYamlAndJsonKeys) {
    const auto yaml = LoadConfig(".yaml",
                                 "enable_http_metadata_server: true\n"
                                 "http_metadata_server_port: 8101\n"
                                 "http_metadata_server_host: 127.0.0.1\n");
    const auto from_yaml = ResolveAdminHttpBootstrapConfig(yaml.get(), {});
    EXPECT_TRUE(from_yaml.enabled);
    EXPECT_EQ(from_yaml.port, 8101u);
    EXPECT_EQ(from_yaml.host, "127.0.0.1");

    const auto json = LoadConfig(
        ".json",
        R"({"enable_http_metadata_server":true,"http_metadata_server_port":8102,"http_metadata_server_host":"127.0.0.2"})");
    const auto from_json = ResolveAdminHttpBootstrapConfig(json.get(), {});
    EXPECT_TRUE(from_json.enabled);
    EXPECT_EQ(from_json.port, 8102u);
    EXPECT_EQ(from_json.host, "127.0.0.2");
}

TEST_F(AdminHttpBootstrapConfigTest, ExplicitCommandLineOverridesFile) {
    const auto file =
        LoadConfig(".yaml",
                   "enable_http_metadata_server: true\n"
                   "http_metadata_server_port: 8101\n"
                   "http_metadata_server_host: configured-host\n");
    AdminHttpBootstrapCommandLineOverrides command_line;
    command_line.enabled = false;
    command_line.port = 0;
    command_line.host = "";
    const auto config =
        ResolveAdminHttpBootstrapConfig(file.get(), command_line);
    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.port, 0u);
    EXPECT_TRUE(config.host.empty());
}

TEST_F(AdminHttpBootstrapConfigTest, AbsentCommandLinePreservesFileValues) {
    const auto file =
        LoadConfig(".yaml",
                   "enable_http_metadata_server: true\n"
                   "http_metadata_server_port: 8101\n"
                   "http_metadata_server_host: configured-host\n");
    const auto config = ResolveAdminHttpBootstrapConfig(file.get(), {});
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.port, 8101u);
    EXPECT_EQ(config.host, "configured-host");
}

TEST_F(AdminHttpBootstrapConfigTest, AcceptsPortRangeBoundaries) {
    const auto yaml = LoadConfig(".yaml", "http_metadata_server_port: 0\n");
    EXPECT_EQ(ResolveAdminHttpBootstrapConfig(yaml.get(), {}).port, 0u);
    const auto json =
        LoadConfig(".json", R"({"http_metadata_server_port":65535})");
    EXPECT_EQ(ResolveAdminHttpBootstrapConfig(json.get(), {}).port, 65535u);

    AdminHttpBootstrapCommandLineOverrides command_line;
    command_line.port = 65535;
    EXPECT_EQ(ResolveAdminHttpBootstrapConfig(nullptr, command_line).port,
              65535u);
}

TEST_F(AdminHttpBootstrapConfigTest, RejectsOutOfRangeFilePorts) {
    const auto yaml = LoadConfig(".yaml", "http_metadata_server_port: 65536\n");
    EXPECT_THROW(ResolveAdminHttpBootstrapConfig(yaml.get(), {}),
                 std::invalid_argument);
    const auto json =
        LoadConfig(".json", R"({"http_metadata_server_port":4294967295})");
    EXPECT_THROW(ResolveAdminHttpBootstrapConfig(json.get(), {}),
                 std::invalid_argument);
}

TEST_F(AdminHttpBootstrapConfigTest,
       ValidCommandLinePortOverridesInvalidFilePort) {
    const auto file = LoadConfig(".yaml", "http_metadata_server_port: 65536\n");
    AdminHttpBootstrapCommandLineOverrides command_line;
    command_line.port = 8102;
    EXPECT_EQ(ResolveAdminHttpBootstrapConfig(file.get(), command_line).port,
              8102u);
}

TEST_F(AdminHttpBootstrapConfigTest, RejectsOutOfRangeCommandLinePort) {
    AdminHttpBootstrapCommandLineOverrides command_line;
    command_line.port = 65536;
    EXPECT_THROW(ResolveAdminHttpBootstrapConfig(nullptr, command_line),
                 std::invalid_argument);
    // A negative int32 flag becomes UINT32_MAX at the existing CLI bridge.
    command_line.port = UINT32_MAX;
    EXPECT_THROW(ResolveAdminHttpBootstrapConfig(nullptr, command_line),
                 std::invalid_argument);
}

class HttpMetadataServerTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int status;
        std::string body;
    };

    HttpResponse Get(int port, const std::string& path) {
        coro_http::coro_http_client client;
        auto response = async_simple::coro::syncAwait(client.async_get(
            "http://127.0.0.1:" + std::to_string(port) + path));
        return {response.status, std::string(response.resp_body)};
    }

    HttpResponse Put(int port, const std::string& path,
                     const std::string& body) {
        coro_http::coro_http_client client;
        auto response = async_simple::coro::syncAwait(
            client.async_put("http://127.0.0.1:" + std::to_string(port) + path,
                             body, coro_http::req_content_type::json));
        return {response.status, std::string(response.resp_body)};
    }

    void WaitUntilReady(int port) {
        for (int i = 0; i < 50; ++i) {
            if (Get(port, "/health").status == 200) {
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        FAIL() << "HTTP metadata server did not become ready";
    }
};

TEST_F(HttpMetadataServerTest, AllowsIdempotentRpcMetaRepublish) {
    int port = getFreeTcpPort();
    HttpMetadataServer server(static_cast<uint16_t>(port), "127.0.0.1");
    ASSERT_TRUE(server.start());
    WaitUntilReady(port);

    const std::string path =
        "/metadata?key=mooncake%2Frpc_meta%2F10.0.0.1%3A12384";
    const std::string body =
        R"({"ip_or_host_name":"10.0.0.1","rpc_port":15228})";

    EXPECT_EQ(Put(port, path, body).status, 200);

    auto second = Put(port, path, body);
    EXPECT_EQ(second.status, 200);
    EXPECT_EQ(second.body, "metadata unchanged");

    auto stored = Get(port, path);
    EXPECT_EQ(stored.status, 200);
    EXPECT_EQ(stored.body, body);

    server.stop();
}

TEST_F(HttpMetadataServerTest, RejectsChangedRpcMetaRepublish) {
    int port = getFreeTcpPort();
    HttpMetadataServer server(static_cast<uint16_t>(port), "127.0.0.1");
    ASSERT_TRUE(server.start());
    WaitUntilReady(port);

    const std::string path =
        "/metadata?key=mooncake%2Frpc_meta%2F10.0.0.1%3A12384";
    const std::string original =
        R"({"ip_or_host_name":"10.0.0.1","rpc_port":15228})";
    const std::string changed =
        R"({"ip_or_host_name":"10.0.0.1","rpc_port":16000})";

    EXPECT_EQ(Put(port, path, original).status, 200);

    auto second = Put(port, path, changed);
    EXPECT_EQ(second.status, 400);
    EXPECT_EQ(second.body, "Duplicate rpc_meta key not allowed");

    auto stored = Get(port, path);
    EXPECT_EQ(stored.status, 200);
    EXPECT_EQ(stored.body, original);

    server.stop();
}

TEST_F(HttpMetadataServerTest, StartReportsBindFailure) {
    int port = getFreeTcpPort();
    HttpMetadataServer first(static_cast<uint16_t>(port), "127.0.0.1");
    ASSERT_TRUE(first.start());
    WaitUntilReady(port);

    // A second server cannot bind the already-taken port; start() must report
    // the failure instead of claiming a healthy server that never came up.
    HttpMetadataServer second(static_cast<uint16_t>(port), "127.0.0.1");
    EXPECT_FALSE(second.start());
    EXPECT_FALSE(second.is_running());

    first.stop();
}

}  // namespace mooncake::testing
