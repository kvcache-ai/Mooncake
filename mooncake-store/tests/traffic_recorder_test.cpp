#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>

#include <async_simple/coro/SyncAwait.h>
#include "common/network.h"
#include "rpc_service.h"
#include "rpc_worker_pool.h"
#include "traffic/traffic_format.h"
#include "traffic/traffic_recorder.h"
#include "traffic/traffic_redaction.h"
#include "traffic/ylt_adapter.h"
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/struct_pack.hpp>

namespace mooncake::traffic {
namespace {

class TempDir {
   public:
    TempDir() {
        setenv("MOONCAKE_TRAFFIC_AUTOSTART", "0", 1);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake_traffic_test_" + std::to_string(::getpid()) + "_" +
                 std::to_string(counter_++));
        std::filesystem::remove_all(path_);
        std::filesystem::create_directories(path_);
    }

    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path_, ignored);
    }

    const std::filesystem::path& path() const { return path_; }

   private:
    std::filesystem::path path_;
    static inline int counter_ = 0;
};

class TrafficTestService {
   public:
    int Echo(int value) { return value; }
};

class FailingRedactor final : public Redactor {
   public:
    bool Redact(std::string_view, std::string&) const override { return false; }
};

TEST(TrafficRecorderTest, RejectsRedactedConfigurations) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](const std::string& routes) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string()
            << R"(","redaction":"redacted")";
        if (!routes.empty()) {
            out << R"(,"routes":)" << routes;
        }
        out << R"(,"control":{"host":"127.0.0.1","port":37999,)"
            << R"("token":"test-token"}})";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    // Without an allowlist the recorder would have to redact every route.
    write_config("");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A numeric route key needs a redactor in the registry.
    write_config(R"(["1"])");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A route name is resolved through the registry, and Ping has no redactor.
    RouteNameRegistry names;
    names.Add(coro_rpc::func_id<&WrappedMasterService::Ping>(),
              coro_rpc::get_func_name<&WrappedMasterService::Ping>());
    write_config(R"(["Ping"])");
    recorder.Install(1, {}, names);
    EXPECT_EQ(recorder.GetStatus().state, "error");
}

TEST(TrafficRecorderTest, ReinstallReplacesPreviousRoutes) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](int route, int control_port) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "segment_seconds": 300,
          "routes": [")"
            << route << R"("],
          "control": {"host": "127.0.0.1", "port": )"
            << control_port << R"(,
                      "token": "test-token"}
        })";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    auto& recorder = TrafficRecorder::Instance();
    write_config(1, 37997);
    recorder.Install(1);
    ASSERT_TRUE(recorder.Start());
    const std::string payload = "payload";
    recorder.OnRequest(RequestView{1, payload, 1, 1, 0});
    recorder.Stop();
    recorder.Shutdown();

    write_config(2, 37998);
    recorder.Install(1);
    ASSERT_TRUE(recorder.Start());
    recorder.OnRequest(RequestView{1, payload, 2, 1, 0});
    recorder.OnRequest(RequestView{2, payload, 2, 2, 0});
    recorder.Stop();
    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.filtered_requests, 1u);
    EXPECT_EQ(status.dropped_requests, 0u);
    EXPECT_TRUE(status.capture_complete);
}

TEST(TrafficRecorderTest, ReportsWhetherStopStarted) {
    TempDir dir;
    const std::filesystem::path output_dir = dir.path() / "out";
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({
          "output_dir": ")"
            << output_dir.string() << R"(",
          "ring_bytes": 262144,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "control": {"host": "127.0.0.1", "port": 38011,
                      "token": "test-token"}
        })";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    // An idle recorder has nothing to drain, and the control endpoint reports
    // that instead of announcing a stop that never happens.
    EXPECT_FALSE(recorder.StopAsync());
    ASSERT_TRUE(recorder.Start());
    // The sink creates output_dir when the token is supplied inline, and the
    // directory must not be readable by other users.
    struct stat output_status{};
    ASSERT_EQ(::stat(output_dir.c_str(), &output_status), 0);
    EXPECT_EQ(output_status.st_mode & 0777, 0700);
    EXPECT_TRUE(recorder.StopAsync());
    // The stop thread stays joinable after it finishes, so a second call has to
    // reap it and report that the recorder is idle rather than claim another
    // stop is running. Without the wait the check below races the first stop.
    for (int attempt = 0; attempt < 200; ++attempt) {
        if (recorder.GetStatus().state != "stopping") {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_FALSE(recorder.StopAsync());
    recorder.Stop();
    EXPECT_FALSE(recorder.StopAsync());
}

TEST(TrafficRecorderTest, CountsRedactionFailuresSeparately) {
    TempDir dir;
    const std::uint32_t route = coro_rpc::func_id<&TrafficTestService::Echo>();
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string() << R"(","routes":[")"
            << route
            << R"("],"redaction":"redacted","control":{"host":"127.0.0.1",)"
            << R"("port":38019,"token":"test-token"}})";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    RedactionRegistry registry;
    registry.Add(route, std::make_shared<FailingRedactor>());
    recorder.Install(1, registry);
    ASSERT_EQ(recorder.GetStatus().state, "idle");
    ASSERT_TRUE(recorder.Start());
    const std::string payload = "payload";
    recorder.OnRequest(RequestView{route, payload, 1, 1, 0});
    recorder.Stop();

    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.dropped_requests, 0u);
    EXPECT_EQ(status.redaction_failed_requests, 1u);
    EXPECT_FALSE(status.capture_complete);

    std::ifstream summary_input(status.session_dir + "/summary.json");
    const std::string summary((std::istreambuf_iterator<char>(summary_input)),
                              std::istreambuf_iterator<char>());
    EXPECT_NE(summary.find("\"redaction_failed_requests\":1"),
              std::string::npos);
}

TEST(TrafficRecorderTest, BatchesMetadataRecordsIntoOneBlock) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string()
            << R"(","redaction":"metadata","control":{"host":"127.0.0.1",)"
            << R"("port":38021,"token":"test-token"}})";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_EQ(recorder.GetStatus().state, "idle");
    ASSERT_TRUE(recorder.Start());
    for (std::uint32_t seq = 0; seq < 3; ++seq) {
        recorder.OnRequest(RequestView{7, "payload", 1, seq, 0});
    }
    recorder.Stop();

    const RecorderStatus status = recorder.GetStatus();
    ASSERT_EQ(status.recorded_requests, 3u);
    ASSERT_FALSE(status.session_dir.empty());

    std::ifstream input(status.session_dir + "/segment-000000.mcr",
                        std::ios::binary);
    ASSERT_TRUE(input);
    const std::string file((std::istreambuf_iterator<char>(input)),
                           std::istreambuf_iterator<char>());
    // Metadata records carry headers only and are batched into a single block
    // instead of one block per record.
    TrafficBlockHeader block{};
    std::memcpy(&block, file.data() + kTrafficFileHeaderSize, sizeof(block));
    EXPECT_EQ(block.magic, kTrafficBlockMagic);
    EXPECT_EQ(block.record_count, 3u);
    EXPECT_EQ(block.block_bytes,
              static_cast<std::uint32_t>(3 * sizeof(TrafficRecordHeader)));
    for (std::uint32_t index = 0; index < 3; ++index) {
        TrafficRecordHeader record{};
        std::memcpy(&record,
                    file.data() + kTrafficFileHeaderSize +
                        kTrafficBlockHeaderSize + index * sizeof(record),
                    sizeof(record));
        EXPECT_EQ(record.total_len, static_cast<std::uint32_t>(sizeof(record)));
        EXPECT_EQ(record.payload_len, 0u);
        EXPECT_EQ(record.route_key, 7u);
        EXPECT_EQ(record.seq_num, index);
    }
}

TEST(TrafficRecorderTest, RejectsConfigurationThatWouldFailOpen) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](const std::string& extra) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string() << '"';
        if (!extra.empty()) {
            out << ',' << extra;
        }
        out << '}';
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    // A misspelled key would otherwise be ignored and the intended limit lost.
    write_config(R"("max_total_byte":1024)");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A non-string redaction would otherwise fall back to "none" and write
    // request bodies the operator expected to be redacted.
    write_config(R"("redaction":true)");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // Record lengths travel as 32-bit fields, so a larger block is truncated.
    write_config(R"("block_bytes":4294967296,"segment_bytes":4294967296)");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A block above the reader's bound would be written but never replayed.
    write_config(R"("block_bytes":134217728,"segment_bytes":268435456)");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A string where an object belongs would otherwise be ignored and the
    // control server started on the default endpoint.
    write_config(R"("control":"127.0.0.1:37901")");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A string where an array belongs would otherwise be ignored and every
    // route recorded.
    write_config(R"("routes":"PutStart")");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // An empty allowlist would otherwise record every route.
    write_config(R"("routes":[])");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    // A wildcard cannot be combined with explicit routes.
    write_config(R"("routes":["*","1"])");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
}

TEST(TrafficRecorderTest, ControlTokenUsesSecureFilePermissions) {
    TempDir dir;
    const auto ports = getFreeTcpPorts(3);
    ASSERT_EQ(ports.size(), 3u);
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](int control_port,
                                  const std::string& token_file) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": )"
            << control_port;
        if (!token_file.empty()) {
            out << R"(, "token_file": ")" << token_file << '"';
        }
        out << "}}";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    write_config(ports[0], {});
    recorder.Install(1);
    ASSERT_EQ(recorder.GetStatus().state, "idle");
    const auto token_path = dir.path() / "control.token";
    struct stat token_stat{};
    ASSERT_EQ(::stat(token_path.c_str(), &token_stat), 0);
    EXPECT_EQ(token_stat.st_mode & 0777, 0600);
    std::ifstream token_input(token_path);
    std::string token;
    std::getline(token_input, token);
    EXPECT_EQ(token.size(), 32u);
    recorder.Shutdown();

    {
        std::ofstream out(token_path, std::ios::trunc);
        out << "partial-token\n";
    }
    ASSERT_EQ(::chmod(token_path.c_str(), 0600), 0);
    write_config(ports[1], {});
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    const auto insecure_token_path = dir.path() / "insecure.token";
    {
        std::ofstream out(insecure_token_path);
        out << "test-token\n";
    }
    ASSERT_EQ(::chmod(insecure_token_path.c_str(), 0644), 0);
    write_config(ports[2], insecure_token_path.string());
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
}

TEST(TrafficRecorderTest, CoroutineAdapterFiltersRequestAttachments) {
    TempDir dir;
    const auto ports = getFreeTcpPorts(2);
    ASSERT_EQ(ports.size(), 2u);
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto route_key = coro_rpc::func_id<&TrafficTestService::Echo>();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 65536,
          "max_record_bytes": 4096,
          "block_bytes": 4096,
          "segment_bytes": 32768,
          "routes": [")"
            << route_key << R"("],
          "control": {"host": "127.0.0.1", "port": )"
            << ports[1] << R"(,
                      "token": "test-token"}
        })";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::filesystem::path path;
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
            std::filesystem::remove_all(path);
        }
    } cleanup{dir.path(), previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_TRUE(recorder.Start());

    TrafficTestService service;
    RpcWorkerPool pool(1);
    RecordingServer server(1, ports[0], "127.0.0.1", std::chrono::seconds(0),
                           true);
    pool.Register<&TrafficTestService::Echo>(server, service);
    auto server_start = server.async_start();
    ASSERT_FALSE(server_start.hasResult());

    coro_rpc::coro_rpc_client client;
    auto ec = async_simple::coro::syncAwait(
        client.connect("127.0.0.1", std::to_string(ports[0])));
    ASSERT_FALSE(ec) << ec.message();
    auto response = async_simple::coro::syncAwait(
        client.call<&TrafficTestService::Echo>(7));
    ASSERT_TRUE(response.has_value()) << response.error().msg;
    EXPECT_EQ(response.value(), 7);

    client.set_req_attachment("attachment");
    auto attached_response = async_simple::coro::syncAwait(
        client.call<&TrafficTestService::Echo>(8));
    ASSERT_TRUE(attached_response.has_value()) << attached_response.error().msg;
    EXPECT_EQ(attached_response.value(), 8);

    server.stop();
    recorder.Stop();
    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.dropped_requests, 1u);
    EXPECT_FALSE(status.capture_complete);
}

TEST(TrafficRecorderTest, RecordsAcrossWrapAndRestarts) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "segment_seconds": 300,
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": 37999,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_TRUE(recorder.Start());
    const std::string payload(128, 'x');
    for (int i = 0; i < 5000; ++i) {
        recorder.OnRequest(
            RequestView{1, payload, 1, static_cast<std::uint32_t>(i), 0});
    }
    recorder.Stop();
    const RecorderStatus first = recorder.GetStatus();
    EXPECT_GT(first.recorded_requests, 0u);
    EXPECT_FALSE(first.session_dir.empty());
    EXPECT_TRUE(std::filesystem::exists(first.session_dir + "/summary.json"));

    ASSERT_TRUE(recorder.Start());
    for (int i = 0; i < 100; ++i) {
        recorder.OnRequest(
            RequestView{1, payload, 2, static_cast<std::uint32_t>(i), 0});
    }
    recorder.Stop();
    const RecorderStatus second = recorder.GetStatus();
    EXPECT_GT(second.recorded_requests, 0u);
    EXPECT_NE(second.session_dir, first.session_dir);

    recorder.Shutdown();
}

TEST(TrafficRecorderTest, WriterFailureCanBeStoppedAndRestarted) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "max_record_bytes": 2048,
          "block_bytes": 2048,
          "segment_bytes": 4096,
          "segment_seconds": 300,
          "routes": ["1"],
          "control": {"host": "127.0.0.1", "port": 38001,
                      "token": "test-token"}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_TRUE(recorder.Start());
    // A directory where the next segment file belongs makes rotation fail.
    const std::string session_dir = recorder.GetStatus().session_dir;
    ASSERT_FALSE(session_dir.empty());
    ASSERT_TRUE(
        std::filesystem::create_directory(session_dir + "/segment-000001.mcr"));
    const std::string payload(512, 'y');
    for (int i = 0; i < 2000; ++i) {
        recorder.OnRequest(
            RequestView{1, payload, 3, static_cast<std::uint32_t>(i), 0});
    }
    for (int i = 0; i < 200 && recorder.GetStatus().state != "error"; ++i) {
        usleep(10000);
    }
    EXPECT_EQ(recorder.GetStatus().state, "error");

    recorder.Stop();
    const RecorderStatus failed = recorder.GetStatus();
    EXPECT_FALSE(failed.format_complete);
    EXPECT_FALSE(failed.capture_complete);

    ASSERT_TRUE(recorder.Start());
    recorder.OnRequest(RequestView{1, payload, 4, 0, 0});
    recorder.Stop();
    EXPECT_EQ(recorder.GetStatus().state, "idle");
    recorder.Shutdown();
}

TEST(TrafficRecorderTest, MinimalConfigurationRecordsAllRoutes) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "max_total_bytes": 1073741824
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    unsetenv("MOONCAKE_TRAFFIC_AUTOSTART");
    setenv("MOONCAKE_TRAFFIC_CONTROL_PORT", "37998", 1);
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_EQ(recorder.GetStatus().state, "recording");

    const std::string payload = "minimal";
    recorder.OnRequest(RequestView{123, payload, 1, 1, 0});
    recorder.Stop();
    const RecorderStatus status = recorder.GetStatus();
    EXPECT_EQ(status.state, "idle");
    EXPECT_EQ(status.observed_requests, 1u);
    EXPECT_EQ(status.recorded_requests, 1u);
    EXPECT_EQ(status.dropped_requests, 0u);

    const std::filesystem::path token_path = dir.path() / "control.token";
    ASSERT_TRUE(std::filesystem::exists(token_path));
    struct stat token_stat{};
    ASSERT_EQ(::stat(token_path.c_str(), &token_stat), 0);
    EXPECT_EQ(token_stat.st_mode & 0777, 0600);

    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONFIG");
    unsetenv("MOONCAKE_TRAFFIC_CONTROL_PORT");
}

TEST(TrafficRecorderTest, StopsAtTheSessionSizeLimit) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")"
            << dir.path().string() << R"(",
          "block_bytes": 4096,
          "segment_bytes": 4096,
          "max_record_bytes": 4096,
          "max_total_bytes": 4096,
          "control": {"port": 38022, "token": "size-limit-token"}
        })";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);
    unsetenv("MOONCAKE_TRAFFIC_AUTOSTART");
    auto& recorder = TrafficRecorder::Instance();
    recorder.Install(1);
    ASSERT_EQ(recorder.GetStatus().state, "recording");

    const std::string payload(3000, 'x');
    recorder.OnRequest(RequestView{11, payload, 1, 1, 0});
    recorder.OnRequest(RequestView{11, payload, 1, 2, 0});

    RecorderStatus status = recorder.GetStatus();
    for (int attempt = 0; attempt < 200 && status.state != "idle"; ++attempt) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        status = recorder.GetStatus();
    }
    EXPECT_EQ(status.state, "idle");
    EXPECT_EQ(status.recorded_requests, 2u);
    EXPECT_EQ(status.dropped_requests, 0u);
    EXPECT_TRUE(status.format_complete);
    EXPECT_FALSE(status.capture_complete);

    recorder.Stop();
    status = recorder.GetStatus();
    EXPECT_EQ(status.state, "idle");
    EXPECT_TRUE(status.format_complete);
    EXPECT_FALSE(status.capture_complete);

    const std::filesystem::path segment =
        std::filesystem::path(status.session_dir) / "segment-000000.mcr";
    ASSERT_TRUE(std::filesystem::exists(segment));
    EXPECT_EQ(std::filesystem::file_size(segment),
              kTrafficFileHeaderSize + kTrafficBlockHeaderSize +
                  kTrafficRecordHeaderSize + payload.size() +
                  kTrafficFooterSize);
    std::ifstream summary(status.session_dir + "/summary.json");
    ASSERT_TRUE(summary);
    const std::string text((std::istreambuf_iterator<char>(summary)),
                           std::istreambuf_iterator<char>());
    EXPECT_NE(text.find("\"capture_complete\":false"), std::string::npos);

    recorder.Shutdown();
    if (previous_config) {
        setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
    } else {
        unsetenv("MOONCAKE_TRAFFIC_CONFIG");
    }
}

TEST(TrafficRecorderTest, RejectsUnknownRouteName) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](const std::string& routes) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string() << R"(","routes":)"
            << routes
            << R"(,"control":{"host":"127.0.0.1","port":38021,"token":"test-token"}})";
    };
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    // coro_rpc reports the handler it stores, which for a pooled handler is the
    // pool's coroutine rather than the service method, so the registry is what
    // tells the recorder which name belongs to a route.
    RouteNameRegistry names;
    names.Add(coro_rpc::func_id<&TrafficTestService::Echo>(),
              coro_rpc::get_func_name<&TrafficTestService::Echo>());

    auto& recorder = TrafficRecorder::Instance();
    write_config(R"(["Echo"])");
    recorder.Install(1, {}, names);
    EXPECT_EQ(recorder.GetStatus().state, "idle");
    recorder.Shutdown();

    write_config(R"(["Echo", "NoSuchMethod"])");
    recorder.Install(1, {}, names);
    EXPECT_EQ(recorder.GetStatus().state, "error");
}

TEST(TrafficRecorderTest, RejectsInvalidControlPort) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    const auto write_config = [&](const std::string& control) {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string()
            << R"(","routes":["*"],"control":)" << control << "}";
    };
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    write_config(R"({"port":0,"token":"test-token"})");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    write_config(R"({"port":"not-a-port","token":"test-token"})");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();

    write_config(R"({"port":38017,"token":"test-token"})");
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONTROL_PORT", "not-a-port", 1), 0);
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "error");
    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONTROL_PORT");

    write_config(R"({"port":38017,"token":"test-token"})");
    recorder.Install(1);
    EXPECT_EQ(recorder.GetStatus().state, "idle");
    recorder.Shutdown();
    unsetenv("MOONCAKE_TRAFFIC_CONFIG");
}

TEST(TrafficRecorderTest, WritesRouteNamesSidecar) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path, std::ios::trunc);
        out << R"({"output_dir":")" << dir.path().string()
            << R"(","routes":["BatchGetReplicaList"],"control":{"host":"127.0.0.1","port":38013,"token":"test-token"}})";
    }
    const char* old_config = std::getenv("MOONCAKE_TRAFFIC_CONFIG");
    const std::optional<std::string> previous_config =
        old_config != nullptr ? std::optional<std::string>(old_config)
                              : std::nullopt;
    struct Cleanup {
        std::optional<std::string> previous_config;
        ~Cleanup() {
            TrafficRecorder::Instance().Shutdown();
            if (previous_config) {
                setenv("MOONCAKE_TRAFFIC_CONFIG", previous_config->c_str(), 1);
            } else {
                unsetenv("MOONCAKE_TRAFFIC_CONFIG");
            }
        }
    } cleanup{previous_config};
    ASSERT_EQ(setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1), 0);

    auto& recorder = TrafficRecorder::Instance();
    RouteNameRegistry names;
    names.Add(
        7,
        coro_rpc::get_func_name<&WrappedMasterService::BatchGetReplicaList>());
    recorder.Install(1, {}, names);
    ASSERT_TRUE(recorder.Start());
    const std::string payload = "payload";
    recorder.OnRequest(RequestView{7, payload, 1, 1, 0});
    recorder.Stop();

    const std::string session_dir = recorder.GetStatus().session_dir;
    std::ifstream in(session_dir + "/routes.json");
    ASSERT_TRUE(in);
    const std::string contents((std::istreambuf_iterator<char>(in)),
                               std::istreambuf_iterator<char>());
    EXPECT_NE(contents.find("BatchGetReplicaList"), std::string::npos);
    EXPECT_NE(contents.find("\"7\""), std::string::npos);
}

TEST(TrafficRedactionTest, MasksPutStartFieldsAndKeepsPayloadDecodable) {
    RedactionRegistry registry;
    RegisterMasterRedactors(registry);

    ReplicateConfig config;
    config.preferred_segments = {"segment-a"};
    config.preferred_segment = "segment-b";
    config.preferred_nof_segments = {"nof-a"};
    config.host_id = "host-a";
    config.group_ids = std::vector<std::string>{"group-a"};
    config.session_ids = std::vector<std::string>{"session-a"};
    config.preferred_hca_names = std::vector<std::string>{"mlx5_0"};
    config.per_key_preferred_hca_names =
        std::vector<std::vector<std::string>>{{"mlx5_0"}};
    config.residency_affinity_ids = std::vector<std::string>{"residency-a"};

    using Args = std::tuple<UUID, std::string, std::uint64_t, ReplicateConfig,
                            std::string>;
    const auto serialized = struct_pack::serialize(
        Args{{1, 2}, "object-key", 4096, config, "tenant-a"});
    const std::string payload(serialized.data(), serialized.size());

    Args original{};
    ASSERT_EQ(struct_pack::deserialize_to(original, payload),
              struct_pack::errc::ok);
    ASSERT_TRUE(std::get<3>(original).session_ids.has_value());

    std::string redacted;
    ASSERT_TRUE(
        registry.Redact(coro_rpc::func_id<&WrappedMasterService::PutStart>(),
                        payload, redacted));
    ASSERT_NE(redacted, payload);

    Args args{};
    ASSERT_EQ(struct_pack::deserialize_to(args, redacted),
              struct_pack::errc::ok);
    const ReplicateConfig& masked = std::get<3>(args);
    const auto is_token = [](const std::string& value) {
        if (value.size() != 20 || value.rfind("<r:", 0) != 0 ||
            value.back() != '>') {
            return false;
        }
        return value.find_first_not_of("0123456789abcdef", 3) == 19;
    };
    EXPECT_TRUE(is_token(std::get<1>(args)));
    EXPECT_TRUE(is_token(std::get<4>(args)));
    EXPECT_TRUE(is_token(masked.preferred_segment));
    EXPECT_TRUE(is_token(masked.host_id));
    ASSERT_TRUE(masked.group_ids.has_value());
    EXPECT_TRUE(is_token(masked.group_ids->at(0)));
    ASSERT_TRUE(masked.session_ids.has_value());
    EXPECT_TRUE(is_token(masked.session_ids->at(0)));

    // Distinct strings keep distinct tokens, so the key cardinality of the
    // recorded workload survives into a session that leaves the machine.
    EXPECT_NE(std::get<1>(args), std::get<4>(args));
    EXPECT_NE(masked.preferred_segment, masked.host_id);
    EXPECT_NE(masked.group_ids->at(0), masked.session_ids->at(0));
    EXPECT_NE(masked.preferred_segments.at(0),
              masked.preferred_nof_segments.at(0));
    ASSERT_TRUE(masked.residency_affinity_ids.has_value());
    EXPECT_NE(masked.residency_affinity_ids->at(0), masked.host_id);

    // The original strings are gone.
    EXPECT_EQ(redacted.find("object-key"), std::string::npos);
    EXPECT_EQ(redacted.find("tenant-a"), std::string::npos);
    EXPECT_EQ(redacted.find("segment-a"), std::string::npos);

    // The token is a function of the content, so the same payload always
    // produces the same bytes.
    std::string again;
    ASSERT_TRUE(registry.Redact(
        coro_rpc::func_id<&WrappedMasterService::PutStart>(), payload, again));
    EXPECT_EQ(again, redacted);

    const auto other_serialized = struct_pack::serialize(
        Args{{1, 2}, "another-key", 4096, config, "tenant-a"});
    const std::string other_payload(other_serialized.data(),
                                    other_serialized.size());
    std::string other_redacted;
    ASSERT_TRUE(
        registry.Redact(coro_rpc::func_id<&WrappedMasterService::PutStart>(),
                        other_payload, other_redacted));
    EXPECT_NE(other_redacted, redacted);
}

TEST(TrafficRedactionTest, MasksBatchUpsertEndKeys) {
    RedactionRegistry registry;
    RegisterMasterRedactors(registry);

    std::vector<ObjectMeta> metas;
    metas.push_back(ObjectMeta{"key-one", std::nullopt});
    metas.push_back(ObjectMeta{"key-two", std::nullopt});
    using Args = std::tuple<UUID, std::vector<ObjectMeta>, std::string>;
    const auto serialized =
        struct_pack::serialize(Args{{3, 4}, metas, "tenant-b"});
    const std::string payload(serialized.data(), serialized.size());

    std::string redacted;
    ASSERT_TRUE(registry.Redact(
        coro_rpc::func_id<&WrappedMasterService::BatchUpsertEnd>(), payload,
        redacted));

    Args args{};
    ASSERT_EQ(struct_pack::deserialize_to(args, redacted),
              struct_pack::errc::ok);
    ASSERT_EQ(std::get<1>(args).size(), 2u);
    EXPECT_NE(std::get<1>(args)[0].key, "key-one");
    EXPECT_NE(std::get<1>(args)[1].key, "key-two");
    EXPECT_NE(std::get<1>(args)[0].key, std::get<1>(args)[1].key);
    EXPECT_EQ(redacted.find("key-one"), std::string::npos);
    EXPECT_EQ(redacted.find("tenant-b"), std::string::npos);
}

}  // namespace
}  // namespace mooncake::traffic
