#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "traffic/file_sink.h"
#include "traffic/traffic_format.h"
#include "traffic/traffic_recorder.h"

namespace mooncake::traffic {
namespace {

class TempDir {
   public:
    TempDir() {
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

TEST(TrafficFormatTest, HeaderSizesAreStable) {
    EXPECT_EQ(sizeof(TrafficFileHeader), kTrafficFileHeaderSize);
    EXPECT_EQ(sizeof(TrafficBlockHeader), kTrafficBlockHeaderSize);
    EXPECT_EQ(sizeof(TrafficRecordHeader), kTrafficRecordHeaderSize);
    EXPECT_EQ(sizeof(TrafficFooter), kTrafficFooterSize);
}

TEST(FileSinkTest, StopsAtMaxTotalBytes) {
    TempDir dir;
    FileSink sink(4096, 300, 1024);
    ASSERT_TRUE(sink.OpenSession(dir.path().string(), TrafficNowMs()));
    std::array<char, 512> data{};
    ASSERT_TRUE(sink.WriteBlock(data.data(), data.size(), 1, kTrafficBlockData));
    EXPECT_FALSE(
        sink.WriteBlock(data.data(), data.size() * 2, 1, kTrafficBlockData));
    EXPECT_TRUE(sink.failed());
    sink.Close();
}

TEST(TrafficRecorderTest, RecordsAcrossWrapAndRestarts) {
    TempDir dir;
    const std::string config_path = (dir.path() / "traffic.json").string();
    {
        std::ofstream out(config_path);
        out << R"({
          "output_dir": ")" << dir.path().string() << R"(",
          "ring_bytes": 262144,
          "ring_shards": 2,
          "max_record_bytes": 2048,
          "block_bytes": 4096,
          "segment_bytes": 65536,
          "segment_seconds": 300,
          "routes": ["*"],
          "control": {"host": "127.0.0.1", "port": 37999}
        })";
    }
    setenv("MOONCAKE_TRAFFIC_CONFIG", config_path.c_str(), 1);

    auto& recorder = TrafficRecorder::Instance();
    recorder.Install();
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

}  // namespace
}  // namespace mooncake::traffic
