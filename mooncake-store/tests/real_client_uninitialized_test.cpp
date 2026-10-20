#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

#include "real_client.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        if (const char* value = std::getenv(name)) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

const int kInvalidParams = toInt(ErrorCode::INVALID_PARAMS);

TEST(RealClientUninitializedTest, BatchApisReturnErrorsWithoutSetup) {
    auto client = RealClient::create();

    const std::vector<std::string> keys = {"k0", "k1"};
    std::vector<char> buffer0(16);
    std::vector<char> buffer1(16);
    const std::vector<void*> buffers = {buffer0.data(), buffer1.data()};
    const std::vector<size_t> sizes = {buffer0.size(), buffer1.size()};
    const std::vector<std::vector<void*>> all_buffers = {{buffer0.data()},
                                                         {buffer1.data()}};
    const std::vector<std::vector<size_t>> all_sizes = {{buffer0.size()},
                                                        {buffer1.size()}};
    const std::vector<int> int_errors(keys.size(), kInvalidParams);
    const std::vector<int64_t> int64_errors(keys.size(), kInvalidParams);

    const auto handles = client->batch_get_buffer(keys);
    ASSERT_EQ(handles.size(), keys.size());
    for (const auto& handle : handles) {
        EXPECT_EQ(handle, nullptr);
    }

    const auto ranges = client->get_into_ranges(
        {buffer0.data()}, {keys}, {{{0}, {8}}}, {{{0}, {0}}}, {{{8}, {8}}});
    const std::vector<std::vector<std::vector<int64_t>>> range_errors = {
        {{kInvalidParams}, {kInvalidParams}}};
    EXPECT_EQ(ranges, range_errors);

    EXPECT_EQ(client->batch_put_from(keys, buffers, sizes), int_errors);
    EXPECT_EQ(client->batch_upsert_from(keys, buffers, sizes), int_errors);
    EXPECT_EQ(client->batch_get_into(keys, buffers, sizes), int64_errors);
    EXPECT_EQ(client->batch_put_from_multi_buffers(keys, all_buffers, all_sizes,
                                                   ReplicateConfig{}, false),
              int_errors);
    EXPECT_EQ(client->batch_upsert_from_multi_buffers(
                  keys, all_buffers, all_sizes, ReplicateConfig{}, false),
              int_errors);
    EXPECT_EQ(client->batch_get_into_multi_buffers(keys, all_buffers, all_sizes,
                                                   false),
              int_errors);
}

TEST(RealClientUninitializedTest, FailedSetupLeavesLaterCallsSafe) {
    ScopedEnvVar embedded_master("MOONCAKE_ENABLE_EMBEDDED_MASTER");
    ScopedEnvVar setup_retries("MC_STORE_CLIENT_SETUP_RETRIES");
    setup_retries.Set("1");

    auto client = RealClient::create();
    const ConfigDict config = {
        {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
        {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
        {CONFIG_KEY_PROTOCOL, "tcp"},
        {CONFIG_KEY_MASTER_SERVER_ADDR, "127.0.0.1:1"},
        {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "0"},
        {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"},
    };
    const auto result = client->setup_internal(config);
    ASSERT_FALSE(result.has_value());

    const auto handles = client->batch_get_buffer({"k"});
    ASSERT_EQ(handles.size(), 1u);
    EXPECT_EQ(handles[0], nullptr);
}

}  // namespace
}  // namespace mooncake
