#include "client_service.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <mutex>
#include <string>

namespace mooncake {
namespace {

class ScopedLogCapture : public google::LogSink {
   public:
    ScopedLogCapture() { google::AddLogSink(this); }
    ~ScopedLogCapture() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity, const char*, const char*, int,
              const struct ::tm*, const char* message,
              size_t message_len) override {
        std::lock_guard<std::mutex> lock(mutex_);
        messages_.append(message, message_len);
        messages_.push_back('\n');
    }

    std::string Messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_;
    }

   private:
    mutable std::mutex mutex_;
    std::string messages_;
};

TEST(ClientHaLoggingTest, InvalidBackendEntryDoesNotLogCredentials) {
    for (const auto* entry : {
             "unsupported://log-review-user:log-review-password@127.0.0.1:6379",
             "etcd://http://log-review-user:log-review-password@127.0.0.1:2379",
         }) {
        SCOPED_TRACE(entry);
        ScopedLogCapture capture;
        auto client =
            Client::Create("127.0.0.1", "", "rpc_only", std::nullopt, entry);
        const auto messages = capture.Messages();

        EXPECT_FALSE(client.has_value());
        EXPECT_NE(messages.find("Invalid HA backend entry"), std::string::npos);
        EXPECT_NE(messages.find("INVALID_PARAMS"), std::string::npos);
        EXPECT_EQ(messages.find("log-review-user"), std::string::npos);
        EXPECT_EQ(messages.find("log-review-password"), std::string::npos);
    }
}

TEST(ClientHaLoggingTest, RedisInitializationFailureDoesNotLogCredentials) {
    for (const auto* entry : {
             "redis://log-review-user:log-review-password@127.0.0.1:6379",
             "redis+cluster://"
             "log-review-user:log-review-password@127.0.0.1:6379",
         }) {
        SCOPED_TRACE(entry);
        ScopedLogCapture capture;
        auto client =
            Client::Create("127.0.0.1", "", "rpc_only", std::nullopt, entry);
        const auto messages = capture.Messages();

        EXPECT_FALSE(client.has_value());
#ifdef STORE_USE_REDIS
        EXPECT_NE(messages.find("Failed to create HA backend coordinator"),
                  std::string::npos);
        EXPECT_NE(messages.find("backend=redis"), std::string::npos);
        EXPECT_NE(messages.find("INVALID_PARAMS"), std::string::npos);
#else
        EXPECT_NE(messages.find("UNAVAILABLE_IN_CURRENT_MODE"),
                  std::string::npos);
#endif
        EXPECT_EQ(messages.find("log-review-user"), std::string::npos);
        EXPECT_EQ(messages.find("log-review-password"), std::string::npos);
    }
}

}  // namespace
}  // namespace mooncake
