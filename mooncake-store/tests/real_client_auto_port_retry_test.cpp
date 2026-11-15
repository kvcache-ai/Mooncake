#include "real_client.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#include "test_server_helpers.h"

namespace mooncake::testing {
namespace {

class FailingHttpMetadataServer {
   public:
    ~FailingHttpMetadataServer() {
        stopping_.store(true);
        if (worker_.joinable()) worker_.join();
        if (listener_ >= 0) close(listener_);
    }

    bool Start() {
        listener_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listener_ < 0) return false;

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        if (bind(listener_, reinterpret_cast<sockaddr*>(&address),
                 sizeof(address)) != 0 ||
            listen(listener_, 16) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }

        socklen_t address_size = sizeof(address);
        if (getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                        &address_size) != 0) {
            close(listener_);
            listener_ = -1;
            return false;
        }
        port_ = ntohs(address.sin_port);
        worker_ = std::thread([this] { Serve(); });
        return true;
    }

    std::string Url() const {
        return "http://127.0.0.1:" + std::to_string(port_) + "/metadata";
    }

    int PutRequests() const { return put_requests_.load(); }

   private:
    void Serve() {
        while (!stopping_.load()) {
            pollfd descriptor{listener_, POLLIN, 0};
            const int ready = poll(&descriptor, 1, 50);
            if (ready <= 0) continue;

            const int client = accept(listener_, nullptr, nullptr);
            if (client < 0) continue;

            std::string request;
            char buffer[4096];
            while (request.find("\r\n\r\n") == std::string::npos) {
                pollfd client_descriptor{client, POLLIN, 0};
                if (poll(&client_descriptor, 1, 1000) <= 0) break;

                const ssize_t received =
                    recv(client, buffer, sizeof(buffer), 0);
                if (received <= 0) break;
                request.append(buffer, received);
            }

            const auto header_end = request.find("\r\n\r\n");
            const auto first_space = request.find(' ');
            if (first_space != std::string::npos &&
                request.compare(0, first_space, "PUT") == 0) {
                put_requests_.fetch_add(1);
            }

            if (header_end != std::string::npos) {
                const std::string response =
                    "HTTP/1.1 500 Internal Server Error\r\n"
                    "Content-Length: 0\r\n"
                    "Connection: close\r\n\r\n";
                (void)send(client, response.data(), response.size(),
                           MSG_NOSIGNAL);
            }
            close(client);
        }
    }

    int listener_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::atomic<int> put_requests_{0};
    std::thread worker_;
};

class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        const char* previous = std::getenv(name);
        if (previous != nullptr) {
            had_previous_ = true;
            previous_ = previous;
        }
        setenv(name, value, 1);
    }

    ~ScopedEnvVar() {
        if (had_previous_) {
            setenv(name_.c_str(), previous_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string previous_;
    bool had_previous_ = false;
};

TEST(RealClientAutoPortRetryTest,
     MetadataWriteFailureDoesNotRetryWithAnotherStorePort) {
    FailingHttpMetadataServer metadata_server;
    ASSERT_TRUE(metadata_server.Start());

    InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));

    ScopedEnvVar retries("MC_STORE_CLIENT_SETUP_RETRIES", "4");

    auto client = RealClient::create();
    const int result =
        client->setup_real("127.0.0.1", metadata_server.Url(), 0, 0, "tcp", "",
                           master.master_address());

    EXPECT_NE(result, 0);
    EXPECT_EQ(metadata_server.PutRequests(), 1);
}

}  // namespace
}  // namespace mooncake::testing
