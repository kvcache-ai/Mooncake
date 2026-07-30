#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>

#include "http_metadata_server.h"
#include "utils.h"

namespace mooncake::testing {

class HttpMetadataServerTest : public ::testing::Test {
   protected:
    struct HttpResponse {
        int status;
        std::string body;
    };

    HttpResponse Request(const std::string& method, int port,
                         const std::string& path,
                         const std::string& body = "") {
        const int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            throw std::runtime_error("failed to create HTTP test socket");
        }

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(static_cast<uint16_t>(port));
        inet_pton(AF_INET, "127.0.0.1", &address.sin_addr);
        if (connect(fd, reinterpret_cast<sockaddr*>(&address),
                    sizeof(address)) != 0) {
            close(fd);
            throw std::runtime_error("failed to connect HTTP test socket");
        }

        std::string request = method + " " + path +
                              " HTTP/1.1\r\nHost: 127.0.0.1\r\n"
                              "Connection: close\r\n";
        if (!body.empty()) {
            request += "Content-Type: application/json\r\nContent-Length: " +
                       std::to_string(body.size()) + "\r\n";
        }
        request += "\r\n" + body;

        size_t sent = 0;
        while (sent < request.size()) {
            const ssize_t bytes =
                send(fd, request.data() + sent, request.size() - sent, 0);
            if (bytes <= 0) {
                close(fd);
                throw std::runtime_error("failed to send HTTP test request");
            }
            sent += static_cast<size_t>(bytes);
        }

        std::string raw_response;
        char buffer[4096];
        while (true) {
            const ssize_t bytes = recv(fd, buffer, sizeof(buffer), 0);
            if (bytes < 0) {
                close(fd);
                throw std::runtime_error("failed to read HTTP test response");
            }
            if (bytes == 0) {
                break;
            }
            raw_response.append(buffer, static_cast<size_t>(bytes));
        }
        close(fd);

        const size_t status_begin = raw_response.find(' ');
        const size_t status_end = raw_response.find(' ', status_begin + 1);
        const size_t body_begin = raw_response.find("\r\n\r\n");
        if (status_begin == std::string::npos ||
            status_end == std::string::npos ||
            body_begin == std::string::npos) {
            throw std::runtime_error("malformed HTTP test response");
        }
        return {std::stoi(raw_response.substr(status_begin + 1,
                                              status_end - status_begin - 1)),
                raw_response.substr(body_begin + 4)};
    }

    HttpResponse Get(int port, const std::string& path) {
        return Request("GET", port, path);
    }

    HttpResponse Put(int port, const std::string& path,
                     const std::string& body) {
        return Request("PUT", port, path, body);
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

TEST_F(HttpMetadataServerTest, RemoteClientDeletesUrlEncodedKey) {
    int port = getFreeTcpPort();
    HttpMetadataServer server(static_cast<uint16_t>(port), "127.0.0.1");
    ASSERT_TRUE(server.start());
    WaitUntilReady(port);

    const std::string key = "mooncake/ram/host name:123?x=1&y=2";
    const std::string path =
        "/metadata?key=mooncake%2Fram%2Fhost%20name%3A123%3Fx%3D1%26y%3D2";
    ASSERT_EQ(Put(port, path, R"({"kind":"ram"})").status, 200);

    HttpMetadataClient client("http://127.0.0.1:" + std::to_string(port) +
                              "/metadata");
    EXPECT_TRUE(client.removeKey(key));
    EXPECT_EQ(Get(port, path).status, 404);

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
