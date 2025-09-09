#include "utils.h"

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <errno.h>

using namespace mooncake;

TEST(UtilsTest, ByteSizeToString) {
    EXPECT_EQ(byte_size_to_string(999), "999 B");
    EXPECT_EQ(byte_size_to_string(2048), "2.00 KB");
    EXPECT_EQ(byte_size_to_string(5ULL * 1024 * 1024 + 1234), "5.00 MB");
    EXPECT_EQ(byte_size_to_string(15ULL * 1024 * 1024 * 1024), "15.00 GB");
    EXPECT_EQ(byte_size_to_string(0), "0 B");
    EXPECT_EQ(byte_size_to_string(1), "1 B");
    EXPECT_EQ(byte_size_to_string(1024), "1.00 KB");
    EXPECT_EQ(byte_size_to_string(1024 * 1024), "1.00 MB");
    EXPECT_EQ(byte_size_to_string(1024ULL * 1024 * 1024), "1.00 GB");
    EXPECT_EQ(byte_size_to_string(1024ULL * 1024 * 1024 * 1024), "1.00 TB");
    EXPECT_EQ(byte_size_to_string(15 * 1024 + 134), "15.13 KB");
    EXPECT_EQ(byte_size_to_string(15 * 1024 * 1024 + 44048), "15.04 MB");
}

TEST(UtilsTest, IsPortAvailable) {
    // Find an available port
    int test_port = -1;
    for (int port = 50000; port < 50010; ++port) {
        if (isPortAvailable(port)) {
            test_port = port;
            break;
        }
    }
    ASSERT_NE(test_port, -1) << "Could not find available port for testing";

    // Initially the port should be available
    EXPECT_TRUE(isPortAvailable(test_port));

    // Bind to the port to make it unavailable
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_NE(sock, -1);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(test_port);

    ASSERT_EQ(bind(sock, (struct sockaddr*)&addr, sizeof(addr)), 0);
    ASSERT_EQ(listen(sock, 1), 0);

    // Now the port should be unavailable
    EXPECT_FALSE(isPortAvailable(test_port));

    // Clean up
    close(sock);

    // Port should be available again
    EXPECT_TRUE(isPortAvailable(test_port));
}

TEST(UtilsTest, IsPortAvailableWithBindingConflict) {
    // Find an available port
    int test_port = -1;
    for (int port = 40000; port < 40010; ++port) {
        if (isPortAvailable(port)) {
            test_port = port;
            break;
        }
    }

    ASSERT_NE(test_port, -1) << "Could not find available port for testing";

    // Initially the port should be available
    EXPECT_TRUE(isPortAvailable(test_port));

    // Bind to the port to make it unavailable
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_NE(sock, -1);

    int opt = 1;
    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(test_port);

    ASSERT_EQ(bind(sock, (struct sockaddr*)&addr, sizeof(addr)), 0);
    ASSERT_EQ(listen(sock, 1), 0);

    // Now the port should be unavailable
    EXPECT_FALSE(isPortAvailable(test_port));

    // Clean up
    close(sock);

    // Port should be available again
    EXPECT_TRUE(isPortAvailable(test_port));
}

TEST(UtilsTest, AutoPortBinderMultipleInstances) {
    // Test that multiple binders get different ports
    AutoPortBinder binder1;
    AutoPortBinder binder2;

    int port1 = binder1.getPort();
    int port2 = binder2.getPort();

    EXPECT_GT(port1, 0);
    EXPECT_GT(port2, 0);

    EXPECT_GE(port1, 12300);  // Should be in default range
    EXPECT_LE(port1, 14300);
    EXPECT_GE(port2, 12300);  // Should be in default range
    EXPECT_LE(port2, 14300);

    // If both successfully bound, they should have different ports
    EXPECT_NE(port1, port2);
}

TEST(UtilsTest, SplitStringBasic) {
    std::string input = "a, b ,c, d";
    auto tokens = splitString(input, ',', true, false);

    ASSERT_EQ(tokens.size(), 4);
    EXPECT_EQ(tokens[0], "a");
    EXPECT_EQ(tokens[1], "b");
    EXPECT_EQ(tokens[2], "c");
    EXPECT_EQ(tokens[3], "d");
}

TEST(UtilsTest, AutoPortBinderRAII) {
    // Test RAII behavior - port should be released when binder is destroyed
    int port;
    {
        AutoPortBinder binder;
        port = binder.getPort();
        EXPECT_GT(port, 0);  // Should successfully get a port

        // Try to bind to the same port - should fail due to conflict
        int test_socket = socket(AF_INET, SOCK_STREAM, 0);
        ASSERT_GE(test_socket, 0);

        sockaddr_in addr = {};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        // This should fail because port is already bound by AutoPortBinder
        int bind_result = bind(test_socket, (sockaddr*)&addr, sizeof(addr));
        EXPECT_EQ(bind_result, -1);    // Should fail
        EXPECT_EQ(errno, EADDRINUSE);  // Should be "address already in use"

        close(test_socket);

    }  // AutoPortBinder destroyed here, port should be released

    // Now try to bind to the same port again - should succeed
    int test_socket2 = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(test_socket2, 0);

    sockaddr_in addr2 = {};
    addr2.sin_family = AF_INET;
    addr2.sin_addr.s_addr = INADDR_ANY;
    addr2.sin_port = htons(port);

    // This should succeed because port was released
    int bind_result2 = bind(test_socket2, (sockaddr*)&addr2, sizeof(addr2));
    EXPECT_EQ(bind_result2, 0);  // Should succeed

    close(test_socket2);
}

TEST(UtilsTest, AutoPortBinderCustomRange) {
    // Test custom port range
    AutoPortBinder binder(50000, 50100);
    int port = binder.getPort();

    EXPECT_GE(port, 50000);
    EXPECT_LE(port, 50100);
}
