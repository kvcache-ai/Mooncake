#include "utils.h"

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <set>

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

TEST(UtilsTest, GetRandomAvailablePort) {
    // Test with default parameters
    int port = getRandomAvailablePort();
    EXPECT_GE(port, 12300);
    EXPECT_LE(port, 14300);
    EXPECT_TRUE(isPortAvailable(port));
}

TEST(UtilsTest, GetRandomAvailablePortCustomRange) {
    // Test with custom range
    int port = getRandomAvailablePort(20000, 20010);
    if (port != -1) {  // If we found a port
        EXPECT_GE(port, 20000);
        EXPECT_LE(port, 20010);
        EXPECT_TRUE(isPortAvailable(port));
    }
}

TEST(UtilsTest, GetRandomAvailablePortInvalidRange) {
    // Test with invalid range (min > max)
    int port = getRandomAvailablePort(14300, 12300);
    // Should still work by swapping or handling gracefully
    EXPECT_TRUE(port == -1 || (port >= 12300 && port <= 14300));
}

TEST(UtilsTest, GetRandomAvailablePortReturnsValidPorts) {
    // Test that multiple calls return valid, potentially different ports
    std::set<int> ports_found;
    for (int i = 0; i < 5; ++i) {
        int port = getRandomAvailablePort(30000, 30100);
        if (port != -1) {
            EXPECT_TRUE(isPortAvailable(port));
            ports_found.insert(port);
        }
    }

    // We should find at least some available ports in this range
    EXPECT_GT(ports_found.size(), 0u);
}
