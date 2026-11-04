#pragma once

#include <string>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

// Network utility functions

/**
 * @brief Check if a TCP port is available for binding
 * @param port The port number to check
 * @return true if port is available, false otherwise
 */
bool isPortAvailable(int port);

// Simple RAII class for automatically binding to an available port
// The socket is bound during construction and released during destruction
class AutoPortBinder {
   public:
    // Constructs binder and attempts to bind to an available port in range
    // [min_port, max_port] After successful construction, the port is bound and
    // reserved until destruction
    AutoPortBinder(int min_port = 12300, int max_port = 14300);
    ~AutoPortBinder();

    // Non-copyable, non-movable
    AutoPortBinder(const AutoPortBinder&) = delete;
    AutoPortBinder& operator=(const AutoPortBinder&) = delete;
    AutoPortBinder(AutoPortBinder&&) = delete;
    AutoPortBinder& operator=(AutoPortBinder&&) = delete;

    int getPort() const { return port_; }

    int rebind();

   private:
    int tryBindPort();

    int socket_fd_;
    int port_;
    int min_port_;
    int max_port_;
};

// HTTP utility: simple GET, returns body on 200, otherwise error code
tl::expected<std::string, int> httpGet(const std::string& url);

// Network utility: obtain an available TCP port on loopback by binding to 0
int getFreeTcpPort();

}  // namespace mooncake
