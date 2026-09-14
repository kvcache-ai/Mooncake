#pragma once

#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

namespace mooncake {

bool isPortAvailable(int port);

class AutoPortBinder {
   public:
    AutoPortBinder(int min_port = 12300, int max_port = 14300);
    ~AutoPortBinder();

    AutoPortBinder(const AutoPortBinder&) = delete;
    AutoPortBinder& operator=(const AutoPortBinder&) = delete;
    AutoPortBinder(AutoPortBinder&&) = delete;
    AutoPortBinder& operator=(AutoPortBinder&&) = delete;

    int getPort() const { return port_; }

   private:
    int socket_fd_;
    int port_;
};

tl::expected<std::string, int> httpGet(const std::string& url);

tl::expected<std::string, std::string> GetInterfaceIPv4Address(
    const std::string& interface_name);

int getFreeTcpPort();

std::vector<int> getFreeTcpPorts(int count);

std::string ResolveMooncakeHostId(const std::string& local_hostname);

}  // namespace mooncake
