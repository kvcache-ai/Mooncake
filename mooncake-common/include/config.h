#pragma once
#include <string>
#include <cstdint>
#include <glog/logging.h>
#include <infiniband/verbs.h>

namespace mooncake {

struct ClientConfig {
    std::string local_hostname;
    uint64_t global_segment_size;
    uint64_t local_buffer_size;
    std::string master_server_address;
};

struct TransferConfig {
    std::string metadata_server;
    std::string protocol;
    std::string device_name;
};

class MoonCakeConfig {
public:
    void LoadFromJson(const std::string& path);

    void LoadFromYAML(const std::string& path);

private:
    TransferConfig transfer_config_;
    ClientConfig client_config_;
};
}