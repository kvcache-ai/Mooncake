#ifndef MOONCAKE_PG_DEVICE_BOOTSTRAP_H
#define MOONCAKE_PG_DEVICE_BOOTSTRAP_H

#include <cstdint>
#include <string>
#include <vector>

#include <transport/device/device_transport.h>

namespace mooncake {

std::string serverHostOnly(const std::string& server_name);

std::string deviceCollectiveP2pKey(int backendIndex, int rank);
std::string deviceCollectiveRdmaKey(int backendIndex, int rank);

std::vector<uint8_t> serializeRdmaMetadata(
    const device::RdmaLocalMetadata& meta);
device::RdmaLocalMetadata deserializeRdmaMetadata(
    const std::vector<uint8_t>& bytes);

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_BOOTSTRAP_H
