#include <string>

#include "transfer_metadata.h"

namespace mooncake {

int parseNicPriorityMatrix(const std::string &nic_priority_matrix,
                           TransferMetadata::PriorityMatrix &priority_map,
                           std::vector<std::string> &rnic_list);

std::string discoverTopologyMatrix();
}  // namespace mooncake
