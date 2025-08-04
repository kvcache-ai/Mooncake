#include "utils.h"

#include <Slab.h>
#include <glog/logging.h>

namespace mooncake {
void *allocate_buffer_allocator_memory(size_t total_size) {
    const size_t alignment = facebook::cachelib::Slab::kSize;
    // Ensure total_size is a multiple of alignment
    if (total_size < alignment) {
        LOG(ERROR) << "Total size must be at least " << alignment;
        return nullptr;
    }
    // Allocate aligned memory
    return aligned_alloc(alignment, total_size);
}

std::string formatDeviceNames(const std::string &device_names) {
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

static std::string loadNicPriorityMatrix(const std::string &device_name) {
    auto device_names = formatDeviceNames(device_name);
    return "{\"cpu:0\": [[" + device_names + "], []]}";
}

void **rdma_args(const std::string &device_name) {
    static auto nic_priority_matrix = loadNicPriorityMatrix(device_name);
    void **args = (void **)malloc(2 * sizeof(void *));
    args[0] = (void *)nic_priority_matrix.c_str();
    args[1] = nullptr;
    return args;
}

}  // namespace mooncake