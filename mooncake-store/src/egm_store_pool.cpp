#include "egm_store_pool.h"

#include <numa.h>
#include <sched.h>

#include <algorithm>
#include <charconv>
#include <cctype>
#include <cerrno>
#include <fstream>
#include <limits>
#include <numeric>
#include <set>
#include <sstream>
#include <system_error>

#include "Slab.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace mooncake {
namespace {

std::string Trim(std::string value) {
    auto not_space = [](unsigned char ch) { return !std::isspace(ch); };
    value.erase(value.begin(),
                std::find_if(value.begin(), value.end(), not_space));
    value.erase(std::find_if(value.rbegin(), value.rend(), not_space).base(),
                value.end());
    return value;
}

EgmStorePoolResult<bool> ParseStrictBool(const std::string& value) {
    std::string normalized = value;
    std::transform(
        normalized.begin(), normalized.end(), normalized.begin(),
        [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    if (normalized == "true" || normalized == "1") return true;
    if (normalized == "false" || normalized == "0") return false;
    return tl::make_unexpected("invalid boolean value '" + value + "'");
}

EgmStorePoolResult<std::vector<int>> ParseNodeCsv(
    const std::string& expression) {
    std::set<int> nodes;
    size_t begin = 0;
    while (begin <= expression.size()) {
        size_t comma = expression.find(',', begin);
        size_t end = comma == std::string::npos ? expression.size() : comma;
        std::string token = Trim(expression.substr(begin, end - begin));
        if (token.empty()) {
            return tl::make_unexpected(
                "NUMA node list contains an empty token");
        }
        if (token.front() == '-') {
            return tl::make_unexpected("NUMA node must not be negative: '" +
                                       token + "'");
        }

        int node = -1;
        const char* first = token.data();
        const char* last = first + token.size();
        auto parsed = std::from_chars(first, last, node, 10);
        if (parsed.ec == std::errc::result_out_of_range) {
            return tl::make_unexpected("NUMA node is out of range: '" + token +
                                       "'");
        }
        if (parsed.ec != std::errc() || parsed.ptr != last) {
            return tl::make_unexpected("invalid NUMA node token: '" + token +
                                       "'");
        }
        nodes.insert(node);

        if (comma == std::string::npos) break;
        begin = comma + 1;
    }

    if (nodes.empty()) {
        return tl::make_unexpected("NUMA node list is empty");
    }
    return std::vector<int>(nodes.begin(), nodes.end());
}

EgmStorePoolResult<size_t> CheckedLcm(size_t left, size_t right) {
    if (left == 0 || right == 0) {
        return tl::make_unexpected("alignment and granularity must be nonzero");
    }
    const size_t divisor = std::gcd(left, right);
    const size_t reduced = left / divisor;
    if (reduced > std::numeric_limits<size_t>::max() / right) {
        return tl::make_unexpected("common alignment overflows size_t");
    }
    return reduced * right;
}

class ProductionEgmStorePoolEnvironment final : public EgmStorePoolEnvironment {
   public:
    EgmStorePoolResult<std::vector<int>> VisibleCudaDevices() const override {
#ifdef USE_CUDA
        int count = 0;
        cudaError_t result = cudaGetDeviceCount(&count);
        if (result != cudaSuccess) {
            return tl::make_unexpected(
                std::string("cudaGetDeviceCount failed: ") +
                cudaGetErrorString(result));
        }
        if (count <= 0) {
            return tl::make_unexpected("no visible CUDA device");
        }
        std::vector<int> devices;
        devices.reserve(static_cast<size_t>(count));
        for (int device = 0; device < count; ++device) {
            devices.push_back(device);
        }
        return devices;
#else
        return tl::make_unexpected("CUDA support is not built");
#endif
    }

    EgmStorePoolResult<std::string> PciBdfForCudaDevice(
        int device_id) const override {
#ifdef USE_CUDA
        char pci_bdf[32] = {};
        cudaError_t result =
            cudaDeviceGetPCIBusId(pci_bdf, sizeof(pci_bdf), device_id);
        if (result != cudaSuccess) {
            return tl::make_unexpected(
                "cudaDeviceGetPCIBusId failed for device " +
                std::to_string(device_id) + ": " + cudaGetErrorString(result));
        }
        if (pci_bdf[0] == '\0') {
            return tl::make_unexpected("empty PCI BDF for CUDA device " +
                                       std::to_string(device_id));
        }
        return std::string(pci_bdf);
#else
        (void)device_id;
        return tl::make_unexpected("CUDA support is not built");
#endif
    }

    EgmStorePoolResult<int> ReadPciNumaNode(
        const std::string& pci_bdf) const override {
        const std::string path =
            "/sys/bus/pci/devices/" + pci_bdf + "/numa_node";
        std::ifstream input(path);
        if (!input.is_open()) {
            return tl::make_unexpected("cannot read " + path);
        }
        long long node = -1;
        if (!(input >> node)) {
            return tl::make_unexpected("invalid NUMA node in " + path);
        }
        input >> std::ws;
        if (!input.eof() || node < 0 ||
            node > std::numeric_limits<int>::max()) {
            return tl::make_unexpected("invalid NUMA node in " + path);
        }
        return static_cast<int>(node);
    }

    bool IsNumaNodeOnline(int node_id) const override {
        if (node_id < 0 || numa_available() < 0 ||
            numa_all_nodes_ptr == nullptr)
            return false;
        return numa_bitmask_isbitset(numa_all_nodes_ptr,
                                     static_cast<unsigned int>(node_id)) != 0;
    }

    EgmStorePoolResult<int> CurrentCpu() const override {
        const int cpu = sched_getcpu();
        if (cpu < 0) {
            return tl::make_unexpected("sched_getcpu failed: errno=" +
                                       std::to_string(errno));
        }
        return cpu;
    }

    EgmStorePoolResult<int> NumaNodeForCpu(int cpu_id) const override {
        const int node = numa_node_of_cpu(cpu_id);
        if (node < 0) {
            return tl::make_unexpected("numa_node_of_cpu failed for CPU " +
                                       std::to_string(cpu_id));
        }
        return node;
    }
};

}  // namespace

EgmStorePoolResult<EgmStorePoolOptions> ParseEgmStorePoolOptions(
    const ConfigDict& config, size_t global_segment_size) {
    EgmStorePoolOptions options;

    auto enabled_it = config.find(CONFIG_KEY_ENABLE_EGM_STORE_POOL);
    if (enabled_it != config.end()) {
        auto enabled = ParseStrictBool(enabled_it->second);
        if (!enabled) return tl::make_unexpected(enabled.error());
        options.enabled = *enabled;
    }

    if (!options.enabled || global_segment_size == 0) return options;

    auto nodes_it = config.find(CONFIG_KEY_EGM_NUMA_NODES);
    if (nodes_it == config.end() || Trim(nodes_it->second) == "auto") {
        return options;
    }

    auto nodes = ParseNodeCsv(nodes_it->second);
    if (!nodes) return tl::make_unexpected(nodes.error());
    options.auto_nodes = false;
    options.nodes = std::move(*nodes);
    return options;
}

EgmStorePoolResult<std::vector<int>> DiscoverEgmStorePoolNodes(
    const EgmStorePoolOptions& options, size_t global_segment_size,
    const EgmStorePoolEnvironment& environment) {
    if (!options.enabled || global_segment_size == 0) {
        return std::vector<int>{};
    }

    if (!options.auto_nodes) {
        if (options.nodes.empty()) {
            return tl::make_unexpected("explicit NUMA node list is empty");
        }
        std::set<int> nodes;
        for (int node : options.nodes) {
            if (node < 0 || !environment.IsNumaNodeOnline(node)) {
                return tl::make_unexpected(
                    "configured NUMA node is invalid or "
                    "offline: " +
                    std::to_string(node));
            }
            nodes.insert(node);
        }
        return std::vector<int>(nodes.begin(), nodes.end());
    }

    auto devices = environment.VisibleCudaDevices();
    if (!devices) return tl::make_unexpected(devices.error());
    if (devices->empty()) {
        return tl::make_unexpected("no visible CUDA device");
    }

    std::set<int> nodes;
    for (int device : *devices) {
        auto pci_bdf = environment.PciBdfForCudaDevice(device);
        if (!pci_bdf) return tl::make_unexpected(pci_bdf.error());
        auto node = environment.ReadPciNumaNode(*pci_bdf);
        if (!node) return tl::make_unexpected(node.error());
        if (*node < 0 || !environment.IsNumaNodeOnline(*node)) {
            return tl::make_unexpected(
                "discovered NUMA node is invalid or "
                "offline for PCI device " +
                *pci_bdf + ": " + std::to_string(*node));
        }
        nodes.insert(*node);
    }
    if (nodes.empty()) {
        return tl::make_unexpected("NUMA discovery returned no nodes");
    }
    return std::vector<int>(nodes.begin(), nodes.end());
}

EgmStorePoolResult<std::optional<int>> ResolveEgmStorePoolLocalNode(
    const EgmStorePoolOptions& options, size_t local_buffer_size,
    const EgmStorePoolEnvironment& environment) {
    if (!options.enabled || local_buffer_size == 0) {
        return std::optional<int>{};
    }

    auto cpu = environment.CurrentCpu();
    if (!cpu) return tl::make_unexpected(cpu.error());
    if (*cpu < 0) {
        return tl::make_unexpected("current CPU is invalid: " +
                                   std::to_string(*cpu));
    }
    auto node = environment.NumaNodeForCpu(*cpu);
    if (!node) return tl::make_unexpected(node.error());
    if (*node < 0 || !environment.IsNumaNodeOnline(*node)) {
        return tl::make_unexpected(
            "setup CPU NUMA node is invalid or offline: " +
            std::to_string(*node));
    }
    return std::optional<int>(*node);
}

EgmStorePoolResult<EgmStorePoolPlan> PlanEgmStorePoolCapacity(
    size_t requested_total,
    const std::vector<std::pair<int, size_t>>& node_granularities,
    size_t max_mr_size, size_t store_alignment) {
    if (requested_total == 0) {
        return tl::make_unexpected("requested capacity must be nonzero");
    }
    if (node_granularities.empty()) {
        return tl::make_unexpected("no NUMA nodes selected");
    }
    if (store_alignment == 0) {
        store_alignment = facebook::cachelib::Slab::kSize;
    }

    std::vector<std::pair<int, size_t>> ordered = node_granularities;
    std::sort(ordered.begin(), ordered.end(),
              [](const auto& left, const auto& right) {
                  return left.first < right.first;
              });

    size_t common_alignment = store_alignment;
    int previous_node = -1;
    bool have_previous = false;
    for (const auto& [node, granularity] : ordered) {
        if (node < 0) {
            return tl::make_unexpected("NUMA node must not be negative");
        }
        if (have_previous && node == previous_node) {
            return tl::make_unexpected("duplicate NUMA node " +
                                       std::to_string(node));
        }
        auto lcm = CheckedLcm(common_alignment, granularity);
        if (!lcm) return tl::make_unexpected(lcm.error());
        common_alignment = *lcm;
        previous_node = node;
        have_previous = true;
    }

    const size_t effective_total =
        (requested_total / common_alignment) * common_alignment;
    const size_t total_units = effective_total / common_alignment;
    if (total_units < ordered.size()) {
        return tl::make_unexpected(
            "effective capacity cannot provide one unit per NUMA node");
    }

    const size_t max_chunk_bytes =
        (max_mr_size / common_alignment) * common_alignment;
    if (max_chunk_bytes == 0) {
        return tl::make_unexpected(
            "max_mr_size is below the required common alignment");
    }

    EgmStorePoolPlan plan;
    plan.requested_total = requested_total;
    plan.effective_total = effective_total;
    plan.common_alignment = common_alignment;
    plan.nodes.reserve(ordered.size());

    const size_t base_units = total_units / ordered.size();
    const size_t remainder = total_units % ordered.size();
    size_t plan_index = 0;
    size_t planned_total = 0;
    for (size_t index = 0; index < ordered.size(); ++index) {
        const auto [node, granularity] = ordered[index];
        const size_t node_units = base_units + (index < remainder ? 1 : 0);
        if (node_units >
            std::numeric_limits<size_t>::max() / common_alignment) {
            return tl::make_unexpected("NUMA node capacity overflows size_t");
        }
        const size_t node_bytes = node_units * common_alignment;
        if (planned_total > std::numeric_limits<size_t>::max() - node_bytes) {
            return tl::make_unexpected("planned capacity overflows size_t");
        }
        planned_total += node_bytes;
        plan.nodes.push_back({node, granularity, node_bytes});

        size_t remaining = node_bytes;
        while (remaining != 0) {
            const size_t chunk_bytes = std::min(remaining, max_chunk_bytes);
            plan.chunks.push_back({node, chunk_bytes, plan_index++});
            remaining -= chunk_bytes;
        }
    }

    if (planned_total != effective_total) {
        return tl::make_unexpected(
            "planned capacity does not match effective "
            "capacity");
    }
    return plan;
}

std::unique_ptr<EgmStorePoolEnvironment>
CreateProductionEgmStorePoolEnvironment() {
    return std::make_unique<ProductionEgmStorePoolEnvironment>();
}

}  // namespace mooncake
