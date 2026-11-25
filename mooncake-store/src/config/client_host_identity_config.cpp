#include "client_host_identity_config.h"

#include <string_view>

#include "ascii_string.h"
#include "common.h"
#include "environ.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

bool IsUsableHostId(std::string_view host_id) {
    return !host_id.empty() &&
           !AsciiCaseInsensitiveEquals(host_id, "localhost") &&
           host_id != "127.0.0.1" && host_id != "0.0.0.0" && host_id != "::1" &&
           host_id != "[::1]" && host_id != "::" && host_id != "[::]";
}

std::string NormalizeHostId(std::string_view value) {
    const std::string hostname(TrimAsciiWhitespace(value));
    const std::string host_id = (hostname == "::1" || hostname == "::")
                                    ? hostname
                                    : std::string(TrimAsciiWhitespace(
                                          getHostNameWithoutPort(hostname)));
    return IsUsableHostId(host_id) ? host_id : "";
}

}  // namespace

ClientHostIdentityConfig ClientHostIdentityConfig::FromEnvironment(
    const std::string& local_hostname) {
    const std::string configured_host_id(TrimAsciiWhitespace(
        Environ::Read(ClientHostIdentityEnvironmentVariables::MOONCAKE_HOST_ID)
            .value_or("")));
    return {.host_id = NormalizeHostId(configured_host_id.empty()
                                           ? local_hostname
                                           : configured_host_id)};
}

std::optional<PhysicalPlacement>
ClientHostIdentityConfig::PhysicalFromEnvironment(
    const std::string& local_hostname) {
    auto read = [](const char* name) {
        return std::string(
            TrimAsciiWhitespace(Environ::Process().Get(name).value_or("")));
    };
    PhysicalPlacement placement;
    placement.fabric_domain = read("MOONCAKE_FABRIC_DOMAIN");
    if (placement.fabric_domain.empty()) return std::nullopt;
    placement.rack = read("MC_RACK");
    placement.tray = read("MC_TRAY");
    if (placement.tray.empty())
        placement.tray = FromEnvironment(local_hostname).host_id;
    auto gpu = read("MC_PHYSICAL_GPU");
    // A single numeric CVD names the physical GPU even though CUDA sees 0.
    // UUID CVD and multi-GPU workers must explicitly provide MC_PHYSICAL_GPU.
    if (gpu.empty()) gpu = read("CUDA_VISIBLE_DEVICES");
    auto [end, ec] = std::from_chars(gpu.data(), gpu.data() + gpu.size(),
                                     placement.physical_gpu);
    if (ec != std::errc{} || end != gpu.data() + gpu.size())
        placement.physical_gpu = -1;
    return placement;
}

}  // namespace mooncake
