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

}  // namespace mooncake
