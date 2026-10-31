#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_PROTOCOL_PROVIDER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_PROTOCOL_PROVIDER_H

#include <span>
#include <string_view>

#include "control_plane/control_types.h"
#include "error_types.h"

namespace mooncake {

class ControlUpdateBuilder;
struct ResolvedGroupView;

// Host-side resources and state for one protocol instance.
class ProtocolProvider {
   public:
    virtual ~ProtocolProvider() = default;

    [[nodiscard]] std::string_view key() const noexcept { return key_; }
    [[nodiscard]] uint32_t endpointVersion() const noexcept {
        return endpoint_version_;
    }

    // Return this protocol's opaque endpoint.
    [[nodiscard]] virtual ProtocolEndpoint localEndpoint() const = 0;

    // Initialize protocol state for the view through the builder.
    virtual PGResult<void> updateState(const ResolvedGroupView& view,
                                       ControlUpdateBuilder& builder) const = 0;

   protected:
    ProtocolProvider(std::string_view key, uint32_t endpoint_version) noexcept
        : key_(key), endpoint_version_(endpoint_version) {}

    template <typename T>
    [[nodiscard]] ProtocolEndpoint encodeEndpoint(const T& metadata) const {
        return ProtocolEndpoint::encode(key(), endpointVersion(), metadata);
    }

    [[nodiscard]] const ProtocolEndpoint* findEndpoint(
        std::span<const ProtocolEndpoint> endpoints) const {
        return findProviderEndpoint(endpoints, key(), endpointVersion());
    }

    template <typename T>
    [[nodiscard]] PGResult<T> decodeEndpoint(
        const ProtocolEndpoint& endpoint) const {
        return endpoint.decode<T>(key(), endpointVersion());
    }

   private:
    std::string_view key_;
    uint32_t endpoint_version_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_PROTOCOLS_PROTOCOL_PROVIDER_H
