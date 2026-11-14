#ifndef MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ALGORITHM_PROVIDER_H
#define MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ALGORITHM_PROVIDER_H

#include <algorithm>
#include <initializer_list>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

#include "control_plane/control_types.h"
#include "device_comm/device_collective/device_collective_types.cuh"
#include "error_types.h"
#include "gpu_runtime.h"

namespace mooncake {

class ControlUpdateBuilder;
class ProtocolProvider;
struct ResolvedGroupView;

// Host-side control and launch interface for one algorithm.
class AlgorithmProvider {
   public:
    virtual ~AlgorithmProvider() = default;

    [[nodiscard]] DeviceCollectiveAlgorithm type() const noexcept {
        return type_;
    }
    [[nodiscard]] std::string_view key() const noexcept { return key_; }
    [[nodiscard]] uint32_t endpointVersion() const noexcept {
        return endpoint_version_;
    }

    // The shared protocol instance must outlive the algorithm.
    [[nodiscard]] const ProtocolProvider& protocol() const noexcept {
        return protocol_;
    }

    [[nodiscard]] virtual AlgorithmEndpoint localEndpoint() const = 0;

    // Update the plan for this view through the builder.
    virtual PGResult<void> updatePlan(const ResolvedGroupView& view,
                                      ControlUpdateBuilder& builder) const = 0;

    // Mark the plan unavailable through the builder.
    virtual PGResult<void> invalidatePlan(
        ControlUpdateBuilder& builder) const = 0;

    [[nodiscard]] bool supports(OpType operation) const noexcept {
        return std::find(supported_operations.begin(),
                         supported_operations.end(),
                         operation) != supported_operations.end();
    }

    PGResult<void> enqueue(const DeviceCollectiveRequest& request,
                           cudaStream_t stream) const {
        return std::visit(
            [&](const auto& value) { return enqueueImpl(value, stream); },
            request);
    }

   protected:
    AlgorithmProvider(DeviceCollectiveAlgorithm type, std::string_view key,
                      uint32_t endpoint_version,
                      const ProtocolProvider& protocol,
                      std::initializer_list<OpType> operations)
        : type_(type),
          key_(key),
          endpoint_version_(endpoint_version),
          protocol_(protocol),
          supported_operations(operations) {}

    template <typename T>
    [[nodiscard]] AlgorithmEndpoint encodeEndpoint(const T& metadata) const {
        return AlgorithmEndpoint::encode(key(), endpointVersion(), metadata);
    }

    [[nodiscard]] const AlgorithmEndpoint* findEndpoint(
        std::span<const AlgorithmEndpoint> endpoints) const {
        return findProviderEndpoint(endpoints, key(), endpointVersion());
    }

    template <typename T>
    [[nodiscard]] PGResult<T> decodeEndpoint(
        const AlgorithmEndpoint& endpoint) const {
        return endpoint.decode<T>(key(), endpointVersion());
    }

    // Operations without an override return NotSupported.
    virtual PGResult<void> enqueueImpl(const AllReduceRequest&,
                                       cudaStream_t) const {
        return makePGError(PGErrorCode::NotSupported,
                           "device collective implementation is unavailable");
    }
    virtual PGResult<void> enqueueImpl(const AllGatherRequest&,
                                       cudaStream_t) const {
        return makePGError(PGErrorCode::NotSupported,
                           "device collective implementation is unavailable");
    }

   private:
    DeviceCollectiveAlgorithm type_;
    std::string_view key_;
    uint32_t endpoint_version_;
    const ProtocolProvider& protocol_;
    std::vector<OpType> supported_operations;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_DEVICE_COMM_DEVICE_COLLECTIVE_ALGORITHMS_ALGORITHM_PROVIDER_H
