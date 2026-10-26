#pragma once

#include <functional>
#include <memory>
#include <optional>

#include "client_service.h"

namespace mooncake {

struct GpuReadRegion {
    void* base;
    size_t size;
};

struct GpuReadStagingImpl;

// One reusable buffer per RealClient. A busy buffer falls back to direct reads;
// caller-owned destination addresses are never cached between operations.
class GpuReadStaging {
   public:
    using ResolveRegion = std::function<std::optional<GpuReadRegion>(void*)>;
    using Results = std::vector<tl::expected<void, ErrorCode>>;

    // Read configuration during setup; disabled/unsupported clients need no
    // staging object. GPU resources remain lazy until the destination is known.
    static std::unique_ptr<GpuReadStaging> Create(
        const std::shared_ptr<Client>& client);
    ~GpuReadStaging();

    // nullopt means no work was submitted and the caller may use direct reads.
    std::optional<Results> Read(
        const std::vector<std::string>& keys,
        const std::vector<QueryResult>& queries,
        const std::unordered_map<std::string, std::vector<Slice>>& slices,
        bool group_by_target, const ResolveRegion& resolve);

   private:
    GpuReadStaging(std::shared_ptr<Client> client, size_t capacity);
    std::unique_ptr<GpuReadStagingImpl> impl_;
};

}  // namespace mooncake
