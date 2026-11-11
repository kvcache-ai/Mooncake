#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "types.h"

namespace mooncake {

class Client;

struct GpuStagingRegion {
    void* base;
    size_t size;
};

struct GpuStagingOptions {
    // Device memory for the staging buffer, split into two halves.
    size_t buffer_bytes = size_t{256} << 20;
    // A transfer that fails after running this long may still have a copy in
    // flight into or out of the buffer: TransferEngineOperationState stops
    // waiting after 60 s without cancelling it. Its half is then not reused
    // for `quarantine_ms`.
    uint64_t late_failure_ms = 55'000;
    uint64_t quarantine_ms = 300'000;
};

struct GpuStagingImpl;

// Coalesces fragmented multi-buffer transfers through a buffer in device
// memory. A key whose parts are spread over many device buffers (for example
// one per layer of a KV cache) costs one transfer per part on the per-part
// path. Through staging, a read moves each key's object into the buffer with
// one transfer and one kernel scatters the parts; a write gathers the parts
// with one kernel and moves each object with one transfer. Stored objects are
// byte-identical to the per-part path, so both kinds of client interoperate.
// While one half's batch is in the kernel, the other half's batch transfers.
class GpuStaging {
   public:
    using Results = std::vector<tl::expected<void, ErrorCode>>;
    using ResolveRegion = std::function<std::optional<GpuStagingRegion>(void*)>;
    // Transfers the keys at `indices` (positions in the caller's batch), key
    // indices[n] to or from slices[n]; returns one result per index.
    using Transfer =
        std::function<Results(const std::vector<size_t>& indices,
                              const std::vector<std::vector<Slice>>& slices)>;

    // Reads MC_NVLINK_COALESCE and MC_NVLINK_COALESCE_STAGING_MB; nullptr
    // when disabled or unsupported by the build or the client.
    static std::unique_ptr<GpuStaging> Create(
        const std::shared_ptr<Client>& client);
    // `client` registers the buffer with its transfer engine; nullptr skips
    // the registration (tests). Device memory is allocated on first use.
    GpuStaging(std::shared_ptr<Client> client, GpuStagingOptions options);
    ~GpuStaging();

    // parts[i] are key i's destination slices; object_bytes[i] is its stored
    // size, or 0 when it must not be staged. Keys that cannot be staged go to
    // `read` with their own slices. nullopt: nothing was staged or
    // transferred, and the caller should use its per-part path.
    std::optional<Results> Read(const std::vector<std::vector<Slice>>& parts,
                                const std::vector<size_t>& object_bytes,
                                const ResolveRegion& resolve,
                                const Transfer& read);

    // parts[i] are key i's source slices, stored in order as one object.
    std::optional<Results> Write(const std::vector<std::vector<Slice>>& parts,
                                 const ResolveRegion& resolve,
                                 const Transfer& write);

   private:
    std::unique_ptr<GpuStagingImpl> impl_;
};

}  // namespace mooncake
