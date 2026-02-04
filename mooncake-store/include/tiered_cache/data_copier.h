#pragma once

#include <functional>
#include <map>
#include <memory>
#include <glog/logging.h>
#include <stdexcept>
#include <vector>

#include "rpc_types.h"
#include "tiered_cache/tiers/cache_tier.h"

namespace mooncake {

using CopyFunction = std::function<tl::expected<void, ErrorCode>(
    const DataSource& src, const DataSource& dst)>;

class DataCopier;

/**
 * @brief A helper class to build a valid DataCopier.
 *
 * This builder enforces the rule that for any new memory type added,
 * its copy functions to and from DRAM *must* be provided via the
 * CopierRegistry.
 */
class DataCopierBuilder {
   public:
    /**
     * @brief Constructs a builder. It automatically pulls all existing
     * registrations from the global CopierRegistry.
     */
    DataCopierBuilder();

    /**
     * @brief (Optional) Registers a highly optimized direct copy path.
     * This will be used instead of the DRAM fallback. Can be used for testing
     * or for paths that are not self-registered.
     * @return A reference to the builder for chaining.
     */
    DataCopierBuilder& AddDirectPath(MemoryType src_type, MemoryType dest_type,
                                     CopyFunction func);

    /**
     * @brief Builds the final, immutable DataCopier object.
     * It verifies that all memory types defined in the MemoryType enum
     * have been registered via the registry before creating the object.
     * @return A unique_ptr to the new DataCopier.
     * @throws std::logic_error if a required to/from DRAM copier is missing.
     */
    std::unique_ptr<DataCopier> Build() const;

   private:
    std::map<std::pair<MemoryType, MemoryType>, CopyFunction> copy_matrix_;
};

/**
 * @brief A central utility for copying data between different memory types.
 * It supports a fallback mechanism via DRAM for any copy paths that are not
 * explicitly registered as a direct path.
 */
class DataCopier {
   public:
    // The constructor is private. Use DataCopierBuilder to create an instance.
    ~DataCopier() = default;
    DataCopier(const DataCopier&) = delete;
    DataCopier& operator=(const DataCopier&) = delete;

    /**
     * @brief Executes a copy from a source to a destination.
     * It first attempts to find a direct copy function (e.g., VRAM -> VRAM).
     * If not found, it automatically falls back to a two-step copy via a
     * temporary DRAM buffer (e.g., VRAM -> DRAM -> SSD).
     * @param src The data source descriptor.
     * @param dest_type The memory type of the destination.
     * @param dest_ptr A pointer to the destination (memory address, handle,
     * etc.).
     */
    tl::expected<void, ErrorCode> Copy(const DataSource& src,
                                       const DataSource& dst) const;

   private:
    friend class DataCopierBuilder;  // Allow builder to access the constructor.
    DataCopier(
        std::map<std::pair<MemoryType, MemoryType>, CopyFunction> copy_matrix);

    CopyFunction FindCopier(MemoryType src_type, MemoryType dest_type) const;
    const std::map<std::pair<MemoryType, MemoryType>, CopyFunction>
        copy_matrix_;
};

}  // namespace mooncake