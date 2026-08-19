#pragma once

#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/data_copier.h"
#include <functional>
#include <map>
#include <string>
#include <vector>

namespace mooncake {

// Forward declaration from data_copier.h to avoid circular dependency
class DataCopierBuilder;

// Holds the registration information for a memory type.
struct MemoryTypeRegistration {
    MemoryType type;
    CopyFunction to_dram_func;
    CopyFunction from_dram_func;
};

// Holds the registration for an optimized direct path.
struct DirectPathRegistration {
    MemoryType src_type;
    MemoryType dest_type;
    CopyFunction func;
};

/**
 * @brief A singleton registry for data copier functions.
 *
 * Modules can register their copy functions here during static initialization.
 * The DataCopierBuilder will then use this registry to construct a DataCopier.
 */
class CopierRegistry {
   public:
    /**
     * @brief Get the singleton instance of the registry.
     */
    static CopierRegistry& GetInstance();

    /**
     * @brief Registers the to/from DRAM copy functions for a memory type.
     */
    void RegisterMemoryType(MemoryType type, CopyFunction to_dram,
                            CopyFunction from_dram);

    /**
     * @brief Registers an optional, optimized direct copy path.
     */
    void RegisterDirectPath(MemoryType src, MemoryType dest, CopyFunction func);

    // These methods are used by the DataCopierBuilder to collect all
    // registrations.
    const std::vector<MemoryTypeRegistration>& GetMemoryTypeRegistrations()
        const;
    const std::vector<DirectPathRegistration>& GetDirectPathRegistrations()
        const;

   private:
    friend class DataCopierBuilder;

    CopierRegistry() = default;
    ~CopierRegistry() = default;
    CopierRegistry(const CopierRegistry&) = delete;
    CopierRegistry& operator=(const CopierRegistry&) = delete;

    std::vector<MemoryTypeRegistration> memory_type_regs_;
    std::vector<DirectPathRegistration> direct_path_regs_;
};

/**
 * @brief A helper class to automatically register copiers at static
 * initialization time.
 *
 * To register a new memory type, simply declare a static instance of this class
 * in the corresponding .cpp file, providing the type and its to/from DRAM
 * copiers.
 */
class CopierRegistrar {
   public:
    CopierRegistrar(MemoryType type, CopyFunction to_dram,
                    CopyFunction from_dram);
};

}  // namespace mooncake