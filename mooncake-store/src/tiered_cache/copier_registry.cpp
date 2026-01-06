#include "tiered_cache/copier_registry.h"
#include "tiered_cache/data_copier.h"
#include <utility>

namespace mooncake {

CopierRegistry& CopierRegistry::GetInstance() {
    static CopierRegistry instance;
    return instance;
}

void CopierRegistry::RegisterMemoryType(MemoryType type, CopyFunction to_dram,
                                        CopyFunction from_dram) {
    memory_type_regs_.push_back(
        {type, std::move(to_dram), std::move(from_dram)});
}

void CopierRegistry::RegisterDirectPath(MemoryType src, MemoryType dest,
                                        CopyFunction func) {
    direct_path_regs_.push_back({src, dest, std::move(func)});
}

const std::vector<MemoryTypeRegistration>&
CopierRegistry::GetMemoryTypeRegistrations() const {
    return memory_type_regs_;
}

const std::vector<DirectPathRegistration>&
CopierRegistry::GetDirectPathRegistrations() const {
    return direct_path_regs_;
}

CopierRegistrar::CopierRegistrar(MemoryType type, CopyFunction to_dram,
                                 CopyFunction from_dram) {
    // When a static CopierRegistrar object is created, it registers the memory
    // type.
    CopierRegistry::GetInstance().RegisterMemoryType(type, std::move(to_dram),
                                                     std::move(from_dram));
}

}  // namespace mooncake