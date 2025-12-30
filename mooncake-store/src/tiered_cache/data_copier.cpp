#include <fstream>
#include <memory>
#include <utility>

#include "tiered_cache/data_copier.h"
#include "tiered_cache/copier_registry.h"

namespace mooncake {

DataCopierBuilder::DataCopierBuilder() {
    // Process all registrations from the global registry.
    const auto& registry = CopierRegistry::GetInstance();

    for (const auto& reg : registry.GetMemoryTypeRegistrations()) {
        copy_matrix_[{reg.type, MemoryType::DRAM}] = reg.to_dram_func;
        copy_matrix_[{MemoryType::DRAM, reg.type}] = reg.from_dram_func;
    }
    for (const auto& reg : registry.GetDirectPathRegistrations()) {
        copy_matrix_[{reg.src_type, reg.dest_type}] = reg.func;
    }
}

DataCopierBuilder& DataCopierBuilder::AddDirectPath(MemoryType src_type,
                                                    MemoryType dest_type,
                                                    CopyFunction func) {
    copy_matrix_[{src_type, dest_type}] = std::move(func);
    return *this;
}

std::unique_ptr<DataCopier> DataCopierBuilder::Build() const {
    const auto& registry = CopierRegistry::GetInstance();
    for (const auto& reg : registry.GetMemoryTypeRegistrations()) {
        if (reg.type == MemoryType::DRAM) {
            continue;
        }
        if (copy_matrix_.find({reg.type, MemoryType::DRAM}) ==
            copy_matrix_.end()) {
            throw std::logic_error(
                "DataCopierBuilder Error: Missing copy function for type " +
                MemoryTypeToString(reg.type) + " TO DRAM.");
        }
        if (copy_matrix_.find({MemoryType::DRAM, reg.type}) ==
            copy_matrix_.end()) {
            throw std::logic_error(
                "DataCopierBuilder Error: Missing copy function for DRAM TO "
                "type " +
                MemoryTypeToString(reg.type) + ".");
        }
    }

    return std::unique_ptr<DataCopier>(new DataCopier(copy_matrix_));
}

DataCopier::DataCopier(
    std::map<std::pair<MemoryType, MemoryType>, CopyFunction> copy_matrix)
    : copy_matrix_(std::move(copy_matrix)) {}

CopyFunction DataCopier::FindCopier(MemoryType src_type,
                                    MemoryType dest_type) const {
    auto it = copy_matrix_.find({src_type, dest_type});
    return (it != copy_matrix_.end()) ? it->second : nullptr;
}

tl::expected<void, ErrorCode> DataCopier::Copy(const DataSource& src,
                                               const DataSource& dest) const {
    MemoryType dest_type = dest.type;
    // Try to find a direct copy function.
    if (auto direct_copier = FindCopier(src.type, dest_type)) {
        VLOG(1) << "Using direct copier for " << MemoryTypeToString(src.type)
                << " -> " << MemoryTypeToString(dest_type);
        return direct_copier(src, dest);
    }

    // If no direct copier, try fallback via DRAM.
    if (src.type != MemoryType::DRAM && dest_type != MemoryType::DRAM) {
        VLOG(1) << "No direct copier. Attempting fallback via DRAM for "
                << MemoryTypeToString(src.type) << " -> "
                << MemoryTypeToString(dest_type);

        auto to_dram_copier = FindCopier(src.type, MemoryType::DRAM);
        auto from_dram_copier = FindCopier(MemoryType::DRAM, dest_type);

        if (to_dram_copier && from_dram_copier) {
            std::unique_ptr<char[]> temp_dram_buffer(
                new (std::nothrow) char[src.size]);
            if (!temp_dram_buffer) {
                LOG(ERROR) << "Failed to allocate temporary DRAM buffer for "
                              "fallback copy.";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            // Step A: Source -> DRAM
            DataSource temp_dram = {
                reinterpret_cast<uint64_t>(temp_dram_buffer.get()), 0, src.size,
                MemoryType::DRAM};
            if (!to_dram_copier(src, temp_dram)) {
                LOG(ERROR) << "Fallback copy failed at Step A (Source -> DRAM)";
                return tl::make_unexpected(ErrorCode::DATA_COPY_FAILED);
            }

            // Step B: DRAM -> Destination
            if (!from_dram_copier(temp_dram, dest)) {
                LOG(ERROR)
                    << "Fallback copy failed at Step B (DRAM -> Destination)";
                return tl::make_unexpected(ErrorCode::DATA_COPY_FAILED);
            }
            return tl::expected<void, ErrorCode>{};
        }
    }

    LOG(ERROR) << "No copier registered for transfer from memory type "
               << MemoryTypeToString(src.type) << " to "
               << MemoryTypeToString(dest_type)
               << ", and fallback path is not available.";
    return tl::make_unexpected(ErrorCode::DATA_COPY_FAILED);
}

}  // namespace mooncake