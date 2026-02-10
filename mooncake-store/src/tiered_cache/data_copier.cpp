#include <fstream>
#include <memory>
#include <utility>

#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/copier_registry.h"
#include "tiered_cache/data_copier.h"

namespace mooncake {

// DRAM <-> DRAM
tl::expected<void, ErrorCode> CopyDramToDram(const DataSource& src,
                                             const DataSource& dest) {
    // Validate buffers exist
    if (!src.buffer || !dest.buffer) {
        LOG(ERROR) << "Invalid buffer: src.buffer="
                   << (src.buffer ? "valid" : "null")
                   << ", dest.buffer=" << (dest.buffer ? "valid" : "null");
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    const void* src_ptr = reinterpret_cast<const void*>(src.buffer->data());
    void* dest_ptr = reinterpret_cast<void*>(dest.buffer->data());
    size_t size = src.buffer->size();

    // Validate pointers and size
    if (!src_ptr || !dest_ptr) {
        LOG(ERROR) << "Invalid pointer: src_ptr=" << src_ptr
                   << ", dest_ptr=" << dest_ptr;
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (size == 0) {
        LOG(WARNING) << "Copy with zero size, skipping memcpy";
        return tl::expected<void, ErrorCode>{};
    }

    // Validate dest buffer size
    if (dest.buffer->size() < size) {
        LOG(ERROR) << "Destination buffer too small: dest_size="
                   << dest.buffer->size() << ", required=" << size;
        return tl::unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

    memcpy(dest_ptr, src_ptr, size);
    return tl::expected<void, ErrorCode>{};
}

DataCopierBuilder::DataCopierBuilder() {
    // Add the default DRAM<->DRAM copier.
    copy_matrix_[{MemoryType::DRAM, MemoryType::DRAM}] = CopyDramToDram;
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
            // Create a temporary DRAM buffer for the fallback path
            size_t buffer_size = src.buffer->size();
            std::unique_ptr<char[]> temp_dram_buffer(
                new (std::nothrow) char[buffer_size]);
            if (!temp_dram_buffer) {
                LOG(ERROR) << "Failed to allocate temporary DRAM buffer for "
                              "fallback copy.";
                return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
            }

            // Step A: Source -> DRAM
            DataSource temp_dram;
            // Transfer ownership to TempDRAMBuffer (it will be released when
            // temp_dram goes out of scope)
            temp_dram.buffer = std::make_unique<TempDRAMBuffer>(
                std::move(temp_dram_buffer), buffer_size);
            temp_dram.type = MemoryType::DRAM;

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