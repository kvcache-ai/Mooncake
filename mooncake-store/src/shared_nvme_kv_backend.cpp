#include "shared_nvme_kv_backend.h"

#include <cstring>
#include <limits>
#include <stdexcept>

#include "device/accelerator_registry.h"
#include "nvme_kv/key_codec.h"

namespace mooncake {
namespace {

constexpr size_t kStagingBytes = 256ULL * 1024 * 1024;
constexpr size_t kStagingKeys = 256;

bool AreContiguous(const std::vector<Slice>& slices) {
    for (size_t i = 1; i < slices.size(); ++i) {
        const auto address = reinterpret_cast<uintptr_t>(slices[i - 1].ptr);
        if (slices[i - 1].size >
                std::numeric_limits<uintptr_t>::max() - address ||
            address + slices[i - 1].size !=
                reinterpret_cast<uintptr_t>(slices[i].ptr)) {
            return false;
        }
    }

    return true;
}

tl::expected<size_t, ErrorCode> ValidateHostSlices(
    const std::vector<Slice>* slices,
    const device::RuntimeAccelerator& accelerators) {
    if (!slices) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }

    size_t bytes = 0;
    for (const auto& slice : *slices) {
        if (!slice.ptr || !slice.size ||
            slice.size > std::numeric_limits<size_t>::max() - bytes) {
            return tl::unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (accelerators.FindDeviceForPointer(slice.ptr)) {
            return tl::unexpected(ErrorCode::NOT_SUPPORTED);
        }
        bytes += slice.size;
    }

    if (!bytes) {
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    }
    return bytes;
}

}  // namespace

SharedNvmeKvBackend::SharedNvmeKvBackend(
    std::vector<std::shared_ptr<SharedNvmeKv>> namespaces)
    : namespaces_(std::move(namespaces)) {
    if (namespaces_.empty()) {
        throw std::invalid_argument(
            "shared KV namespace list must not be empty");
    }

    for (const auto& ns : namespaces_) {
        if (!ns) {
            throw std::invalid_argument("invalid shared KV namespace");
        }
    }
}

std::vector<tl::expected<void, ErrorCode>> SharedNvmeKvBackend::BatchWrite(
    const std::vector<SharedKvIORequest>& requests) {
    return BatchTransfer(requests, true);
}

std::vector<tl::expected<void, ErrorCode>> SharedNvmeKvBackend::BatchRead(
    const std::vector<SharedKvIORequest>& requests) {
    return BatchTransfer(requests, false);
}

std::vector<tl::expected<void, ErrorCode>> SharedNvmeKvBackend::BatchTransfer(
    const std::vector<SharedKvIORequest>& requests, bool write) {
    std::vector<tl::expected<void, ErrorCode>> results(
        requests.size(), tl::unexpected(ErrorCode::INVALID_PARAMS));

    std::vector<std::vector<size_t>> groups(namespaces_.size());
    for (size_t i = 0; i < requests.size(); ++i) {
        const auto& key = requests[i].key;
        const auto hash = ComputeNvmeKvChecksum(
            {reinterpret_cast<const uint8_t*>(key.data()), key.size()});
        groups[hash % namespaces_.size()].push_back(i);
    }

    const auto accelerators =
        device::GetAcceleratorRegistry().RuntimeAccelerators();
    for (size_t ns = 0; ns < groups.size(); ++ns) {
        auto& backend = *namespaces_[ns];
        size_t next = 0;

        while (next < groups[ns].size()) {
            std::vector<SharedNvmeKv::WriteRequest> writes;
            std::vector<SharedNvmeKv::ReadRequest> reads;
            std::vector<std::string> staging;
            std::vector<size_t> indices;
            std::vector<bool> scatter;

            staging.reserve(kStagingKeys);
            size_t bytes = 0;

            while (next < groups[ns].size() && indices.size() < kStagingKeys) {
                const size_t i = groups[ns][next];
                const auto& request = requests[i];
                const auto length =
                    ValidateHostSlices(request.slice_lists, accelerators);
                if (!length || *length != request.descriptor.object_size ||
                    request.descriptor.allocation_id == 0) {
                    results[i] = tl::unexpected(
                        length ? ErrorCode::INVALID_PARAMS : length.error());
                    ++next;
                    continue;
                }

                auto encoded = backend.EncodedSize(request.key, *length);
                if (!encoded) {
                    results[i] = tl::unexpected(encoded.error());
                    ++next;
                    continue;
                }
                if (!indices.empty() && *encoded > kStagingBytes - bytes) {
                    break;
                }

                const auto& slices = *request.slice_lists;
                const bool needs_staging = !AreContiguous(slices);
                char* payload = static_cast<char*>(slices.front().ptr);
                if (needs_staging) {
                    staging.emplace_back(*length, '\0');
                    payload = staging.back().data();
                    if (write) {
                        size_t offset = 0;
                        for (const auto& slice : slices) {
                            std::memcpy(payload + offset, slice.ptr,
                                        slice.size);
                            offset += slice.size;
                        }
                    }
                }

                if (write) {
                    writes.push_back({request.key,
                                      request.descriptor.allocation_id,
                                      {payload, *length}});
                } else {
                    reads.push_back({request.key,
                                     request.descriptor.allocation_id,
                                     {payload, *length}});
                }

                indices.push_back(i);
                scatter.push_back(needs_staging);
                bytes += *encoded;
                ++next;
                if (bytes >= kStagingBytes) {
                    break;
                }
            }

            if (write) {
                backend.PutBatch(writes);
            } else {
                backend.GetBatch(reads);
            }

            for (size_t j = 0; j < indices.size(); ++j) {
                const size_t i = indices[j];
                if (write) {
                    results[i] = writes[j].result;
                } else if (!reads[j].result) {
                    results[i] = tl::unexpected(reads[j].result.error());
                } else {
                    if (scatter[j]) {
                        size_t offset = 0;
                        for (const auto& slice : *requests[i].slice_lists) {
                            std::memcpy(slice.ptr,
                                        reads[j].data.data() + offset,
                                        slice.size);
                            offset += slice.size;
                        }
                    }
                    results[i] = {};
                }
            }
        }
    }

    return results;
}

}  // namespace mooncake
