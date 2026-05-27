// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file platform.h
 * @brief Platform interface - platform-independent core
 *
 * The core Platform class delegates all platform-specific operations
 * to dynamically loaded plugin implementations.
 */

#ifndef PLATFORM_H
#define PLATFORM_H

#include "tent/runtime/topology.h"
#include "tent/common/config.h"
#include "tent/common/status.h"

#include <memory>

namespace mooncake {
namespace tent {

enum MemoryType {
    MTYPE_UNKNOWN,
    MTYPE_CPU,
    MTYPE_CUDA,    // NVIDIA CUDA
    MTYPE_ASCEND,  // Huawei Ascend NPU
    MTYPE_HIP,     // AMD HIP (ROCm)
    MTYPE_MUSA,    // Moore Threads MUSA
    MTYPE_MACA,    // Iluvatar CoreX MACA
};

/**
 * @brief Platform backend interface for plugin implementations
 *
 * All platform-specific operations (GPU, NPU, etc.) are implemented
 * by plugins that implement this interface.
 */
class IPlatformBackend {
   public:
    virtual ~IPlatformBackend() = default;

    virtual std::string name() const = 0;
    virtual Status initialize(std::shared_ptr<Config> config) = 0;

    // Device discovery
    virtual Status probe(std::vector<Topology::NicEntry>& nic_list,
                         std::vector<Topology::MemEntry>& mem_list) = 0;

    // Memory operations
    virtual Status allocate(void** pptr, size_t size,
                            MemoryOptions& options) = 0;
    virtual Status free(void* ptr, size_t size) = 0;
    virtual Status copy(void* dst, void* src, size_t length) = 0;
    virtual MemoryType getMemoryType(void* addr) = 0;
    virtual const std::vector<RangeLocation> getLocation(
        void* start, size_t len, bool skip_prefault = false) = 0;

    // Device info
    virtual int getDeviceCount() const = 0;
    virtual std::string getPrefix() const = 0;  // e.g., "cuda:", "ascend:"
};

/**
 * @brief Unified Platform class - platform-independent core
 *
 * This class provides the interface used by the rest of TENT.
 * It delegates all operations to a loaded platform backend plugin.
 */
class Platform {
   public:
    /**
     * @brief Get the singleton Platform instance
     * @param config Optional configuration
     * @return Reference to the Platform instance
     */
    static Platform& getInstance(std::shared_ptr<Config> config = nullptr);

    /**
     * @brief Alias for getInstance() for backward compatibility
     */
    static Platform& getLoader(std::shared_ptr<Config> conf = nullptr) {
        return getInstance(conf);
    }

    Platform(std::shared_ptr<Config> config);
    virtual ~Platform();

    // Forward all operations to the backend plugin
    Status probe(std::vector<Topology::NicEntry>& nic_list,
                 std::vector<Topology::MemEntry>& mem_list);
    Status allocate(void** pptr, size_t size, MemoryOptions& options);
    Status free(void* ptr, size_t size);
    Status copy(void* dst, void* src, size_t length);
    MemoryType getMemoryType(void* addr);
    const std::vector<RangeLocation> getLocation(void* start, size_t len,
                                                 bool skip_prefault = false);
    const std::string type() const;

    /**
     * @brief Get the active backend (for testing/diagnosis)
     */
    std::shared_ptr<IPlatformBackend> getBackend() const { return backend_; }

   private:
    std::shared_ptr<Config> config_;
    std::shared_ptr<IPlatformBackend> backend_;

    // Load platform backend plugin
    Status loadBackend();

    // CPU fallback implementation (used when no plugin is available)
    Status probeCpu(std::vector<Topology::NicEntry>& nic_list,
                    std::vector<Topology::MemEntry>& mem_list);
};

}  // namespace tent
}  // namespace mooncake

#endif  // PLATFORM_H