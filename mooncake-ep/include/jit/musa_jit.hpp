// SPDX-License-Identifier: Apache-2.0
// Runtime compiler/loader for MUSA EP device kernels.

#pragma once

#include <musa.h>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace mooncake::jit {

struct MusaJitCompileConfig {
    std::filesystem::path compiler;
    std::filesystem::path bundler;
    std::filesystem::path objcopy;
    std::filesystem::path work_dir;
    std::vector<std::filesystem::path> include_dirs;
    // Empty selects the active MUSA device's compute capability (mp_<major><minor>).
    // A hard-coded architecture can load successfully and then fail at execution
    // time with opaque device errors when it does not match the installed GPU.
    std::string target_arch;
    std::vector<std::string> extra_options;
};

struct MusaJitCompileResult {
    bool ok = false;
    std::vector<char> image;
    std::string log;
    double compiler_sec = 0.0;
};

// Compile a MUSA translation unit and extract the device member from the
// clang offload bundle. The returned image is suitable for muModuleLoadData.
MusaJitCompileResult compile_musa_to_image(
    std::string_view source, std::string_view stem,
    const MusaJitCompileConfig& config);

class MusaJitModule final {
   public:
    MusaJitModule() = default;
    ~MusaJitModule();
    MusaJitModule(const MusaJitModule&) = delete;
    MusaJitModule& operator=(const MusaJitModule&) = delete;

    bool load(const std::vector<char>& image, std::string* error);
    void reset();

    bool get_function(std::string_view name, MUfunction* function,
                      std::string* error) const;
    bool launch(MUfunction function, unsigned grid_x, unsigned grid_y,
                unsigned grid_z, unsigned block_x, unsigned block_y,
                unsigned block_z, unsigned shared_bytes, MUstream stream,
                void** args, std::string* error) const;

   private:
    MUmodule module_ = nullptr;
};

bool launch_musa_kernel_cached(std::string_view source, std::string_view stem,
                               std::string_view entry, unsigned grid_x,
                               unsigned grid_y, unsigned grid_z,
                               unsigned block_x, unsigned block_y,
                               unsigned block_z, unsigned shared_bytes,
                               MUstream stream, void** args,
                               const MusaJitCompileConfig& config,
                               std::string* error);

}  // namespace mooncake::jit
