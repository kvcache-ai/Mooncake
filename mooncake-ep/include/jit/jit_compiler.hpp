/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Ported from nccl-extensions: nccl_ep/device/jit/jit_compiler.hpp.
 * Implement this interface to add a compiler backend (NVRTC, MUSA mcc, ...)
 * alongside NvccCompiler.
 */

#pragma once

#include <filesystem>
#include <string>
#include <vector>

namespace mooncake {
namespace jit {

struct JitCompileConfig {
    int sm = 0;
    std::string target_arch;
    std::filesystem::path source_dir;
    std::filesystem::path build_include_dir;
    std::filesystem::path cuda_include_dir;
};

struct JitCompileInput {
    std::string source;
    std::vector<std::string> options;
    std::filesystem::path source_path;
    std::filesystem::path cubin_path;
    bool log_enabled = false;
};

struct JitCompileOutput {
    std::string cubin;
    std::string log;
    double compiler_sec = 0.0;
};

class JitCompiler {
   public:
    virtual ~JitCompiler() = default;

    virtual std::string compiler_id() const = 0;
    virtual std::vector<std::string> compile_options(
        const JitCompileConfig& config) const = 0;
    virtual bool compile_to_cubin(const JitCompileInput& input,
                                  JitCompileOutput* output) const = 0;
};

}  // namespace jit
}  // namespace mooncake
