/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Ported from nccl-extensions: nccl_ep/device/jit/nvcc_compiler.hpp.
 */

#pragma once

#include <jit/jit_compiler.hpp>

namespace mooncake {
namespace jit {

class NvccCompiler final : public JitCompiler {
   public:
    std::string compiler_id() const override;
    std::vector<std::string> compile_options(
        const JitCompileConfig& config) const override;
    bool compile_to_cubin(const JitCompileInput& input,
                          JitCompileOutput* output) const override;
};

}  // namespace jit
}  // namespace mooncake
