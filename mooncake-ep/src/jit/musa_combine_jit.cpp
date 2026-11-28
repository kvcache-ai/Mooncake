#include <jit/musa_combine_jit.h>

#include <jit/musa_jit.hpp>

#include <sstream>
#include <stdexcept>

namespace mooncake::jit {
namespace {
constexpr const char* kEntry = "mooncake_ep_jit_combine_kernel";
}  // namespace

void launch_combine_jit_musa(int hidden, int num_max_topk, int num_warp_groups,
                             int num_warps_per_group, int num_sms,
                             const CombineKernelArgs& args, void* stream) {
    std::ostringstream source;
    source << "#include <mooncake_ep_combine.cuh>\n"
           << "extern \"C\" __global__ void " << kEntry
           << "(const mooncake::CombineKernelArgs p) {"
           << " mooncake::combine_kernel_impl<" << num_warp_groups << ", "
           << num_warps_per_group << ", " << hidden << ", " << num_max_topk
           << ">(p); }\n";
    const unsigned block =
        static_cast<unsigned>(num_warp_groups * num_warps_per_group * 32);
    void* kernel_args[] = {const_cast<CombineKernelArgs*>(&args)};
    std::string error;
    if (!launch_musa_kernel_cached(source.str(), "combine", kEntry,
                                   static_cast<unsigned>(num_sms), 1, 1, block,
                                   1, 1, 0, reinterpret_cast<MUstream>(stream),
                                   kernel_args, &error)) {
        throw std::runtime_error("MUSA combine JIT failed: " + error);
    }
}
}  // namespace mooncake::jit
