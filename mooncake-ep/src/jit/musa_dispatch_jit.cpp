#include <jit/musa_dispatch_jit.h>

#include <jit/musa_jit.hpp>

#include <sstream>
#include <stdexcept>

namespace mooncake::jit {
namespace {
constexpr const char* kEntry = "mooncake_ep_jit_dispatch_kernel";

}  // namespace

void launch_dispatch_jit_musa(int hidden, bool use_fp8, int num_warp_groups,
                              int num_warps_per_group, int num_sms,
                              const DispatchKernelArgs& args, void* stream) {
    std::ostringstream source;
    source << "#include <mooncake_ep_dispatch.cuh>\n"
           << "extern \"C\" __global__ void " << kEntry
           << "(const mooncake::DispatchKernelArgs p) {"
           << " mooncake::dispatch_kernel_impl<" << (use_fp8 ? "true" : "false")
           << ", " << num_warp_groups << ", " << num_warps_per_group << ", "
           << hidden << ">(p); }\n";
    const unsigned block =
        static_cast<unsigned>(num_warp_groups * num_warps_per_group * 32);
    void* kernel_args[] = {const_cast<DispatchKernelArgs*>(&args)};
    std::string error;
    auto config = make_musa_jit_compile_config();
    if (!resolve_musa_jit_target_arch(&config, &error)) {
        throw std::runtime_error("MUSA dispatch JIT failed: " + error);
    }
    if (!launch_musa_kernel_cached(source.str(), "dispatch", kEntry,
                                   static_cast<unsigned>(num_sms), 1, 1, block,
                                   1, 1, 0, reinterpret_cast<MUstream>(stream),
                                   kernel_args, config, &error)) {
        throw std::runtime_error("MUSA dispatch JIT failed: " + error);
    }
}
}  // namespace mooncake::jit
