#include <jit/musa_dispatch_jit.h>

#include <jit/musa_jit.hpp>

#include <cstdlib>
#include <sstream>
#include <stdexcept>

namespace mooncake::jit {
namespace {
constexpr const char* kEntry = "mooncake_ep_jit_dispatch_kernel";

MusaJitCompileConfig config_for(const char* stem) {
    MusaJitCompileConfig config;
    config.compiler = "/usr/local/musa/bin/mcc";
    config.bundler = "/usr/local/musa/bin/clang-offload-bundler";
    config.objcopy = "/usr/bin/objcopy";
    const char* cache = std::getenv("MOONCAKE_MUSA_JIT_CACHE_DIR");
    config.work_dir = cache != nullptr && *cache != '\0' ? cache : "/tmp/mooncake-musa-jit";
    const char* source = std::getenv("MOONCAKE_EP_JIT_SOURCE_DIR");
    const char* transfer = std::getenv("MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR");
#ifdef MOONCAKE_EP_JIT_SOURCE_DIR
    if (source == nullptr || *source == '\0') source = MOONCAKE_EP_JIT_SOURCE_DIR;
#endif
#ifdef MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR
    if (transfer == nullptr || *transfer == '\0') transfer = MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR;
#endif
    if (source != nullptr && *source != '\0') config.include_dirs.emplace_back(source);
    if (transfer != nullptr && *transfer != '\0') {
        config.include_dirs.emplace_back(transfer);
        config.include_dirs.emplace_back(std::string(transfer) + "/include");
    }
    config.extra_options = {"-DUSE_MUSA", "-DMOONCAKE_EP_USE_MUSA=1"};
    (void)stem;
    return config;
}
}

void launch_dispatch_jit_musa(int hidden, bool use_fp8, int num_warp_groups,
                              int num_warps_per_group, int num_sms,
                              const DispatchKernelArgs& args, void* stream) {
    std::ostringstream source;
    source << "#include <mooncake_ep_dispatch.cuh>\n"
           << "extern \"C\" __global__ void " << kEntry << "(const mooncake::DispatchKernelArgs p) {"
           << " mooncake::dispatch_kernel_impl<" << (use_fp8 ? "true" : "false") << ", "
           << num_warp_groups << ", " << num_warps_per_group << ", " << hidden
           << ">(p); }\n";
    const unsigned block = static_cast<unsigned>(num_warp_groups * num_warps_per_group * 32);
    void* kernel_args[] = {const_cast<DispatchKernelArgs*>(&args)};
    std::string error;
    if (!launch_musa_kernel_cached(source.str(), "dispatch", kEntry,
                                   static_cast<unsigned>(num_sms), 1, 1, block, 1, 1,
                                   0, reinterpret_cast<MUstream>(stream), kernel_args,
                                   config_for("dispatch"), &error)) {
        throw std::runtime_error("MUSA dispatch JIT failed: " + error);
    }
}
}  // namespace mooncake::jit
