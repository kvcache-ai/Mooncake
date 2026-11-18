// SPDX-License-Identifier: Apache-2.0

#include <jit/musa_jit.hpp>

#include <chrono>
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string_view>
#include <unordered_map>
#include <dlfcn.h>
#include <sys/wait.h>
#include <unistd.h>

namespace mooncake::jit {
namespace {

struct ProcessResult {
    int exit_code = -1;
    std::string output;
};

bool run_process(const std::vector<std::string>& argv, ProcessResult* result) {
    if (argv.empty() || result == nullptr) return false;
    int pipe_fd[2] = {-1, -1};
    if (pipe(pipe_fd) != 0) {
        result->output = std::strerror(errno);
        return false;
    }
    std::vector<char*> raw;
    raw.reserve(argv.size() + 1);
    for (const auto& arg : argv) raw.push_back(const_cast<char*>(arg.c_str()));
    raw.push_back(nullptr);
    const pid_t pid = fork();
    if (pid < 0) {
        result->output = std::strerror(errno);
        close(pipe_fd[0]);
        close(pipe_fd[1]);
        return false;
    }
    if (pid == 0) {
        close(pipe_fd[0]);
        dup2(pipe_fd[1], STDOUT_FILENO);
        dup2(pipe_fd[1], STDERR_FILENO);
        close(pipe_fd[1]);
        execvp(raw[0], raw.data());
        _exit(127);
    }
    close(pipe_fd[1]);
    char buffer[4096];
    for (;;) {
        const ssize_t n = read(pipe_fd[0], buffer, sizeof(buffer));
        if (n > 0) {
            result->output.append(buffer, static_cast<size_t>(n));
        } else if (n == 0) {
            break;
        } else if (errno != EINTR) {
            break;
        }
    }
    close(pipe_fd[0]);
    int status = 0;
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
    result->exit_code =
        WIFEXITED(status) ? WEXITSTATUS(status) : 128 + WTERMSIG(status);
    return result->exit_code == 0;
}

bool write_file(const std::filesystem::path& path, std::string_view data) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    out.write(data.data(), static_cast<std::streamsize>(data.size()));
    return out.good();
}

std::vector<char> read_file(const std::filesystem::path& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return {};
    return std::vector<char>((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
}

std::string musa_error(MUresult result) {
    const char* text = nullptr;
    muGetErrorString(result, &text);
    std::ostringstream out;
    out << result;
    if (text != nullptr) out << " " << text;
    return out.str();
}

std::filesystem::path library_dir() {
    Dl_info info{};
    if (dladdr(reinterpret_cast<const void*>(&library_dir), &info) == 0 ||
        info.dli_fname == nullptr) {
        return {};
    }
    return std::filesystem::path(info.dli_fname).parent_path();
}

}  // namespace

void apply_musa_jit_environment(MusaJitCompileConfig* config) {
    if (config == nullptr) return;
    auto set_path = [](std::filesystem::path* path, const char* name) {
        if (const char* value = std::getenv(name);
            value != nullptr && *value != '\0') {
            *path = value;
        }
    };
    set_path(&config->compiler, "MOONCAKE_MUSA_JIT_COMPILER");
    set_path(&config->bundler, "MOONCAKE_MUSA_JIT_BUNDLER");
    set_path(&config->objcopy, "MOONCAKE_MUSA_JIT_OBJCOPY");
    if (config->target_arch.empty()) {
        if (const char* value = std::getenv("MOONCAKE_MUSA_JIT_ARCH");
            value != nullptr && *value != '\0') {
            config->target_arch = value;
        }
    }
    if (const char* value = std::getenv("MOONCAKE_MUSA_JIT_EXTRA_OPTIONS");
        value != nullptr && *value != '\0') {
        std::istringstream options(value);
        std::string option;
        while (options >> option) config->extra_options.push_back(option);
    }
}

MusaJitCompileConfig make_musa_jit_compile_config() {
    MusaJitCompileConfig config;
    const char* cache = std::getenv("MOONCAKE_MUSA_JIT_CACHE_DIR");
    config.work_dir =
        cache != nullptr && *cache != '\0' ? cache : "/tmp/mooncake-musa-jit";
    std::filesystem::path source_dir;
    std::filesystem::path transfer_dir;
    if (const char* value = std::getenv("MOONCAKE_EP_JIT_SOURCE_DIR");
        value != nullptr && *value != '\0') {
        source_dir = value;
    }
    if (const char* value =
            std::getenv("MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR");
        value != nullptr && *value != '\0') {
        transfer_dir = value;
    }
#ifdef MOONCAKE_EP_JIT_SOURCE_DIR
    if (source_dir.empty()) source_dir = MOONCAKE_EP_JIT_SOURCE_DIR;
#endif
#ifdef MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR
    if (transfer_dir.empty())
        transfer_dir = MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR;
#endif
    const auto installed_root = library_dir() / "mooncake_ep_jit";
    const auto installed_source = installed_root / "include" / "mooncake_ep";
    if (source_dir.empty() || !std::filesystem::is_directory(source_dir)) {
        source_dir = installed_source;
    }
    if (transfer_dir.empty() || !std::filesystem::is_directory(transfer_dir))
        transfer_dir = installed_root;
    config.include_dirs = {source_dir, transfer_dir, transfer_dir / "include"};
    config.extra_options = {"-DUSE_MUSA", "-DMOONCAKE_EP_USE_MUSA=1"};
    apply_musa_jit_environment(&config);
    return config;
}

bool resolve_musa_jit_target_arch(MusaJitCompileConfig* config,
                                  std::string* error) {
    if (config == nullptr) return false;
    if (!config->target_arch.empty()) return true;
    if (const char* value = std::getenv("MOONCAKE_MUSA_JIT_ARCH");
        value != nullptr && *value != '\0') {
        config->target_arch = value;
        return true;
    }
    static std::mutex arch_mutex;
    static std::string detected_arch;
    std::lock_guard<std::mutex> lock(arch_mutex);
    if (!detected_arch.empty()) {
        config->target_arch = detected_arch;
        return true;
    }
    if (muInit(0) != MUSA_SUCCESS) {
        if (error != nullptr)
            *error = "muInit failed while detecting MUSA arch";
        return false;
    }
    MUdevice device = 0;
    int major = 0;
    int minor = 0;
    if (muDeviceGet(&device, 0) != MUSA_SUCCESS ||
        muDeviceGetAttribute(&major,
                             MU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MAJOR,
                             device) != MUSA_SUCCESS ||
        muDeviceGetAttribute(&minor,
                             MU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MINOR,
                             device) != MUSA_SUCCESS) {
        if (error != nullptr) {
            *error =
                "could not query MUSA device compute capability; set "
                "MOONCAKE_MUSA_JIT_ARCH";
        }
        return false;
    }
    detected_arch = "mp_" + std::to_string(major) + std::to_string(minor);
    config->target_arch = detected_arch;
    return true;
}

MusaJitCompileResult compile_musa_to_image(std::string_view source,
                                           std::string_view stem,
                                           const MusaJitCompileConfig& config) {
    MusaJitCompileResult result;
    const auto started = std::chrono::steady_clock::now();
    std::error_code ec;
    std::filesystem::create_directories(config.work_dir, ec);
    if (ec) {
        result.log = "mkdir failed: " + ec.message();
        return result;
    }
    if (config.target_arch.empty()) {
        result.log = "MUSA JIT target architecture is unset";
        return result;
    }
    const std::string& target_arch = config.target_arch;
    const std::string prefix =
        std::string(stem) + ".p" + std::to_string(getpid());
    const auto source_path = config.work_dir / (prefix + ".cu");
    const auto object_path = config.work_dir / (prefix + ".o");
    const auto bundle_path = config.work_dir / (prefix + ".bundle");
    const auto image_path = config.work_dir / (prefix + ".device.o");
    if (!write_file(source_path, source)) {
        result.log = "could not write source: " + source_path.string();
        return result;
    }

    std::vector<std::string> compile = {
        config.compiler.empty() ? "mcc" : config.compiler.string(),
        "-x",
        "musa",
        "-std=c++20",
        "-O3",
        "-fPIC",
        "-c",
        "--cuda-gpu-arch=" + target_arch,
        source_path.string(),
        "-o",
        object_path.string()};
    for (const auto& include : config.include_dirs)
        compile.push_back("-I" + include.string());
    compile.insert(compile.end(), config.extra_options.begin(),
                   config.extra_options.end());
    ProcessResult compile_result;
    if (!run_process(compile, &compile_result)) {
        result.log = "mcc failed (exit " +
                     std::to_string(compile_result.exit_code) + ")\n" +
                     compile_result.output;
        return result;
    }

    std::vector<std::string> dump = {
        config.objcopy.empty() ? "objcopy" : config.objcopy.string(),
        "--dump-section", ".mt_fatbin=" + bundle_path.string(),
        object_path.string()};
    ProcessResult dump_result;
    if (!run_process(dump, &dump_result)) {
        result.log = "objcopy failed (exit " +
                     std::to_string(dump_result.exit_code) + ")\n" +
                     dump_result.output;
        return result;
    }

    // clang-offload-bundler requires both members in --outputs, even though
    // the host member is intentionally discarded.
    const auto host_path = config.work_dir / (prefix + ".host.o");
    const std::string target = "musa-mtgpu-mt-musa--" + target_arch;
    std::vector<std::string> unbundle = {
        config.bundler.empty() ? "clang-offload-bundler"
                               : config.bundler.string(),
        "--unbundle",
        "--type=o",
        "--targets=host-x86_64-unknown-linux," + target,
        "--inputs=" + bundle_path.string(),
        "--outputs=" + host_path.string() + "," + image_path.string()};
    ProcessResult unbundle_result;
    if (!run_process(unbundle, &unbundle_result)) {
        result.log = "clang-offload-bundler failed (exit " +
                     std::to_string(unbundle_result.exit_code) + ")\n" +
                     unbundle_result.output;
        return result;
    }
    result.image = read_file(image_path);
    result.ok = !result.image.empty();
    result.log = compile_result.output + unbundle_result.output;
    result.compiler_sec = std::chrono::duration<double>(
                              std::chrono::steady_clock::now() - started)
                              .count();
    if (!result.ok) result.log += "device image is empty\n";
    return result;
}

MusaJitModule::~MusaJitModule() { reset(); }

bool MusaJitModule::load(const std::vector<char>& image, std::string* error) {
    reset();
    const MUresult result = muModuleLoadData(&module_, image.data());
    if (result == MUSA_SUCCESS) return true;
    if (error != nullptr)
        *error = "muModuleLoadData failed: " + musa_error(result);
    module_ = nullptr;
    return false;
}

void MusaJitModule::reset() {
    if (module_ != nullptr) {
        muModuleUnload(module_);
        module_ = nullptr;
    }
}

bool MusaJitModule::get_function(std::string_view name, MUfunction* function,
                                 std::string* error) const {
    if (module_ == nullptr || function == nullptr) return false;
    const std::string owned(name);
    const MUresult result =
        muModuleGetFunction(function, module_, owned.c_str());
    if (result == MUSA_SUCCESS) return true;
    if (error != nullptr)
        *error = "muModuleGetFunction failed: " + musa_error(result);
    return false;
}

bool MusaJitModule::launch(MUfunction function, unsigned grid_x,
                           unsigned grid_y, unsigned grid_z, unsigned block_x,
                           unsigned block_y, unsigned block_z,
                           unsigned shared_bytes, MUstream stream, void** args,
                           std::string* error) const {
    const MUresult result =
        muLaunchKernel(function, grid_x, grid_y, grid_z, block_x, block_y,
                       block_z, shared_bytes, stream, args, nullptr);
    if (result == MUSA_SUCCESS) return true;
    if (error != nullptr)
        *error = "muLaunchKernel failed: " + musa_error(result);
    return false;
}

bool launch_musa_kernel_cached(
    std::string_view source, std::string_view stem, std::string_view entry,
    unsigned grid_x, unsigned grid_y, unsigned grid_z, unsigned block_x,
    unsigned block_y, unsigned block_z, unsigned shared_bytes, MUstream stream,
    void** args, const MusaJitCompileConfig& config, std::string* error) {
    struct Loaded {
        MusaJitModule module;
        MUfunction function = nullptr;
    };
    static std::mutex mutex;
    // Keep modules alive until process exit without invoking MUSA unload after
    // the runtime context has already been torn down.
    static auto* cache =
        new std::unordered_map<std::string, std::shared_ptr<Loaded>>;
    const std::string key =
        std::string(stem) + ":" +
        std::to_string(std::hash<std::string_view>{}(source));
    std::shared_ptr<Loaded> loaded;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = cache.find(key);
        if (it != cache.end()) loaded = it->second;
        if (!loaded) {
            auto compiled = compile_musa_to_image(source, stem, config);
            if (!compiled.ok) {
                if (error != nullptr) *error = compiled.log;
                return false;
            }
            loaded = std::make_shared<Loaded>();
            if (!loaded->module.load(compiled.image, error) ||
                !loaded->module.get_function(entry, &loaded->function, error)) {
                return false;
            }
            cache.emplace(key, loaded);
        }
    }
    return loaded->module.launch(loaded->function, grid_x, grid_y, grid_z,
                                 block_x, block_y, block_z, shared_bytes,
                                 stream, args, error);
}

}  // namespace mooncake::jit
