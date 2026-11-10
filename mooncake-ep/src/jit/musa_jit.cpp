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
#include <unordered_map>
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
    while (waitpid(pid, &status, 0) < 0 && errno == EINTR) {}
    result->exit_code = WIFEXITED(status) ? WEXITSTATUS(status)
                                          : 128 + WTERMSIG(status);
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

std::string musa_error(MUresult result);

std::string resolve_target_arch(std::string configured, std::string* error) {
    if (!configured.empty()) return configured;
    if (const char* value = std::getenv("MOONCAKE_MUSA_JIT_ARCH");
        value != nullptr && *value != '\0') {
        return value;
    }
    MUresult init_result = muInit(0);
    if (init_result != MUSA_SUCCESS) {
        if (error != nullptr) *error = "muInit failed while detecting MUSA arch: " + musa_error(init_result);
        return {};
    }
    MUdevice device;
    int major = 0;
    int minor = 0;
    if (muDeviceGet(&device, 0) != MUSA_SUCCESS ||
        muDeviceGetAttribute(&major, MU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MAJOR, device) != MUSA_SUCCESS ||
        muDeviceGetAttribute(&minor, MU_DEVICE_ATTRIBUTE_COMPUTE_CAPABILITY_MINOR, device) != MUSA_SUCCESS) {
        if (error != nullptr) *error = "could not query active MUSA device compute capability";
        return {};
    }
    return "mp_" + std::to_string(major) + std::to_string(minor);
}

std::string musa_error(MUresult result) {
    const char* text = nullptr;
    muGetErrorString(result, &text);
    std::ostringstream out;
    out << result;
    if (text != nullptr) out << " " << text;
    return out.str();
}

}  // namespace

MusaJitCompileResult compile_musa_to_image(
    std::string_view source, std::string_view stem,
    const MusaJitCompileConfig& config) {
    MusaJitCompileResult result;
    const auto started = std::chrono::steady_clock::now();
    std::error_code ec;
    std::filesystem::create_directories(config.work_dir, ec);
    if (ec) {
        result.log = "mkdir failed: " + ec.message();
        return result;
    }
    std::string arch_error;
    const std::string target_arch = resolve_target_arch(config.target_arch, &arch_error);
    if (target_arch.empty()) {
        result.log = arch_error;
        return result;
    }
    const auto source_path = config.work_dir / (std::string(stem) + ".cu");
    const auto object_path = config.work_dir / (std::string(stem) + ".o");
    const auto bundle_path = config.work_dir / (std::string(stem) + ".bundle");
    const auto image_path = config.work_dir / (std::string(stem) + ".device.o");
    if (!write_file(source_path, source)) {
        result.log = "could not write source: " + source_path.string();
        return result;
    }

    std::vector<std::string> compile = {
        config.compiler.empty() ? "mcc" : config.compiler.string(),
        "-x", "musa", "-std=c++17", "-O3", "-fPIC", "-c",
        "--cuda-gpu-arch=" + target_arch,
        source_path.string(), "-o", object_path.string()};
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
    const auto host_path = config.work_dir / (std::string(stem) + ".host.o");
    const std::string target = "musa-mtgpu-mt-musa--" + target_arch;
    std::vector<std::string> unbundle = {
        config.bundler.empty() ? "clang-offload-bundler" : config.bundler.string(),
        "--unbundle", "--type=o",
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
    if (error != nullptr) *error = "muModuleLoadData failed: " + musa_error(result);
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
    const MUresult result = muModuleGetFunction(function, module_, owned.c_str());
    if (result == MUSA_SUCCESS) return true;
    if (error != nullptr) *error = "muModuleGetFunction failed: " + musa_error(result);
    return false;
}

bool MusaJitModule::launch(MUfunction function, unsigned grid_x,
                           unsigned grid_y, unsigned grid_z, unsigned block_x,
                           unsigned block_y, unsigned block_z,
                           unsigned shared_bytes, MUstream stream, void** args,
                           std::string* error) const {
    const MUresult result = muLaunchKernel(function, grid_x, grid_y, grid_z,
                                            block_x, block_y, block_z,
                                            shared_bytes, stream, args, nullptr);
    if (result == MUSA_SUCCESS) return true;
    if (error != nullptr) *error = "muLaunchKernel failed: " + musa_error(result);
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
    static std::unordered_map<std::string, std::shared_ptr<Loaded>> cache;
    const std::string key = std::string(stem) + ":" +
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
