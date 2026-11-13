// Copyright 2026 KVCache.AI
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

#include <gtest/gtest.h>
#include <jit/jit_runtime.hpp>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <system_error>

namespace mooncake {
namespace jit {
namespace {

namespace fs = std::filesystem;

constexpr int kCompileFailedExit = 23;
fs::path test_executable;

const char kFakeNvcc[] = R"sh(#!/bin/sh
mode=${FAKE_NVCC_MODE:?}
if [ "${1:-}" = "--version" ]; then
  if [ "$mode" = "version_fail" ]; then
    printf 'version probe failed\n' >&2
    exit 1
  fi
  if [ "$mode" = "exec_vanish" ]; then
    rm -f -- "$0"
  fi
  printf 'fake nvcc release 1.0\n'
  exit 0
fi
printf 'attempt\n' >> "${FAKE_NVCC_COUNT_FILE:?}"
case "$mode" in
  success)
    output=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "-o" ]; then
        shift
        output=$1
      fi
      shift
    done
    printf 'fake cubin\n' > "$output"
    exit 0
    ;;
  compile_error)
    printf 'kernel.cu(1): error: invalid device code\n' >&2
    exit 1
    ;;
  signal)
    kill -TERM "$$"
    exit 128
    ;;
  exit_127)
    exit 127
    ;;
  missing_cubin)
    exit 0
    ;;
  *)
    exit 64
    ;;
esac
)sh";

std::string ReadFile(const fs::path& path) {
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
}

void WriteNvcc(const fs::path& path) {
    {
        std::ofstream output(path, std::ios::trunc);
        ASSERT_TRUE(output.good());
        output << kFakeNvcc;
    }
    ASSERT_EQ(chmod(path.c_str(), 0700), 0);
}

int AttemptCount(const fs::path& path) {
    const std::string contents = ReadFile(path);
    int count = 0;
    for (char c : contents) count += c == '\n';
    return count;
}

bool HasNamedFile(const fs::path& root, const std::string& name) {
    std::error_code ec;
    if (!fs::exists(root, ec)) return false;
    fs::recursive_directory_iterator it(root, ec);
    const fs::recursive_directory_iterator end;
    while (!ec && it != end) {
        if (it->path().filename() == name) return true;
        it.increment(ec);
    }
    return false;
}

class TestDirectory {
   public:
    TestDirectory() {
        const char* configured = std::getenv("MOONCAKE_EP_TEST_TMPDIR");
        fs::path base = configured != nullptr ? fs::path(configured)
                                              : fs::temp_directory_path();
        fs::create_directories(base);
        fs::path pattern = base / "ep-jit-compile-failure-XXXXXX";
        std::string value = pattern.string();
        std::vector<char> buffer(value.begin(), value.end());
        buffer.push_back('\0');
        char* created = mkdtemp(buffer.data());
        EXPECT_NE(created, nullptr);
        if (created != nullptr) path_ = created;
    }

    ~TestDirectory() {
        if (path_.empty()) return;
        std::error_code ec;
        fs::remove_all(path_, ec);
    }

    const fs::path& path() const { return path_; }

   private:
    fs::path path_;
};

int RunChild(const fs::path& cache_root, const fs::path& nvcc,
             const std::string& mode, const fs::path& count_file,
             const fs::path& source_dir, const fs::path& log_path) {
    const pid_t pid = fork();
    if (pid == 0) {
        const int log_fd =
            open(log_path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0600);
        if (log_fd >= 0) {
            dup2(log_fd, STDOUT_FILENO);
            dup2(log_fd, STDERR_FILENO);
            close(log_fd);
        }
        setenv("MOONCAKE_EP_JIT_CACHE_DIR", cache_root.c_str(), 1);
        setenv("MOONCAKE_EP_JIT_NVCC", nvcc.c_str(), 1);
        setenv("MOONCAKE_EP_JIT_SOURCE_DIR", source_dir.c_str(), 1);
        setenv("MOONCAKE_EP_JIT_LOG", "1", 1);
        setenv("FAKE_NVCC_MODE", mode.c_str(), 1);
        setenv("FAKE_NVCC_COUNT_FILE", count_file.c_str(), 1);
        setenv("NVCC_ARCH_FLAGS", "-arch=sm_90", 1);
        unsetenv("NVCC");
        unsetenv("CUDA_HOME");
        unsetenv("CUDA_PATH");
        unsetenv("MOONCAKE_EP_HOME");
        unsetenv("MOONCAKE_EP_JIT_BUILD_INCLUDE_DIR");
        unsetenv("MOONCAKE_EP_JIT_CUDA_INCLUDE_DIR");
        unsetenv("NCCL_HOME");
        unsetenv("NVCC_EXTRA_FLAGS");
        execl(test_executable.c_str(), test_executable.c_str(), "--jit-child",
              cache_root.c_str(), nvcc.c_str(), mode.c_str(),
              count_file.c_str(), source_dir.c_str(), nullptr);
        _exit(126);
    }
    if (pid < 0) return -1;
    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) return -1;
    }
    if (WIFEXITED(status)) return WEXITSTATUS(status);
    if (WIFSIGNALED(status)) return 128 + WTERMSIG(status);
    return -1;
}

struct ChildCase {
    fs::path root;
    fs::path cache;
    fs::path nvcc;
    fs::path count;
    fs::path source;
    fs::path log;
};

ChildCase MakeCase(const fs::path& root) {
    ChildCase item{root,
                   root / "cache",
                   root / "nvcc",
                   root / "count",
                   root / "include" / "mooncake_ep",
                   root / "child.log"};
    fs::create_directories(item.source);
    return item;
}

int LaunchChild(int argc, char** argv) {
    if (argc != 7) return 125;
    setenv("MOONCAKE_EP_JIT_CACHE_DIR", argv[2], 1);
    setenv("MOONCAKE_EP_JIT_NVCC", argv[3], 1);
    setenv("FAKE_NVCC_MODE", argv[4], 1);
    setenv("FAKE_NVCC_COUNT_FILE", argv[5], 1);
    setenv("MOONCAKE_EP_JIT_SOURCE_DIR", argv[6], 1);

    static const char kernel_source[] = "extern \"C\" __global__ void k() {}";
    static const int identity = 0;
    JitKernelVariant variant;
    variant.kernel_family = "cache_test";
    variant.variant_name = "retry";
    variant.source = kernel_source;
    variant.entry_name = "k";
    variant.identity = &identity;
    variant.runtime_key = 7;
    variant.num_blocks = 1;
    variant.block_dim = 1;
    variant.min_sm = 80;
    variant.cooperative = false;

    std::string error;
    const JitKernelStatus status =
        launch_jit_kernel(variant, nullptr, 0, nullptr, &error);
    if (!error.empty()) std::cerr << error << '\n';
    return status == JitKernelStatus::kLaunched ? 0 : kCompileFailedExit;
}

TEST(JitCompileFailureTest, KilledCompilerIsRetriedByTheNextProcess) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    WriteNvcc(item.nvcc);

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "signal", item.count, item.source,
                       item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 1);
    EXPECT_FALSE(HasNamedFile(item.cache, "compile.failed"));

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "success", item.count,
                       item.source, item.log),
              0);
    EXPECT_EQ(AttemptCount(item.count), 2);
    EXPECT_TRUE(HasNamedFile(item.cache, "kernel.cubin"));
}

TEST(JitCompileFailureTest, Exit127IsRetriedByTheNextProcess) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    WriteNvcc(item.nvcc);

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "exit_127", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 1);
    EXPECT_FALSE(HasNamedFile(item.cache, "compile.failed"));

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "success", item.count,
                       item.source, item.log),
              0);
    EXPECT_EQ(AttemptCount(item.count), 2);
}

TEST(JitCompileFailureTest, MissingCubinIsRetriedByTheNextProcess) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    WriteNvcc(item.nvcc);

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "missing_cubin", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 1);
    EXPECT_FALSE(HasNamedFile(item.cache, "compile.failed"));

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "success", item.count,
                       item.source, item.log),
              0);
    EXPECT_EQ(AttemptCount(item.count), 2);
}

TEST(JitCompileFailureTest, LegacyTransientMarkerDoesNotBlockRetry) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    WriteNvcc(item.nvcc);

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "missing_cubin", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 1);

    fs::path legacy_marker;
    std::error_code ec;
    fs::recursive_directory_iterator it(item.cache, ec);
    const fs::recursive_directory_iterator end;
    while (!ec && it != end) {
        if (it->path().filename() == "compile.log") {
            legacy_marker = it->path().parent_path() / "compile.failed";
            break;
        }
        it.increment(ec);
    }
    ASSERT_FALSE(legacy_marker.empty());
    {
        std::ofstream marker(legacy_marker, std::ios::trunc);
        marker << "NVCC compile failed: cubin was not produced\nexit_code=0\n";
    }

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "success", item.count,
                       item.source, item.log),
              0);
    EXPECT_EQ(AttemptCount(item.count), 2);
}

TEST(JitCompileFailureTest, ExecFailureIsRetriedByTheNextProcess) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    WriteNvcc(item.nvcc);

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "exec_vanish", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 0);
    EXPECT_FALSE(HasNamedFile(item.cache, "compile.failed"));

    WriteNvcc(item.nvcc);
    ASSERT_EQ(RunChild(item.cache, item.nvcc, "success", item.count,
                       item.source, item.log),
              0);
    EXPECT_EQ(AttemptCount(item.count), 1);
}

TEST(JitCompileFailureTest, CompilerErrorRemainsCachedAcrossProcesses) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    WriteNvcc(item.nvcc);

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "compile_error", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 1);
    EXPECT_TRUE(HasNamedFile(item.cache, "compile.failed"));

    ASSERT_EQ(RunChild(item.cache, item.nvcc, "success", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 1);
}

TEST(JitCompileFailureTest, FailedVersionProbeDoesNotCreateCacheArtifacts) {
    TestDirectory directory;
    const ChildCase item = MakeCase(directory.path());
    const fs::path missing_nvcc = directory.path() / "missing-nvcc";

    ASSERT_EQ(RunChild(item.cache, missing_nvcc, "success", item.count,
                       item.source, item.log),
              kCompileFailedExit);
    EXPECT_EQ(AttemptCount(item.count), 0);
    EXPECT_FALSE(HasNamedFile(item.cache, "compile.failed"));
    EXPECT_FALSE(HasNamedFile(item.cache, "compile.log"));
    const std::string log = ReadFile(item.log);
    EXPECT_NE(log.find("compiler_probe=failed"), std::string::npos);
    EXPECT_NE(log.find("No such file"), std::string::npos);
    EXPECT_NE(log.find("errno=2"), std::string::npos);
}

}  // namespace
}  // namespace jit
}  // namespace mooncake

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--jit-child") {
        return mooncake::jit::LaunchChild(argc, argv);
    }
    mooncake::jit::test_executable = std::filesystem::absolute(argv[0]);
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
