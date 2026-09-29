from conan import ConanFile
from conan.tools.cmake import cmake_layout, CMakeToolchain
from pathlib import Path

class MooncakeRecipe(ConanFile):
    name = "mooncake"
    version = "0.1"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps"  # CMakeDeps负责生成依赖的Config.cmake

    requires = [
        "yaml-cpp/0.8.0",
        "jsoncpp/1.9.5",
        "glog/0.7.1",
        "gflags/2.2.2"
    ]

    def configure(self):
        # glog 开启gflags支持，解决 fLS::FLAGS_log_dir 缺失
        self.options["glog"].with_gflags = True

    def layout(self):
        cmake_layout(self)
        self.folders.generators = "generators"

    def generate(self):
        # 手动创建 CMakeToolchain，生成 conan_toolchain.cmake
        tc = CMakeToolchain(self)
        if self.settings.compiler == "clang" and self.settings.compiler.libcxx == "libc++":
            tc.preprocessor_definitions["_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS"] = ""
            tc.extra_cxxflags.append("-fexperimental-library")
        # Force CMake find_* to only look inside CMAKE_FIND_ROOT_PATH (conan packages), skip system paths
        tc.variables["CMAKE_FIND_ROOT_PATH_MODE_PACKAGE"] = "ONLY"
        tc.variables["CMAKE_FIND_ROOT_PATH_MODE_LIBRARY"] = "ONLY"
        tc.variables["CMAKE_FIND_ROOT_PATH_MODE_INCLUDE"] = "ONLY"
        tc.generate()
