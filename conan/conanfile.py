from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.cmake import cmake_layout, CMakeToolchain, CMake

class MooncakeRecipe(ConanFile):
    name = "mooncake"
    version = "0.1"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps"

    requires = [
        "yaml-cpp/0.8.0",
        "jsoncpp/1.9.5",
        "glog/0.7.1",
        "gflags/2.2.2"
    ]

    def configure(self):
        self.options["glog"].with_gflags = True

    def layout(self):
        cmake_layout(self, src_folder="..", build_folder="../build")

    def generate(self):
        tc = CMakeToolchain(self)
        
        tc.user_presets_path = False # 不生成 CMakeUserPresets.json

        if self.settings.compiler == "clang" and self.settings.compiler.libcxx == "libc++":
            tc.preprocessor_definitions["_LIBCPP_ENABLE_THREAD_SAFETY_ANNOTATIONS"] = ""
            tc.extra_cxxflags.append("-fexperimental-library")

        # 严格限制find_package仅查找conan管理的包，不检索系统库
        tc.variables["CMAKE_FIND_ROOT_PATH_MODE_PACKAGE"] = "ONLY"
        tc.variables["CMAKE_FIND_ROOT_PATH_MODE_LIBRARY"] = "ONLY"
        tc.variables["CMAKE_FIND_ROOT_PATH_MODE_INCLUDE"] = "ONLY"

        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.generator = "Ninja"

        cmake.configure()
        cmake.build()
