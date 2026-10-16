cmake_minimum_required(VERSION 3.16)
project(mooncake-ep)

find_package(
  Python3
  COMPONENTS Interpreter Development.Module
  REQUIRED)

if(USE_CUDA)
  enable_language(CUDA)
  find_package(CUDAToolkit REQUIRED)
endif()

include_directories(include)
add_subdirectory(src)

# The JIT engine is only built for CUDA, and its tests need it.
if(BUILD_UNIT_TESTS AND USE_CUDA)
  add_subdirectory(tests)
endif()
