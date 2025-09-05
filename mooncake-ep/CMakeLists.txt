cmake_minimum_required(VERSION 3.16)
project(mooncake-ep)

# Find PyTorch's CMake prefix path
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import torch; print(torch.utils.cmake_prefix_path)"
    OUTPUT_VARIABLE PYTORCH_CMAKE_PATH
    OUTPUT_STRIP_TRAILING_WHITESPACE
)
if(NOT PYTORCH_CMAKE_PATH)
  message(WARNING "Could not find PyTorch CMake path! Please set Torch_DIR.")
else ()
  message(STATUS "Found PyTorch CMake path: ${PYTORCH_CMAKE_PATH}")
  list(APPEND CMAKE_PREFIX_PATH "${PYTORCH_CMAKE_PATH}/Torch")
endif()

set(TORCH_CUDA_ARCH_LIST "8.0;9.0")

find_package(CUDAToolkit REQUIRED)
find_package(Torch REQUIRED)
include_directories(${TORCH_INCLUDE_DIRS})

include_directories(include)
add_subdirectory(include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()

if (BUILD_EXAMPLES)
  add_subdirectory(example)
endif()
