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

option(MOONCAKE_EP_USE_MUSA "Build with MUSA (Moore Threads) instead of CUDA" OFF)

if(NOT MOONCAKE_EP_USE_MUSA)
  set(TORCH_CUDA_ARCH_LIST "8.0;9.0" CACHE STRING "CUDA architectures")
  find_package(CUDAToolkit REQUIRED)
  # https://discuss.pytorch.org/t/failed-to-find-nvtoolsext/179635/13
  if(NOT TARGET CUDA::nvToolsExt AND TARGET CUDA::nvtx3)
    add_library(CUDA::nvToolsExt INTERFACE IMPORTED)
    target_compile_definitions(
        CUDA::nvToolsExt INTERFACE
        TORCH_CUDA_USE_NVTX3
    )
    target_link_libraries(CUDA::nvToolsExt INTERFACE CUDA::nvtx3)
  endif()
endif()

find_package(Torch REQUIRED)
include_directories(${TORCH_INCLUDE_DIRS})

find_package(Python REQUIRED COMPONENTS Interpreter Development)
include_directories(${Python_INCLUDE_DIRS})

if(MOONCAKE_EP_USE_MUSA)
    # Use torch_musa's CMake integration for correct include ordering
    execute_process(
        COMMAND ${PYTHON_EXECUTABLE} -c "import torch_musa; print(torch_musa.__path__[0])"
        OUTPUT_VARIABLE TORCH_MUSA_PATH
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    execute_process(
        COMMAND ${PYTHON_EXECUTABLE} -c "import os; print(os.environ.get('MUSA_HOME', '/usr/local/musa'))"
        OUTPUT_VARIABLE MUSA_HOME
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
    # Find MUSA toolchain (provides MUSA_ADD_LIBRARY for .mu compilation)
    list(APPEND CMAKE_MODULE_PATH ${MUSA_HOME}/cmake)
    find_package(MUSA REQUIRED)
    # TorchMusa handles include ordering to avoid header conflicts
    list(APPEND CMAKE_PREFIX_PATH ${TORCH_MUSA_PATH}/share/cmake)
    find_package(TorchMusa REQUIRED)
    # Add generated_cuda_compatible as SYSTEM include (lower priority than torch)
    # to provide ATen/musa/MUSAContext.h without conflicting with real ATen headers
    include_directories(SYSTEM ${TORCH_MUSA_PATH}/share/generated_cuda_compatible/include)
    # MUSAContext.h includes "torch_musa/csrc/..." — need the parent of torch_musa/
    get_filename_component(TORCH_MUSA_PARENT ${TORCH_MUSA_PATH} DIRECTORY)
    include_directories(SYSTEM ${TORCH_MUSA_PARENT})
endif()

include_directories(include)
add_subdirectory(src)
