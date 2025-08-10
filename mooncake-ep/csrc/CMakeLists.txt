# NOTES: this CMake is only for debugging; for setup, please use Torch extension
cmake_minimum_required(VERSION 3.10)
project(mxa_ep LANGUAGES CUDA CXX)
set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC")
set(CUDA_SEPARABLE_COMPILATION ON)
list(APPEND CUDA_NVCC_FLAGS "-O3")
list(APPEND CUDA_NVCC_FLAGS "--ptxas-options=--verbose,--register-usage-level=10,--warn-on-local-memory-usage")

find_package(CUDAToolkit REQUIRED)
find_package(pybind11 REQUIRED)
find_package(Torch REQUIRED)

# Seems bugs with CMake, NVCC 12 and C++ 17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CUDA_STANDARD 14)

include_directories(${CUDA_TOOLKIT_ROOT_DIR}/include ${TORCH_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS} ${TRANSFER_ENGINE_INCLUDE_DIR})
link_directories(${TORCH_INSTALL_PREFIX}/lib ${CUDA_TOOLKIT_ROOT_DIR}/lib)

# Link CPP and CUDA together
pybind11_add_module(mxa_ep_cpp mxa_ep.cpp mlx5gda.cpp mxa_kernel.cu)
target_link_libraries(mxa_ep_cpp PRIVATE ${EP_CUDA_LIBRARIES} ${TORCH_LIBRARIES} torch_python)
