cmake_minimum_required(VERSION 3.18 FATAL_ERROR)
project(minimal_pybind LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Torch REQUIRED)         # pulls in PyTorch C++ ABI
find_package(Python3 COMPONENTS Development REQUIRED)

add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/pybind11) # assumes you vendor pybind11

pybind11_add_module(minimal MODULE test_binding.cpp)

target_link_libraries(minimal PRIVATE
        ${TORCH_LIBRARIES}
)

target_include_directories(minimal PRIVATE
        ${Python3_INCLUDE_DIRS}
        ${TORCH_INCLUDE_DIRS}
)
