cmake_minimum_required(VERSION 3.16)
project(mooncake CXX C)

# indicates cmake is invoked from top-level dir
set(GLOBAL_CONFIG "true")

include(mooncake-common/FindJsonCpp.cmake)
include(mooncake-common/FindGLOG.cmake)
include(mooncake-common/common.cmake)
# unit test
if (BUILD_UNIT_TESTS)
  enable_testing()
endif()

option(WITH_TE "build mooncake transfer engine and sample code" ON)
option(WITH_STORE "build mooncake store library and sample code" ON)
option(WITH_P2P_STORE "build p2p store library and sample code" OFF)
option(WITH_RUST_EXAMPLE "build the Rust interface and sample code for the transfer engine" OFF)
option(WITH_EP "build mooncake with expert parallelism support" OFF)

# pybind11: prefer system/finder (>=2.13), then vendored extern, finally FetchContent
find_package(pybind11 2.13 CONFIG QUIET)

if (NOT pybind11_FOUND)
  if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11/CMakeLists.txt)
    message(STATUS "Using vendored pybind11 from extern/pybind11")
    add_subdirectory(${CMAKE_CURRENT_SOURCE_DIR}/extern/pybind11)
  else()
    include(FetchContent)
    message(STATUS "Fetching pybind11 (v2.13.6) via FetchContent")
    FetchContent_Declare(pybind11
      GIT_REPOSITORY https://github.com/pybind/pybind11.git
      GIT_TAG v2.13.6
      GIT_SHALLOW TRUE
    )
    FetchContent_MakeAvailable(pybind11)
  endif()
endif()
set(PYTHON_EXECUTABLE "python3")
execute_process(
    COMMAND ${PYTHON_EXECUTABLE} -c "import sys; print(sys.path[-1])"
    OUTPUT_VARIABLE PYTHON_SYS_PATH
)
string(STRIP ${PYTHON_SYS_PATH} PYTHON_SYS_PATH)

if (USE_ETCD)
  add_compile_definitions(USE_ETCD)
  if (USE_ETCD_LEGACY)
    add_compile_definitions(USE_ETCD_LEGACY)
    message(STATUS "etcd as metadata server support is enabled (etcd-cpp-api-v3)")
  else()
    message(STATUS "etcd as metadata server support is enabled (go package)")
  endif()
endif()
option(STORE_USE_ETCD "build mooncake store with etcd" OFF)
if (STORE_USE_ETCD)
  add_compile_definitions(STORE_USE_ETCD)
endif()

option(STORE_USE_JEMALLOC "Use jemalloc in mooncake store master" OFF)

add_subdirectory(mooncake-common)
include_directories(mooncake-common/etcd)
include_directories(mooncake-common/include)

if (WITH_TE)
  add_subdirectory(mooncake-transfer-engine)
  include_directories(mooncake-transfer-engine/include)
endif()

if (WITH_STORE)
  message(STATUS "Mooncake Store will be built")
  add_subdirectory(mooncake-store)
  include_directories(mooncake-store/include)
endif()

if (WITH_EP)
  message(STATUS "Mooncake EP will be built")
  add_subdirectory(mooncake-ep)
  include_directories(mooncake-ep/include)
endif()

add_subdirectory(mooncake-integration)

if (WITH_P2P_STORE)
  add_subdirectory(mooncake-p2p-store)
  message(STATUS "P2P Store will be built")
endif()
