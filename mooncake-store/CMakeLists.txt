cmake_minimum_required(VERSION 3.28)
project(MooncakeStore VERSION 2.0.0)

# Use C++20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Include common cmake modules from mooncake-common
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common")
include(FindGLOG)
include(FindJsonCpp)

# Find gflags - system package provides gflags_shared target
find_package(gflags REQUIRED CONFIG
  PATHS /usr/lib/x86_64-linux-gnu/cmake/gflags
  NO_DEFAULT_PATH)
# Create gflags::gflags alias for compatibility
if(TARGET gflags_shared AND NOT TARGET gflags::gflags)
  add_library(gflags::gflags ALIAS gflags_shared)
endif()

# transfer_engine imported target (already built by mooncake-transfer-engine)
if(NOT TARGET transfer_engine)
  add_library(transfer_engine STATIC IMPORTED)
  set_target_properties(transfer_engine PROPERTIES
    IMPORTED_LOCATION "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/build/src/libtransfer_engine.a"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include"
    INTERFACE_LINK_LIBRARIES "ibverbs;jsoncpp;glog::glog;gflags::gflags")
endif()

# asio_shared from mooncake-common
if(NOT TARGET asio_shared)
  # Check if asio_shared is already built (e.g., by transfer-engine)
  find_library(ASIO_SHARED_LIB asio_shared
    PATHS ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/build/mooncake-common-src
    NO_DEFAULT_PATH)
  if(ASIO_SHARED_LIB)
    add_library(asio_shared SHARED IMPORTED)
    set_target_properties(asio_shared PROPERTIES
      IMPORTED_LOCATION ${ASIO_SHARED_LIB}
      INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common/include"
      INTERFACE_COMPILE_DEFINITIONS "ASIO_DYN_LINK;ASIO_SEPARATE_COMPILATION")
  else()
    # Build from source as a fallback
    add_subdirectory(../mooncake-common/src
                     ${CMAKE_CURRENT_BINARY_DIR}/mooncake-common-src)
  endif()
endif()

option(ENABLE_KV_EVENTS
       "Build master KV events ZMQ publisher (requires libzmq when ON)" OFF)

# Extract version components for C++ usage
set(MOONCAKE_STORE_VERSION ${PROJECT_VERSION})

# Read display version from mooncake-wheel/pyproject.toml
set(PYPROJECT_TOML_PATH "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-wheel/pyproject.toml")
if(EXISTS "${PYPROJECT_TOML_PATH}")
    file(READ "${PYPROJECT_TOML_PATH}" PYPROJECT_CONTENT)
    string(REGEX MATCH "version = \"([^\"]+)\"" _ "${PYPROJECT_CONTENT}")
    set(MOONCAKE_DISPLAY_VERSION_BASE "${CMAKE_MATCH_1}")
endif()
if(NOT MOONCAKE_DISPLAY_VERSION_BASE)
    message(WARNING "Could not read display version from ${PYPROJECT_TOML_PATH}; falling back to MOONCAKE_STORE_VERSION")
    set(MOONCAKE_DISPLAY_VERSION_BASE "${MOONCAKE_STORE_VERSION}")
endif()

# Get short git commit hash for display
find_package(Git QUIET)
if(GIT_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        OUTPUT_VARIABLE GIT_SHORT_HASH
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
    )
endif()
if(NOT GIT_SHORT_HASH)
    set(GIT_SHORT_HASH "unknown")
endif()
set(MOONCAKE_DISPLAY_VERSION "${MOONCAKE_DISPLAY_VERSION_BASE} (git: ${GIT_SHORT_HASH})")

configure_file(include/version.h.in include/version.h @ONLY)

if (STORE_USE_ETCD)
    set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)
endif()

if (STORE_USE_K8S_LEASE)
    set(K8S_LEASE_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/k8s-lease/)
    set(K8S_LEASE_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/k8s-lease/libk8s_lease_wrapper.so)
endif()

if (STORE_USE_REDIS)
    find_path(MOONCAKE_STORE_HIREDIS_INCLUDE_DIR hiredis/hiredis.h REQUIRED)
    find_library(MOONCAKE_STORE_HIREDIS_LIBRARY hiredis REQUIRED)
    message(STATUS "Redis HA backend: Enabled")
endif()

if (NOT STORE_USE_ETCD AND NOT STORE_USE_REDIS AND NOT STORE_USE_K8S_LEASE)
    message(STATUS "Store HA backends are disabled")
endif()

# Add include directories
include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/fake_include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/cachelib_memory_allocator/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/mooncake-store/proto/
    ${CMAKE_CURRENT_SOURCE_DIR}/include/
    ${CMAKE_CURRENT_BINARY_DIR}/include/
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-transfer-engine/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-common/include
    ${ETCD_WRAPPER_INCLUDE}
    ${K8S_LEASE_WRAPPER_INCLUDE}
)

# Add subdirectories
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
endif()

if (BUILD_BENCHMARK)
    add_subdirectory(benchmarks)
endif()
