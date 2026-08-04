project(MooncakeStore VERSION 2.0.0)

set(MOONCAKE_STORE_INCLUDE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/include")

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

find_path(ZSTD_INCLUDE_DIR zstd.h REQUIRED)
find_library(ZSTD_LIBRARY zstd REQUIRED)
add_library(zstd::libzstd UNKNOWN IMPORTED)
set_target_properties(
  zstd::libzstd PROPERTIES IMPORTED_LOCATION "${ZSTD_LIBRARY}"
                           INTERFACE_INCLUDE_DIRECTORIES "${ZSTD_INCLUDE_DIR}")

find_path(XXHASH_INCLUDE_DIR xxhash.h REQUIRED)
find_library(XXHASH_LIBRARY NAMES xxhash libxxhash REQUIRED)
add_library(xxHash::xxhash UNKNOWN IMPORTED)
set_target_properties(
  xxHash::xxhash
  PROPERTIES IMPORTED_LOCATION "${XXHASH_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES
                                                   "${XXHASH_INCLUDE_DIR}")

find_path(URING_INCLUDE_DIR liburing.h)
find_library(URING_LIBRARY uring)
if(URING_INCLUDE_DIR AND URING_LIBRARY)
  add_library(liburing::liburing UNKNOWN IMPORTED)
  set_target_properties(
    liburing::liburing
    PROPERTIES IMPORTED_LOCATION "${URING_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${URING_INCLUDE_DIR}")
  message(STATUS "io_uring: Enabled for mooncake_store")
else()
  message(STATUS "io_uring: Disabled (liburing not found)")
endif()

if(ENABLE_KV_EVENTS)
  find_path(ZMQ_INCLUDE_DIR zmq.h REQUIRED)
  find_library(ZMQ_LIBRARY NAMES zmq libzmq REQUIRED)
  add_library(libzmq::libzmq UNKNOWN IMPORTED)
  set_target_properties(
    libzmq::libzmq
    PROPERTIES IMPORTED_LOCATION "${ZMQ_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES
                                                  "${ZMQ_INCLUDE_DIR}")
endif()

if(STORE_USE_REDIS)
  find_path(HIREDIS_INCLUDE_DIR hiredis/hiredis.h REQUIRED)
  find_library(HIREDIS_LIBRARY hiredis REQUIRED)
  add_library(hiredis::hiredis UNKNOWN IMPORTED)
  set_target_properties(
    hiredis::hiredis
    PROPERTIES IMPORTED_LOCATION "${HIREDIS_LIBRARY}"
               INTERFACE_INCLUDE_DIRECTORIES "${HIREDIS_INCLUDE_DIR}")
  message(STATUS "Redis HA backend: Enabled")
endif()

if (NOT STORE_USE_ETCD AND NOT STORE_USE_REDIS AND NOT STORE_USE_K8S_LEASE)
    message(STATUS "Store HA backends are disabled")
endif()

# Add subdirectories
add_subdirectory(src)

if (STORE_USE_ETCD)
    add_subdirectory(tools)
endif()

if (BUILD_UNIT_TESTS)
    add_subdirectory(tests)
endif()

if (BUILD_BENCHMARK)
    add_subdirectory(benchmarks)
endif()
