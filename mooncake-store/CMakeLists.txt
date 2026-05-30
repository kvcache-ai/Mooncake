project(MooncakeStore VERSION 2.0.0)

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
