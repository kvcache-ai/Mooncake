if(NOT TARGET transfer_engine)
    message(FATAL_ERROR "WITH_STORE_P2P=ON requires WITH_TE=ON")
endif()

set(MOONCAKE_STORE_SOURCE_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../mooncake-store")
set(MOONCAKE_STORE_INCLUDE_ROOT "${MOONCAKE_STORE_SOURCE_ROOT}/include")
# Match the RPC protocol version of the legacy P2P build.
set(MOONCAKE_STORE_VERSION 2.0.0)

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

if(STORE_USE_ETCD)
    set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/)
    set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/../mooncake-common/etcd/libetcd_wrapper.so)
endif()

# Reuse the allocator target when the existing Store has already registered it.
if(NOT TARGET cachelib_memory_allocator)
    add_subdirectory("${MOONCAKE_STORE_SOURCE_ROOT}/src/cachelib_memory_allocator"
                     cachelib_memory_allocator)
endif()

configure_file("${MOONCAKE_STORE_INCLUDE_ROOT}/version.h.in" include/version.h @ONLY)
add_subdirectory(src)

add_custom_target(build_store_p2p)
add_dependencies(build_store_p2p mooncake_store_p2p mooncake_p2p_master mooncake_p2p_client)

if(BUILD_UNIT_TESTS)
    add_subdirectory(tests)
endif()

if(BUILD_BENCHMARK)
    add_subdirectory(benchmarks)
endif()
