include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

set(ENABLE_PULL
    OFF
    CACHE BOOL "Build prometheus-cpp pull library" FORCE)
set(ENABLE_PUSH
    OFF
    CACHE BOOL "Build prometheus-cpp push library" FORCE)
set(ENABLE_COMPRESSION
    OFF
    CACHE BOOL "Enable prometheus-cpp compression" FORCE)
set(ENABLE_TESTING
    OFF
    CACHE BOOL "Build prometheus-cpp tests" FORCE)
set(USE_THIRDPARTY_LIBRARIES
    OFF
    CACHE BOOL "Use prometheus-cpp third-party libraries" FORCE)
set(GENERATE_PKGCONFIG
    OFF
    CACHE BOOL "Generate prometheus-cpp pkg-config files" FORCE)
set(OVERRIDE_CXX_STANDARD_FLAGS
    OFF
    CACHE BOOL "Override prometheus-cpp C++ standard flags" FORCE)
set(THIRDPARTY_CIVETWEB_WITH_SSL
    OFF
    CACHE BOOL "Enable SSL for prometheus-cpp's civetweb" FORCE)

FetchContent_Declare(
  prometheus-cpp
  URL ${GH_MIRROR}https://github.com/jupp0r/prometheus-cpp/archive/refs/tags/v1.3.0.tar.gz
  URL_HASH
    SHA256=ac6e958405a29fbbea9db70b00fa3c420e16ad32e1baf941ab233ba031dd72ee)

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(prometheus-cpp)
FetchContent_Populate(prometheus-cpp)
# Build core as a static PIC library so it is absorbed into mooncake_metrics and
# never needs to be installed next to Mooncake's shared libraries.
set(_mooncake_saved_build_shared_libs ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF)
add_subdirectory(${prometheus-cpp_SOURCE_DIR} ${prometheus-cpp_BINARY_DIR}
                 EXCLUDE_FROM_ALL)
set(BUILD_SHARED_LIBS ${_mooncake_saved_build_shared_libs})
set_target_properties(core PROPERTIES POSITION_INDEPENDENT_CODE ON)
