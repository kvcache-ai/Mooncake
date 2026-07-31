include_guard(GLOBAL)

set(MOONCAKE_GFLAGS_VERSION "2.3.0")
set(MOONCAKE_GFLAGS_REVISION "v2.3.0")
set(MOONCAKE_GFLAGS_URL
    "https://codeload.github.com/gflags/gflags/tar.gz/refs/tags/v2.3.0")
set(MOONCAKE_GFLAGS_SHA256
    "f619a51371f41c0ad6837b2a98af9d4643b3371015d873887f7e8d3237320b2f")

FetchContent_Declare(
  mooncake_gflags
  URL "${MOONCAKE_GFLAGS_URL}"
  URL_HASH "SHA256=${MOONCAKE_GFLAGS_SHA256}")
FetchContent_GetProperties(mooncake_gflags)
if(NOT mooncake_gflags_POPULATED)
  FetchContent_Populate(mooncake_gflags)
endif()

function(_mooncake_add_gflags)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(GFLAGS_BUILD_SHARED_LIBS OFF)
  set(GFLAGS_BUILD_STATIC_LIBS ON)
  set(GFLAGS_BUILD_gflags_LIB ON)
  set(GFLAGS_BUILD_gflags_nothreads_LIB OFF)
  set(GFLAGS_BUILD_PACKAGING OFF)
  set(GFLAGS_BUILD_TESTING OFF)
  set(NAMESPACE "google;gflags")
  set(GFLAGS_INSTALL_HEADERS OFF)
  set(GFLAGS_INSTALL_SHARED_LIBS OFF)
  set(GFLAGS_INSTALL_STATIC_LIBS OFF)
  set(GFLAGS_REGISTER_BUILD_DIR OFF)
  set(GFLAGS_REGISTER_INSTALL_PREFIX OFF)
  add_subdirectory("${mooncake_gflags_SOURCE_DIR}"
                   "${mooncake_gflags_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_gflags()

add_library(Mooncake::Gflags ALIAS gflags_static)
if(NOT TARGET gflags::gflags)
  add_library(gflags::gflags ALIAS gflags_static)
endif()
mooncake_configure_bundled_target(gflags_static)

mooncake_register_dependency(
  NAME
  gflags
  VERSION
  "${MOONCAKE_GFLAGS_VERSION}"
  REVISION
  "${MOONCAKE_GFLAGS_REVISION}"
  URL
  "${MOONCAKE_GFLAGS_URL}"
  SHA256
  "${MOONCAKE_GFLAGS_SHA256}"
  SOURCE_DIR
  "${mooncake_gflags_SOURCE_DIR}"
  LICENSE_FILES
  COPYING.txt)
