include_guard(GLOBAL)

set(MOONCAKE_ZSTD_VERSION "1.5.7")
set(MOONCAKE_ZSTD_REVISION "v1.5.7")
set(MOONCAKE_ZSTD_URL
    "https://codeload.github.com/facebook/zstd/tar.gz/refs/tags/v1.5.7")
set(MOONCAKE_ZSTD_SHA256
    "37d7284556b20954e56e1ca85b80226768902e2edabd3b649e9e72c0c9012ee3")

FetchContent_Declare(
  mooncake_zstd
  URL "${MOONCAKE_ZSTD_URL}"
  URL_HASH "SHA256=${MOONCAKE_ZSTD_SHA256}")
FetchContent_GetProperties(mooncake_zstd)
if(NOT mooncake_zstd_POPULATED)
  FetchContent_Populate(mooncake_zstd)
endif()

function(_mooncake_add_zstd)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED_LIBS OFF)
  set(ZSTD_BUILD_STATIC ON)
  set(ZSTD_BUILD_SHARED OFF)
  set(ZSTD_BUILD_PROGRAMS OFF)
  set(ZSTD_BUILD_CONTRIB OFF)
  set(ZSTD_BUILD_TESTS OFF)
  add_subdirectory("${mooncake_zstd_SOURCE_DIR}/build/cmake"
                   "${mooncake_zstd_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_zstd()

add_library(Mooncake::Zstd ALIAS libzstd_static)
add_library(ZSTD::ZSTD ALIAS libzstd_static)
mooncake_configure_bundled_target(libzstd_static)

mooncake_register_dependency(
  NAME
  zstd
  VERSION
  "${MOONCAKE_ZSTD_VERSION}"
  REVISION
  "${MOONCAKE_ZSTD_REVISION}"
  URL
  "${MOONCAKE_ZSTD_URL}"
  SHA256
  "${MOONCAKE_ZSTD_SHA256}"
  SOURCE_DIR
  "${mooncake_zstd_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE
  COPYING)
