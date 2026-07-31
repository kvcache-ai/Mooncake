include_guard(GLOBAL)

set(MOONCAKE_XXHASH_VERSION "0.8.3")
set(MOONCAKE_XXHASH_REVISION "v0.8.3")
set(MOONCAKE_XXHASH_URL
    "https://codeload.github.com/Cyan4973/xxHash/tar.gz/refs/tags/v0.8.3")
set(MOONCAKE_XXHASH_SHA256
    "aae608dfe8213dfd05d909a57718ef82f30722c392344583d3f39050c7f29a80")

FetchContent_Declare(
  mooncake_xxhash
  URL "${MOONCAKE_XXHASH_URL}"
  URL_HASH "SHA256=${MOONCAKE_XXHASH_SHA256}")
FetchContent_GetProperties(mooncake_xxhash)
if(NOT mooncake_xxhash_POPULATED)
  FetchContent_Populate(mooncake_xxhash)
endif()

function(_mooncake_add_xxhash)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED_LIBS OFF)
  set(XXHASH_BUNDLED_MODE ON)
  set(XXHASH_BUILD_XXHSUM OFF)
  add_subdirectory("${mooncake_xxhash_SOURCE_DIR}/cmake_unofficial"
                   "${mooncake_xxhash_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_xxhash()

add_library(Mooncake::XxHash ALIAS xxhash)
mooncake_configure_bundled_target(xxhash)

mooncake_register_dependency(
  NAME
  xxHash
  VERSION
  "${MOONCAKE_XXHASH_VERSION}"
  REVISION
  "${MOONCAKE_XXHASH_REVISION}"
  URL
  "${MOONCAKE_XXHASH_URL}"
  SHA256
  "${MOONCAKE_XXHASH_SHA256}"
  SOURCE_DIR
  "${mooncake_xxhash_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE)
