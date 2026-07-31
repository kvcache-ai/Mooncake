include_guard(GLOBAL)

set(MOONCAKE_HIREDIS_VERSION "1.4.0")
set(MOONCAKE_HIREDIS_REVISION "v1.4.0")
set(MOONCAKE_HIREDIS_URL
    "https://codeload.github.com/redis/hiredis/tar.gz/refs/tags/v1.4.0")
set(MOONCAKE_HIREDIS_SHA256
    "5fa6e719e59cd4f8ae435c52a18ac4035d135251f9ee54e7a045bccf59107ed8")

FetchContent_Declare(
  mooncake_hiredis
  URL "${MOONCAKE_HIREDIS_URL}"
  URL_HASH "SHA256=${MOONCAKE_HIREDIS_SHA256}")
FetchContent_GetProperties(mooncake_hiredis)
if(NOT mooncake_hiredis_POPULATED)
  FetchContent_Populate(mooncake_hiredis)
endif()

function(_mooncake_add_hiredis)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED_LIBS OFF)
  set(ENABLE_SSL OFF)
  set(DISABLE_TESTS ON)
  set(ENABLE_SSL_TESTS OFF)
  set(ENABLE_EXAMPLES OFF)
  set(ENABLE_ASYNC_TESTS OFF)
  set(ENABLE_NUGET OFF)
  add_subdirectory("${mooncake_hiredis_SOURCE_DIR}"
                   "${mooncake_hiredis_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_hiredis()

add_library(Mooncake::Hiredis ALIAS hiredis)
mooncake_configure_bundled_target(hiredis)

mooncake_register_dependency(
  NAME
  hiredis
  VERSION
  "${MOONCAKE_HIREDIS_VERSION}"
  REVISION
  "${MOONCAKE_HIREDIS_REVISION}"
  URL
  "${MOONCAKE_HIREDIS_URL}"
  SHA256
  "${MOONCAKE_HIREDIS_SHA256}"
  SOURCE_DIR
  "${mooncake_hiredis_SOURCE_DIR}"
  LICENSE_FILES
  COPYING)
