include_guard(GLOBAL)

set(MOONCAKE_LIBZMQ_VERSION "4.3.5")
set(MOONCAKE_LIBZMQ_REVISION "v4.3.5")
set(MOONCAKE_LIBZMQ_URL
    "https://codeload.github.com/zeromq/libzmq/tar.gz/refs/tags/v4.3.5")
set(MOONCAKE_LIBZMQ_SHA256
    "6c972d1e6a91a0ecd79c3236f04cf0126f2f4dfbbad407d72b4606a7ba93f9c6")

FetchContent_Declare(
  mooncake_libzmq
  URL "${MOONCAKE_LIBZMQ_URL}"
  URL_HASH "SHA256=${MOONCAKE_LIBZMQ_SHA256}")
FetchContent_GetProperties(mooncake_libzmq)
if(NOT mooncake_libzmq_POPULATED)
  FetchContent_Populate(mooncake_libzmq)
endif()

function(_mooncake_add_libzmq)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(BUILD_SHARED
      OFF
      CACHE BOOL "" FORCE)
  set(BUILD_STATIC
      ON
      CACHE BOOL "" FORCE)
  set(BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)
  set(ZMQ_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)
  set(WITH_DOC
      OFF
      CACHE BOOL "" FORCE)
  set(WITH_DOCS
      OFF
      CACHE BOOL "" FORCE)
  set(WITH_PERF_TOOL
      OFF
      CACHE BOOL "" FORCE)
  set(ENABLE_CPACK
      OFF
      CACHE BOOL "" FORCE)
  set(ENABLE_DRAFTS
      OFF
      CACHE BOOL "" FORCE)
  set(ENABLE_CURVE
      OFF
      CACHE BOOL "" FORCE)
  set(WITH_LIBSODIUM
      OFF
      CACHE BOOL "" FORCE)
  set(WITH_LIBBSD
      OFF
      CACHE BOOL "" FORCE)
  set(ENABLE_WS
      OFF
      CACHE BOOL "" FORCE)
  add_subdirectory("${mooncake_libzmq_SOURCE_DIR}"
                   "${mooncake_libzmq_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_libzmq()

add_library(Mooncake::LibZmq ALIAS libzmq-static)
mooncake_configure_bundled_target(libzmq-static)

mooncake_register_dependency(
  NAME
  libzmq
  VERSION
  "${MOONCAKE_LIBZMQ_VERSION}"
  REVISION
  "${MOONCAKE_LIBZMQ_REVISION}"
  URL
  "${MOONCAKE_LIBZMQ_URL}"
  SHA256
  "${MOONCAKE_LIBZMQ_SHA256}"
  SOURCE_DIR
  "${mooncake_libzmq_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE)
