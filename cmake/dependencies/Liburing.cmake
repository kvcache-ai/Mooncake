include_guard(GLOBAL)

include(ExternalProject)

set(MOONCAKE_LIBURING_VERSION "2.14")
set(MOONCAKE_LIBURING_REVISION "liburing-2.14")
set(MOONCAKE_LIBURING_URL
    "https://codeload.github.com/axboe/liburing/tar.gz/refs/tags/liburing-2.14")
set(MOONCAKE_LIBURING_SHA256
    "5f80964108981c6ad979c735f0b4877d5f49914c2a062f8e88282f26bf61de0c")

FetchContent_Declare(
  mooncake_liburing_source
  URL "${MOONCAKE_LIBURING_URL}"
  URL_HASH "SHA256=${MOONCAKE_LIBURING_SHA256}")
FetchContent_GetProperties(mooncake_liburing_source)
if(NOT mooncake_liburing_source_POPULATED)
  FetchContent_Populate(mooncake_liburing_source)
endif()

find_program(MOONCAKE_MAKE_EXECUTABLE NAMES gmake make)
if(NOT MOONCAKE_MAKE_EXECUTABLE)
  message(FATAL_ERROR "A Make implementation is required to build liburing")
endif()

set(mooncake_liburing_archive
    "${mooncake_liburing_source_SOURCE_DIR}/src/liburing.a")
ExternalProject_Add(
  mooncake_liburing_project
  SOURCE_DIR "${mooncake_liburing_source_SOURCE_DIR}"
  DOWNLOAD_COMMAND ""
  UPDATE_COMMAND ""
  CONFIGURE_COMMAND
    "${CMAKE_COMMAND}" -E env "CC=${CMAKE_C_COMPILER}"
    "CXX=${CMAKE_CXX_COMPILER}" "CFLAGS=-fPIC -fvisibility=hidden"
    "${mooncake_liburing_source_SOURCE_DIR}/configure"
    "--prefix=${mooncake_liburing_source_BINARY_DIR}/install"
  BUILD_COMMAND "${MOONCAKE_MAKE_EXECUTABLE}" -C
                "${mooncake_liburing_source_SOURCE_DIR}/src" liburing.a
  BUILD_BYPRODUCTS "${mooncake_liburing_archive}"
  INSTALL_COMMAND ""
  BUILD_IN_SOURCE TRUE)

add_library(mooncake_liburing STATIC IMPORTED GLOBAL)
set_target_properties(
  mooncake_liburing
  PROPERTIES IMPORTED_LOCATION "${mooncake_liburing_archive}"
             INTERFACE_INCLUDE_DIRECTORIES
             "${mooncake_liburing_source_SOURCE_DIR}/src/include")
add_dependencies(mooncake_liburing mooncake_liburing_project)
add_library(Mooncake::Liburing ALIAS mooncake_liburing)

set(URING_LIB
    Mooncake::Liburing
    CACHE INTERNAL "Bundled liburing target")
set(URING_INCLUDE
    "${mooncake_liburing_source_SOURCE_DIR}/src/include"
    CACHE INTERNAL "Bundled liburing include directory")

mooncake_register_dependency(
  NAME
  liburing
  VERSION
  "${MOONCAKE_LIBURING_VERSION}"
  REVISION
  "${MOONCAKE_LIBURING_REVISION}"
  URL
  "${MOONCAKE_LIBURING_URL}"
  SHA256
  "${MOONCAKE_LIBURING_SHA256}"
  SOURCE_DIR
  "${mooncake_liburing_source_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE
  COPYING
  COPYING.GPL)
