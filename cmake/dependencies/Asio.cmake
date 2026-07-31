include_guard(GLOBAL)

set(MOONCAKE_ASIO_VERSION "1.30.2")
set(MOONCAKE_ASIO_REVISION "asio-1-30-2")
set(MOONCAKE_ASIO_URL
    "https://codeload.github.com/chriskohlhoff/asio/tar.gz/refs/tags/asio-1-30-2"
)
set(MOONCAKE_ASIO_SHA256
    "755bd7f85a4b269c67ae0ea254907c078d408cce8e1a352ad2ed664d233780e8")

FetchContent_Declare(
  mooncake_asio_source
  URL "${MOONCAKE_ASIO_URL}"
  URL_HASH "SHA256=${MOONCAKE_ASIO_SHA256}")
FetchContent_GetProperties(mooncake_asio_source)
if(NOT mooncake_asio_source_POPULATED)
  FetchContent_Populate(mooncake_asio_source)
endif()

find_package(Threads REQUIRED)
add_library(mooncake_asio STATIC
            "${MOONCAKE_SOURCE_ROOT}/mooncake-common/src/asio_impl.cpp")
add_library(Mooncake::Asio ALIAS mooncake_asio)
add_library(asio_shared ALIAS mooncake_asio)
if(WITH_STORE_C_SHARED)
  add_library(asio_static ALIAS mooncake_asio)
endif()
target_include_directories(
  mooncake_asio
  PUBLIC "$<BUILD_INTERFACE:${mooncake_asio_source_SOURCE_DIR}/asio/include>")
target_compile_definitions(mooncake_asio PUBLIC ASIO_SEPARATE_COMPILATION
                                                ASIO_STANDALONE)
target_link_libraries(mooncake_asio PUBLIC Threads::Threads)
set_target_properties(mooncake_asio PROPERTIES OUTPUT_NAME asio)
mooncake_configure_bundled_target(mooncake_asio)

set(ASIO_INCLUDE_DIR
    "${mooncake_asio_source_SOURCE_DIR}/asio/include"
    CACHE INTERNAL "Bundled standalone Asio include directory")

mooncake_register_dependency(
  NAME
  Asio
  VERSION
  "${MOONCAKE_ASIO_VERSION}"
  REVISION
  "${MOONCAKE_ASIO_REVISION}"
  URL
  "${MOONCAKE_ASIO_URL}"
  SHA256
  "${MOONCAKE_ASIO_SHA256}"
  SOURCE_DIR
  "${mooncake_asio_source_SOURCE_DIR}"
  LICENSE_FILES
  asio/LICENSE_1_0.txt
  asio/COPYING)
