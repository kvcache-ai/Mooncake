include_guard(GLOBAL)

set(MOONCAKE_YALANTINGLIBS_REVISION "7801bc9ad9021781f15217552214e325a1cf7373")
set(MOONCAKE_YALANTINGLIBS_VERSION
    "revision-${MOONCAKE_YALANTINGLIBS_REVISION}")
set(MOONCAKE_YALANTINGLIBS_URL
    "https://codeload.github.com/alibaba/yalantinglibs/tar.gz/${MOONCAKE_YALANTINGLIBS_REVISION}"
)
set(MOONCAKE_YALANTINGLIBS_SHA256
    "2a4b93c256c09fa84e1507bf4d5b33571ee47a4e2c65316ca255680ad3cdfc1e")

FetchContent_Declare(
  mooncake_yalantinglibs
  URL "${MOONCAKE_YALANTINGLIBS_URL}"
  URL_HASH "SHA256=${MOONCAKE_YALANTINGLIBS_SHA256}")
FetchContent_GetProperties(mooncake_yalantinglibs)
if(NOT mooncake_yalantinglibs_POPULATED)
  FetchContent_Populate(mooncake_yalantinglibs)
endif()

function(_mooncake_add_yalantinglibs)
  set(CMAKE_SKIP_INSTALL_RULES ON)
  set(YLT_ENABLE_CUDA OFF)
  set(YLT_ENABLE_SSL OFF)
  set(YLT_ENABLE_NTLS OFF)
  set(YLT_ENABLE_IBV ON)
  set(YLT_ENABLE_IO_URING OFF)
  set(YLT_ENABLE_FILE_IO_URING OFF)
  set(INSTALL_THIRDPARTY OFF)
  set(INSTALL_STANDALONE OFF)
  set(INSTALL_INDEPENDENT_THIRDPARTY OFF)
  set(INSTALL_INDEPENDENT_STANDALONE OFF)
  add_subdirectory("${mooncake_yalantinglibs_SOURCE_DIR}"
                   "${mooncake_yalantinglibs_BINARY_DIR}" EXCLUDE_FROM_ALL)
endfunction()
_mooncake_add_yalantinglibs()

# yalantinglibs carries a private Asio snapshot. Put the project provider first
# so all <asio/...> includes and separate-compilation symbols use Asio 1.30.2.
target_include_directories(yalantinglibs BEFORE
                           INTERFACE "$<BUILD_INTERFACE:${ASIO_INCLUDE_DIR}>")
target_compile_definitions(yalantinglibs INTERFACE ASIO_SEPARATE_COMPILATION
                                                   ASIO_STANDALONE)
add_library(Mooncake::Yalantinglibs ALIAS yalantinglibs)

set(MOONCAKE_YALANTINGLIBS_INCLUDE_DIR
    "${mooncake_yalantinglibs_SOURCE_DIR}/include"
    CACHE INTERNAL "Bundled yalantinglibs include directory")

mooncake_register_dependency(
  NAME
  yalantinglibs
  VERSION
  "${MOONCAKE_YALANTINGLIBS_VERSION}"
  REVISION
  "${MOONCAKE_YALANTINGLIBS_REVISION}"
  URL
  "${MOONCAKE_YALANTINGLIBS_URL}"
  SHA256
  "${MOONCAKE_YALANTINGLIBS_SHA256}"
  SOURCE_DIR
  "${mooncake_yalantinglibs_SOURCE_DIR}"
  LICENSE_FILES
  LICENSE
  NOTICE)
