include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

set(BUILD_EXAMPLES
    OFF
    CACHE BOOL "Build YLT examples")
set(BUILD_BENCHMARK
    OFF
    CACHE BOOL "Build YLT benchmarks")
set(BUILD_UNIT_TESTS
    OFF
    CACHE BOOL "Build YLT unittests")
set(INSTALL_THIRDPARTY
    OFF
    CACHE BOOL "Install YLT thirdparty")
set(INSTALL_STANDALONE
    OFF
    CACHE BOOL "Install YLT standalone")
set(INSTALL_INDEPENDENT_STANDALONE
    OFF
    CACHE BOOL "Install YLT independent standalone")

set(YLT_VERSION 0.5.7)
FetchContent_Declare(
  yalantinglibs
  URL https://github.com/alibaba/yalantinglibs/archive/refs/tags/${YLT_VERSION}.tar.gz
  URL_HASH
    SHA256=1c1057289e5488f90dd326fd2bb9d3173bad11eb5b06bc0a8bf0fa80857e1cfa
)

FetchContent_MakeAvailable(yalantinglibs)
