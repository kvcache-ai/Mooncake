include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

set(YLT_VERSION 0.5.7)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/refs/tags/${YLT_VERSION}.tar.gz
  URL_HASH
    SHA256=1c1057289e5488f90dd326fd2bb9d3173bad11eb5b06bc0a8bf0fa80857e1cfa
)

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)
add_subdirectory(
    ${yalantinglibs_SOURCE_DIR}
    ${yalantinglibs_BINARY_DIR}
    EXCLUDE_FROM_ALL
)
