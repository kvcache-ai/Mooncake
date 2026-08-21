include_guard(GLOBAL)

include(FetchContent)

if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

set(YLT_COMMIT 7801bc9ad9021781f15217552214e325a1cf7373)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.zip
  URL_HASH SHA256=30d8b2647651533c0eb6fbeab8877b230d2334bf5b8bc61bec82dd996e07bd06
)

# Exclude from install for CMake before 3.28
FetchContent_GetProperties(yalantinglibs)
FetchContent_Populate(yalantinglibs)
add_subdirectory(
  ${yalantinglibs_SOURCE_DIR}
  ${yalantinglibs_BINARY_DIR}
  EXCLUDE_FROM_ALL
)
