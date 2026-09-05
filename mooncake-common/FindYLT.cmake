include_guard(GLOBAL)

include(FetchContent)

set(YLT_ENABLE_IBV ON CACHE BOOL "Enable yalantinglibs ibverbs support")

set(YLT_COMMIT 7801bc9ad9021781f15217552214e325a1cf7373)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.tar.gz
  URL_HASH SHA256=2a4b93c256c09fa84e1507bf4d5b33571ee47a4e2c65316ca255680ad3cdfc1e
  EXCLUDE_FROM_ALL
  SYSTEM
)
FetchContent_MakeAvailable(yalantinglibs)
