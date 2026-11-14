include_guard(GLOBAL)

include(FetchContent)

if(APPLE)
  # macOS has no usable ibverbs; build yalantinglibs RPC over TCP only.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support")
elseif(USE_SHCA)
  # SHCA redefines IB LID as u17, which stock yalanting ib_socket.hpp cannot
  # compile against. Control-plane RPC stays on TCP here; data-plane RDMA goes
  # through Transfer Engine / SHCA, not yalanting ibverbs.
  set(YLT_ENABLE_IBV
      OFF
      CACHE BOOL "Enable yalantinglibs ibverbs support" FORCE)
  message(STATUS "USE_SHCA=ON: forcing YLT_ENABLE_IBV=OFF")
else()
  set(YLT_ENABLE_IBV
      ON
      CACHE BOOL "Enable yalantinglibs ibverbs support")
endif()

set(YLT_COMMIT 54f9c0642386e7860e8e11ada698c484dc562d7d)
FetchContent_Declare(
  yalantinglibs
  URL ${GH_MIRROR}https://github.com/alibaba/yalantinglibs/archive/${YLT_COMMIT}.tar.gz
  URL_HASH
    SHA256=11040b1d3f859599e5453ef7e9b5f84f8afa2388b057330f4c667973e5b20f01
  SYSTEM
)
FetchContent_MakeAvailable(yalantinglibs)
