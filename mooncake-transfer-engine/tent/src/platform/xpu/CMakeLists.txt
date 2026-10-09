if(USE_XPU)
  file(GLOB TENT_PLATFORM_XPU_SOURCES "*.cpp")
  add_library(platform_xpu STATIC ${TENT_PLATFORM_XPU_SOURCES})
  target_link_libraries(platform_xpu PUBLIC tent_common)

  # Native / direct-link SYCL: these sources include <sycl/sycl.hpp> and call
  # libsycl directly, so they must be compiled by the Intel DPC++ compiler
  # (icpx) with -fsycl. common.cmake enforces that CMAKE_CXX_COMPILER is a SYCL
  # compiler when USE_XPU=ON. -fsycl is PUBLIC so every target that links
  # platform_xpu also links libsycl and runs the SYCL device-link step.
  target_compile_options(platform_xpu PUBLIC -fsycl)
  target_link_options(platform_xpu PUBLIC -fsycl)
endif()
