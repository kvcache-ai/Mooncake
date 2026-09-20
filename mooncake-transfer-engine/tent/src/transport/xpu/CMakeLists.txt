if(USE_XPU)
  add_library(tent_xport_xpu STATIC xpu_transport.cpp)
  # XpuTransport binds to the concrete XpuPlatform (as NVLinkTransport binds to
  # CudaPlatform) and drives USM classification and the VRAM<->host copy through
  # it, so it links platform_xpu. The SYCL dependency stays confined to
  # platform_xpu.
  target_link_libraries(tent_xport_xpu PUBLIC tent_rpc tent_common platform_xpu)
endif()
