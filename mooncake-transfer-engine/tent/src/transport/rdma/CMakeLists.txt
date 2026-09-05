if(IBVERBS_INCLUDE)
  file(GLOB XPORT_SOURCES "*.cpp")
  add_library(tent_xport_rdma STATIC ${XPORT_SOURCES})
  target_include_directories(tent_xport_rdma PUBLIC ${IBVERBS_INCLUDE})
  target_link_libraries(tent_xport_rdma PUBLIC tent_common)
  if(CUDAToolkit_FOUND)
    # Driver API: cuMemGetHandleForAddressRange / cuPointerGetAttribute for GPU
    # DMA-BUF export.
    target_link_libraries(tent_xport_rdma PUBLIC CUDA::cuda_driver CUDA::cudart)
  endif()
endif()
