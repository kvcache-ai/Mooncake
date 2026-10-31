if((CUDAToolkit_FOUND OR USE_XPU_GDS)
   AND CUFILE_LIB
   AND CUFILE_INCLUDE)
  add_definitions(-DUSE_GDS)
  file(GLOB TEV1_XPORT_GDS_SOURCES "*.cpp")
  add_library(tent_xport_gds STATIC ${TEV1_XPORT_GDS_SOURCES})
  target_include_directories(tent_xport_gds PUBLIC ${CUFILE_INCLUDE})
  target_link_libraries(tent_xport_gds PUBLIC tent_common ${CUFILE_LIB})
  # The transport only calls cuFile; the CUDA runtime is linked for NVIDIA GDS
  # builds, not for the cuFile-compatible library used by USE_XPU_GDS.
  if(NOT USE_XPU_GDS)
    target_link_libraries(tent_xport_gds PUBLIC CUDA::cuda_driver CUDA::cudart)
  endif()
endif()
