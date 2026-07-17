set(MOONCAKE_EP_SOURCES
    ep_py.cpp
    ../benchmarks/legacy_buffer_perf.cpp
    mooncake_ep_buffer.cpp
    mooncake_ep_elastic_buffer.cpp
    mooncake_ep_kernel.cu
    mooncake_ep_elastic_kernel.cu)

pybind11_add_module(_ep MODULE ${MOONCAKE_EP_SOURCES})
set_target_properties(_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(_ep PROPERTIES INSTALL_RPATH "$ORIGIN")
if(USE_CUDA)
  set_target_properties(_ep PROPERTIES CUDA_ARCHITECTURES "90")
endif()
if(USE_MUSA)
  target_compile_definitions(_ep PRIVATE MOONCAKE_EP_USE_MUSA=1)
endif()
if(USE_MACA)
  target_compile_definitions(_ep PRIVATE MOONCAKE_EP_USE_MACA=1)
endif()

target_include_directories(_ep PRIVATE ${Python3_INCLUDE_DIRS})
target_link_libraries(_ep PRIVATE transfer_engine ibverbs mlx5 glog::glog
                                  gflags::gflags)

if(USE_CUDA)
  target_compile_options(
      _ep PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-O3>
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-g0>
      $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
  target_link_libraries(_ep PRIVATE CUDA::cudart)
endif()
