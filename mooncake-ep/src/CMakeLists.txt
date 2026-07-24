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
  set(_ep_cuda_architectures "")
  foreach(_torch_arch IN LISTS TORCH_CUDA_ARCH_LIST)
    string(REGEX MATCH "^([0-9]+)\\.([0-9]+)(\\+PTX)?$"
                 _torch_arch_match "${_torch_arch}")
    if(NOT _torch_arch_match)
      message(FATAL_ERROR "Unsupported TORCH_CUDA_ARCH_LIST entry: ${_torch_arch}")
    endif()
    set(_ep_cuda_arch "${CMAKE_MATCH_1}${CMAKE_MATCH_2}")
    if(CMAKE_MATCH_3)
      list(APPEND _ep_cuda_architectures "${_ep_cuda_arch}-real"
           "${_ep_cuda_arch}-virtual")
    else()
      list(APPEND _ep_cuda_architectures "${_ep_cuda_arch}")
    endif()
  endforeach()
  set_target_properties(_ep PROPERTIES
                        CUDA_ARCHITECTURES "${_ep_cuda_architectures}")
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
