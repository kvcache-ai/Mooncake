if(USE_XPU)
  # Intel XPU: a SYCL implementation of the same _ep module for ranks on one
  # node (peers over Level Zero IPC; no RDMA path). common.cmake has already
  # required the DPC++ compiler.
  set(MOONCAKE_EP_XPU_AOT_DEVICES
      "bmg"
      CACHE
        STRING
        "Intel GPUs to compile the XPU EP kernels ahead of time for (ocloc -device list, e.g. bmg,pvc); others JIT-compile on first use. Empty: JIT only"
  )
  find_library(MOONCAKE_EP_ZE_LOADER ze_loader)
  if(NOT MOONCAKE_EP_ZE_LOADER)
    message(FATAL_ERROR "Mooncake EP on XPU needs the Level Zero loader "
                        "(libze_loader)")
  endif()
  pybind11_add_module(
    _ep MODULE ep_py.cpp xpu/mooncake_ep_xpu_buffer.cpp xpu/xpu_ipc.cpp
    xpu/xpu_ep_kernels.cpp)
  target_compile_definitions(_ep PRIVATE MOONCAKE_EP_USE_XPU=1)
  set(_ep_xpu_sycl_flags -fsycl)
  if(MOONCAKE_EP_XPU_AOT_DEVICES)
    list(
      APPEND
      _ep_xpu_sycl_flags
      -fsycl-targets=spir64_gen,spir64
      "SHELL:-Xsycl-target-backend=spir64_gen \"-device ${MOONCAKE_EP_XPU_AOT_DEVICES}\""
    )
  endif()
  # Correctly rounded division and no contraction: the FP8 scale, 448 / amax,
  # must round as the CUDA kernel's does. Ahead-of-time device code is generated
  # at link time, so the link needs these too.
  list(APPEND _ep_xpu_sycl_flags -fp-model=precise -foffload-fp32-prec-div)
  target_compile_options(_ep PRIVATE ${_ep_xpu_sycl_flags})
  target_link_options(_ep PRIVATE ${_ep_xpu_sycl_flags})
  target_link_libraries(_ep PRIVATE glog::glog ${MOONCAKE_EP_ZE_LOADER})
  set_target_properties(_ep PROPERTIES POSITION_INDEPENDENT_CODE ON
                                       INSTALL_RPATH "$ORIGIN")
  return()
endif()

set(MOONCAKE_EP_HOST_SOURCES
    ep_py.cpp
    ../benchmarks/legacy_buffer_perf.cpp
    mooncake_ep_buffer.cpp
    mooncake_ep_elastic_buffer.cpp)

set(MOONCAKE_EP_DEVICE_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/mooncake_ep_kernel.cu"
    "${CMAKE_CURRENT_SOURCE_DIR}/mooncake_ep_elastic_kernel.cu")

if(USE_CUDA)
  enable_language(CUDA)
  find_package(CUDAToolkit REQUIRED)

  set(MOONCAKE_EP_JIT_SOURCES
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/combine_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/dispatch_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_combine_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_combine_reduce_epilogue_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_dispatch_copy_epilogue_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_dispatch_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_dispatch_prologue_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_hybrid_combine_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/elastic_hybrid_dispatch_jit.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/jit_runtime.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/jit_cache.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/jit_utils.cpp"
      "${CMAKE_CURRENT_SOURCE_DIR}/jit/nvcc_compiler.cpp")

  # The JIT engine runs nvcc at runtime, so the kernel headers the generated
  # source #includes have to exist as files then. Stage a copy into the build
  # tree and bake its path into the engine via jit_config.h.
  #
  # configure_file tracks each explicitly listed header, so edits refresh the
  # staged copy and invalidate the JIT cache fingerprint.
  set(MOONCAKE_EP_JIT_STAGE_ROOT "${CMAKE_BINARY_DIR}/mooncake_ep_jit")
  set(MOONCAKE_EP_JIT_SOURCE_DIR
      "${MOONCAKE_EP_JIT_STAGE_ROOT}/include/mooncake_ep")
  set(MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR
      "${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine")

  # Do not put jit_config.h under mooncake_ep_jit/: everything there goes into
  # the wheel, but this header is only used to build this library.
  set(MOONCAKE_EP_JIT_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/jit_generated")

  set(MOONCAKE_EP_JIT_CUDA_INCLUDE_DIR "")
  if(CUDAToolkit_INCLUDE_DIRS)
    list(GET CUDAToolkit_INCLUDE_DIRS 0 MOONCAKE_EP_JIT_CUDA_INCLUDE_DIR)
  endif()

  # NCCL DeviceTransport is the only kernel dependency living outside the
  # Mooncake tree and the CUDA toolkit.
  set(MOONCAKE_EP_JIT_BUILD_INCLUDE_DIR "")
  if(USE_NCCL_DEVICE AND NCCL_DEVICE_INCLUDE_DIR)
    set(MOONCAKE_EP_JIT_BUILD_INCLUDE_DIR "${NCCL_DEVICE_INCLUDE_DIR}")
  endif()

  set(MOONCAKE_EP_JIT_HEADERS
      mooncake_ep_configs.cuh
      mooncake_ep_device.h
      mooncake_ep_dispatch.cuh
      mooncake_ep_combine.cuh
      mooncake_ep_exception.cuh
      mooncake_ep_utils.cuh
      elastic/mooncake_ep_elastic_combine_official.cuh
      elastic/mooncake_ep_elastic_combine_reduce_epilogue.cuh
      elastic/mooncake_ep_elastic_combine_utils.cuh
      elastic/mooncake_ep_elastic_comm.cuh
      elastic/mooncake_ep_elastic_compiled.cuh
      elastic/mooncake_ep_elastic_dispatch_copy_epilogue.cuh
      elastic/mooncake_ep_elastic_dispatch_deterministic_prologue.cuh
      elastic/mooncake_ep_elastic_dispatch_official.cuh
      elastic/mooncake_ep_elastic_exception.cuh
      elastic/mooncake_ep_elastic_hybrid_combine_official.cuh
      elastic/mooncake_ep_elastic_hybrid_dispatch_official.cuh
      elastic/mooncake_ep_elastic_launch.cuh
      elastic/mooncake_ep_elastic_layout.cuh
      elastic/mooncake_ep_elastic_math.cuh
      elastic/mooncake_ep_elastic_ptx.cuh
      elastic/mooncake_ep_elastic_transport.cuh)

  set(MOONCAKE_EP_JIT_TRANSFER_HEADERS
      include/cuda_alike.h
      include/cuda_fabric_compat.h
      include/transport/device/comm_device.cuh
      include/transport/device/cuda/cuda_ops.cuh
      include/transport/device/device_ops.cuh
      include/transport/device/ibgda/mlx5gda.h
      include/transport/device/ibgda_device.cuh
      include/transport/device/nccl_device.cuh
      include/transport/device/nccl_device_transport.h
      include/transport/device/p2p_device.cuh
      tent/include/tent/common/cuda_fabric_compat.h)

  file(MAKE_DIRECTORY "${MOONCAKE_EP_JIT_SOURCE_DIR}")
  foreach(header IN LISTS MOONCAKE_EP_JIT_HEADERS)
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/../include/${header}"
                   "${MOONCAKE_EP_JIT_SOURCE_DIR}/${header}" COPYONLY)
  endforeach()
  foreach(header IN LISTS MOONCAKE_EP_JIT_TRANSFER_HEADERS)
    get_filename_component(header_dir "${header}" DIRECTORY)
    file(MAKE_DIRECTORY "${MOONCAKE_EP_JIT_STAGE_ROOT}/${header_dir}")
    configure_file(
      "${MOONCAKE_EP_JIT_TRANSFER_ENGINE_SOURCE_DIR}/${header}"
      "${MOONCAKE_EP_JIT_STAGE_ROOT}/${header}" COPYONLY)
  endforeach()

  configure_file("${CMAKE_CURRENT_SOURCE_DIR}/jit/jit_config.h.in"
                 "${MOONCAKE_EP_JIT_GENERATED_DIR}/jit/jit_config.h" @ONLY)

  # Keep CUDA fatbins in a separate library.  The Python host extension can
  # then pass through auditwheel while this library is injected afterwards.
  add_library(mooncake_ep_device SHARED ${MOONCAKE_EP_DEVICE_SOURCES}
                                        ${MOONCAKE_EP_JIT_SOURCES})
  set_target_properties(mooncake_ep_device PROPERTIES
                        POSITION_INDEPENDENT_CODE ON)
  if(CUDAToolkit_VERSION VERSION_GREATER_EQUAL "13.0")
    set_target_properties(mooncake_ep_device PROPERTIES
                          CUDA_ARCHITECTURES "80;90;103")
  else()
    set_target_properties(mooncake_ep_device PROPERTIES
                          CUDA_ARCHITECTURES "80;90")
  endif()
  target_include_directories(
    mooncake_ep_device PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include
    ${MOONCAKE_EP_JIT_GENERATED_DIR})
  target_compile_options(
    mooncake_ep_device PRIVATE
    $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-O3>
    $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-g0>
    $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
  target_link_libraries(mooncake_ep_device PRIVATE CUDA::cudart
                                                   CUDA::cuda_driver
                                                   ${CMAKE_DL_LIBS})
  if(USE_NCCL_DEVICE)
    target_link_libraries(mooncake_ep_device PRIVATE NCCL::nccl)
  endif()
elseif(USE_MUSA)
  set(MOONCAKE_EP_SOURCES ${MOONCAKE_EP_HOST_SOURCES})
  if(DEFINED ENV{MUSA_HOME} AND NOT "$ENV{MUSA_HOME}" STREQUAL "")
    set(_ep_musa_compiler_hint "$ENV{MUSA_HOME}/bin")
  else()
    set(_ep_musa_compiler_hint /usr/local/musa/bin)
  endif()
  find_program(_ep_musa_compiler NAMES mcc HINTS "${_ep_musa_compiler_hint}")
  if(NOT _ep_musa_compiler)
    message(FATAL_ERROR "USE_MUSA=ON requires the MUSA compiler (mcc)")
  endif()
  set(_ep_musa_depfile_supported FALSE)
  if(CMAKE_GENERATOR MATCHES "^Ninja" OR
     (CMAKE_GENERATOR MATCHES "Makefiles" AND
      CMAKE_VERSION VERSION_GREATER_EQUAL 3.20))
    set(_ep_musa_depfile_supported TRUE)
  endif()

  foreach(_ep_device_source IN LISTS MOONCAKE_EP_DEVICE_SOURCES)
    get_filename_component(_ep_device_name "${_ep_device_source}" NAME_WE)
    set(_ep_device_object
        "${CMAKE_CURRENT_BINARY_DIR}/${_ep_device_name}_musa.o")
    set(_ep_device_depfile
        "${CMAKE_CURRENT_BINARY_DIR}/${_ep_device_name}_musa.d")
    set(_ep_musa_depfile_argument)
    if(_ep_musa_depfile_supported)
      set(_ep_musa_depfile_argument DEPFILE "${_ep_device_depfile}")
    endif()
    add_custom_command(
      OUTPUT "${_ep_device_object}"
      COMMAND "${_ep_musa_compiler}"
              -x musa
              -std=c++20
              -O3
              -fPIC
              -DUSE_MUSA
              -DMOONCAKE_EP_USE_MUSA=1
              -MMD
              -MT "${_ep_device_object}"
              -MF "${_ep_device_depfile}"
              --cuda-gpu-arch=mp_21
              --cuda-gpu-arch=mp_31
              "-I${CMAKE_CURRENT_SOURCE_DIR}/../include"
              "-I${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include"
              -c "${_ep_device_source}" -o "${_ep_device_object}"
      DEPENDS "${_ep_device_source}"
      ${_ep_musa_depfile_argument}
      COMMENT "Compiling Mooncake EP MUSA device source ${_ep_device_name}"
      VERBATIM)
    set_source_files_properties("${_ep_device_object}"
                                PROPERTIES GENERATED TRUE EXTERNAL_OBJECT TRUE)
    list(APPEND MOONCAKE_EP_SOURCES "${_ep_device_object}")
  endforeach()
elseif(USE_MACA)
  # MACA legacy EP kernels use the SDK cu-bridge nvcc front-end. mxcc is used
  # for the PG worker, but does not provide CUDA compatibility headers such as
  # cooperative_groups.h required by these translation units.
  set(MOONCAKE_EP_SOURCES
      ep_py.cpp
      ../benchmarks/legacy_buffer_perf.cpp
      mooncake_ep_buffer.cpp)
  # The elastic kernel contains NVIDIA PTX inline assembly and is not part of
  # the legacy Buffer API exercised on MACA.
  set(_ep_maca_device_sources
      "${CMAKE_CURRENT_SOURCE_DIR}/mooncake_ep_kernel.cu")
  if(DEFINED MACA_ROOT AND NOT "${MACA_ROOT}" STREQUAL "")
    set(_ep_maca_root "${MACA_ROOT}")
  elseif(DEFINED ENV{MACA_HOME} AND NOT "$ENV{MACA_HOME}" STREQUAL "")
    set(_ep_maca_root "$ENV{MACA_HOME}")
  else()
    set(_ep_maca_root /opt/maca)
  endif()
  find_program(_ep_maca_compiler NAMES nvcc
               HINTS "${_ep_maca_root}/tools/cu-bridge/bin")
  if(NOT _ep_maca_compiler)
    message(FATAL_ERROR "USE_MACA=ON requires the MACA cu-bridge compiler (nvcc)")
  endif()
  foreach(_ep_device_source IN LISTS _ep_maca_device_sources)
    get_filename_component(_ep_device_name "${_ep_device_source}" NAME_WE)
    set(_ep_device_object
        "${CMAKE_CURRENT_BINARY_DIR}/${_ep_device_name}_maca.o")
    add_custom_command(
      OUTPUT "${_ep_device_object}"
      COMMAND "${_ep_maca_compiler}"
              -std=c++17 -O3 -Xcompiler=-fPIC
              -DUSE_MACA -DMOONCAKE_EP_USE_MACA=1
              -include cuda_bf16.h
              "-I${CMAKE_CURRENT_SOURCE_DIR}/../include"
              "-I${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include"
              "-I${_ep_maca_root}/tools/cu-bridge/include"
              -c "${_ep_device_source}" -o "${_ep_device_object}"
      DEPENDS "${_ep_device_source}"
      COMMENT "Compiling Mooncake EP MACA device source ${_ep_device_name}"
      VERBATIM)
    set_source_files_properties("${_ep_device_object}"
                                PROPERTIES GENERATED TRUE EXTERNAL_OBJECT TRUE)
    list(APPEND MOONCAKE_EP_SOURCES "${_ep_device_object}")
  endforeach()
else()
  set(MOONCAKE_EP_SOURCES ${MOONCAKE_EP_HOST_SOURCES})
endif()

if(USE_CUDA)
  pybind11_add_module(_ep MODULE ${MOONCAKE_EP_HOST_SOURCES})
else()
  pybind11_add_module(_ep MODULE ${MOONCAKE_EP_SOURCES})
endif()
set_target_properties(_ep PROPERTIES POSITION_INDEPENDENT_CODE ON)
set_target_properties(_ep PROPERTIES INSTALL_RPATH "$ORIGIN")
if(USE_MUSA)
  target_compile_definitions(_ep PRIVATE MOONCAKE_EP_USE_MUSA=1)
endif()
if(USE_MACA)
  target_compile_definitions(_ep PRIVATE MOONCAKE_EP_USE_MACA=1)
endif()

target_include_directories(_ep PRIVATE ${Python3_INCLUDE_DIRS})
if(USE_CUDA)
  target_link_libraries(_ep PRIVATE transfer_engine ibverbs mlx5 glog::glog
                                    gflags::gflags mooncake_ep_device
                                    CUDA::cudart)
else()
  target_link_libraries(_ep PRIVATE transfer_engine ibverbs mlx5 glog::glog
                                    gflags::gflags)
endif()

if(USE_CUDA)
  # The host binding contains no CUDA fatbin; only the device library above
  # is kept out of auditwheel and injected into the wheel afterwards.
elseif(USE_MUSA)
  set_target_properties(_ep PROPERTIES LINKER_LANGUAGE CXX)
  target_link_libraries(_ep PRIVATE musa musart rt)
elseif(USE_MACA)
  set_target_properties(_ep PROPERTIES LINKER_LANGUAGE CXX)
  if(NOT DEFINED MACA_RUNTIME_LIBS)
    set(MACA_RUNTIME_LIBS mcruntime mxc-runtime64 rt)
  endif()
  target_link_libraries(_ep PRIVATE ${MACA_RUNTIME_LIBS})
endif()
