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

  # Keep CUDA fatbins in a separate library.  The Python host extension can
  # then pass through auditwheel while this library is injected afterwards.
  add_library(mooncake_ep_device SHARED ${MOONCAKE_EP_DEVICE_SOURCES})
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
    ${CMAKE_CURRENT_SOURCE_DIR}/../../mooncake-transfer-engine/include)
  target_compile_options(
    mooncake_ep_device PRIVATE
    $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-O3>
    $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-g0>
    $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
  target_link_libraries(mooncake_ep_device PRIVATE CUDA::cudart)
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
endif()
