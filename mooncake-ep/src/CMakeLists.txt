set(MOONCAKE_EP_SOURCES
    ep_py.cpp
    ../benchmarks/legacy_buffer_perf.cpp
    mooncake_ep_buffer.cpp
    mooncake_ep_elastic_buffer.cpp
    mooncake_ep_kernel.cu
    mooncake_ep_elastic_kernel.cu)

pybind11_add_module(_ep MODULE ${MOONCAKE_EP_SOURCES})
set_target_properties(_ep PROPERTIES POSITION_INDEPENDENT_CODE ON
                      BUILD_RPATH "$ORIGIN" INSTALL_RPATH "$ORIGIN"
                      BUILD_WITH_INSTALL_RPATH TRUE)
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
# Keep the EP extension's ELF dependency boundary aligned with the legacy
# setup.py build: depend on the packaged engine.so, not Transfer Engine's
# transitive link interface. This lets auditwheel repair engine.so's optional
# dependency closure before _ep is injected into the repaired wheel.
add_dependencies(_ep engine)
execute_process(
  COMMAND ${Python3_EXECUTABLE} -c
          "import sysconfig; print(sysconfig.get_config_var('EXT_SUFFIX'))"
  OUTPUT_VARIABLE _ep_python_ext_suffix
  OUTPUT_STRIP_TRAILING_WHITESPACE)
set(_ep_engine_link "${CMAKE_BINARY_DIR}/mooncake-integration/engine.so")
add_custom_command(
    OUTPUT "${_ep_engine_link}"
    COMMAND ${CMAKE_COMMAND} -E create_symlink
            "engine${_ep_python_ext_suffix}"
            "${_ep_engine_link}"
    DEPENDS engine)
add_custom_target(ep_engine_link DEPENDS "${_ep_engine_link}")
add_dependencies(_ep ep_engine_link)
target_link_options(_ep PRIVATE
  "-L$<TARGET_FILE_DIR:engine>"
  "-Wl,--push-state,--no-as-needed"
  "-l:engine.so"
  "-Wl,--pop-state")
target_link_libraries(_ep PRIVATE ibverbs mlx5 gflags::gflags)

if(DEFINED EP_PG_STAGING_DIR)
  add_custom_command(
      TARGET _ep
      POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E make_directory "${EP_PG_STAGING_DIR}"
      COMMAND ${CMAKE_COMMAND} -E copy_if_different "$<TARGET_FILE:_ep>"
              "${EP_PG_STAGING_DIR}/$<TARGET_FILE_NAME:_ep>")
endif()

if(USE_CUDA)
  target_compile_options(
      _ep PRIVATE
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-O3>
      $<$<COMPILE_LANGUAGE:CUDA>:-Xcompiler=-g0>
      $<$<COMPILE_LANGUAGE:CUDA>:--expt-relaxed-constexpr>)
  target_link_libraries(_ep PRIVATE CUDA::cudart)
endif()
