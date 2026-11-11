set(_extra_build_opts "")
set(_extra_build_depends "")
if(USE_HIP)
  list(APPEND _extra_build_opts --use-hipcc)
  list(APPEND _extra_build_depends
       ${CMAKE_CURRENT_SOURCE_DIR}/../../include/gpu_vendor/hip.h)
elseif(USE_MUSA)
  list(APPEND _extra_build_opts --use-mcc)
  list(APPEND _extra_build_depends
       ${CMAKE_CURRENT_SOURCE_DIR}/../../include/gpu_vendor/musa.h)
elseif(USE_MACA)
  list(APPEND _extra_build_opts --use-maca)
  list(APPEND _extra_build_depends
       ${CMAKE_CURRENT_SOURCE_DIR}/../../include/gpu_vendor/maca.h)
endif()

if(USE_HYLINK)
  list(APPEND _extra_build_opts --with-hylink)
endif()

if(USE_CUDA
   OR USE_HIP
   OR USE_MUSA
   OR USE_MACA)
  set(_output_path "${CMAKE_CURRENT_BINARY_DIR}/nvlink_allocator.so")
  # Track the artifact so subsequent build/install steps do not rebuild it.
  add_custom_command(
    OUTPUT "${_output_path}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_CURRENT_BINARY_DIR}"
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/build.sh" ${_extra_build_opts}
            "${CMAKE_CURRENT_BINARY_DIR}" ""
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/nvlink_allocator.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include/cuda_alike.h
            ${CMAKE_CURRENT_SOURCE_DIR}/../../scripts/allocator_build_common.sh
            ${_extra_build_depends}
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Building nvlink allocator to ${CMAKE_CURRENT_BINARY_DIR}"
    VERBATIM)
  add_custom_target(build_nvlink_allocator ALL DEPENDS "${_output_path}")
else()
  add_custom_target(build_nvlink_allocator ALL)
endif()

add_dependencies(build_nvlink_allocator transfer_engine)
