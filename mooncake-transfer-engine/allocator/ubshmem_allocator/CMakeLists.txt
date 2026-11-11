if(USE_UBSHMEM)
  set(_output_path "${CMAKE_CURRENT_BINARY_DIR}/ubshmem_fabric_allocator.so")
  # Track the artifact so subsequent build/install steps do not rebuild it.
  add_custom_command(
    OUTPUT "${_output_path}"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${CMAKE_CURRENT_BINARY_DIR}"
    COMMAND bash "${CMAKE_CURRENT_SOURCE_DIR}/build.sh"
            "${CMAKE_CURRENT_BINARY_DIR}" ""
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build.sh
            ${CMAKE_CURRENT_SOURCE_DIR}/ubshmem_fabric_allocator.cpp
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include/cuda_alike.h
            ${CMAKE_CURRENT_SOURCE_DIR}/../../include/gpu_vendor/ubshmem.h
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Building ubshmem allocator to ${CMAKE_CURRENT_BINARY_DIR}"
    VERBATIM)
  add_custom_target(build_ubshmem_allocator ALL DEPENDS "${_output_path}")
else()
  add_custom_target(build_ubshmem_allocator ALL)
endif()

add_dependencies(build_ubshmem_allocator transfer_engine)
