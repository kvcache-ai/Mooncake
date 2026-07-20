function(add_fabric_allocator_build_target)
  set(options)
  set(oneValueArgs TARGET_NAME BUILD_SCRIPT OUTPUT_NAME COMMENT ENABLE_BUILD)
  set(multiValueArgs BUILD_ARGS BUILD_DEPENDS)
  cmake_parse_arguments(FAB "${options}" "${oneValueArgs}" "${multiValueArgs}"
                        ${ARGN})

  if(NOT FAB_TARGET_NAME)
    message(
      FATAL_ERROR
        "TARGET_NAME is required for add_fabric_allocator_build_target")
  endif()
  if(NOT FAB_BUILD_SCRIPT)
    message(
      FATAL_ERROR
        "BUILD_SCRIPT is required for add_fabric_allocator_build_target")
  endif()

  if(FAB_ENABLE_BUILD)
    if(NOT FAB_OUTPUT_NAME)
      message(
        FATAL_ERROR
          "OUTPUT_NAME is required for enabled fabric allocator build targets")
    endif()

    set(_output_path "${CMAKE_CURRENT_BINARY_DIR}/${FAB_OUTPUT_NAME}")
    # Track the allocator artifact so install targets do not rebuild it after a
    # successful normal build.
    add_custom_command(
      OUTPUT "${_output_path}"
      COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
      COMMAND bash ${FAB_BUILD_SCRIPT} ${FAB_BUILD_ARGS}
              ${CMAKE_CURRENT_BINARY_DIR} ""
      DEPENDS ${FAB_BUILD_SCRIPT} ${FAB_BUILD_DEPENDS}
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "${FAB_COMMENT}"
      VERBATIM)
    add_custom_target(${FAB_TARGET_NAME} ALL DEPENDS "${_output_path}")
  else()
    add_custom_target(${FAB_TARGET_NAME} ALL)
  endif()

  add_dependencies(${FAB_TARGET_NAME} transfer_engine)
endfunction()
