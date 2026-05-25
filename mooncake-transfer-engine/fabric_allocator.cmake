function(add_fabric_allocator_build_target)
  set(options)
  set(oneValueArgs TARGET_NAME BUILD_SCRIPT COMMENT ENABLE_BUILD)
  set(multiValueArgs BUILD_ARGS)
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

  add_custom_target(${FAB_TARGET_NAME} DEPENDS transfer_engine)

  get_target_property(_include_dirs ${FAB_TARGET_NAME} INCLUDE_DIRECTORIES)
  if(NOT _include_dirs)
    set(_include_dirs "")
  endif()
  string(REPLACE ";" " " _include_dirs_str "${_include_dirs}")

  if(FAB_ENABLE_BUILD)
    add_custom_command(
      TARGET ${FAB_TARGET_NAME}
      COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}
      COMMAND bash ${FAB_BUILD_SCRIPT} ${FAB_BUILD_ARGS}
              ${CMAKE_CURRENT_BINARY_DIR} "${_include_dirs_str}"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
      COMMENT "${FAB_COMMENT}"
      VERBATIM)
  endif()

  set_property(TARGET ${FAB_TARGET_NAME} PROPERTY EXCLUDE_FROM_ALL FALSE)
endfunction()
