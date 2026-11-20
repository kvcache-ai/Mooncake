find_program(
  GO_EXECUTABLE
  NAMES go
  PATHS "$ENV{MOONCAKE_GO_INSTALL_PREFIX}" ${CMAKE_SYSTEM_PREFIX_PATH}
  PATH_SUFFIXES go/bin)
if(NOT GO_EXECUTABLE)
  message(
    FATAL_ERROR
      "Go is required to build ${CMAKE_CURRENT_SOURCE_DIR}. Put go on PATH or set -DGO_EXECUTABLE=/path/to/go."
  )
endif()

# Track manifest changes and keep any Go checksum updates in the build tree.
set(_go_module_dir "${CMAKE_CURRENT_BINARY_DIR}/go-deps")
configure_file(go.mod "${_go_module_dir}/go.mod" COPYONLY)
configure_file(go.sum "${_go_module_dir}/go.sum" COPYONLY)
set(_go_modfile "-modfile=${_go_module_dir}/go.mod")
# Go's -modfile requires module mode even inside a developer's Go workspace.
set(_go_command "${CMAKE_COMMAND}" -E env GOWORK=off "${GO_EXECUTABLE}")
execute_process(
  COMMAND ${_go_command} mod download
  WORKING_DIRECTORY "${_go_module_dir}"
  RESULT_VARIABLE _go_download_result)
if(NOT _go_download_result STREQUAL "0")
  message(
    FATAL_ERROR
      "Failed to download Go modules for ${CMAKE_CURRENT_SOURCE_DIR}: ${_go_download_result}"
  )
endif()

# Content edits rebuild the wrapper without reconfiguring the project.
file(GLOB _go_sources CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/*.go")
list(FILTER _go_sources EXCLUDE REGEX "_test\\.go$")
