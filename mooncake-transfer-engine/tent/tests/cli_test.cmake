# Exercise the built CLI, including dispatch, output streams and exit codes.
if(NOT DEFINED TENT_CLI OR NOT EXISTS "${TENT_CLI}")
  message(FATAL_ERROR "TENT_CLI must point to the built tent binary")
endif()

function(check_cli name expected_code stdout_pattern stderr_pattern)
  execute_process(
    COMMAND "${TENT_CLI}" ${ARGN}
    RESULT_VARIABLE code
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr
    TIMEOUT 10)
  if(NOT "${code}" STREQUAL "${expected_code}")
    message(FATAL_ERROR "${name}: expected exit ${expected_code}, got ${code}")
  endif()
  foreach(stream stdout stderr)
    if("${${stream}_pattern}" STREQUAL "")
      if(NOT "${${stream}}" STREQUAL "")
        message(FATAL_ERROR "${name}: unexpected ${stream}: ${${stream}}")
      endif()
    elseif(NOT "${${stream}}" MATCHES "${${stream}_pattern}")
      message(FATAL_ERROR "${name}: unexpected ${stream}: ${${stream}}")
    endif()
  endforeach()
endfunction()

set(root_usage "Usage: tent <command>.*diagnostics")
set(diagnostics_usage "Usage: tent diagnostics version")
check_cli(root_help 0 "${root_usage}" "" --help)
check_cli(diagnostics_help 0 "${diagnostics_usage}" "" diagnostics --help)
check_cli(version_text 0 "build.info.available.*build information is available"
          "" diagnostics version)
check_cli(
  version_json
  0
  "\"command\": *\"version\""
  ""
  diagnostics
  version
  --json)
check_cli(no_command 2 "" "${root_usage}")
check_cli(unknown_command 2 "" "${root_usage}" unknown)
check_cli(old_version_command 2 "" "${root_usage}" version)
check_cli(no_diagnostic_command 2 "" "${diagnostics_usage}" diagnostics)
check_cli(unknown_diagnostic_command 2 "" "${diagnostics_usage}" diagnostics
          unknown)
check_cli(
  unknown_option
  2
  ""
  "${diagnostics_usage}"
  diagnostics
  version
  --bad)
check_cli(
  extra_argument
  2
  ""
  "${diagnostics_usage}"
  diagnostics
  version
  --json
  extra)
check_cli(
  extra_help_argument
  2
  ""
  "${diagnostics_usage}"
  diagnostics
  --help
  extra)
message(STATUS "All tent CLI cases passed")
