# SPDX-License-Identifier: Apache-2.0 Exercise process exit, where pending
# native cleanup can race global teardown.
foreach(attempt RANGE 1 100)
  execute_process(
    COMMAND "${PROGRAM}"
    RESULT_VARIABLE status
    TIMEOUT 5)
  if(NOT status STREQUAL "0")
    message(FATAL_ERROR "Client lifecycle process ${attempt} failed: ${status}")
  endif()
endforeach()
