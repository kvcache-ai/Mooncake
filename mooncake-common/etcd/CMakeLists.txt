# Go 1.26's randomized heap base can overlap Ascend's fixed FabricMem window.
# This is a compatibility measure, not a guarantee against all VA conflicts.
set(_etcd_go_env)
if(USE_ASCEND_DIRECT)
  execute_process(
    COMMAND go env GOVERSION
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    OUTPUT_VARIABLE _etcd_go_version
    OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
  if(_etcd_go_version MATCHES "^go1\\.26($|\\.)")
    set(_etcd_go_experiment "$ENV{GOEXPERIMENT}")
    if(NOT _etcd_go_experiment STREQUAL "")
      string(APPEND _etcd_go_experiment ",")
    endif()
    string(APPEND _etcd_go_experiment "norandomizedheapbase64")
    set(_etcd_go_env "GOEXPERIMENT=${_etcd_go_experiment}")
  endif()
endif()

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so
  COMMAND go mod tidy
  COMMAND ${CMAKE_COMMAND} -E env ${_etcd_go_env} go build -buildmode=c-shared
          -o ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so etcd_wrapper.go
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h
          ${CMAKE_CURRENT_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building Go shared library"
  DEPENDS etcd_wrapper.go go.mod go.sum build.sh)

set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h)
set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so)

add_custom_target(build_etcd_wrapper DEPENDS ${ETCD_WRAPPER_LIB})

if(BUILD_UNIT_TESTS)
  add_test(
    NAME etcd_wrapper_go_test
    COMMAND go test ./...
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

  find_program(ETCD_TEST_EXECUTABLE NAMES etcd)
  if(ETCD_TEST_EXECUTABLE)
    add_test(
      NAME etcd_wrapper_tls_integration_test
      COMMAND
        ${CMAKE_COMMAND} -E env "MOONCAKE_TEST_ETCD=${ETCD_TEST_EXECUTABLE}"
        "MOONCAKE_TEST_ETCD_WRAPPER=${ETCD_WRAPPER_LIB}" ${PYTHON_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/test_etcd_tls.py -v)
    set_tests_properties(etcd_wrapper_tls_integration_test
                         PROPERTIES TIMEOUT 180 PROCESSORS 2)
  else()
    message(
      STATUS "etcd binary not found; etcd TLS integration test unavailable")
  endif()
endif()

install(FILES ${ETCD_WRAPPER_LIB} DESTINATION lib)
