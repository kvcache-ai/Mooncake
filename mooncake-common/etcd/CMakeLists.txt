add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so
  COMMAND
    bash -c "go mod tidy" && bash -c
    "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so etcd_wrapper.go"
    && cp ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h
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
