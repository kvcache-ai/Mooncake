include(../PrepareGoModule.cmake)

set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h)
set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so)

add_custom_command(
  OUTPUT ${ETCD_WRAPPER_LIB} ${ETCD_WRAPPER_INCLUDE}
  COMMAND ${_go_command} build -mod=readonly "${_go_modfile}"
          -buildmode=c-shared -o "${ETCD_WRAPPER_LIB}" .
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${ETCD_WRAPPER_INCLUDE}"
          "${CMAKE_CURRENT_SOURCE_DIR}"
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building Go shared library"
  DEPENDS ${_go_sources} go.mod go.sum
  VERBATIM)

add_custom_target(build_etcd_wrapper DEPENDS ${ETCD_WRAPPER_LIB}
                                             ${ETCD_WRAPPER_INCLUDE})

if(BUILD_UNIT_TESTS)
  add_test(
    NAME etcd_wrapper_go_test
    COMMAND ${_go_command} test -mod=readonly "${_go_modfile}" ./...
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
