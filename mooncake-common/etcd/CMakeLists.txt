add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so
  COMMAND
    bash -c "go mod tidy" && bash -c
    "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so ."
    && cp ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h
    ${CMAKE_CURRENT_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building Go shared library"
  DEPENDS etcd_wrapper.go security_config.go go.mod go.sum build.sh)

set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h)
set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so)

add_custom_target(build_etcd_wrapper DEPENDS ${ETCD_WRAPPER_LIB})

<<<<<<< HEAD
if(BUILD_UNIT_TESTS)
  add_test(
    NAME etcd_wrapper_go_test
    COMMAND go test ./...
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
endif()

install(
    FILES ${ETCD_WRAPPER_LIB}
    DESTINATION lib
)
=======
install(FILES ${ETCD_WRAPPER_LIB} DESTINATION lib)
>>>>>>> 7b069106 ([Common] Add TLS handshake tests and document etcd security limits)
