file(GLOB_RECURSE _go_sources CONFIGURE_DEPENDS
     "${CMAKE_CURRENT_SOURCE_DIR}/*.go")
set_property(
  DIRECTORY
  APPEND
  PROPERTY CMAKE_CONFIGURE_DEPENDS ${_go_sources}
           "${CMAKE_CURRENT_SOURCE_DIR}/go.mod")
execute_process(
  COMMAND go mod tidy
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  RESULT_VARIABLE _go_tidy_result)
if(NOT _go_tidy_result STREQUAL "0")
  message(
    FATAL_ERROR
      "Failed to prepare K8s lease Go dependencies: ${_go_tidy_result}")
endif()
set_property(
  DIRECTORY
  APPEND
  PROPERTY CMAKE_CONFIGURE_DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/go.sum")

add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so
         ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.h
  COMMAND
    bash -c
    "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so k8s_lease_wrapper.go"
    && cp ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.h
    ${CMAKE_CURRENT_SOURCE_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building K8s Lease Go shared library"
  DEPENDS k8s_lease_wrapper.go go.mod go.sum)

set(K8S_LEASE_WRAPPER_INCLUDE
    ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.h)
set(K8S_LEASE_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so)

add_custom_target(build_k8s_lease_wrapper DEPENDS ${K8S_LEASE_WRAPPER_LIB})

install(FILES ${K8S_LEASE_WRAPPER_LIB} DESTINATION lib)
