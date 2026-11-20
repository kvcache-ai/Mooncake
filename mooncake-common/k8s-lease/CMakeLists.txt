include(../PrepareGoModule.cmake)

set(K8S_LEASE_WRAPPER_INCLUDE
    ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.h)
set(K8S_LEASE_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so)

add_custom_command(
  OUTPUT ${K8S_LEASE_WRAPPER_LIB} ${K8S_LEASE_WRAPPER_INCLUDE}
  COMMAND ${_go_command} build -mod=readonly "${_go_modfile}"
          -buildmode=c-shared -o "${K8S_LEASE_WRAPPER_LIB}" .
  COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${K8S_LEASE_WRAPPER_INCLUDE}"
          "${CMAKE_CURRENT_SOURCE_DIR}"
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building K8s Lease Go shared library"
  DEPENDS ${_go_sources} go.mod go.sum
  VERBATIM)

add_custom_target(build_k8s_lease_wrapper DEPENDS ${K8S_LEASE_WRAPPER_LIB}
                                                  ${K8S_LEASE_WRAPPER_INCLUDE})

install(FILES ${K8S_LEASE_WRAPPER_LIB} DESTINATION lib)
