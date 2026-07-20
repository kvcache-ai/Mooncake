add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so
    COMMAND bash -c "go mod tidy" && bash -c "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so k8s_lease_wrapper.go" && cp ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.h ${CMAKE_CURRENT_SOURCE_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building K8s Lease Go shared library"
    DEPENDS k8s_lease_wrapper.go
)

set(K8S_LEASE_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.h)
set(K8S_LEASE_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libk8s_lease_wrapper.so)

add_custom_target(
    build_k8s_lease_wrapper
    DEPENDS ${K8S_LEASE_WRAPPER_LIB}
)

install(
    FILES ${K8S_LEASE_WRAPPER_LIB}
    DESTINATION lib
)
