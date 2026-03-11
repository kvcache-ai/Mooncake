add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libk8s_wrapper.so
    COMMAND bash -c "go mod tidy" && bash -c "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libk8s_wrapper.so utils.go kubernetes_wrapper.go" && cp ${CMAKE_CURRENT_BINARY_DIR}/libk8s_wrapper.h ${CMAKE_CURRENT_SOURCE_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Go shared library"
    DEPENDS kubernetes_wrapper.go utils.go
)

set(K8S_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/libk8s_wrapper.h)
set(K8S_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libk8s_wrapper.so)

add_custom_target(
    build_k8s_wrapper
    DEPENDS ${K8S_WRAPPER_LIB}
)

install(
    FILES ${K8S_WRAPPER_LIB}
    DESTINATION lib
)