add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so
    COMMAND bash -c "go mod tidy" && bash -c "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so etcd_wrapper.go" && cp ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h ${CMAKE_CURRENT_SOURCE_DIR}
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Go shared library"
    DEPENDS etcd_wrapper.go
)

set(ETCD_WRAPPER_INCLUDE ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.h)
set(ETCD_WRAPPER_LIB ${CMAKE_CURRENT_BINARY_DIR}/libetcd_wrapper.so)

add_custom_target(
    build_etcd_wrapper
    DEPENDS ${ETCD_WRAPPER_LIB}
)

install(
    FILES ${ETCD_WRAPPER_LIB}
    DESTINATION lib
)