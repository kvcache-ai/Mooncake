add_custom_command(
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.so
         ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.h
  COMMAND bash -c "go mod tidy"
  COMMAND
    bash -c
    "go build -buildmode=c-shared -o ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.so etcd_wrapper.go k8s_lease_wrapper.go"
  COMMAND
    ${CMAKE_COMMAND} -E copy
    ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.h
    ${CMAKE_CURRENT_SOURCE_DIR}/libmooncake_ha_wrapper.h
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMENT "Building unified Go HA wrapper"
  DEPENDS etcd_wrapper.go k8s_lease_wrapper.go go.mod)

set(HA_WRAPPER_INCLUDE
    ${CMAKE_CURRENT_BINARY_DIR}
    PARENT_SCOPE)
set(HA_WRAPPER_LIB
    ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.so
    PARENT_SCOPE)

add_custom_target(build_ha_wrapper
                  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.so)

install(FILES ${CMAKE_CURRENT_BINARY_DIR}/libmooncake_ha_wrapper.so
        DESTINATION lib)
