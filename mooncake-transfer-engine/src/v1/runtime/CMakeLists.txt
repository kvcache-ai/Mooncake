file(GLOB TENT_RUNTIME_SOURCES "*.cpp")
add_library(tent_runtime OBJECT ${TENT_RUNTIME_SOURCES})

if (TARGET redis_static)
  add_definitions(-DUSE_REDIS)
  target_link_libraries(tent_runtime PUBLIC redis_static)
endif()

if (TARGET http_static)
  add_definitions(-DUSE_HTTP)
  target_link_libraries(tent_runtime PUBLIC http_static)
endif()

if (TARGET etcd_static)
  add_definitions(-DUSE_ETCD)
  target_link_libraries(tent_runtime PUBLIC etcd_static)
endif()

target_link_libraries(tent_runtime PUBLIC tent_common tent_rpc platform_all transport_all)
