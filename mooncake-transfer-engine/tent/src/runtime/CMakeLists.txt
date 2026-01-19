file(GLOB TENT_RUNTIME_SOURCES "*.cpp")
add_library(tent_runtime OBJECT ${TENT_RUNTIME_SOURCES})

if (TARGET metastore_redis)
  add_definitions(-DUSE_REDIS)
  target_link_libraries(tent_runtime PUBLIC metastore_redis)
endif()

if (TARGET metastore_http)
  add_definitions(-DUSE_HTTP)
  target_link_libraries(tent_runtime PUBLIC metastore_http)
endif()

if (TARGET metastore_etcd)
  add_definitions(-DUSE_ETCD)
  target_link_libraries(tent_runtime PUBLIC metastore_etcd)
endif()

target_link_libraries(tent_runtime PUBLIC tent_common tent_rpc tent_platform_all tent_transport_all tent_metrics)
