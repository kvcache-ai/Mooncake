file(GLOB TENT_RUNTIME_SOURCES "*.cpp")
add_library(tent_runtime OBJECT ${TENT_RUNTIME_SOURCES})

if(BUILD_UNIT_TESTS)
  # Resolve-call counter for tent_transport_route_cache_test. Production builds
  # leave the field and fetch_add out of TransferEngineImpl.
  target_compile_definitions(tent_runtime PUBLIC TENT_TRANSPORT_RESOLVE_COUNTER)
endif()

if(TARGET metastore_redis)
  add_definitions(-DUSE_REDIS)
  target_link_libraries(tent_runtime PUBLIC metastore_redis)
endif()

if(TARGET metastore_http)
  add_definitions(-DUSE_HTTP)
  target_link_libraries(tent_runtime PUBLIC metastore_http)
endif()

if(TARGET metastore_etcd)
  add_definitions(-DUSE_ETCD)
  target_link_libraries(tent_runtime PUBLIC metastore_etcd)
endif()

target_link_libraries(
  tent_runtime PUBLIC tent_common tent_rpc tent_platform_all tent_transport_all
                      tent_metrics)
# The runtime references the Transfer Engine client I/O pool accessor
# (mooncake::CreateRpcClientIoContextPool / mooncake::Environ) from the common
# library. Declaring the dependency here keeps the shared library link ordered
# correctly; otherwise libtent_shared.so is emitted with dangling undefined
# symbols (Ubuntu's default --as-needed drops libmooncake_common.so because it
# is placed before the archives that reference it), and consumers linking only
# against libtent_shared.so (e.g. the NIXL Mooncake plugin) fail at load time.
if(TARGET mooncake_common)
  target_link_libraries(tent_runtime PUBLIC mooncake_common)
endif()
