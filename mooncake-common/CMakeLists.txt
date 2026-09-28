if ((USE_ETCD AND NOT USE_ETCD_LEGACY) OR STORE_USE_ETCD)
    add_subdirectory(etcd)
endif()

if (STORE_USE_K8S_LEASE)
    add_subdirectory(k8s-lease)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  # Test-only private access to Environ. Never link this into production
  # targets.
  add_library(environ_test_peer INTERFACE)
  target_include_directories(environ_test_peer
                             INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/tests)
  target_link_libraries(environ_test_peer INTERFACE mooncake_common)
  add_subdirectory(tests)
endif()
