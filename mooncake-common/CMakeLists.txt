if ((USE_ETCD AND NOT USE_ETCD_LEGACY) OR STORE_USE_ETCD)
    add_subdirectory(etcd)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(src)

if (BUILD_UNIT_TESTS)
  add_subdirectory(tests)
endif()
