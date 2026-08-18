find_package(Threads REQUIRED)

add_library(mooncake_local_ssd STATIC manager.cpp)

target_include_directories(
  mooncake_local_ssd PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                            $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)

target_link_libraries(
  mooncake_local_ssd PUBLIC cachelib_memory_allocator
                            yalantinglibs::yalantinglibs Threads::Threads)
