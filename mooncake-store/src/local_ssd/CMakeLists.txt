find_package(Threads REQUIRED)

add_library(mooncake_local_ssd STATIC manager.cpp)

target_include_directories(
  mooncake_local_ssd PUBLIC $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
                            $<BUILD_INTERFACE:${PROJECT_BINARY_DIR}/include>)

# manager.h includes Boost headers. They are on the default search path on Linux
# but not with Homebrew on macOS, so add the directory explicitly.
find_path(MOONCAKE_LOCAL_SSD_BOOST_INCLUDE_DIR boost/functional/hash.hpp)
if(MOONCAKE_LOCAL_SSD_BOOST_INCLUDE_DIR)
  target_include_directories(mooncake_local_ssd SYSTEM
                             PUBLIC ${MOONCAKE_LOCAL_SSD_BOOST_INCLUDE_DIR})
endif()

target_link_libraries(
  mooncake_local_ssd PUBLIC cachelib_memory_allocator
                            yalantinglibs::yalantinglibs Threads::Threads)
