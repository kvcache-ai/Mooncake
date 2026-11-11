# Keep Sunrise header-only while giving consumers a target for its include paths
# and runtime dependencies, just like the compiled allocators.
add_library(sunrise_allocator INTERFACE)
target_include_directories(
  sunrise_allocator
  INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../include>
            $<INSTALL_INTERFACE:include>)
target_link_libraries(sunrise_allocator INTERFACE glog::glog)

if(USE_SUNRISE)
  target_compile_definitions(sunrise_allocator INTERFACE USE_SUNRISE)
  target_include_directories(sunrise_allocator
                             INTERFACE ${MC_TANGRT_ROOT}/include)
  target_link_directories(
    sunrise_allocator INTERFACE ${MC_TANGRT_ROOT}/lib/linux-x86_64
    ${MC_TANGRT_ROOT}/lib)
  target_link_libraries(sunrise_allocator INTERFACE tangrt_shared ptml_shared
                                                    ${CMAKE_DL_LIBS})
endif()
