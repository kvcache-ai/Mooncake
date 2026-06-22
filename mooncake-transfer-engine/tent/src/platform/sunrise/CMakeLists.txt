if(USE_SUNRISE)
  file(GLOB TENT_PLATFORM_SUNRISE_SOURCES "*.cpp")
  add_library(platform_sunrise STATIC ${TENT_PLATFORM_SUNRISE_SOURCES})
  target_include_directories(platform_sunrise PUBLIC ${MC_TANGRT_ROOT}/include)
  target_link_directories(platform_sunrise PUBLIC
                          ${MC_TANGRT_ROOT}/lib/linux-x86_64
                          ${MC_TANGRT_ROOT}/lib)
  target_link_libraries(platform_sunrise PUBLIC tent_common tangrt_shared
                                                ptml_shared dl)
endif()
