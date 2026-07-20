if(USE_SUNRISE)
  file(GLOB TENT_PLATFORM_SUNRISE_SOURCES "*.cpp")
  add_library(platform_sunrise STATIC ${TENT_PLATFORM_SUNRISE_SOURCES})
  target_include_directories(platform_sunrise PUBLIC /usr/local/tangrt/include)
  target_link_directories(platform_sunrise PUBLIC
                          /usr/local/tangrt/lib/linux-x86_64)
  target_link_libraries(platform_sunrise PUBLIC tent_common tangrt_shared
                                                ptml_shared dl)
endif()
