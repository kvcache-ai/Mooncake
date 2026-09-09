cmake_minimum_required(VERSION 3.20)

# Add base sub directory.
add_subdirectory(base)
list(APPEND SUB_STATIC_LIBS base)

if(MOONCAKE_ENABLE_ADAPTIVE_CC)
  add_subdirectory(adaptive_congestion_control)
endif()

set(SUB_STATIC_LIBS
    ${SUB_STATIC_LIBS}
    PARENT_SCOPE)
