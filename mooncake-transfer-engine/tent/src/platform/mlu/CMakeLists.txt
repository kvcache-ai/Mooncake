if(USE_MLU)
  find_path(
    TENT_MLU_INCLUDE_DIR cnrt.h
    HINTS ${MLU_INCLUDE_DIR} ${NEUWARE_ROOT}/include $ENV{NEUWARE_HOME}/include
          /usr/local/neuware/include REQUIRED)
  find_library(
    TENT_CNRT_LIBRARY cnrt
    HINTS ${MLU_LIB_DIR} ${NEUWARE_ROOT}/lib64 $ENV{NEUWARE_HOME}/lib64
          /usr/local/neuware/lib64 REQUIRED)
  find_library(
    TENT_CNDRV_LIBRARY cndrv
    HINTS ${MLU_LIB_DIR} ${NEUWARE_ROOT}/lib64 $ENV{NEUWARE_HOME}/lib64
          /usr/local/neuware/lib64 REQUIRED)
  target_compile_definitions(tent_interface INTERFACE USE_MLU)
  add_library(platform_mlu STATIC mlu_platform.cpp)
  target_include_directories(platform_mlu PUBLIC ${TENT_MLU_INCLUDE_DIR})
  target_link_libraries(platform_mlu PUBLIC tent_common ${TENT_CNRT_LIBRARY}
                                            ${TENT_CNDRV_LIBRARY})
endif()
