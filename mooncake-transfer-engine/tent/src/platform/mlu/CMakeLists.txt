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
  include(CheckCXXSourceCompiles)
  include(CMakePushCheckState)
  cmake_push_check_state(RESET)
  set(CMAKE_REQUIRED_INCLUDES ${TENT_MLU_INCLUDE_DIR})
  check_cxx_source_compiles(
    "#include <cn_api.h>
     int main() {
       using Export = decltype(&cnMemGetHandleForAddressRange);
       static_assert(sizeof(Export) > 0);
       return CN_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED == 0 ||
              CN_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD == 0;
     }"
    TENT_MLU_HAS_DMABUF)
  cmake_pop_check_state()
  target_compile_definitions(tent_interface INTERFACE USE_MLU)
  add_library(platform_mlu STATIC mlu_platform.cpp)
  target_include_directories(platform_mlu PUBLIC ${TENT_MLU_INCLUDE_DIR})
  if(TENT_MLU_HAS_DMABUF)
    target_compile_definitions(platform_mlu PRIVATE TENT_MLU_HAS_DMABUF)
  endif()
  target_link_libraries(
    platform_mlu PUBLIC tent_common ${TENT_CNRT_LIBRARY} ${TENT_CNDRV_LIBRARY}
                        ${CMAKE_DL_LIBS})
endif()
