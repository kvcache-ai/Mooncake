if(USE_MLU)
  add_library(tent_xport_mlu STATIC mlu_transport.cpp)
  target_link_libraries(tent_xport_mlu PUBLIC tent_rpc tent_common platform_mlu)
endif()
