if(USE_HYLINK)
  file(GLOB XPORT_SOURCES "*.cpp")

  add_library(tent_xport_hylink STATIC ${XPORT_SOURCES})
  target_link_libraries(tent_xport_hylink PUBLIC hip::host tent_common)
  target_compile_definitions(tent_xport_hylink PUBLIC USE_HYLINK)
endif()
