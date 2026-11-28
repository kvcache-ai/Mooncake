if(USE_NDS)
  find_path(
    NDS_INCLUDE_DIR ascend_nds.h
    HINTS ${NDS_ROOT}
    PATH_SUFFIXES include)
  find_library(
    NDS_LIBRARY ascend_nds
    HINTS ${NDS_ROOT}
    PATH_SUFFIXES lib lib64)
  if(NOT NDS_INCLUDE_DIR OR NOT NDS_LIBRARY)
    message(
      FATAL_ERROR
        "USE_NDS requires ascend_nds.h and libascend_nds; set NDS_ROOT or NDS_INCLUDE_DIR/NDS_LIBRARY"
    )
  endif()
  target_compile_definitions(tent_interface INTERFACE USE_NDS)
  add_library(tent_xport_nds STATIC nds_transport.cpp)
  target_include_directories(tent_xport_nds PRIVATE ${NDS_INCLUDE_DIR})
  target_link_libraries(tent_xport_nds PUBLIC tent_common ${NDS_LIBRARY})
endif()
