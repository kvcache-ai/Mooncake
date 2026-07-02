if(USE_UB)
  file(GLOB UB_TENT_SOURCES "*.cpp")

  # Keep this library limited to the TENT UB adapter sources.  The old-TE
  # ub_transport objects are already provided by the transfer_engine target when
  # USE_UB is enabled; embedding them here as well makes final executables link
  # the same object files twice.
  add_library(tent_xport_ub STATIC ${UB_TENT_SOURCES})

  # Old-TE public headers (transfer_metadata.h, topology.h, transport/*.h).
  # These are already on the global include path when building inside the
  # mooncake-transfer-engine tree (set by include_directories(include) in
  # mooncake-transfer-engine/CMakeLists.txt).  The explicit entry below handles
  # standalone TENT builds where that global path is absent.
  target_include_directories(
    tent_xport_ub
    PUBLIC
      $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/mooncake-transfer-engine/include>
      $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/mooncake-common/include>
      ${urma_INCLUDE_DIR})

  target_link_libraries(
    tent_xport_ub
    PUBLIC tent_common
    PRIVATE JsonCpp::JsonCpp glog::glog pthread)

  if(URMA_LIBRARY)
    target_link_libraries(tent_xport_ub PUBLIC ${URMA_LIBRARY})
  endif()

  message(STATUS "tent_xport_ub: built (USE_UB=ON)")
endif()
