if(USE_UB)
  file(GLOB TENT_UB_SOURCES CONFIGURE_DEPENDS "*.cpp")
  add_library(tent_xport_ub STATIC ${TENT_UB_SOURCES})
  target_link_libraries(tent_xport_ub PUBLIC tent_common Urma::urma)
  if(URMA_LIBRARY)
    target_compile_definitions(tent_xport_ub PRIVATE TENT_HAS_REAL_URMA=1)
  endif()
  # Test-only loopback adapter. Its source lives under tests/ and is linked in
  # only when USE_UB_MOCK=ON, so normal builds are unchanged.
  if(USE_UB_MOCK)
    set(TENT_UB_MOCK_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../tests/ub_mock)
    target_sources(tent_xport_ub
                   PRIVATE ${TENT_UB_MOCK_DIR}/loopback_urma_adapter.cpp)
    target_include_directories(tent_xport_ub PUBLIC ${TENT_UB_MOCK_DIR})
    target_compile_definitions(tent_xport_ub PRIVATE TENT_UB_MOCK_ADAPTER=1)
    message(WARNING "USE_UB_MOCK is ON: TENT UB includes a test-only "
                    "loopback adapter (MC_TENT_UB_MOCK=1 enables it)")
  endif()
endif()
