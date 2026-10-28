add_library(
  adaptive_congestion_control_core STATIC
  adaptive_congestion_control.cpp adaptive_congestion_control_config.cpp
  rdma_error_classifier.cpp)
set_target_properties(
  adaptive_congestion_control_core PROPERTIES POSITION_INDEPENDENT_CODE ON
                                              CXX_VISIBILITY_PRESET hidden)
target_compile_features(adaptive_congestion_control_core PUBLIC cxx_std_20)
target_compile_definitions(adaptive_congestion_control_core
                           PUBLIC MOONCAKE_ENABLE_ADAPTIVE_CONGESTION_CONTROL=1)
target_include_directories(
  adaptive_congestion_control_core
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
