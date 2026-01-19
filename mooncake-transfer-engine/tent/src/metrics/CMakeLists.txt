# Option to enable/disable TENT metrics at compile time
option(TENT_METRICS_ENABLED "Enable TENT metrics collection" OFF)

file(GLOB TENT_METRICS_SOURCES "*.cpp")
add_library(tent_metrics STATIC ${TENT_METRICS_SOURCES})
target_link_libraries(tent_metrics PUBLIC tent_common tent_interface glog pthread)

# Pass compile definition based on option
if(TENT_METRICS_ENABLED)
    target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=1)
    message(STATUS "TENT metrics: ENABLED")
else()
    target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=0)
    message(STATUS "TENT metrics: DISABLED (zero overhead)")
endif()

# Add yalantinglibs dependency for metrics and HTTP server
# Note: yalantinglibs bundles Asio in include/ylt/thirdparty/, no external Asio required
find_path(YLT_INCLUDE_DIR ylt/metric.hpp PATHS ${CMAKE_SOURCE_DIR}/thirdparties/yalantinglibs/include)
if(YLT_INCLUDE_DIR)
    target_include_directories(tent_metrics PUBLIC ${YLT_INCLUDE_DIR})
    message(STATUS "Found yalantinglibs at: ${YLT_INCLUDE_DIR}")
else()
    if(TENT_METRICS_ENABLED)
        message(FATAL_ERROR "yalantinglibs not found, but it is required when TENT_METRICS_ENABLED is ON.")
    else()
        message(WARNING "yalantinglibs not found, metrics will be disabled.")
    endif()
endif()