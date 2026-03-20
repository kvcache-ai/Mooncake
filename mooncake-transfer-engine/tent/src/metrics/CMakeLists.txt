# Option to enable/disable TENT metrics at compile time
option(TENT_METRICS_ENABLED "Enable TENT metrics collection" OFF)

file(GLOB TENT_METRICS_SOURCES "*.cpp")
add_library(tent_metrics STATIC ${TENT_METRICS_SOURCES})
target_link_libraries(tent_metrics PUBLIC tent_common tent_interface yalantinglibs::yalantinglibs glog pthread)

# Pass compile definition based on option
if(TENT_METRICS_ENABLED)
    target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=1)
    message(STATUS "TENT metrics: ENABLED")
else()
    target_compile_definitions(tent_metrics PUBLIC TENT_METRICS_ENABLED=0)
    message(STATUS "TENT metrics: DISABLED (zero overhead)")
endif()