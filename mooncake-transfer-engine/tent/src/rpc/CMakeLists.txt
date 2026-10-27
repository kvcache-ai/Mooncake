file(GLOB TENT_RPC_SOURCES "*.cpp")
add_library(tent_rpc OBJECT ${TENT_RPC_SOURCES})
target_link_libraries(tent_rpc PUBLIC yalantinglibs::yalantinglibs tent_common)
