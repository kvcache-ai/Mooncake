find_program(GO_EXECUTABLE go)
if(NOT GO_EXECUTABLE)
    message(FATAL_ERROR "Go compiler not found. Please install Go.")
endif()

add_custom_target(mooncake_conductor ALL
    COMMAND ./build.sh
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
    COMMENT "Building Go program: mooncake_conductor"
    VERBATIM
)

set(GO_EXECUTABLE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/conductor-ctrl/mooncake_conductor" 
    CACHE INTERNAL "Path to Go executable")

# Install target
install(PROGRAMS ${GO_EXECUTABLE_PATH} DESTINATION bin)