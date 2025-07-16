set(THREEFS_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/threefs_file.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/threefs_resource_manager.cpp
)

include_directories("include")

set(THREEFS_SOURCES ${THREEFS_SOURCES} PARENT_SCOPE)