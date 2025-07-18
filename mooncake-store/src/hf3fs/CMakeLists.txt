set(HF3FS_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/hf3fs_file.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/hf3fs_resource_manager.cpp
)

include_directories("include")

set(HF3FS_SOURCES ${HF3FS_SOURCES} PARENT_SCOPE)