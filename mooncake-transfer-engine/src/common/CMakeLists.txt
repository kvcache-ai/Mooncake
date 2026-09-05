cmake_minimum_required(VERSION 4.0)

# Add base sub directory.
add_subdirectory(base)
list(APPEND SUB_STATIC_LIBS base)

set(SUB_STATIC_LIBS
    ${SUB_STATIC_LIBS}
    PARENT_SCOPE)
