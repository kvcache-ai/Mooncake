file(GLOB HIP_TRANSPORT_SOURCES "*.cpp")

add_library(hip_transport OBJECT ${HIP_TRANSPORT_SOURCES})

target_include_directories(hip_transport PUBLIC ${HIP_INCLUDE_DIRS})

# Treat warnings as errors for hip_transport
target_compile_options(hip_transport PRIVATE 
    -Werror
)
