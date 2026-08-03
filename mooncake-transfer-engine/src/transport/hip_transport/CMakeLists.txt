# Copyright (c) 2026 Hygon Information Technology Co., Ltd.
# SPDX-License-Identifier: Apache-2.0
# Modified by Hygon Information Technology Co., Ltd., 2026.

file(GLOB HIP_TRANSPORT_SOURCES "*.cpp")

if (USE_HYGON)
    set(COPY_KERNEL ${CMAKE_CURRENT_BINARY_DIR}/mc_copy_kernel.co)

    add_custom_command(
        OUTPUT ${COPY_KERNEL}
        COMMAND hipcc --genco -fPIC -x hip
                ${CMAKE_CURRENT_SOURCE_DIR}/mc_copy_kernel.hip
                -o ${COPY_KERNEL}
        DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/mc_copy_kernel.hip
    )

    add_library(hip_transport OBJECT
        ${HIP_TRANSPORT_SOURCES}
        ${COPY_KERNEL}
    )
else()
    add_library(hip_transport OBJECT ${HIP_TRANSPORT_SOURCES})
endif()

target_include_directories(hip_transport PUBLIC ${HIP_INCLUDE_DIRS})

# Treat warnings as errors for hip_transport
target_compile_options(hip_transport PRIVATE 
    -Werror
)
