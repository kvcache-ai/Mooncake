# Copyright (c) 2026 绿算技术
# All rights reserved.
#
# @File: mooncake-store/src/spdk/CMakeLists.txt
#
# 修改履历：
# 2026-07-30 | 绿算技术  | 添加新源文件 

set(SPDK_WRAPPER_SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/spdk_wrapper.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/nof_connection.cpp     # 新增
    ${CMAKE_CURRENT_SOURCE_DIR}/nof_segment.cpp        # 新增
)

set(SPDK_WRAPPER_SOURCES ${SPDK_WRAPPER_SOURCES} PARENT_SCOPE)