include(FetchContent)

# UMDK 头文件库
FetchContent_Declare(
        urma
        GIT_REPOSITORY https://atomgit.com/openeuler/umdk.git
        GIT_TAG        v25.12.0
)

FetchContent_MakeAvailable(urma)

# 输出实际路径，确认位置
message(STATUS "URMA source dir: ${urma_SOURCE_DIR}")
message(STATUS "URMA binary dir: ${urma_BINARY_DIR}")

# 假设 UMDK 头文件在其 include 目录下
set(urma_INCLUDE_DIR ${urma_SOURCE_DIR}/src/urma/lib/urma/core/include)

# 添加到需要的目标
message(STATUS "urma_INCLUDE_DIR: ${urma_INCLUDE_DIR}")