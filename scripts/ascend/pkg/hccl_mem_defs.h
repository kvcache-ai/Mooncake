/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 * Description: HCCL内存类型定义文件，声明内存数据类型
 */

#ifndef HCCL_MEM_DEFS_H
#define HCCL_MEM_DEFS_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif  // __cplusplus

/**
 * @enum HcclMemType
 * @brief 内存类型枚举定义
 */
typedef enum {
    HCCL_MEM_TYPE_DEVICE,  ///< 设备侧内存（如NPU等）
    HCCL_MEM_TYPE_HOST,    ///< 主机侧内存
    HCCL_MEM_TYPE_NUM      ///< 内存类型数量
} HcclMemType;

/**
 * @struct HcclMem
 * @brief 内存段元数据描述结构体
 * @var type  - 内存物理位置类型，参见HcclMemType
 * @var addr  - 内存虚拟地址
 * @var size  - 内存区域字节数
 */
typedef struct {
    HcclMemType type;
    void *addr;
    uint64_t size;
} HcclMem;

#ifdef __cplusplus
}
#endif  // __cplusplus
#endif