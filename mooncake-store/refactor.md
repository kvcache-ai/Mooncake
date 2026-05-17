这是一个重构说明书文档
读取kv.md , GDS\_KV\_Integration\_Analysis.md,  read.md, spec.md, object_storage_integration.md   filestorage_storagebackend_relationship.md

nds_interface.h 


# 需求如下：
- client.cpp 的实现有些问题, client.cpp.org 是原始的文件，
- 分析client.cpp.org和client.cpp的差异diff，找出问题所在
- 尽量把client.cpp 还原成client.cpp.org, 基于diff 进行重构，把差异内容是现在 backend->StoreObject(path, value); 改为NDS::put , LoadObject 改为NDS::get

重构完保证编译可以通过


