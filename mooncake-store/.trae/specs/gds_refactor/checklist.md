# GDS 重构项目 - 验证清单

- [x] gds_mock.cpp 中不再包含 NDSMock 类的定义
- [x] gds_mock.cpp 直接实现了 gds_interface.h 暴露的接口
- [x] client.cpp 中不再包含 NDSMock::instance() 的调用
- [x] 测试代码中不再包含 NDSMock::instance() 的调用
- [x] 项目编译成功，没有错误
- [x] 所有测试用例通过
- [x] NDSMock 类的辅助方法（如 clear()、size()、hasBlock()）得到合理处理