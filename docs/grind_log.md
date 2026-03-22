# Grind 优化日志

## 第 1 轮 | 2026-03-23

**评分**: 52.5 → 预估 62（+9.5）
**优化内容**: 技术完整性提升 — 添加 S3-FIFO 和 Bloom Filter 单元测试，更新故事线
**测试结果**:
- S3-FIFO: 10/10 测试通过（含 KVCache 工作负载模拟，系统提示词 100% 存活）
- Bloom Filter: 6/6 测试通过（FPR=0.004%，并发安全，零假阴性）
**故事线调整**: 砍掉 PMEM（硬件不可用），强化自适应调度器叙事
**Commit**: 4255d9c
**结论**: ✅ 有效 — 技术完整性从"代码能编译"提升到"有测试覆盖+数据支撑"

## 第 2 轮 | 2026-03-23

**评分**: 预估 62 → 预估 72（+10）
**优化内容**: 创新性+场景适配性 — 自适应调度器模式切换验证 + 3 阶段 LLM 负载 benchmark
**关键结果**:
- 自适应调度器成功检测 PREFIX_HEAVY 模式（hit=0.76，自动调整 watermark=0.92, pin=1h）
- 工作负载切换时 EWMA 平滑过渡到 MIXED 模式，无震荡
- 3 阶段 benchmark: PREFIX_HEAVY(hit=0.90) → SCAN_HEAVY(hit=0.10) → MIXED(hit=0.50)
- 自适应调度器日志清晰记录了模式转换和参数调整
**Master 调度日志**:
```
PREFIX_HEAVY hit=0.76 watermark=0.92 evict_ratio=0.03 soft_pin=3600s
→ MIXED hit=0.55 watermark=0.85 evict_ratio=0.05 soft_pin=1800s
→ MIXED hit=0.32 watermark=0.85 evict_ratio=0.05 soft_pin=1800s
```
**Commit**: 90a90c9
**结论**: ✅ 有效 — 自适应调度器在真实负载模式下正确切换参数
