# Claude Code 创建的文档索引

本文件夹包含由 Claude Code 在开发过程中创建的所有文档，按类别分组：

## 📋 核心文档

### 1. 指标计算与存储系统
- **`IMPLEMENTATION_SUMMARY.md`** - 指标计算与存储系统的完整实现总结
  - 包含所有新增文件清单、修改内容、功能特性
  - 核心API说明、数据库设计、测试验证
  - 重要修复记录总结

- **`METRICS_COMPUTE_STORAGE_GUIDE.md`** - 完整的指标计算与存储使用指南
  - 系统架构说明
  - 数据库表结构
  - API接口文档
  - 实际使用示例

### 2. 关键修复文档
- **`DIMENSION_FIELD_FIX.md`** - 维度字段设置修复说明
  - 问题：KpiStorageService 错误地将指标编码当作维度字段
  - 解决方案：使用 getDimFieldNames() 只获取真正的维度字段

- **`CACHE_AND_FILTER_FIX.md`** - Redis缓存和条件性返回修复说明
  - 问题1：Redis缓存key缺少配置参数导致配置失效
  - 问题2：不需要的数据字段仍然返回
  - 解决方案：缓存key包含开关参数，实现条件性字段过滤

- **`COMPLEX_EXPRESSION_FIX.md`** - 复杂表达式查询修复完成报告
  - 移除单个表达式查询限制
  - 修复硬编码表名问题
  - 统一处理流程
  - 清理遗留代码

## 📊 配置与参数

### 1. API相关
- **`API_OUTPUT_PARAMS.md`** - API输出参数说明
  - KpiQueryResult 字段详解
  - 查询参数控制说明

### 2. 系统配置
- **`BUSI_README.md`** - 业务场景说明
  - 数据存储模型设计
  - 查询优化策略

## 🏗️ 技术实现

### 1. 存储引擎
- **`STORAGE_COMPUTE_ENGINE.md`** - 存储计算引擎架构
  - 支持MySQL和SQLite两种引擎
  - 引擎切换机制

## 📚 开发指南

### 1. Gradle相关
- **`GRADLE_MIGRATION.md`** - Gradle迁移指南
- **`GRADLE_WRAPPER_README.md`** - Gradle Wrapper说明
- **`QUICK_REFERENCE.md`** - 快速参考指南

### 2. 项目说明
- **`README.md`** - 项目总体说明
- **`CLAUDE.md`** - Claude Code使用说明

## 📁 文档分类

| 类别 | 文档 | 描述 |
|------|------|------|
| **核心实现** | IMPLEMENTATION_SUMMARY.md | 完整实现总结 |
| | METRICS_COMPUTE_STORAGE_GUIDE.md | 使用指南 |
| **重要修复** | DIMENSION_FIELD_FIX.md | 维度字段修复 |
| | CACHE_AND_FILTER_FIX.md | 缓存和字段过滤修复 |
| | COMPLEX_EXPRESSION_FIX.md | 复杂表达式修复 |
| **配置参数** | API_OUTPUT_PARAMS.md | API参数说明 |
| | BUSI_README.md | 业务场景说明 |
| **技术架构** | STORAGE_COMPUTE_ENGINE.md | 引擎架构 |
| **开发工具** | QUICK_REFERENCE.md | 快速参考 |

## 🎯 使用建议

1. **新用户**: 从 `IMPLEMENTATION_SUMMARY.md` 和 `METRICS_COMPUTE_STORAGE_GUIDE.md` 开始
2. **问题排查**: 查看对应的修复文档
3. **功能开发**: 参考 `QUICK_REFERENCE.md` 和相关指南
4. **API集成**: 查看 `API_OUTPUT_PARAMS.md`

## 📝 文档版本

- 所有文档创建时间：2025-11-10 至 2025-11-12
- 状态：✅ 持续更新中
- 最后更新：2025-11-12 15:56

---

*注意：本文件夹中的文档为 Claude Code 在开发过程中的产物，包含了完整的开发记录和修复历史。*
