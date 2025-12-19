# 表到达 API 技术文档

本文档描述指标库系统的"表到达"(`srcTableComplete`) API 完整业务逻辑，供其他项目实现相同接口参考。

---

## 1. API 接口定义

### 1.1 接口信息

| 项目 | 值 |
|------|-----|
| URL | `POST /api/open/kpi/srcTableComplete` |
| Content-Type | `application/json` |

### 1.2 请求参数

```json
{
  "srcTableName": "DW_FACT_ORDER_D",
  "opTime": "20251219"
}
```

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| srcTableName | String | 是 | ETL完成的来源表名 |
| opTime | String | 是 | 批次号，如 20251219 |

### 1.3 响应格式

```json
{
  "status": "SUCCESS",
  "triggeredModels": [
    {
      "modelId": "MODEL-01",
      "opTime": "20251219",
      "status": "SUCCESS",
      "computeCount": 1000,
      "storageCount": 1000
    }
  ],
  "waitingModels": ["MODEL-02"],
  "skippedModels": ["MODEL-03"]
}
```

---

## 2. 数据库表结构

### 2.1 模型依赖表 `metrics_model_dependency`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT AUTO_INCREMENT | 主键 |
| model_id | VARCHAR(255) | 指标模型ID |
| dependency_table_name | VARCHAR(255) | 依赖的来源表名 |

### 2.2 ETL表到达日志 `metrics_etl_log`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT AUTO_INCREMENT | 主键 |
| table_name | VARCHAR(255) | 来源表名 |
| op_time | VARCHAR(255) | 批次号 |
| arrival_time | DATETIME | 表到达时间（ETL重跑时更新） |

> **唯一约束**: `(table_name, op_time)`

### 2.3 任务执行日志 `metrics_task_log`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT AUTO_INCREMENT | 主键 |
| model_id | VARCHAR(255) | 模型ID |
| op_time | VARCHAR(255) | 批次号 |
| status | VARCHAR(255) | RUNNING / SUCCESS / FAILED |
| start_time | DATETIME | 开始时间 |
| end_time | DATETIME | 结束时间 |
| message | VARCHAR(2000) | 执行消息 |
| compute_count | INT | 计算条数 |
| storage_count | INT | 存储条数 |

### 2.4 Webhook订阅表 `metrics_webhook_subscription`

| 字段 | 类型 | 说明 |
|------|------|------|
| id | INT AUTO_INCREMENT | 主键 |
| kpi_id | VARCHAR(64) | 订阅的指标ID |
| callback_url | VARCHAR(512) | 回调地址 |
| secret | VARCHAR(128) | 鉴权三元组，格式见下 |
| retry_num | INT | 重试次数，默认 3 |
| status | VARCHAR(16) | 1=启用，0=禁用 |
| team_name | VARCHAR(64) | 租户标识 |
| user_id | VARCHAR(64) | 用户ID |
| created_at | DATETIME | 创建时间 |

---

## 3. 核心业务逻辑

### 3.1 主流程

```
┌────────────────────────────────────────────────────────────────┐
│                   srcTableComplete(srcTableName, opTime)        │
├────────────────────────────────────────────────────────────────┤
│  1. logTableArrival()   记录/更新表到达时间                      │
│         ↓                                                       │
│  2. findByDependencyTableName()  查找依赖此表的所有模型           │
│         ↓                                                       │
│  3. 对每个模型:                                                  │
│     ├─ isModelPublished()   检查模型是否已发布 (state=1)         │
│     ├─ isModelReady()       检查所有依赖表是否已到达             │
│     ├─ shouldReExecute()    检查是否需要重跑（ETL重做检测）       │
│     └─ executeModelWithLogging()  执行抽取并记录日志             │
│         ↓                                                       │
│  4. Webhook通知：notifyKpiUpdated()                              │
└────────────────────────────────────────────────────────────────┘
```

### 3.2 表到达记录 (UPSERT语义)

```java
// 首次到达：INSERT
// 再次到达：UPDATE arrival_time（标记ETL重跑）

if (existingLog.isPresent()) {
    log.setArrivalTime(now);  // 更新时间
    save(log);
} else {
    newLog.setTableName(tableName);
    newLog.setOpTime(opTime);
    newLog.setArrivalTime(now);
    save(newLog);
}
```

### 3.3 多依赖检查 (isModelReady)

```java
// 模型可能依赖多张表，只有全部到达才执行
for (dependency : modelDependencies) {
    if (!etlLogExists(dependency.tableName, opTime)) {
        return false;  // 还有依赖未到达
    }
}
return true;
```

### 3.4 ETL重跑检测 (shouldReExecute)

```java
// 规则：如果任意依赖表的 arrival_time > 上次成功执行的 end_time
//       则需要重新执行

lastSuccess = findLastSuccessTask(modelId, opTime);
if (!lastSuccess) return true;  // 从未执行过

for (dependency : modelDependencies) {
    etlLog = findEtlLog(dependency.tableName, opTime);
    if (etlLog.arrivalTime > lastSuccess.endTime) {
        return true;  // ETL重跑了，需要重新抽取
    }
}
return false;
```

### 3.5 并发控制

```java
// 检查是否有正在执行的任务
if (existsRunningTask(modelId, opTime)) {
    return SKIPPED;  // 防止并发重复执行
}

// 创建 RUNNING 状态的任务日志
taskLog.setStatus("RUNNING");
save(taskLog);

// 执行完成后更新为 SUCCESS 或 FAILED
```

---

## 4. Webhook 通知机制

### 4.1 通知触发

模型执行成功后，调用 `WebhookNotificationService.notifyKpiUpdated()`：

```java
// 1. 获取本次抽取的派生指标
List<String> extendedKpiIds = ["KD1001", "KD1002"];

// 2. 追踪影响范围（递归查找依赖这些指标的复合指标）
Set<String> affectedKpiIds = extendedKpiIds + findComputedKpisDependingOn(extendedKpiIds);

// 3. 查询订阅并按配置聚合
subscriptions = findByKpiIdIn(affectedKpiIds).groupBy(callback+retry+secret);

// 4. 异步发送通知（指数回退重试）
for (group : subscriptions) {
    asyncNotifyWithRetry(group, modelId, opTime, affectedKpiIds);
}
```

### 4.2 通知内容

```json
{
  "event": "KPI_UPDATED",
  "modelId": "MODEL-01",
  "opTime": "20251219",
  "affectedKpiIds": ["KD1001", "KD1002"],
  "timestamp": "2025-12-19T16:00:00+08:00"
}
```

### 4.3 Secret 鉴权三元组

`secret` 字段格式：`类型,键,值`

| 类型 | 示例 | 效果 |
|------|------|------|
| Query | `Query,token,abc123` | URL追加 `?token=abc123` |
| Header | `Header,Authorization,Bearer xxx` | 添加请求头 |
| Body | `Body,json,{"appId":"xxx"}` | 合并到请求体 |

### 4.4 重试策略

- 最大重试次数：配置在 `retry_num`，默认 3
- 回退间隔：指数回退 (1s, 2s, 4s, ...)
- 异步执行：使用独立线程池，不阻塞主流程

---

## 5. 时序图

```
ETL系统                  指标库                      数据库                 Webhook订阅方
   │                       │                          │                         │
   │ POST srcTableComplete │                          │                         │
   │──────────────────────>│                          │                         │
   │                       │ UPSERT metrics_etl_log   │                         │
   │                       │─────────────────────────>│                         │
   │                       │                          │                         │
   │                       │ 查询依赖此表的模型         │                         │
   │                       │─────────────────────────>│                         │
   │                       │<─────────────────────────│                         │
   │                       │                          │                         │
   │                       │ 检查依赖是否满足           │                         │
   │                       │─────────────────────────>│                         │
   │                       │<─────────────────────────│                         │
   │                       │                          │                         │
   │                       │ 执行指标抽取              │                         │
   │                       │─────────────────────────>│ (写入指标数据)           │
   │                       │<─────────────────────────│                         │
   │                       │                          │                         │
   │                       │                          │   POST webhook          │
   │                       │                          │────────────────────────>│
   │                       │                          │<────────────────────────│
   │                       │                          │                         │
   │<──────────────────────│                          │                         │
   │   { status: SUCCESS } │                          │                         │
```

---

## 6. 模型状态说明

| state值 | 说明 | 能否被ETL触发 |
|---------|------|---------------|
| 0 | 新建/未发布 | ❌ 否 |
| 1 | 已发布 | ✅ 是 |

---

## 7. 错误处理

| 场景 | 响应 |
|------|------|
| 无模型依赖此表 | `{status: "IGNORED", message: "无模型依赖此表"}` |
| 依赖未全满足 | 模型加入 `waitingModels` 列表 |
| 模型未发布 | 模型加入 `skippedModels` 列表 |
| 并发执行中 | 模型返回 `{status: "SKIPPED", message: "任务正在执行中"}` |
| 执行失败 | 模型返回 `{status: "FAILED", message: "错误信息"}` |

---

## 8. 实现要点总结

1. **表到达记录**：使用 UPSERT 语义，首次 INSERT，再次 UPDATE `arrival_time`
2. **多依赖检查**：遍历模型所有依赖，全部存在才执行
3. **ETL重跑检测**：比较 `arrival_time` 与 `last_success_end_time`
4. **并发控制**：检查 `RUNNING` 状态任务，防止重复执行
5. **Webhook通知**：异步+指数回退重试，按配置聚合
