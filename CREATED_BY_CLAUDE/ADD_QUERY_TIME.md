# 添加查询耗时到返回消息

## 修改说明

在KpiSQLiteEngine的查询返回中增加耗时信息，让用户能看到查询耗时。

## 修改位置

**文件**: `KpiSQLiteEngine.java:126-134`

### 修改前
```java
// 5. 转换为标准格式
KpiQueryResult result = convertToStandardFormat(allKpiData, request);

long elapsedTime = System.currentTimeMillis() - startTime;
log.info("SQLite查询完成: 耗时 {} ms, 返回 {} 条记录", elapsedTime, result.dataArray().size());

return result;  // 返回结果不包含耗时信息
```

### 修改后
```java
// 5. 转换为标准格式
KpiQueryResult result = convertToStandardFormat(allKpiData, request);

long elapsedTime = System.currentTimeMillis() - startTime;
log.info("SQLite查询完成: 耗时 {} ms, 返回 {} 条记录", elapsedTime, result.dataArray().size());

// 在msg中添加查询耗时
String msgWithTime = String.format("查询成功！耗时 %d ms，共 %d 条记录", elapsedTime, result.dataArray().size());
return KpiQueryResult.success(result.dataArray(), msgWithTime);
```

## 效果对比

### 修改前
```json
{
  "dataArray": [...],
  "status": "0000",
  "msg": "查询成功！"  // ❌ 没有耗时信息
}
```

### 修改后
```json
{
  "dataArray": [...],
  "status": "0000",
  "msg": "查询成功！耗时 1250 ms，共 15 条记录"  // ✅ 包含耗时和记录数
}
```

## 统一性

- ✅ **KpiRdbEngine**: 已有耗时信息
- ✅ **KpiSQLiteEngine**: 现在添加耗时信息

两个引擎的返回格式保持一致！

## 编译状态

```
BUILD SUCCESSFUL in 9s
```

---

**总结**: 现在SQLite查询引擎的返回消息包含了查询耗时和记录数，与RDB引擎保持一致。
