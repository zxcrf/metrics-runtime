# SQLite文件路径格式修复总结

## 问题描述
SQLite文件的路径格式不符合BUSI_README.md规范，需要修复以支持组合维度编码(compDimCode)。

## 修复内容

### 1. SQLiteFileManager.java (3处修改)

#### 方法1: downloadAndCacheDB()
```java
// 修改前
public String downloadAndCacheDB(String kpiId, String opTime, String compDimCode)

// 方法参数已正确更新
```

#### 方法2: buildCacheKey()
```java
// 修改前: buildCacheKey(String metricName, String timeRange)
// 修改后:
private String buildCacheKey(String kpiId, String opTime, String compDimCode)
    return kpiId + "_" + opTime + "_" + compDimCode;
```

#### 方法3: buildS3Key()
```java
// 修改前: buildS3Key(String metricName, String timeRange)
    return String.format("metrics/%s/%s.db.gz", metricName, timeRange);

// 修改后:
private String buildS3Key(String kpiId, String opTime, String compDimCode)
    String fileName = String.format("%s_%s_%s.db.gz", kpiId, opTime, compDimCode);
    return String.format("metrics/%s/%s/%s/%s", kpiId, opTime, compDimCode, fileName);
```

#### 方法4: buildLocalPath()
```java
// 修改前: buildLocalPath(String metricName, String timeRange)
    return String.format("/tmp/cache/%s_%s.db", metricName, timeRange);

// 修改后:
private String buildLocalPath(String kpiId, String opTime, String compDimCode)
    return String.format("/tmp/cache/%s_%s_%s.db", kpiId, opTime, compDimCode);
```

#### 方法5: uploadResultDB()
```java
// 修改前: uploadResultDB(String localPath, String metricName, String timeRange)
// 修改后:
public void uploadResultDB(String localPath, String metricName, String timeRange, String compDimCode)
```

### 2. KpiSQLiteEngine.java (4处修改)

#### downloadSqliteFile()方法
```java
// 修改前:
private String downloadSqliteFile(String kpiId, KpiQueryRequest request) {
    String timeRange = request.opTimeArray().getFirst();
    return sqliteFileManager.downloadAndCacheDB(kpiId, timeRange);
}

// 修改后:
private String downloadSqliteFile(String kpiId, KpiQueryRequest request, KpiDefinition def) {
    String timeRange = request.opTimeArray().getFirst();
    String compDimCode = def.compDimCode();
    return sqliteFileManager.downloadAndCacheDB(kpiId, timeRange, compDimCode);
}
```

#### 调用点更新 (3处)
在以下方法中更新了downloadSqliteFile()的调用:
- querySimpleKpi() - Line 159
- queryExtendedKpi() - Line 182
- computeComputedKpi() - Line 206

### 3. MinIOService.java (3处修改)

#### 方法1: downloadToLocal()
```java
// 修改前:
public String downloadToLocal(String metricName, String timeRange)

// 修改后:
public String downloadToLocal(String metricName, String timeRange, String compDimCode)
```

#### 方法2: buildS3Key()
```java
// 修改前:
private String buildS3Key(String metricName, String timeRange)
    return String.format("metrics/%s/%s.db", metricName, timeRange);

// 修改后:
private String buildS3Key(String metricName, String timeRange, String compDimCode)
    String fileName = String.format("%s_%s_%s.db", metricName, timeRange, compDimCode);
    return String.format("metrics/%s/%s/%s/%s", metricName, timeRange, compDimCode, fileName);
```

#### 方法3: buildLocalPath()
```java
// 修改前:
private String buildLocalPath(String metricName, String timeRange)
    return String.format("/tmp/cache/%s_%s.db", metricName, timeRange);

// 修改后:
private String buildLocalPath(String metricName, String timeRange, String compDimCode)
    return String.format("/tmp/cache/%s_%s_%s.db", metricName, timeRange, compDimCode);
```

#### 方法4: fileExists()
```java
// 修改前: fileExists(String metricName, String timeRange)
// 修改后: fileExists(String metricName, String timeRange, String compDimCode)
```

## 路径格式对比

### 修改前
```
S3路径: metrics/{kpi_id}/{timeRange}.db
文件: {kpi_id}_{timeRange}.db
本地: /tmp/cache/{kpi_id}_{timeRange}.db
```

### 修改后
```
S3路径: metrics/{kpi_id}/{op_time}/{compDimCode}/{kpi_id}_{op_time}_{compDimCode}.db
文件: {kpi_id}_{op_time}_{compDimCode}.db
本地: /tmp/cache/{kpi_id}_{op_time}_{compDimCode}.db
```

### 示例
```
KPI ID: KD1002
操作时间: 20251101
组合维度编码: CD003

修改前路径: metrics/KD1002/20251101.db
修改后路径: metrics/KD1002/20251101/CD003/KD1002_20251101_CD003.db

修改前文件: KD1002_20251101.db
修改后文件: KD1002_20251101_CD003.db
```

## 编译结果
✅ 编译成功 - BUILD SUCCESSFUL

## 修改的文件列表
1. src/main/java/com/asiainfo/metrics/service/SQLiteFileManager.java
2. src/main/java/com/asiainfo/metrics/service/KpiSQLiteEngine.java
3. src/main/java/com/asiainfo/metrics/service/MinIOService.java

## 验证要点
1. ✅ SQLite文件路径格式符合BUSI_README.md规范
2. ✅ 包含组合维度编码(compDimCode)
3. ✅ 路径层级: metrics/{kpi_id}/{op_time}/{compDimCode}/
4. ✅ 文件命名: {kpi_id}_{op_time}_{compDimCode}.db
5. ✅ 编译通过，无语法错误

## 注意事项
- compDimCode来自KpiDefinition.compDimCode()字段
- 所有相关方法都已更新以支持新的3参数格式
- 路径格式完全符合BUSI_README.md第43-47行规范
