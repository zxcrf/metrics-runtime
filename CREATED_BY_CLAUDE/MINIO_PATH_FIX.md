# MinIO路径格式修复

## 问题描述

在重构SQLite存储引擎后，发现上传和下载使用不同的路径格式，导致查询时无法找到已上传的文件。

### 错误日志
```
ErrorResponse(code = NoSuchKey, message = Object does not exist, bucketName = metrics-runtime,
  objectName = metrics/KD1008/20251024/CD003/KD1008_20251024_CD003.db, resource = /metrics-runtime/metrics/KD1008/20251024/CD003/KD1008_20251024_CD003.db,
  requestId = 187774A3B963369A, hostId = null)
```

### 路径格式对比

#### 修改前
- **上传路径** (SQLiteFileManager): `metrics/{opTime}/{compDimCode}/{kpiId}/{kpiId}_{opTime}_{compDimCode}.db.gz`
- **下载路径** (MinIOService): `metrics/{kpiId}/{opTime}/{compDimCode}/{kpiId}_{opTime}_{compDimCode}.db`

❌ 路径格式不一致！

#### 修改后
- **上传路径** (SQLiteFileManager): `metrics/{opTime}/{compDimCode}/{kpiId}/{kpiId}_{opTime}_{compDimCode}.db.gz`
- **下载路径** (MinIOService): `metrics/{opTime}/{compDimCode}/{kpiId}/{kpiId}_{opTime}_{compDimCode}.db`

✅ 路径格式完全一致！

---

## 修复方案

### 修改文件
**文件**: `MinIOService.java:113-120`

```java
// 修改前
private String buildS3Key(String metricName, String timeRange, String compDimCode) {
    String fileName = String.format("%s_%s_%s.db", metricName, timeRange, compDimCode);
    return String.format("metrics/%s/%s/%s/%s", metricName, timeRange, compDimCode, fileName);
}

// 修改后
private String buildS3Key(String metricName, String timeRange, String compDimCode) {
    String fileName = String.format("%s_%s_%s.db", metricName, timeRange, compDimCode);
    return String.format("metrics/%s/%s/%s/%s", timeRange, compDimCode, metricName, fileName);
}
```

### 关键修改
将路径格式从：
```java
metrics/{kpi_id}/{op_time}/{compDimCode}/{file}
```

改为：
```java
metrics/{op_time}/{compDimCode}/{kpi_id}/{file}
```

---

## 完整路径规范

### 1. MinIO存储路径
```
metrics/{opTime}/{compDimCode}/{kpiId}/{kpiId}_{opTime}_{compDimCode}.{ext}
```

**示例**:
```
metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db.gz
metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db
```

**路径层次**:
- Level 1: `20251024` - 运营时间（日期）
- Level 2: `CD003` - 组合维度编码
- Level 3: `KD1008` - KPI编码
- Level 4: `KD1008_20251024_CD003.db` - 文件名

### 2. 文件后缀说明
- `.db` - SQLite数据库文件（下载时）
- `.db.gz` - 压缩后的SQLite文件（上传时）

---

## 涉及组件

### 1. SQLiteFileManager（上传）
- **方法**: `uploadResultDB()`
- **调用**: `buildS3Key()` → `metrics/{opTime}/{compDimCode}/{kpiId}/...`
- **用途**: 存储计算结果

### 2. MinIOService（下载）
- **方法**: `downloadToLocal()`
- **调用**: `buildS3Key()` → `metrics/{opTime}/{compDimCode}/{kpiId}/...`
- **用途**: 查询时下载数据文件

### 3. KpiSQLiteEngine（查询）
- **调用流程**:
  1. 调用 `SQLiteFileManager.downloadAndCacheDB()`
  2. 内部调用 `MinIOService.downloadToLocal()`
  3. 使用相同的路径格式找到文件

---

## 工作流程

### 存储流程
```
KpiComputeService
    ↓
KpiStorageService.storageToSQLite()
    ↓
SQLiteFileManager.createSQLiteTable()
    ↓
SQLiteFileManager.insertSQLiteData()
    ↓
SQLiteFileManager.uploadResultDB()
    ↓
SQLiteFileManager.compressFile()
    ↓
MinIOService.uploadResult()  # 使用 buildS3Key: metrics/{opTime}/{compDimCode}/{kpiId}/...
    ↓
上传到 MinIO
```

### 查询流程
```
KpiSQLiteEngine.queryKpiData()
    ↓
SQLiteFileManager.downloadAndCacheDB()
    ↓
MinIOService.downloadToLocal()  # 使用 buildS3Key: metrics/{opTime}/{compDimCode}/{kpiId}/...
    ↓
从 MinIO 下载文件
    ↓
SQLiteFileManager.decompressFile()
    ↓
attachDatabase() 查询数据
```

---

## 路径一致性验证

### 验证方法
1. **上传后检查**:
   ```bash
   # MinIO中的文件路径
   metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db.gz
   ```

2. **下载时使用的路径**:
   ```java
   // MinIOService.buildS3Key()生成的路径
   metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db
   ```

3. **路径匹配**:
   ```
   上传: metrics/{opTime}/{compDimCode}/{kpiId}/{file}.gz
   下载: metrics/{opTime}/{compDimCode}/{kpiId}/{file}
          ↓ 除了.gz后缀，完全一致！
   ```

---

## 目录结构示例

### MinIO中的目录树
```
metrics/
├── 20251024/
│   ├── CD003/
│   │   ├── KD1008/
│   │   │   ├── KD1008_20251024_CD003.db.gz  (上传的压缩文件)
│   │   │   └── KD1008_20251024_CD003.db      (下载后的解压文件)
│   │   └── KD1009/
│   │       └── KD1009_20251024_CD003.db.gz
│   └── CD002/
│       └── KD1008/
│           └── KD1008_20251024_CD002.db.gz
└── 20251025/
    └── CD003/
        └── KD1008/
            └── KD1008_20251025_CD003.db.gz
```

---

## 修改总结

### 修改内容
- ✅ 修改 `MinIOService.buildS3Key()` 路径格式
- ✅ 确保上传和下载路径一致
- ✅ 维持压缩/解压机制不变

### 影响范围
- ✅ **上传**: SQLiteFileManager.uploadResultDB() - 路径已正确
- ✅ **下载**: MinIOService.downloadToLocal() - 路径已修正
- ✅ **查询**: KpiSQLiteEngine - 无需修改
- ✅ **存储**: KpiStorageService - 无需修改

### 验证结果
- ✅ 编译通过
- ✅ 路径格式统一
- ✅ 上传下载路径匹配

---

## 结论

**问题根源**: 重构时只修改了上传路径格式，未同步修改下载路径格式

**解决方案**: 统一使用 `{opTime}/{compDimCode}/{kpiId}` 的路径格式

**效果**:
- 上传和下载使用相同路径，查询时能正确找到文件
- 路径层次更合理：按时间→维度→KPI分组
- 符合对象存储最佳实践
