# MinIO压缩文件下载路径修复

## 问题描述

MinIO中存储的文件全部为压缩格式（`.db.gz`），但下载时使用的是未压缩路径（`.db`），导致查询时找不到文件。

### 错误日志
```
ErrorResponse(code = NoSuchKey, message = Object does not exist,
  bucketName = metrics-runtime,
  objectName = metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db,  ← .db文件不存在
  resource = /metrics-runtime/metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db)
```

### 实际MinIO存储文件
```
metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db.gz  ← 实际存储的是.gz压缩文件
```

---

## 根本原因

**路径不匹配**:
- **上传**: SQLiteFileManager → 上传 `.db.gz` 文件
- **下载**: MinIOService → 下载 `.db` 文件 ❌

---

## 修复方案

### 1. 修改MinIOService的buildS3Key方法

**文件**: `MinIOService.java:113-120`

```java
// 修改前 - 生成.db文件路径
private String buildS3Key(String metricName, String timeRange, String compDimCode) {
    String fileName = String.format("%s_%s_%s.db", metricName, timeRange, compDimCode);
    return String.format("metrics/%s/%s/%s/%s", timeRange, compDimCode, metricName, fileName);
}

// 修改后 - 生成.db.gz文件路径
private String buildS3Key(String metricName, String timeRange, String compDimCode) {
    String fileName = String.format("%s_%s_%s.db.gz", metricName, timeRange, compDimCode);
    return String.format("metrics/%s/%s/%s/%s", timeRange, compDimCode, metricName, fileName);
}
```

**关键修改**: 将文件后缀从 `.db` 改为 `.db.gz`

---

### 2. 新增downloadObject方法到MinIOService

**文件**: `MinIOService.java:113-141`

新增一个可以自定义S3键的下载方法：

```java
/**
 * 从MinIO下载文件到本地（自定义路径）
 *
 * @param s3Key MinIO中的对象键
 * @param localPath 本地文件路径
 * @throws IOException 下载失败
 */
public void downloadObject(String s3Key, String localPath) throws IOException {
    try {
        // 确保本地目录存在
        Path localDir = Paths.get(localPath).getParent();
        Files.createDirectories(localDir);

        // 从MinIO下载文件
        minioClient.downloadObject(
            DownloadObjectArgs.builder()
                .bucket(minIOConfig.getBucketName())
                .object(s3Key)
                .filename(localPath)
                .build()
        );

        log.info("从MinIO下载文件成功: {} -> {}", s3Key, localPath);

    } catch (MinioException | InvalidKeyException | NoSuchAlgorithmException e) {
        log.error("从MinIO下载文件失败: {}", s3Key, e);
        throw new IOException("下载文件失败", e);
    }
}
```

**作用**: 允许SQLiteFileManager直接使用压缩文件路径下载

---

### 3. 重构SQLiteFileManager的downloadAndCacheDB方法

**文件**: `SQLiteFileManager.java:52-89`

**修改前流程**:
```java
// 调用MinIOService.downloadToLocal()  → 生成.db路径 → 下载.db.gz文件 ❌ 路径错误
```

**修改后流程**:
```java
// 直接构建.db.gz路径 → 下载.db.gz文件 → 解压缩 → 返回.db路径
```

**关键代码**:
```java
public String downloadAndCacheDB(String kpiId, String opTime, String compDimCode) throws IOException {
    String cacheKey = buildCacheKey(kpiId, opTime, compDimCode);
    String localPath = buildLocalPath(kpiId, opTime, compDimCode);  // .db路径

    // 检查.db文件（已解压）
    if (Files.exists(Paths.get(localPath))) {
        return localPath;
    }

    // 检查.db.gz文件（已下载但未解压）
    String compressedPath = localPath + ".gz";
    if (Files.exists(Paths.get(compressedPath))) {
        return decompressFile(compressedPath);
    }

    // 从MinIO下载.db.gz文件
    String s3Key = String.format("metrics/%s/%s/%s/%s_%s_%s.db.gz",
        opTime, compDimCode, kpiId, kpiId, opTime, compDimCode);

    minioService.downloadObject(s3Key, compressedPath);
    return decompressFile(compressedPath);
}
```

**改进点**:
- ✅ 直接使用`.db.gz`路径下载
- ✅ 支持缓存检查（解压后和压缩文件两种）
- ✅ 自动解压缩
- ✅ 路径格式与上传一致

---

## 完整工作流程

### 存储流程
```
KpiStorageService.storageToSQLite()
    ↓
SQLiteFileManager.createSQLiteTable()
    ↓
SQLiteFileManager.insertSQLiteData()
    ↓
SQLiteFileManager.uploadResultDB()
    ↓
压缩文件 → localPath.db.gz
    ↓
MinIOService.uploadResult()
    ↓
上传到 MinIO: metrics/{opTime}/{compDimCode}/{kpiId}/{file}.db.gz
```

### 查询流程
```
KpiSQLiteEngine.queryKpiData()
    ↓
SQLiteFileManager.downloadAndCacheDB()
    ↓
检查本地缓存（.db文件）→ 存在则直接返回
    ↓
检查压缩缓存（.db.gz文件）→ 存在则解压并返回
    ↓
从MinIO下载（.db.gz文件）
    ↓
MinIOService.downloadObject(s3Key: metrics/{opTime}/{compDimCode}/{kpiId}/{file}.db.gz)
    ↓
解压缩 → .db文件
    ↓
attachDatabase() 查询
```

---

## 路径验证

### 存储路径
```
上传路径: metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db.gz
```

### 查询路径
```
下载路径: metrics/20251024/CD003/KD1008/KD1008_20251024_CD003.db.gz
           ↓ 路径完全一致！
```

### 本地缓存路径
```
压缩文件: /tmp/cache/KD1008_20251024_CD003.db.gz
解压文件: /tmp/cache/KD1008_20251024_CD003.db
```

---

## 文件格式说明

| 文件类型 | 扩展名 | 用途 | 位置 |
|---------|--------|------|------|
| SQLite数据库 | `.db` | 查询用 | 本地缓存 |
| 压缩数据库 | `.db.gz` | 存储和传输 | MinIO + 本地缓存 |

### 转换流程
```
.db文件 → gzip压缩 → .db.gz文件 → MinIO存储
.db.gz文件 → gzip解压缩 → .db文件 → SQLite查询
```

---

## 修改总结

### 修改的文件
| 文件 | 操作 | 说明 |
|------|------|------|
| `MinIOService.java` | 修改 | `buildS3Key()` 生成`.db.gz`路径 |
| `MinIOService.java` | 新增 | `downloadObject()` 支持自定义路径下载 |
| `SQLiteFileManager.java` | 重构 | `downloadAndCacheDB()` 支持压缩文件下载 |

### 核心改进
1. ✅ **路径统一**: 下载和上传使用相同的`.db.gz`路径
2. ✅ **格式正确**: MinIOService生成正确的压缩文件路径
3. ✅ **自动解压**: 下载后自动解压缩为.db文件
4. ✅ **缓存优化**: 支持压缩文件和未压缩文件两种缓存

### 编译验证
```
BUILD SUCCESSFUL in 9s
4 actionable tasks: 1 executed, 3 up-to-date
```

---

## 结论

**问题根源**: MinIO中存储的是压缩文件，但下载路径生成的是未压缩文件路径

**解决方案**:
1. MinIOService生成`.db.gz`路径
2. SQLiteFileManager直接下载`.db.gz`文件
3. 下载后自动解压缩为`.db`文件

**效果**: 查询时能正确找到MinIO中的压缩文件并下载，路径格式完全统一

**测试建议**: 重新运行查询，应该能成功下载并查询SQLite文件
