# DataOS Metrics Runtime Engine

## 项目概述

DataOS Metrics Runtime Engine 是一个高性能的KPI查询引擎，基于 **Quarkus 3.27 + JDK 21 虚拟线程 + SQLite** 技术栈构建，专为云原生环境优化。

### 核心特性

- 🚀 **极致性能**: Quarkus 3.27 + JDK 21 虚拟线程 + Native编译
- 📈 **高扩展性**: Pod级别水平扩展，线性性能增长
- 💰 **低成本**: 小规格实例，资源利用率高
- 🔧 **易维护**: 清晰的层次架构，便于调试和故障排查
- ⚡ **零阻塞**: 虚拟线程自动挂起/恢复，I/O密集型任务零阻塞

## 技术架构

### 三层存储架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Load Balancer                            │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                 API Gateway (Quarkus)                       │
└─┬─────────────┬─────────────┬─────────────┬─────────────────┘
  │             │             │             │
  ▼             ▼             ▼             ▼
┌───────┐ ┌───────────┐ ┌─────────┐ ┌─────────────────┐
│MetaDB │ │ Compute   │ │ Compute │ │     MinIO       │
│(MySQL)│ │ Pod 1     │ │ Pod N   │ │   (SQLite.db)   │
└───────┘ └───────────┘ └─────────┘ └─────────────────┘
```

- **MetaDB (MySQL)**: 存储指标定义、依赖关系、计算逻辑
- **DataDB (MinIO/S3)**: 存储SQLite.db文件（派生指标）
- **ComputeEngine (SQLite In-Memory)**: 内存计算（计算指标）

### 技术选型

| 技术 | 版本              | 用途 |
|------|-----------------|------|
| **Quarkus** | **3.27.3**      | 应用框架 (LTS稳定版) |
| **JDK** | **21.0.5+ LTS** | 虚拟线程 (正式稳定版) |
| **Gradle** | **8.10+**       | 构建工具 |
| JDBC-PostgreSQL | 42.7+           | MetaDB连接 |
| SQLite JDBC | 3.45+           | 内存数据库 |
| MinIO Java SDK | 8.5.7           | S3对象存储 |
| Micrometer | 1.12+           | 指标监控 |

## 快速开始

### 环境要求

- **JDK 21.0.5+ LTS** (推荐使用 Eclipse Temurin 或 GraalVM)
- **Gradle 8.10+** (或使用 Gradle Wrapper)
- **Docker 20.10+**
- **Kubernetes 1.24+** (可选)

### 开发模式

```bash
# 1. 克隆项目
git clone <repository-url>
cd dataos-metrics-runtime

# 2. 配置PostgreSQL
# 创建数据库: metrics_dev

# 3. 启动开发服务
./gradlew quarkusDev

# 4. 访问应用
open http://localhost:8080/api/kpi/health
```

### 构建Native镜像

```bash
# 构建Native镜像 (带性能优化参数)
./gradlew build -x test
./gradlew buildNative

# 或者使用优化参数直接构建
./gradlew build -Dquarkus.package.type=native \
  -Dquarkus.native.monitoring=heapdump \
  -Dquarkus.native.additional-build-args=-march=native

# 运行Native镜像
./build/*-runner
```

**性能对比**:
- **JVM模式**: 启动~3秒，内存~512MB，QPS~2万
- **Native模式**: 启动~0.1秒，内存~64MB，QPS~5万+

### 构建Docker镜像

```bash
# JVM模式
docker build -f Dockerfile -t dataos-metrics-runtime:1.0.0 .

# Native模式
docker build -f Dockerfile.native -t dataos-metrics-runtime-native:1.0.0 .
```

### 部署到Kubernetes

```bash
# 部署PostgreSQL
kubectl apply -f k8s/postgres/

# 部署Redis
kubectl apply -f k8s/redis/

# 部署MinIO
kubectl apply -f k8s/minio/

# 部署应用
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/hpa.yaml
```

## API文档

### 1. 同步查询KPI数据

```bash
curl -X POST http://localhost:8080/api/kpi/queryKpiData \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["KD10002", "KD10006"],
    "opTimeArray": ["20251101"],
    "dimCodeArray": ["city_id"],
    "dimConditionArray": [],
    "sortOptions": {
      "city_id": "asc",
      "KD10002": "desc"
    }
  }'
```

**响应示例**:
```json
{
  "dataArray": [
    {
      "city_id": "999",
      "opTime": "20251101",
      "kpiValues": {
        "KD10002": {
          "current": "100.0",
          "lastYear": "90.0",
          "lastCycle": "80.0"
        },
        "KD10006": {
          "current": "200.0",
          "lastYear": "180.0",
          "lastCycle": "160.0"
        }
      }
    }
  ],
  "hasNext": false,
  "total": 1
}
```

### 2. 异步查询KPI数据

```bash
curl -X POST http://localhost:8080/api/kpi/queryKpiDataAsync \
  -H "Content-Type: application/json" \
  -d '{
    "kpiArray": ["KD10002"],
    "opTimeArray": ["20251101"],
    "dimCodeArray": ["city_id"]
  }'
```

## 配置说明

### application.properties

```properties
# ===== 核心优化配置 =====

# Virtual Threads (JDK 21 虚拟线程)
quarkus.virtual-threads=true              # 启用虚拟线程
quarkus.thread-pool.core-threads=0        # 完全虚拟化
quarkus.thread-pool.max-threads=1000      # 最大线程数
quarkus.http.idle-timeout=5m              # 长连接优化

# SQLite 内存数据库优化
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.cache_size=400000
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.journal_mode=OFF
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.synchronous=OFF

# PostgreSQL 连接池优化
quarkus.datasource.metadb.jdbc.max-size=100
quarkus.datasource.metadb.jdbc.additional-jdbc-properties.preparedStatementCacheSize=500

# ===== 数据库配置 =====

# 数据库配置
quarkus.datasource.metadb.jdbc.url=jdbc:postgresql://localhost:5432/metrics_dev
quarkus.datasource.metadb.username=metrics_user
quarkus.datasource.metadb.password=metrics_password

# SQLite配置
quarkus.datasource.sqlite.jdbc.url=jdbc:sqlite::memory:
quarkus.datasource.sqlite.jdbc.max-size=20

# MinIO配置
minio.endpoint=http://localhost:9000
minio.access-key=minioadmin
minio.secret-key=minioadmin
minio.bucket.metrics=metrics-data

# 缓存配置
quarkus.cache.redis.expire-after-write=3600

# KPI存储模式
dataos-metrics.runtime.kpi-data-storage-mode=compDimMode
```

### 环境变量

| 变量名 | 默认值 | 描述 |
|--------|--------|------|
| POSTGRES_URL | jdbc:postgresql://postgres:5432/metrics_prod | PostgreSQL连接URL |
| POSTGRES_USERNAME | metrics_user | PostgreSQL用户名 |
| POSTGRES_PASSWORD | metrics_password | PostgreSQL密码 |
| MINIO_ENDPOINT | http://minio:9000 | MinIO端点 |
| MINIO_ACCESS_KEY | minioadmin | MinIO访问键 |
| MINIO_SECRET_KEY | minioadmin | MinIO密钥 |
| MINIO_BUCKET | metrics-data | MinIO桶名 |
| REDIS_PASSWORD | - | Redis密码 |
| KPI_STORAGE_MODE | compDimMode | KPI存储模式 |

## 性能优化

### 1. 虚拟线程

使用JDK 21虚拟线程处理I/O密集型任务：

```java
Executor executor = Executors.newVirtualThreadPerTaskExecutor();

CompletableFuture.supplyAsync(() -> {
    // I/O操作
    return queryDataFromDatabase();
}, executor);
```

**优势**:
- 无队列限制
- 无线程数限制
- 按需创建，用完即销毁
- 内存占用：每个虚拟线程 ~KB级别

### 2. SQLite优化

```properties
# 内存数据库最优配置
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.journal_mode=OFF
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.synchronous=OFF
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.locking_mode=EXCLUSIVE
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.temp_store=memory
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.cache_size=200000
quarkus.datasource.sqlite.jdbc.additional-jdbc-properties.page_size=4096
```

### 3. 缓存策略

- **KPI元数据缓存**: 1800秒
- **SQLite文件缓存**: 3600秒
- **Redis缓存**: 3600秒

## 监控与指标

### 1. 访问指标

```bash
curl http://localhost:8080/q/metrics
```

### 2. 关键指标

- `kpi_query_count`: KPI查询次数
- `kpi_query_time`: KPI查询耗时
- `jvm_memory_used_bytes`: JVM内存使用量
- `process_cpu_usage`: CPU使用率

### 3. 健康检查

- `Liveness Probe`: 检测应用是否存活
- `Readiness Probe`: 检测应用是否就绪

## 故障排查

### 1. 日志查看

```bash
# 查看Pod日志
kubectl logs -f deployment/dataos-metrics-runtime

# 进入Pod
kubectl exec -it <pod-name> -- /bin/bash
```

### 2. 常见问题

#### 问题1: 连接PostgreSQL失败

**解决方案**:
```bash
# 检查PostgreSQL状态
kubectl get pods | grep postgres

# 检查网络连接
kubectl exec -it <pod-name> -- psql -h postgres -U metrics_user -d metrics_dev
```

#### 问题2: MinIO下载失败

**解决方案**:
```bash
# 检查MinIO状态
kubectl get pods | grep minio

# 检查桶是否存在
kubectl exec -it <minio-pod> -- mc ls minio/metrics-data
```

#### 问题3: 内存不足

**解决方案**:
```yaml
# 调整资源限制
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

## 贡献指南

1. Fork 本仓库
2. 创建特性分支: `git checkout -b feature/new-feature`
3. 提交更改: `git commit -am 'Add new feature'`
4. 推送分支: `git push origin feature/new-feature`
5. 提交PR

## 许可证

MIT License

## 联系方式

- 项目地址: [GitHub Repository]
- 邮箱: support@asiainfo.com
- 文档: [Wiki]

---

**Made with ❤️ by DataOS Team**
