# 深度性能分析报告

## 问题

单指标简单查询的性能为什么只有 ~180 RPS？还没涉及多指标、虚拟指标、复杂聚合。

## 详细性能分解（基于实测数据）

从详细日志中抽取典型样本：

```
[PERF] Total=110ms (Parse=0ms, Prepare=7ms, SQLGen=15ms, Query=88ms) | Tables=1 | Rows=100
[SQLite] Conn=52ms, Attach=8ms, QueryExec=25ms, TotalBeforeDetach=85ms, Rows=100

[PERF] Total=190ms (Parse=0ms, Prepare=8ms, SQLGen=14ms, Query=168ms) | Tables=1 | Rows=100
[SQLite] Conn=87ms, Attach=2ms, QueryExec=77ms, TotalBeforeDetach=166ms, Rows=100

[PERF] Total=235ms (Parse=0ms, Prepare=22ms, SQLGen=0ms, Query=213ms) | Tables=1 | Rows=100
[SQLite] Conn=166ms, Attach=0ms, QueryExec=47ms, TotalBeforeDetach=213ms, Rows=100
```

### 时间分布分析

#### UnifiedMetricEngine 层面（Total 平均 ~150-200ms）

| 阶段 | 耗时 | 占比 | 说明 |
|------|------|------|------|
| **Parse** | 0-5ms | <5% | 解析指标依赖，几乎可忽略 |
| **Prepare** | 7-150ms | 10-60% | 下载/准备物理表（高度变化） |
| **SQLGen** | 0-17ms | 5-10% | 生成 SQL |
| **Query** | 88-213ms | 40-90% | **主要瓶颈** |

#### SQLiteExecutor 层面（Query 阶段详细）

| 子阶段 | 耗时 | 占比 | 说明 |
|--------|------|------|------|
| **Conn** | 0-195ms | 0-90% | 从连接池获取连接（**高度变化**） |
| **Attach** | 0-17ms | 0-10% | 附加数据库 |
| **QueryExec** | 25-120ms | 10-60% | 实际 SQL 执行 + 结果映射 |
| **DETACH** | ? | ? | 未明确测量（在 Query 之后） |

### 关键发现

#### 1. **连接池等待是最大瓶颈**

```
Conn=0ms   -> 连接立即可用
Conn=52ms  -> 轻微等待
Conn=166ms -> 严重等待
Conn=195ms -> 极严重等待
```

**问题分析**：
- 连接池大小：200
- 并发请求：100
- 理论上应该够用，但实际上连接被**长时间占用**
- 每个连接的完整生命周期：Conn + Attach + QueryExec + DETACH + ResultMapping
- 如果单个请求占用连接 200ms，那么 200 个连接只能支持 1000 RPS
- 但我们的实际 RPS 只有 180，说明还有其他开销

#### 2. **QueryExec 本身并不慢（25-120ms）**

SQLite 查询 100 行数据只需要 25-120ms，这个性能是合理的。

问题不在于 SQLite 查询慢，而在于：
- **连接获取慢**（Conn=0-195ms）
- **总时间长**（Total=110-235ms）

#### 3. **Prepare 阶段高度不稳定（7-150ms）**

`Prepare` 阶段是并行下载物理表，时间跨度从 7ms 到 150ms：
- 7ms：文件已缓存，直接返回
- 150ms：文件可能正在被其他线程下载（锁等待）+ 从 MinIO 下载

#### 4. **DETACH 开销未知但可疑**

从日志看：
```
TotalBeforeDetach=85ms, Total=110ms  -> DETACH+其他=25ms
TotalBeforeDetach=166ms, Query=168ms -> DETACH+其他=2ms
TotalBeforeDetach=213ms, Query=213ms -> DETACH+其他=0ms
```

DETACH 时间不稳定，可能在 0-25ms 之间。

## 根本原因分析

### 架构层面的问题

当前架构的核心问题：**每个请求都需要完整的 Conn → Attach → Query → DETACH → Release 流程**

```
请求 A: |---Conn---|--Attach--|---Query---|--DETACH--|
请求 B:           等待        |---Conn---|--Attach--|---Query---|
请求 C:                      等待                   |---Conn---|
```

#### 计算理论吞吐量

假设：
- 连接池大小：200
- 单个请求平均占用连接时间：150ms（Conn excluded）
- Conn 平均等待：50ms

**理论最大 RPS** = 200 / 0.15 = **1333 RPS**

但实际只有 **180 RPS**，只达到理论值的 **13.5%**！

### 为什么差距这么大？

1. **连接池没有被充分利用**
   - 虽然池大小是 200，但因为请求时间长，大部分连接都在忙碌
   - 100 并发请求 × 200ms 平均时间 = 需要 20 个连接
   - 但由于时间分布不均（0-235ms），峰值可能需要更多连接

2. **等待时间占比过高**
   - Conn 等待占了 0-195ms
   - Prepare 等待（锁）占了 0-150ms
   - 真正的查询执行只占 25-120ms

3. **虚拟线程调度开销**
   - Quarkus 使用虚拟线程，100 个并发可能映射到少数物理线程
   - 上下文切换、调度开销被低估

## 优化建议

### 短期优化（可立即实施）

#### 1. 连接预热 + 保持活跃

```java
@Startup
public void warmupConnections() {
    for (int i = 0; i < 200; i++) {
        CompletableFuture.runAsync(() -> {
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1");
            }
        });
    }
}
```

#### 2. 减少 DETACH 次数

**思路**：为热门数据库保持附加状态，使用连接标记

```java
// 为最近 3 天的数据保持专用连接池
Map<String, Connection> hotConnections = new ConcurrentHashMap<>();
```

#### 3. 批量查询合并

**思路**：如果客户端需要多个时间点，一次性查询所有

```sql
-- 当前：每个时间点一次查询
SELECT * FROM kpi_KD9999_20251116_CD002 WHERE ...

-- 优化：一次查询多个时间点
SELECT * FROM kpi_KD9999_20251116_CD002 WHERE ...
UNION ALL
SELECT * FROM kpi_KD9999_20251117_CD002 WHERE ...
```

### 中期优化（需要架构调整）

#### 1. 连接亲和性（Connection Affinity）

**思路**：为每个数据库文件维护专用连接池

```java
class DatabaseConnectionPool {
    private final Map<String, BlockingQueue<Connection>> poolByDatabase;
    
    Connection getConnection(String dbPath) {
        return poolByDatabase
            .computeIfAbsent(dbPath, k -> createPoolFor(k))
            .take();
    }
}
```

**优势**：
- 数据库保持附加状态，无需 ATTACH/DETACH
- 连接利用率更高
- 延迟更低（消除 Attach 开销）

#### 2. 只读模式 + MMAP

```java
String url = "jdbc:sqlite:file:" + path + "?mode=ro&mmap_size=268435456";
```

**优势**：
- 只读模式消除锁开销
- MMAP 利用 OS 页缓存

#### 3. 虚拟线程 → 平台线程（对于热路径）

**思路**：为 SQLite 查询使用专用的平台线程池

```java
ExecutorService sqliteExecutor = Executors.newFixedThreadPool(
    Runtime.getRuntime().availableProcessors() * 2
);
```

### 长期优化（需要业务配合）

#### 1. 数据预聚合

将 100 行明细数据预聚合为少量汇总数据，减少传输和序列化开销。

#### 2. 列式存储 + 压缩

SQLite 的行式存储不适合大规模分析查询，考虑：
- DuckDB（列式，分析型）
- Parquet + Arrow（列式，零拷贝）

#### 3. 缓存预热

在低峰期预先查询热门数据，填充 Redis 缓存。

## 结论

**为什么性能这么低？**

1. **连接池等待占用了 0-195ms**（最大瓶颈）
2. **ATTACH/DETACH 开销累积**（每次请求都要做）
3. **架构设计不适合高并发**（per-request ATTACH 模式）

**对比：为什么简单查询不应该这么慢？**

如果是传统的 MySQL/PostgreSQL：
- 连接池：预连接，立即可用（Conn=0ms）
- 无需 ATTACH：表已在数据库中（Attach=0ms）
- 查询执行：25-50ms
- **总耗时**：30-60ms，理论 RPS = 3000-6000

当前架构因为 SQLite 的 **动态 ATTACH 模式**，引入了额外的 100-200ms 开销，这是性能低的根本原因。

**优先级建议**：
1. 🔥 **立即实施**：连接预热 + 批量查询合并
2. ⚡ **短期实施**：连接亲和性 + MMAP 模式
3. 🎯 **长期考虑**：业务层预聚合 + 缓存预热
