# Kill Cards — Quiz 001 三大弱点专题

> 考前速记卡。每个弱点浓缩成决策规则，不需要理解原理，只需要在考场上秒判。

---

## 弱点 1: Unity Catalog 权限 — 最小权限 + 精确语法

### 决策规则

```
需要创建表？ → GRANT CREATE TABLE ON SCHEMA
需要读数据？ → GRANT SELECT ON SCHEMA（自动继承到所有子表）
需要管内容？ → USE CATALOG + USE SCHEMA（遍历权限，不能 DROP）
需要全部权限？→ 几乎永远不选 ALL PRIVILEGES（违反 least privilege）
需要改所有者？→ 不选 OWNER（能 DROP catalog）
```

### 语法速记

```sql
-- Tags（唯一正确语法）
ALTER TABLE t SET TAGS ('key' = 'value');

-- 持久化表属性（永久生效，写入 Delta Log）
ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 15 days');

-- 会话级配置（仅当前 notebook/cluster，重启即失效）
spark.conf.set("spark.databricks.delta.xxx", "value")  -- ❌ 不持久化
```

### 陷阱识别

| 看到这个选项 | 反应 |
|---|---|
| `ALL PRIVILEGES` | 几乎永远错，除非题目明确要求"完全控制" |
| `OWNER` | 危险，能 DROP/ALTER PERMISSIONS |
| `SET TAGS FOR table_name` | 虚构语法 ❌ |
| `GRANT SELECT ON ANY TABLE IN SCHEMA` | 可能对但不如 `SELECT ON SCHEMA`（继承更优雅） |
| `spark.conf.set` 设置保留策略 | 不持久化 ❌，要用 TBLPROPERTIES |

---

## 弱点 2: DLT/Lakeflow 对象选型

### 三种对象的决策矩阵

| 场景 | 用什么 | 为什么 |
|---|---|---|
| 原始数据摄入（Bronze） | **Streaming Table** | 追加写入，保留完整历史 |
| 最新状态/去重（Silver） | **Materialized View** | 自动计算"当前状态"，不是追加 |
| 聚合/报表（Gold） | **Materialized View** | 自动重算聚合结果 |
| 临时转换、不存数据 | **Live View** | 虚拟视图，不占存储 |
| CDC / SCD Type 1&2 | **APPLY CHANGES INTO** + Streaming Table | 专用 CDC 处理 |

### 去重方式决策

```
在 DLT 声明式管道内？
  ├─ 流去重（按 key）      → dropDuplicates("key") + watermark
  ├─ CDC 冲突解决          → APPLY CHANGES INTO + SEQUENCE BY timestamp
  └─ ❌ 不用 MERGE INTO foreachBatch（那是命令式，绕过 DLT 声明层）

在普通 Structured Streaming？
  ├─ 需要 Upsert/MERGE    → foreachBatch(merge_function)
  └─ 简单去重              → dropDuplicates + watermark
```

### 关键区分

- **Streaming Table 只追加**：如果题目要"最新状态"、"latest per key"，ST 不对
- **Materialized View 自动重算**：适合聚合和状态快照
- **SEQUENCE BY 不是可选的**：APPLY CHANGES INTO 必须有 SEQUENCE BY 来解决时序冲突
- **ignoreDuplicates 不存在**于 `dlt.read_stream()`

---

## 弱点 3: API/语法真假辨别

### 虚构 API 速查（考试常见陷阱）

| 虚构的（❌） | 真实的（✅） |
|---|---|
| `df.writeStream.option("updateMode", "merge")` | `df.writeStream.foreachBatch(fn)` |
| `ignoreDuplicates=true` in dlt.read_stream | `dropDuplicates("key")` + watermark |
| `SET TAGS FOR table_name AS (...)` | `ALTER TABLE t SET TAGS (...)` |
| `OPTIMIZE t PARTITION BY (col)` | `OPTIMIZE t WHERE date = ...` or `OPTIMIZE t ZORDER BY (col)` |
| `dbutils.notebooks.getParam()` | `dbutils.widgets.get()` |
| `cloudFiles.allowDuplicates=false` | Auto Loader 默认就去重，不需要此选项 |

### SQL 函数偏好

| 功能 | 考试偏好 | 也能用但不选 |
|---|---|---|
| 提取日期 | `DATE(timestamp)` | `TO_DATE()`, `CAST(... AS DATE)` |
| 日期截断 | `DATE_TRUNC('day', ts)` | 手动 CAST |
| 聚合 | `SUM(quantity)` | `COUNT(quantity)` 算的是行数不是总量 |

### Spark 性能层级（必背）

```
Native Column Expressions  ← 最快，JVM 内执行，Catalyst 全优化
    (F.col("x") - 32) * 5/9

Pandas UDF (@pandas_udf)   ← 中等，Arrow 批量传输，仍有 Python 开销
    
Python UDF (spark.udf)     ← 最慢，逐行序列化到 Python 进程

rdd.map()                  ← 最差，丢失 DataFrame 优化
```

**规则：能用 Native 就永远不用 UDF。只有无法用内置函数表达的复杂逻辑才用 Pandas UDF。**

### 其他速记

- **Spark UI SQL/DataFrame tab** = Query Plan DAG 可视化（不是 Query Profiler）
- **Lakehouse Federation** = `CREATE CONNECTION` → `CREATE FOREIGN CATALOG`（不是 Partner Connect）
- **Compute Policy** = 现代方案管理 Spark conf + env vars + libs（替代 DBFS init scripts）
- **VACUUM** 清理物理文件；`deletedFileRetentionDuration` 是安全窗口；两者值冲突会报错

---

## 考场快速检查清单

遇到不确定的题，按顺序检查：

1. **这个 API/语法真的存在吗？** 如果你没在文档里见过，大概率是虚构的
2. **是不是给了过大的权限？** 选最小权限的那个选项
3. **DLT 内还是外？** DLT 内用声明式（dropDuplicates, MV），不用命令式（foreachBatch, MERGE）
4. **持久化还是临时？** 需要永久生效的用 TBLPROPERTIES，不用 spark.conf
5. **简单运算还是复杂逻辑？** 简单运算永远用 Native Column Expression
