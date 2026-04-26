# Data Engineer 考试笔记整理

> 来源：Notion 导出笔记  
> 整理时间：2026-02-09  
> 涵盖考试：Databricks Data Engineer Associate & Professional, Azure DP-203, AWS/GCP Data Engineer

---

## 目录

1. [考试概览](#一考试概览)
2. [核心概念](#二核心概念)
3. [Delta Lake 详解](#三delta-lake-详解)
4. [Unity Catalog](#四unity-catalog)
5. [Structured Streaming](#五structured-streaming)
6. [性能优化](#六性能优化)
7. [CI/CD 与部署](#七cicd-与部署)
8. [Delta Live Tables](#八delta-live-tables)
9. [数据隐私与合规](#九数据隐私与合规)
10. [架构设计](#十架构设计)
11. [备考建议](#十一备考建议)

---

## 一、考试概览

### 1.1 认证对比

| 考试 | 提供商 | 级别 | 难度 | 重点 |
|------|--------|------|------|------|
| **Databricks DE Associate** | Databricks | Associate | ⭐⭐⭐ | 基础概念、PySpark、Delta Lake |
| **Databricks DE Professional** | Databricks | Professional | ⭐⭐⭐⭐⭐ | 场景题、优化、架构设计 |
| Azure DP-203 | Microsoft | Associate | ⭐⭐⭐⭐ | Azure 生态、Synapse、Data Factory |
| AWS Data Engineer Associate | Amazon | Associate | ⭐⭐⭐⭐ | AWS 服务、Glue、Redshift |
| GCP Professional Data Engineer | Google | Professional | ⭐⭐⭐⭐⭐ | GCP 生态、BigQuery、Dataflow |

### 1.2 Databricks 核心优势

Databricks 是基于 **Apache Spark** 的 SaaS 平台，主打 **Lakehouse (湖仓一体)** 架构：

| 特性 | Data Lake | Data Warehouse | Lakehouse |
|------|-----------|----------------|-----------|
| 存储成本 | 低 | 高 | 低 |
| 灵活性 | 高 | 低 | 高 |
| SQL 性能 | 低 | 高 | 高 |
| ML/AI 支持 | 好 | 差 | 好 |
| ACID 事务 | ❌ | ✅ | ✅ |

---

## 二、核心概念

### 2.1 Databricks 核心组件

```
┌─────────────────────────────────────────────────────────┐
│                    Databricks Workspace                  │
├─────────────┬─────────────┬─────────────┬───────────────┤
│  Notebooks  │  Clusters   │  Workflows  │  Unity Catalog │
│  (交互开发)  │  (计算资源)  │  (任务调度)  │  (数据治理)    │
└─────────────┴─────────────┴─────────────┴───────────────┘
```

- **Notebooks**：类似 Jupyter，支持 Python/SQL/Scala
- **Clusters**：计算集群，支持自动扩缩容
- **Workflows**：工作流编排（类似 Airflow）
- **Unity Catalog**：统一数据目录和治理

### 2.2 角色定位

| 角色 | 主要职责 |
|------|----------|
| **Data Engineer** | PySpark ETL、Delta Lake、Workflows、性能优化 |
| **Data Scientist** | Notebook 探索、MLflow、Feature Store |
| **Data Analyst** | SQL 查询、Dashboard、BI 工具 |

---

## 三、Delta Lake 详解

### 3.1 核心特性

```python
# Delta Lake 关键特性
Delta Lake = Parquet + Transaction Log + Schema Enforcement
```

| 特性 | 说明 | 使用场景 |
|------|------|----------|
| **ACID** | 原子性、一致性、隔离性、持久性 | 并发写入、数据一致性 |
| **Time Travel** | 历史版本回溯 | 数据恢复、审计、A/B 测试 |
| **Schema Enforcement** | Schema 校验 | 数据质量控制 |
| **Schema Evolution** | Schema 自动演进 | 字段增删、类型变更 |
| **Z-Ordering** | 多维数据布局 | 点查询优化 |
| **Liquid Clustering** | 自动数据聚类 | 替代 Z-Ordering (新特性) |

### 3.2 重要操作

#### VACUUM (清理)

```sql
-- 清理 7 天前的旧版本文件
VACUUM table_name RETAIN 168 HOURS

-- 清理所有旧版本（危险！）
VACUUM table_name RETAIN 0 HOURS
```

⚠️ **注意**：VACUUM 后无法 Time Travel 到被清理的版本

#### Time Travel (时间旅行)

```sql
-- 查看历史版本
DESCRIBE HISTORY table_name

-- 查询 1 小时前的数据
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01 10:00:00'

-- 查询特定版本
SELECT * FROM table_name VERSION AS OF 5
```

#### Clone (克隆)

| 类型 | 说明 | 使用场景 |
|------|------|----------|
| **Deep Clone** | 完整复制数据和元数据 | 生产备份、环境复制 |
| **Shallow Clone** | 仅复制元数据，引用原数据 | 快速测试、开发环境 |

```sql
-- Deep Clone
CREATE TABLE new_table DEEP CLONE source_table

-- Shallow Clone
CREATE TABLE new_table SHALLOW CLONE source_table
```

### 3.3 CDC (Change Data Capture)

```sql
-- 启用 CDF
ALTER TABLE table_name SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- 查询变更
SELECT * FROM table_changes('table_name', 1, 5)
```

CDF 输出字段：
- `_change_type`: INSERT/UPDATE_PREIMAGE/UPDATE_POSTIMAGE/DELETE
- `_commit_version`: 版本号
- `_commit_timestamp`: 时间戳

---

## 四、Unity Catalog

### 4.1 三层命名空间

```
Metastore
└── Catalog (数据域/项目)
    └── Schema (数据库)
        ├── Table (表)
        ├── View (视图)
        ├── Volume (非结构化数据)
        └── Function (函数)
```

完整路径：`catalog.schema.table`

### 4.2 访问控制 (ACLs)

```sql
-- 授权
GRANT SELECT ON TABLE catalog.schema.table TO group_name

-- 撤销
REVOKE SELECT ON TABLE catalog.schema.table FROM group_name
```

| 权限 | 说明 |
|------|------|
| USE CATALOG | 使用 Catalog |
| USE SCHEMA | 使用 Schema |
| CREATE TABLE | 创建表 |
| SELECT | 查询 |
| MODIFY | 修改 |
| EXECUTE | 执行函数 |

### 4.3 动态视图 (Dynamic Views)

```sql
-- 行级安全 + 列掩码
CREATE VIEW secure_view AS
SELECT 
    id,
    CASE WHEN current_user() IN ('admin') THEN email ELSE '***' END as email,
    amount
FROM transactions
WHERE region = current_region()
```

### 4.4 System Tables (系统表)

| 表 | 用途 |
|----|------|
| `system.information_schema` | 元数据查询 |
| `system.billing.usage` | 计费日志 |
| `system.access.audit` | 审计日志 (Preview) |
| `system.lineage` | 数据血缘 (Preview) |

---

## 五、Structured Streaming

### 5.1 核心概念

```
┌────────────────────────────────────────────────────────────┐
│                    Spark Structured Streaming               │
├─────────────┬─────────────┬─────────────┬──────────────────┤
│   Source    │  Transform  │    Sink     │   Checkpoint     │
│  (数据源)    │   (转换)     │   (输出)    │    (检查点)       │
└─────────────┴─────────────┴─────────────┴──────────────────┘
```

**关键特性**：
- Exactly-once 处理语义
- 容错（通过 Checkpoint）
- 自动处理迟到数据（Watermark）
- 统一批处理和流处理 API

### 5.2 代码示例

```python
from pyspark.sql.functions import *

# 读取流
stream_df = (spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "host:port")
    .option("subscribe", "topic")
    .load()
)

# 转换
parsed_df = stream_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# 写入
query = (parsed_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/job")
    .trigger(processingTime="1 minute")
    .start("/delta/table")
)
```

### 5.3 Output Modes (输出模式)

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **Append** | 只输出新增行 | 无聚合的流 |
| **Update** | 输出更新的行 | 有聚合/状态 |
| **Complete** | 输出完整结果表 | 聚合结果 |

### 5.4 Trigger 类型

| Trigger | 说明 |
|---------|------|
| `trigger(processingTime="10 seconds")` | 固定间隔微批 |
| `trigger(once=True)` | 一次性处理 |
| `trigger(availableNow=True)` | 处理所有可用数据后停止 |
| `trigger(continuous="1 second")` | 持续处理（低延迟） |

### 5.5 Watermarking (水印)

```python
# 定义水印：允许 10 分钟的迟到数据
watermarked_df = (df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window("event_time", "5 minutes"),
        "user_id"
    )
    .count()
)
```

**水印公式**：
```
Event Time < (Max Event Time - Watermark Threshold)
```

---

## 六、性能优化

### 6.1 查询优化流程

```
Query → Unresolved Logical Plan → Logical Plan 
      → Optimized Logical Plan → Physical Plans 
      → Selected Physical Plan → RDDs
```

### 6.2 Adaptive Query Execution (AQE)

Spark 3.0+ 特性，运行时动态优化：

1. **动态合并 Shuffle 分区**
2. **动态切换 Join 策略**
3. **动态优化倾斜 Join**

```python
# 启用 AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 6.3 Join 优化

| Join 类型 | 适用场景 | 注意事项 |
|-----------|----------|----------|
| **Broadcast Join** | 小表 (<10MB) | 避免 OOM |
| **Shuffle Hash Join** | 中等表 | 内存充足 |
| **Sort Merge Join** | 大表 | 默认选择 |
| **Skew Join** | 数据倾斜 | 使用 AQE 或 Hint |

```python
# Broadcast Join Hint
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")
```

### 6.4 文件布局优化

| 技术 | 说明 | 使用场景 |
|------|------|----------|
| **Partitioning** | 按列分区 | 低基数列、GDPR 删除 |
| **Z-Ordering** | 多维聚类 | 点查询、过滤 |
| **Liquid Clustering** | 自动聚类 | 新表首选 |
| **Data Skipping** | 跳过无关文件 | 自动应用 |

```sql
-- Z-Ordering
OPTIMIZE table_name ZORDER BY (column1, column2)

-- Liquid Clustering (新特性)
CREATE TABLE table_name (...) CLUSTER BY (column1, column2)
```

### 6.5 集群配置建议

| 场景 | 实例类型 | 配置 |
|------|----------|------|
| ETL (Shuffle 重) | Compute Optimized | 1:2 核心内存比 |
| SQL/BI | General Purpose | 1:4 核心内存比 |
| 缓存重 | Memory Optimized | 1:8 核心内存比 |

**Photon 引擎**：
- C++ 向量引擎，加速 SQL 处理
- 自动启用（SQL Warehouse）
- 30-40% 性能提升

---

## 七、CI/CD 与部署

### 7.1 Databricks Asset Bundles (DABs)

**核心文件**：`databricks.yml`

```yaml
bundle:
  name: my_project
  
variables:
  catalog:
    default: dev_catalog
    
resources:
  jobs:
    etl_pipeline:
      name: ETL Pipeline
      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: ./src/bronze.py
      
targets:
  development:
    default: true
    workspace:
      host: https://dev.databricks.com
  production:
    workspace:
      host: https://prod.databricks.com
    variables:
      catalog: prod_catalog
```

### 7.2 CLI 命令

```bash
# 验证配置
databricks bundle validate

# 部署到开发环境
databricks bundle deploy -t development

# 运行 Job
databricks bundle run -t development etl_pipeline

# 销毁资源
databricks bundle destroy
```

### 7.3 GitFlow 工作流

```
main (生产) ←── release/* ←── develop ←── feature/*
  ↑__________________________________________|
  └────────── hotfix/* ──────────────────────┘
```

| 分支 | 用途 |
|------|------|
| `main` | 生产环境 |
| `develop` | 开发集成 |
| `feature/*` | 新功能开发 |
| `release/*` | 发布准备 |
| `hotfix/*` | 紧急修复 |

### 7.4 环境隔离

| 环境 | Catalog | 用途 |
|------|---------|------|
| Dev | `dev_catalog` | 开发测试 |
| Staging | `staging_catalog` | 预发布验证 |
| Prod | `prod_catalog` | 生产运行 |

---

## 八、Delta Live Tables

### 8.1 表类型

| 类型 | 特性 | 适用场景 |
|------|------|----------|
| **Streaming Table** | 增量、Append-only | Bronze 层摄取 |
| **Materialized View** | 自动刷新、聚合 | Silver/Gold 层 |
| **Temporary View** | 不持久化 | 中间转换 |

### 8.2 Pipeline 定义

```sql
-- Bronze 层：原始数据摄取
CREATE OR REFRESH STREAMING TABLE bronze_events
AS SELECT * FROM cloud_files("/raw/events", "json")

-- Silver 层：清洗转换
CREATE OR REFRESH MATERIALIZED VIEW silver_events
AS 
SELECT 
    id,
    timestamp,
    parse_json(payload) as data
FROM STREAM(LIVE.bronze_events)
WHERE id IS NOT NULL

-- Gold 层：业务聚合
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_metrics
AS
SELECT 
    date(timestamp) as day,
    count(*) as event_count
FROM LIVE.silver_events
GROUP BY date(timestamp)
```

### 8.3 Data Quality (Expectations)

```sql
-- 警告模式
CREATE OR REFRESH MATERIALIZED VIEW silver_clean
AS SELECT *
FROM LIVE.bronze_raw
EXPECT (id IS NOT NULL)
EXPECT (amount > 0)

-- 丢弃模式
CREATE OR REFRESH MATERIALIZED VIEW silver_valid
AS SELECT *
FROM LIVE.bronze_raw
EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW

-- 失败模式
CREATE OR REFRESH MATERIALIZED VIEW silver_strict
AS SELECT *
FROM LIVE.bronze_raw
EXPECT (id IS NOT NULL) ON VIOLATION FAIL UPDATE
```

### 8.4 SCD (Slowly Changing Dimensions)

```sql
-- SCD Type 1：直接覆盖
CREATE OR REFRESH MATERIALIZED VIEW customers_scd1
AS SELECT * FROM LIVE.source_customers

-- SCD Type 2：保留历史
CREATE OR REFRESH MATERIALIZED VIEW customers_scd2
AS SELECT * FROM LIVE.source_customers
STORED AS SCD TYPE 2
```

---

## 九、数据隐私与合规

### 9.1 法规概览

| 法规 | 地区 | 关键要求 |
|------|------|----------|
| **GDPR** | 欧盟 | 数据主体权利、同意管理、删除权 |
| **CCPA** | 加州 | 知情权、删除权、选择退出 |
| **HIPAA** | 美国 | 医疗数据保护 |

### 9.2 PII 处理策略

| 技术 | 说明 | 可逆性 |
|------|------|--------|
| **Data Masking** | 部分隐藏 | 不可逆 |
| **Pseudonymization** | 假名化 | 可逆（需映射表） |
| **Anonymization** | 完全匿名 | 不可逆 |
| **Hashing** | 哈希 | 不可逆 |
| **Tokenization** | 令牌化 | 可逆 |

### 9.3 Unity Catalog 隐私功能

```sql
-- 列级掩码
CREATE VIEW masked_users AS
SELECT 
    id,
    CASE 
        WHEN is_member('admin') THEN email 
        ELSE regexp_replace(email, '.+@', '***@') 
    END as email
FROM users

-- 行级安全
CREATE VIEW regional_sales AS
SELECT * FROM sales
WHERE region = current_user_region()
```

### 9.4 GDPR 删除流程

```python
# 1. 识别用户数据
user_data = spark.table("events").filter(col("user_id") == "U123")

# 2. 使用 CDF 追踪影响
df_changes = spark.read \
    .format("delta") \
    .option("readChangeFeed", "true") \
    .table("events")

# 3. 级联删除
tables = ["bronze", "silver", "gold"]
for table in tables:
    spark.sql(f"DELETE FROM {table} WHERE user_id = 'U123'")

# 4. VACUUM 清理
spark.sql("VACUUM events RETAIN 0 HOURS")
```

---

## 十、架构设计

### 10.1 Medallion Architecture (奖牌架构)

```
┌─────────────────────────────────────────────────────────────┐
│                         Gold 层                              │
│              (业务聚合、报表、ML 特征)                          │
│    ┌──────────┐  ┌──────────┐  ┌──────────┐                │
│    │ 日报表    │  │ 用户画像  │  │ ML 特征  │                │
│    └──────────┘  └──────────┘  └──────────┘                │
├─────────────────────────────────────────────────────────────┤
│                        Silver 层                             │
│              (清洗、标准化、去重、关联)                         │
│    ┌──────────┐  ┌──────────┐  ┌──────────┐                │
│    │ 清洗事件  │  │ 用户维度  │  │ 产品维度  │                │
│    └──────────┘  └──────────┘  └──────────┘                │
├─────────────────────────────────────────────────────────────┤
│                        Bronze 层                             │
│              (原始数据、保留原始格式)                           │
│    ┌──────────┐  ┌──────────┐  ┌──────────┐                │
│    │ Kafka    │  │ S3 文件  │  │ API 数据  │                │
│    └──────────┘  └──────────┘  └──────────┘                │
└─────────────────────────────────────────────────────────────┘
```

| 层级 | 特性 | 保留策略 |
|------|------|----------|
| **Bronze** | 原始、未修改、可能重复 | 短期（30天） |
| **Silver** | 清洗、去重、标准化 | 中期（1年） |
| **Gold** | 聚合、业务就绪 | 长期（多年） |

### 10.2 数据摄取模式

#### Simplex vs Multiplex

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **Simplex** | 单一数据源 → 单一目标 | 简单管道 |
| **Multiplex** | 多数据源 → 统一 Bronze 层 | 复杂事件流 |

```python
# Multiplex 摄取示例
bronze_df = (spark
    .readStream
    .format("kafka")
    .option("subscribe", "events.*")  # 订阅多个 topic
    .load()
    .withColumn("event_type", get_json_object(col("value"), "$.type"))
    .withColumn("data", from_json(col("value"), schema))
)
```

### 10.3 工具对比

| 工具 | 类型 | 最佳场景 |
|------|------|----------|
| **Databricks** | Lakehouse | ML + SQL + 流处理 |
| **Snowflake** | Data Warehouse | 纯 SQL、BI |
| **BigQuery** | Data Warehouse | GCP 生态 |
| **Redshift** | Data Warehouse | AWS 生态 |
| **Delta Lake** | 存储格式 | 开放格式、多引擎 |
| **Iceberg** | 存储格式 | 开放格式、Hive 兼容 |

---

## 十一、备考建议

### 11.1 学习路径

```
阶段 1：基础 (2-3周)
├── Databricks Academy - Data Engineer Associate
├── 完成所有 Hands-on Labs
└── 熟悉 PySpark API

阶段 2：进阶 (2-3周)
├── Databricks Academy - Data Engineer Professional
├── 学习性能优化和架构设计
└── 实践 CI/CD 和 DABs

阶段 3：冲刺 (1周)
├── Exam Topic 练习题
├── 官方文档查漏补缺
└── 模拟考试
```

### 11.2 重点考点

**Associate 级别**：
- PySpark DataFrame API
- Delta Lake 基本操作
- Auto Loader 配置
- 基础 SQL

**Professional 级别**：
- 性能优化策略
- 复杂架构设计
- CI/CD 最佳实践
- 安全和合规
- 故障排查

### 11.3 考试技巧

1. **场景题**：先识别问题类型（性能、安全、架构）
2. **排除法**：明显错误的选项先排除
3. **关键词**：注意 "best practice"、"most efficient"、"least cost"
4. **时间分配**：每题约 2-3 分钟，先易后难

### 11.4 推荐资源

| 资源 | 链接 | 用途 |
|------|------|------|
| Databricks Academy | https://www.databricks.com/learn/training | 官方课程 |
| Documentation | https://docs.databricks.com | 查漏补缺 |
| Exam Guide | Databricks 官网 | 考试大纲 |
| Blog | Databricks Tech Blog | 新特性 |

---

## 附录：常用代码片段

### A.1 完整 ETL Pipeline

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("ETL").getOrCreate()

# Bronze: 摄取
bronze_df = (spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/bronze_schema")
    .load("/raw/events/")
    .withColumn("_ingestion_time", current_timestamp())
)

(bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/bronze")
    .trigger(availableNow=True)
    .start("/delta/bronze/events")
)

# Silver: 清洗
silver_df = (spark
    .readStream
    .format("delta")
    .load("/delta/bronze/events")
    .filter(col("user_id").isNotNull())
    .withColumn("event_date", to_date(col("timestamp")))
    .dropDuplicates(["event_id"])
)

(silver_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/checkpoints/silver")
    .start("/delta/silver/events")
)

# Gold: 聚合
spark.sql("""
    CREATE OR REPLACE TABLE gold.daily_metrics
    USING DELTA
    AS
    SELECT 
        event_date,
        event_type,
        count(*) as event_count,
        count(distinct user_id) as unique_users
    FROM delta.`/delta/silver/events`
    GROUP BY event_date, event_type
""")
```

### A.2 性能优化检查清单

```python
# 1. 启用 AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# 2. 设置 Shuffle 分区
spark.conf.set("spark.sql.shuffle.partitions", "200")

# 3. 启用 Delta 优化
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# 4. 使用 Photon（自动）
# SQL Warehouse 自动启用

# 5. 缓存常用表
spark.sql("CACHE TABLE popular_table")
```

---

*祝考试顺利！🎯*
