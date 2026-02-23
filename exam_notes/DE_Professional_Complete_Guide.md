# Databricks Certified Data Engineer Professional — 完整复习指南

> 基于 2025 年 9 月更新的考试大纲 (Exam Guide v2025-03-01)
> 整理时间：2026-02-09
> 补充来源：官方文档、社区经验、原始学习笔记

---

## 考试基本信息

| 项目 | 详情 |
|------|------|
| 题数 | 60 道选择题（含少量不计分的测试题） |
| 时长 | 120 分钟 |
| 通过分 | 官方标注 70%，但社区报告 **实际可能需要 80%** |
| 费用 | $200 USD |
| 语言 | 代码以 Python 和 SQL 为主 |
| 有效期 | 2 年，需重新认证 |
| 建议经验 | 1+ 年 Databricks 实战经验 |

### 考试域权重

| # | 考试域 | 权重 | 约题数 |
|---|--------|------|--------|
| 1 | Databricks Tooling（工具与平台） | 20% | 12 |
| 2 | Data Processing（数据处理） | 30% | 18 |
| 3 | Data Modeling（数据建模） | 20% | 12 |
| 4 | Security and Governance（安全与治理） | 10% | 6 |
| 5 | Monitoring and Logging（监控与日志） | 10% | 6 |
| 6 | Testing and Deployment（测试与部署） | 10% | 6 |

---

# 域 1：Databricks Tooling（20%）

## 1.1 Python 项目结构与 DABs

### 模块化项目结构

```
my_project/
├── databricks.yml              # DAB 核心配置
├── resources/
│   ├── jobs/
│   │   └── etl_pipeline.yml    # Job 定义
│   └── pipelines/
│       └── dlt_pipeline.yml    # DLT Pipeline 定义
├── src/
│   ├── __init__.py
│   ├── bronze/
│   │   ├── __init__.py
│   │   └── ingest.py
│   ├── silver/
│   │   ├── __init__.py
│   │   └── transform.py
│   ├── gold/
│   │   ├── __init__.py
│   │   └── aggregate.py
│   └── common/
│       ├── __init__.py
│       ├── schemas.py          # Schema 定义
│       └── utils.py            # 工具函数
├── tests/
│   ├── unit/
│   │   └── test_transform.py
│   └── integration/
│       └── test_pipeline.py
├── setup.py                    # 打包配置
└── requirements.txt
```

### DABs 核心配置 (databricks.yml)

```yaml
bundle:
  name: my_etl_project

variables:
  catalog:
    default: dev_catalog
  schema:
    default: default

# 多目标环境
targets:
  development:
    default: true
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      catalog: dev_catalog
  staging:
    workspace:
      host: https://staging.cloud.databricks.com
    variables:
      catalog: staging_catalog
  production:
    workspace:
      host: https://prod.cloud.databricks.com
    variables:
      catalog: prod_catalog
    run_as:
      service_principal_name: "etl-prod-sp"

resources:
  jobs:
    etl_pipeline:
      name: "ETL Pipeline - ${bundle.target}"
      schedule:
        quartz_cron_expression: "0 0 8 * * ?"
        timezone_id: "UTC"
      tasks:
        - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./src/bronze/ingest.py
            base_parameters:
              catalog: ${var.catalog}
        - task_key: silver_transform
          depends_on:
            - task_key: bronze_ingest
          notebook_task:
            notebook_path: ./src/silver/transform.py
        - task_key: gold_aggregate
          depends_on:
            - task_key: silver_transform
          notebook_task:
            notebook_path: ./src/gold/aggregate.py
```

### DABs CLI 命令

```bash
# 初始化项目
databricks bundle init

# 验证配置
databricks bundle validate

# 部署（不运行）
databricks bundle deploy -t development

# 部署并运行
databricks bundle run -t development etl_pipeline

# 销毁已部署资源
databricks bundle destroy -t development
```

**考点**：DABs 如何实现环境隔离？→ 通过 `targets` 配置不同 workspace + variables

---

## 1.2 第三方库管理

### 安装方式对比

| 方式 | 适用场景 | 配置位置 |
|------|----------|----------|
| **PyPI** (`%pip install`) | 通用 Python 库 | Notebook 顶部 |
| **Cluster Library** | 集群级别共享库 | Cluster Config → Libraries |
| **Init Script** | 需要系统级安装的库 | Cluster Config → Init Scripts |
| **Wheel 文件** | 自定义包、离线安装 | DBFS/Volume + pip install |
| **requirements.txt** | 批量安装 | `%pip install -r /path/requirements.txt` |

### 常见代码

```python
# Notebook 内安装（推荐方式）
%pip install pandas==2.1.0 scikit-learn==1.3.0

# 安装自定义 wheel
%pip install /Volumes/catalog/schema/volume/my_package-1.0-py3-none-any.whl

# 使用 requirements.txt
%pip install -r /Workspace/Repos/project/requirements.txt

# 安装后重启 Python 环境
dbutils.library.restartPython()
```

### 常见问题与排查

| 问题 | 原因 | 解决 |
|------|------|------|
| `ModuleNotFoundError` | 库未安装或安装在错误 scope | 用 `%pip` 在 notebook 内安装 |
| 版本冲突 | 集群预装库版本冲突 | 使用 `%pip install pkg==version` 指定版本 |
| Init Script 失败 | 脚本权限或路径错误 | 检查 DBFS 路径和脚本内容 |
| DLT 中装库 | DLT 不支持 `%pip` | 在 Pipeline 配置中添加 `configuration: {"pipelines.channel": "CURRENT"}` 和 `libraries` |

**考点**：DLT Pipeline 如何安装第三方库？→ Pipeline Settings → Configuration 中添加 PyPI 依赖

---

## 1.3 UDF（用户定义函数）

### UDF 类型对比

| UDF 类型 | 输入/输出 | 性能 | 使用场景 |
|----------|-----------|------|----------|
| **Python UDF** | 标量 → 标量 | 慢（JVM↔Python 序列化） | 简单转换 |
| **Pandas UDF (Vectorized)** | pd.Series → pd.Series | 快（Arrow 批量序列化） | 批量处理、数学运算 |
| **Pandas UDF (Grouped Map)** | pd.DataFrame → pd.DataFrame | 快 | 分组聚合 |
| **SQL UDF** | SQL 表达式 | 最快（Catalyst 优化） | 简单 SQL 逻辑 |

### 代码示例

```python
# === Python UDF（慢，避免大规模使用）===
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def mask_email(email):
    if email and '@' in email:
        parts = email.split('@')
        return f"***@{parts[1]}"
    return email

df = df.withColumn("masked_email", mask_email(col("email")))

# === Pandas UDF（推荐，性能好）===
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("double")
def celsius_to_fahrenheit(temps: pd.Series) -> pd.Series:
    return temps * 9 / 5 + 32

df = df.withColumn("temp_f", celsius_to_fahrenheit(col("temp_c")))

# === Pandas UDF - Grouped Map ===
from pyspark.sql.functions import pandas_udf, PandasUDFType

@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
def normalize_group(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["normalized"] = (pdf["value"] - pdf["value"].mean()) / pdf["value"].std()
    return pdf

result = df.groupby("category").apply(normalize_group)

# === SQL UDF（Unity Catalog 中注册）===
# CREATE OR REPLACE FUNCTION catalog.schema.mask_email(email STRING)
# RETURNS STRING
# RETURN CONCAT('***@', SPLIT(email, '@')[1])
```

**考点**：为什么 Pandas UDF 比 Python UDF 快？→ 使用 Apache Arrow 做批量序列化，减少 JVM↔Python 开销

---

## 1.4 Databricks Compute

### 集群类型

| 集群类型 | 适用场景 | 特点 |
|----------|----------|------|
| **All-Purpose Cluster** | 交互开发、Notebook 探索 | 可共享、手动管理 |
| **Job Cluster** | 自动化 Job 执行 | 按需创建销毁、成本低 |
| **SQL Warehouse** | SQL 分析查询 | Photon 引擎、Serverless 可选 |
| **Serverless Compute** | 快速启动、按需扩缩 | 无需管理、自动优化 |

### Cluster Policy

```json
{
  "spark_version": {
    "type": "regex",
    "pattern": "14\\.[0-9]+\\.x-scala.*",
    "defaultValue": "14.3.x-scala2.12"
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 10,
    "defaultValue": 2
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["i3.xlarge", "i3.2xlarge"]
  }
}
```

**考点**：Job Cluster vs All-Purpose Cluster 的成本区别？→ Job Cluster 按使用付费，比 All-Purpose 便宜约 50%

---

## 1.5 Lakeflow Jobs（原 Workflows）

### 任务编排模式

```
┌─────────────────────────────────────────────────────┐
│                   Job 编排模式                        │
├──────────────┬──────────────┬────────────────────────┤
│   Sequential │   Fan-out    │   Funnel               │
│   A → B → C  │   A → B      │   A ──┐                │
│              │   A → C      │   B ──┼→ D             │
│              │   A → D      │   C ──┘                │
└──────────────┴──────────────┴────────────────────────┘
```

### 任务类型

| 任务类型 | 说明 | 使用场景 |
|----------|------|----------|
| **Notebook Task** | 运行 Notebook | 通用 ETL |
| **Python Script Task** | 运行 .py 文件 | 非 Notebook 代码 |
| **SQL Task** | 运行 SQL 查询 | 数据转换 |
| **DLT Pipeline Task** | 触发 DLT Pipeline | 声明式 ETL |
| **dbt Task** | 运行 dbt 项目 | dbt 集成 |
| **If/Else Task** | 条件分支 | 动态流程控制 |
| **For Each Task** | 循环执行 | 参数化批处理 |
| **Run Job Task** | 触发其他 Job | 跨 Job 编排 |

### Job 参数传递

```python
# Task A: 设置输出参数
dbutils.jobs.taskValues.set(key="row_count", value=12345)
dbutils.jobs.taskValues.set(key="status", value="success")

# Task B: 读取 Task A 的输出
count = dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count")
status = dbutils.jobs.taskValues.get(taskKey="task_a", key="status")
```

### 重试与失败策略

```yaml
tasks:
  - task_key: bronze_ingest
    max_retries: 3
    min_retry_interval_millis: 60000    # 1分钟
    retry_on_timeout: true
    timeout_seconds: 3600               # 1小时
    notification_settings:
      alert_on_last_attempt: true
    email_notifications:
      on_failure:
        - team@company.com
```

**考点**：如何在 Task 之间传递数据？→ `dbutils.jobs.taskValues.set/get`

---

# 域 2：Data Processing（30%）— 最大权重

## 2.1 Delta Lake 核心

### Transaction Log (事务日志)

```
_delta_log/
├── 00000000000000000000.json    # 第 0 个 commit
├── 00000000000000000001.json    # 第 1 个 commit
├── ...
├── 00000000000000000010.checkpoint.parquet  # 每 10 个 commit 一个 checkpoint
└── _last_checkpoint                        # 最新 checkpoint 信息
```

**关键机制**：
- 每次写操作生成一个新的 JSON commit 文件
- 每 10 个 commit 自动生成 checkpoint（Parquet 格式，快速重建状态）
- **Optimistic Concurrency Control (OCC)**：多个写入并发时，后提交者检测冲突，冲突则重试

### Optimistic Concurrency Control

```
Writer A: Read v1 → Compute → Write v2 ✓
Writer B: Read v1 → Compute → Write v2 ✗ (conflict detected)
Writer B: Read v2 → Compute → Write v3 ✓ (retry succeeds)
```

**冲突规则**：
| 操作 A | 操作 B | 冲突？ |
|--------|--------|--------|
| APPEND | APPEND | 不冲突 |
| APPEND | DELETE/UPDATE | 可能冲突（取决于 predicate） |
| DELETE | DELETE | 冲突（同一文件） |
| UPDATE | UPDATE | 冲突（同一文件） |

### VACUUM

```sql
-- 清理 7 天前的旧版本文件（默认保留期）
VACUUM table_name RETAIN 168 HOURS

-- 查看会被清理的文件（不实际删除）
VACUUM table_name RETAIN 168 HOURS DRY RUN
```

⚠️ **重要**：
- 默认保留期 7 天，短于 7 天需设置 `delta.deletedFileRetentionDuration`
- VACUUM 后无法 Time Travel 到被清理的版本
- VACUUM 不删除 `_delta_log` 文件，只删除数据文件

### Time Travel

```sql
-- 查看历史版本
DESCRIBE HISTORY table_name

-- 按时间查询
SELECT * FROM table_name TIMESTAMP AS OF '2024-01-01 10:00:00'

-- 按版本查询
SELECT * FROM table_name VERSION AS OF 5

-- 恢复到历史版本
RESTORE TABLE table_name TO VERSION AS OF 5
```

### Clone

| 类型 | 数据复制 | 元数据复制 | 使用场景 |
|------|----------|------------|----------|
| **Deep Clone** | ✅ 完整复制 | ✅ | 生产备份、环境复制 |
| **Shallow Clone** | ❌ 引用原数据 | ✅ | 快速测试、开发环境 |

```sql
CREATE TABLE backup DEEP CLONE source_table
CREATE TABLE dev_copy SHALLOW CLONE source_table
```

**考点**：Shallow Clone 的数据文件在哪里？→ 引用原表数据文件，不占额外空间；但对 clone 的写入是独立的

---

## 2.2 CDC (Change Data Capture) 与 CDF

### Change Data Feed (CDF)

```sql
-- 启用 CDF
ALTER TABLE events SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

-- 按版本查询变更
SELECT * FROM table_changes('events', 2, 5)

-- 按时间查询变更
SELECT * FROM table_changes('events', '2024-01-01', '2024-01-31')
```

CDF 输出字段：

| 字段 | 说明 |
|------|------|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | 事务版本号 |
| `_commit_timestamp` | 提交时间 |

### MERGE INTO (Upsert)

```sql
MERGE INTO target AS t
USING source AS s
ON t.id = s.id
WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
WHEN MATCHED AND s.op = 'UPDATE' THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

```python
# Python MERGE
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "catalog.schema.target")

delta_table.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll(
    condition="s.op = 'UPDATE'"
).whenMatchedDelete(
    condition="s.op = 'DELETE'"
).whenNotMatchedInsertAll().execute()
```

**考点**：CDF 的 `update_preimage` 和 `update_postimage` 分别代表什么？→ preimage = 更新前的行，postimage = 更新后的行

---

## 2.3 Structured Streaming

### 核心架构

```
Source (数据源) → Processing (转换) → Sink (输出) → Checkpoint (状态)
```

- **Exactly-once** 语义通过 Checkpoint + Write-Ahead Log 保证
- Checkpoint 存储：当前 offset、状态数据、已完成的 batch ID

### Auto Loader（考试重点）

```python
# 基础 Auto Loader
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/raw/events/")
)

# Schema Evolution 配置
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # 自动添加新列
    .option("cloudFiles.schemaHints", "id long, timestamp timestamp")  # Schema 提示
    .load("/raw/events/")
)
```

#### Auto Loader Schema Evolution 模式

| 模式 | 行为 |
|------|------|
| `addNewColumns` | 自动添加新发现的列 |
| `failOnNewColumns` | 发现新列时失败 |
| `rescue` | 新列放入 `_rescued_data` 列 |
| `none` | 忽略新列 |

#### Rescue Data Column

```python
# 无法解析的数据自动放入 _rescued_data
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/schema")
    .option("rescuedDataColumn", "_rescued_data")  # 显式指定
    .load("/raw/")
)
```

**考点**：Auto Loader 使用什么方式发现新文件？→ Directory listing (默认) 或 File notification (基于云事件，更高效)

#### File Notification vs Directory Listing

| 模式 | 机制 | 适用场景 |
|------|------|----------|
| **Directory Listing** | 定期扫描目录 | 小目录、简单场景 |
| **File Notification** | 云事件（S3 SNS/SQS、ADLS Event Grid） | 大目录、生产环境 |

```python
# 使用 File Notification 模式
raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .load("s3://bucket/raw/")
)
```

### Output Modes

| 模式 | 说明 | 适用 |
|------|------|------|
| **Append** | 只输出新增行 | 无聚合/无更新 |
| **Update** | 只输出变化的行 | 有聚合，只看变更 |
| **Complete** | 每次输出完整结果 | 聚合结果全量展示 |

### Trigger 类型

| Trigger | 行为 | 使用场景 |
|---------|------|----------|
| `processingTime="10 seconds"` | 固定间隔微批 | 近实时 |
| `once=True` | 处理一批后停止 | **已废弃** |
| `availableNow=True` | 处理所有可用数据后停止 | **推荐替代 once** |
| `continuous="1 second"` | 持续低延迟 | 超低延迟（实验性） |

**考点**：`Trigger.Once` vs `Trigger.AvailableNow` 的区别？
- `Once`: 只处理一个微批（可能不处理所有积压数据）
- `AvailableNow`: 处理所有积压数据，可能分成多个微批，全部完成后停止（更可靠）

### Watermark（水印）

```python
# 允许 10 分钟的迟到数据
result = (df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window("event_time", "5 minutes"),
        "user_id"
    )
    .count()
)
```

**水印逻辑**：
- 系统维护 `max_event_time`（已见最大事件时间）
- Watermark = `max_event_time - threshold`
- 事件时间 < Watermark 的数据被丢弃
- 只在 Append 和 Update 模式下有效

### Stream-Stream Joins vs Stream-Static Joins

| 类型 | 左流 | 右流 | 要求 |
|------|------|------|------|
| **Stream-Stream** | Streaming | Streaming | 必须有 Watermark + 时间范围约束 |
| **Stream-Static** | Streaming | 批量表 | 无特殊要求 |

```python
# Stream-Stream Join（必须有 watermark 和时间约束）
impressions = spark.readStream.table("impressions").withWatermark("imp_time", "2 hours")
clicks = spark.readStream.table("clicks").withWatermark("click_time", "3 hours")

joined = impressions.join(
    clicks,
    expr("""
        imp_id = click_imp_id AND
        click_time >= imp_time AND
        click_time <= imp_time + interval 1 hour
    """),
    "leftOuter"
)

# Stream-Static Join（静态表不需要 watermark）
stream_df = spark.readStream.table("orders")
static_df = spark.read.table("products")

enriched = stream_df.join(static_df, "product_id", "left")
```

**考点**：Stream-Static join 中静态表什么时候读取？→ 每个微批开始时重新读取静态表的最新快照

### foreachBatch

```python
def process_batch(batch_df, batch_id):
    # 可以在每个微批中执行任意操作
    # 例如：写入多个目标、调用外部 API、执行 MERGE
    batch_df.write.mode("append").saveAsTable("target_1")

    delta_table = DeltaTable.forName(spark, "target_2")
    delta_table.alias("t").merge(
        batch_df.alias("s"), "t.id = s.id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

query = (stream_df
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/checkpoints/multi_sink")
    .start()
)
```

**考点**：为什么要用 foreachBatch？→ (1) 写入多个目标 (2) 在流中执行 MERGE (3) 调用不支持流式写入的 API

---

## 2.4 性能优化

### Adaptive Query Execution (AQE)

Spark 3.0+ 运行时动态优化：

| 功能 | 说明 |
|------|------|
| 动态合并 Shuffle 分区 | 合并过小的分区，减少 Task 数 |
| 动态切换 Join 策略 | 运行时发现小表，自动切换为 Broadcast Join |
| 动态优化 Skew Join | 自动拆分倾斜分区 |

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Databricks 默认启用
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Join 策略选择

| Join 类型 | 条件 | 性能 |
|-----------|------|------|
| **Broadcast Hash Join** | 一侧 < 10MB（可配置） | 最快，无 Shuffle |
| **Shuffle Hash Join** | 一侧可放入内存 | 中等 |
| **Sort Merge Join** | 两侧都很大 | 稳定，默认 |

```python
# 手动 Broadcast
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# 调整 Broadcast 阈值
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20m")  # 改为 20MB
```

### 文件布局优化

| 技术 | 说明 | 何时使用 |
|------|------|----------|
| **Partitioning** | 按列值物理分区 | 低基数列（<1000 个值），且经常按该列过滤 |
| **Z-Ordering** | 多维数据聚类 | 经常按多列过滤的点查询 |
| **Liquid Clustering** | 自动增量聚类 | **新表首选**，替代 Partitioning + Z-Ordering |

#### Partitioning vs Z-Ordering vs Liquid Clustering 场景选择

| 场景 | 推荐 | 原因 |
|------|------|------|
| 按日期过滤大表 | Partitioning by date | 高效分区剪裁 |
| 按多列 (user_id, date) 过滤 | Z-Ordering | 多维聚类优化 |
| 新建表，查询模式不确定 | **Liquid Clustering** | 自动优化，无需手动管理 |
| 已有分区表，查询模式变化 | **Liquid Clustering** | 可以迁移过来 |
| GDPR 删除场景 | Partitioning by user region | 缩小删除范围 |

```sql
-- Partitioning
CREATE TABLE events (...) USING DELTA PARTITIONED BY (event_date)

-- Z-Ordering（手动运行 OPTIMIZE）
OPTIMIZE events ZORDER BY (user_id, event_type)

-- Liquid Clustering（新特性，自动增量）
CREATE TABLE events (...) USING DELTA CLUSTER BY (user_id, event_date)

-- 修改聚类列
ALTER TABLE events CLUSTER BY (user_id, region)
```

**Liquid Clustering 优势**：
- 无需手动运行 OPTIMIZE
- 写入时自动聚类
- 可动态修改聚类列
- 支持高基数列

### Optimize Write vs Auto Compaction

| 功能 | 时机 | 作用 |
|------|------|------|
| **Optimize Write** | 写入时 | 自动合并小文件，减少输出文件数 |
| **Auto Compaction** | 写入后 | 后台合并小文件 |
| **OPTIMIZE 命令** | 手动触发 | 合并小文件 + 可加 ZORDER |

```python
# 启用 Optimize Write（减少写入时的小文件）
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")

# 启用 Auto Compaction（写入后自动合并）
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# 手动运行 OPTIMIZE
spark.sql("OPTIMIZE catalog.schema.table")
```

### Photon 引擎

- C++ 向量化执行引擎
- SQL Warehouse 默认启用
- 对 Scan、Filter、Join、Aggregation 加速明显
- **适合**：SQL 重、扫描重、聚合重的工作负载
- **不适合**：UDF 密集（Photon 不加速 Python UDF）

**考点**：Photon 对哪些操作没有加速效果？→ Python UDF、复杂的自定义函数

---

# 域 3：Data Modeling（20%）

## 3.1 Medallion Architecture（奖牌架构）

```
Raw Sources → Bronze (原始) → Silver (清洗) → Gold (业务)
```

| 层级 | 特性 | 表类型 | 保留策略 |
|------|------|--------|----------|
| **Bronze** | 原始数据、保留原始格式、可能重复 | Streaming Table | 短期（30天） |
| **Silver** | 清洗、去重、标准化、关联 | Materialized View | 中期（1年） |
| **Gold** | 聚合、业务就绪、维度建模 | Materialized View | 长期 |

### Simplex vs Multiplex 摄取

| 模式 | 说明 | 适用场景 |
|------|------|----------|
| **Singleplex** | 一个数据源 → 一个 Bronze 表 | 简单管道 |
| **Multiplex** | 多个数据源 → 一个 Bronze 表（带 source 标识） | 统一事件流 |

```python
# Multiplex: 多 topic 写入一个 Bronze 表
bronze_df = (spark.readStream
    .format("kafka")
    .option("subscribe", "events.*")
    .load()
    .withColumn("topic", col("topic"))
    .withColumn("event_type", get_json_object(col("value").cast("string"), "$.type"))
)

# 从 Multiplex Bronze 拆分到 Silver
silver_orders = spark.readStream.table("bronze_events").filter(col("event_type") == "order")
silver_clicks = spark.readStream.table("bronze_events").filter(col("event_type") == "click")
```

## 3.2 维度建模

### Star Schema vs Snowflake Schema

| 模式 | 结构 | 查询性能 | 存储 |
|------|------|----------|------|
| **Star Schema** | 事实表 + 维度表（非规范化） | 快（少 Join） | 略大 |
| **Snowflake Schema** | 事实表 + 规范化维度表 | 慢（多 Join） | 更小 |

Lakehouse 中**推荐 Star Schema**，因为存储便宜，查询性能更重要。

### SCD (Slowly Changing Dimensions)

| 类型 | 行为 | 历史保留 | 实现方式 |
|------|------|----------|----------|
| **SCD Type 1** | 直接覆盖 | ❌ | MERGE INTO ... WHEN MATCHED UPDATE |
| **SCD Type 2** | 新增行，标记有效期 | ✅ | 添加 `start_date`, `end_date`, `is_current` |

```sql
-- SCD Type 2 实现（在 DLT 中）
CREATE OR REFRESH STREAMING TABLE customers_scd2;

APPLY CHANGES INTO LIVE.customers_scd2
FROM STREAM(LIVE.raw_customers)
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2;
```

**考点**：DLT 中如何实现 SCD Type 2？→ 使用 `APPLY CHANGES INTO ... STORED AS SCD TYPE 2`

---

## 3.3 Lakeflow Declarative Pipelines（原 Delta Live Tables / DLT）

### 表类型

| 类型 | 语法 | 特性 | 适用场景 |
|------|------|------|----------|
| **Streaming Table** | `CREATE STREAMING TABLE` | 增量处理、Append-only | Bronze 层摄取 |
| **Materialized View** | `CREATE MATERIALIZED VIEW` | 全量重算、支持聚合 | Silver/Gold 层 |
| **Temporary View** | `CREATE TEMPORARY LIVE VIEW` | 不持久化 | 中间转换 |

### Pipeline 定义示例

```sql
-- Bronze: Auto Loader 摄取
CREATE OR REFRESH STREAMING TABLE bronze_events
AS SELECT * FROM cloud_files("/raw/events", "json",
    map("cloudFiles.inferColumnTypes", "true"))

-- Silver: 清洗（带数据质量约束）
CREATE OR REFRESH MATERIALIZED VIEW silver_events (
    CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_amount EXPECT (amount > 0) ON VIOLATION DROP ROW
)
AS SELECT
    id,
    timestamp,
    parse_json(payload) as data,
    amount
FROM LIVE.bronze_events
WHERE id IS NOT NULL

-- Gold: 聚合
CREATE OR REFRESH MATERIALIZED VIEW gold_daily_metrics
AS SELECT
    date(timestamp) as day,
    count(*) as event_count,
    sum(amount) as total_amount
FROM LIVE.silver_events
GROUP BY date(timestamp)
```

### Data Quality - Expectations

| 行为 | 语法 | 效果 |
|------|------|------|
| **Warn** (默认) | `EXPECT (condition)` | 记录违规，保留行 |
| **Drop** | `EXPECT ... ON VIOLATION DROP ROW` | 丢弃违规行 |
| **Fail** | `EXPECT ... ON VIOLATION FAIL UPDATE` | 停止 Pipeline |

### APPLY CHANGES (CDC 处理)

```sql
-- 从 CDC 源处理变更
CREATE OR REFRESH STREAMING TABLE target;

APPLY CHANGES INTO LIVE.target
FROM STREAM(LIVE.cdc_source)
KEYS (id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY sequence_num
COLUMNS * EXCEPT (operation, sequence_num)
STORED AS SCD TYPE 1;  -- 或 SCD TYPE 2
```

### Pipeline 设置

| 设置 | 说明 |
|------|------|
| **Triggered** | 手动或定时运行，运行后停止 |
| **Continuous** | 持续运行，实时处理 |
| **Development** | 开发模式，不重试失败 |
| **Production** | 生产模式，自动重试 |

**考点**：DLT Pipeline 的 Development vs Production 模式区别？→ Development 模式不自动重试、重用集群、便于调试；Production 模式自动重试、每次启动新集群

---

# 域 4：Security and Governance（10%）

## 4.1 Unity Catalog

### 三层命名空间

```
Metastore
└── Catalog（数据域/项目）
    └── Schema（数据库）
        ├── Table
        ├── View
        ├── Volume（非结构化数据存储）
        ├── Function
        └── Model（ML 模型）
```

完整路径：`catalog.schema.object`

### 权限模型

| 权限 | 作用对象 | 说明 |
|------|----------|------|
| `USE CATALOG` | Catalog | 允许访问 Catalog |
| `USE SCHEMA` | Schema | 允许访问 Schema |
| `CREATE TABLE` | Schema | 允许创建表 |
| `SELECT` | Table/View | 允许查询 |
| `MODIFY` | Table | 允许写入/更新/删除 |
| `EXECUTE` | Function | 允许执行函数 |
| `ALL PRIVILEGES` | 任何 | 全部权限 |

```sql
-- 授权
GRANT SELECT ON TABLE catalog.schema.table TO `group_name`
GRANT USE CATALOG ON CATALOG my_catalog TO `data_team`
GRANT USE SCHEMA ON SCHEMA my_catalog.my_schema TO `data_team`

-- 撤销
REVOKE SELECT ON TABLE catalog.schema.table FROM `group_name`

-- 查看权限
SHOW GRANTS ON TABLE catalog.schema.table
```

**重要**：访问表需要同时拥有 `USE CATALOG` + `USE SCHEMA` + `SELECT` 权限

### Managed vs External Tables

| 特性 | Managed Table | External Table |
|------|---------------|----------------|
| 存储位置 | Unity Catalog 管理 | 用户指定的外部路径 |
| DROP 行为 | 删除数据 + 元数据 | 只删除元数据 |
| 治理 | 完全受 UC 管控 | 元数据受 UC 管控 |

**考点**：为什么推荐 Managed Table？→ 减少运维开销、自动管理存储、更好的治理和安全性

### Dynamic Views（动态视图）

```sql
-- 列级掩码 + 行级安全
CREATE VIEW secure_transactions AS
SELECT
    id,
    CASE
        WHEN is_account_group_member('finance_team') THEN amount
        ELSE 0
    END AS amount,
    CASE
        WHEN is_account_group_member('admin') THEN email
        ELSE regexp_replace(email, '.+@', '***@')
    END AS email,
    region
FROM transactions
WHERE
    CASE
        WHEN is_account_group_member('global_team') THEN TRUE
        ELSE region = current_user_region()
    END
```

常用安全函数：
| 函数 | 说明 |
|------|------|
| `current_user()` | 当前用户 |
| `is_account_group_member('group')` | 是否属于某组 |
| `is_member('group')` | 同上（旧语法） |

---

## 4.2 Delta Sharing

### 概念

Delta Sharing 是开放协议，允许跨组织安全共享数据，无需复制数据。

| 模式 | 说明 | 使用场景 |
|------|------|----------|
| **Databricks-to-Databricks (D2D)** | 两个 Databricks workspace 之间 | 内部跨团队共享 |
| **Databricks-to-Open (D2O)** | 向非 Databricks 平台共享 | 对外共享、多平台 |

```sql
-- 创建 Share
CREATE SHARE IF NOT EXISTS customer_data_share;

-- 添加表到 Share
ALTER SHARE customer_data_share ADD TABLE catalog.schema.customers;

-- 创建 Recipient
CREATE RECIPIENT IF NOT EXISTS partner_org
    USING ID 'partner-account-id';

-- 授权
GRANT SELECT ON SHARE customer_data_share TO RECIPIENT partner_org;
```

### 接收端读取

```python
# Open Sharing (非 Databricks 平台)
import delta_sharing

profile = "/path/to/profile.share"
table_url = f"{profile}#share_name.schema_name.table_name"
df = delta_sharing.load_as_pandas(table_url)

# Databricks-to-Databricks
df = spark.read.table("shared_catalog.schema.table")
```

---

## 4.3 Lakehouse Federation

跨平台联邦查询，无需复制数据：

```sql
-- 创建外部连接
CREATE CONNECTION postgres_conn
    TYPE POSTGRESQL
    OPTIONS (
        host 'db.example.com',
        port '5432',
        user secret('scope', 'pg_user'),
        password secret('scope', 'pg_password')
    );

-- 创建外部 Catalog
CREATE FOREIGN CATALOG pg_catalog
    USING CONNECTION postgres_conn
    OPTIONS (database 'mydb');

-- 直接查询外部数据
SELECT * FROM pg_catalog.public.users WHERE status = 'active';
```

支持的数据源：PostgreSQL、MySQL、SQL Server、Snowflake、BigQuery、Redshift 等

**考点**：Lakehouse Federation vs ETL 复制的区别？→ Federation 是实时查询不复制数据，适合低频查询；ETL 复制数据到 Lakehouse，适合高频查询

---

## 4.4 Secrets Management

```python
# 使用 Databricks Secrets API
# 1. CLI 创建 Scope
# databricks secrets create-scope my_scope

# 2. CLI 添加 Secret
# databricks secrets put-secret my_scope my_key --string-value "my_value"

# 3. Notebook 中使用
password = dbutils.secrets.get(scope="my_scope", key="db_password")
api_key = dbutils.secrets.get(scope="my_scope", key="api_key")

# 4. SQL 中使用
# SELECT secret('my_scope', 'db_password')
```

**考点**：`dbutils.secrets.get()` 返回的值在 Notebook 输出中显示什么？→ 显示 `[REDACTED]`，防止意外泄露

---

## 4.5 Service Principal

- 用于自动化和 CI/CD 的非人类身份
- 可以拥有与用户相同的 Unity Catalog 权限
- 生产 Job 应使用 Service Principal 运行（而非个人账户）

```yaml
# DABs 中指定 Service Principal
targets:
  production:
    run_as:
      service_principal_name: "etl-prod-sp"
```

---

## 4.6 数据隐私与合规

### PII 处理策略

| 技术 | 说明 | 可逆性 | 使用场景 |
|------|------|--------|----------|
| **Data Masking** | 部分隐藏 `***@email.com` | 不可逆 | 展示层 |
| **Pseudonymization** | 假名化替换 | 可逆（需映射表） | 分析保留关联 |
| **Anonymization** | 完全匿名 | 不可逆 | 公开数据 |
| **Tokenization** | Token 替换 | 可逆 | 支付数据 |
| **Hashing** | SHA256 等 | 不可逆 | 去标识化 |

### GDPR 删除流程

```python
# 1. 识别用户数据
user_tables = ["bronze.events", "silver.events", "gold.metrics"]

# 2. 级联删除（所有层）
for table in user_tables:
    spark.sql(f"DELETE FROM {table} WHERE user_id = 'U123'")

# 3. VACUUM 清理物理文件
for table in user_tables:
    spark.sql(f"VACUUM {table} RETAIN 0 HOURS")

# 4. 审计记录
spark.sql("""
    INSERT INTO audit.gdpr_deletions
    VALUES ('U123', current_timestamp(), 'completed')
""")
```

**考点**：为什么 GDPR 删除后需要 VACUUM？→ DELETE 只标记数据为删除，物理文件仍存在；VACUUM 才真正删除物理文件

---

# 域 5：Monitoring and Logging（10%）

## 5.1 Spark UI 解读

### 关键 Tab

| Tab | 查看内容 | 关注指标 |
|-----|----------|----------|
| **Jobs** | 所有 Job 和 Stage | 失败的 Job |
| **Stages** | Stage 详情和 Task 分布 | Shuffle Read/Write、Task 时间分布 |
| **Storage** | 缓存 RDD/DataFrame | 缓存命中率 |
| **SQL/DataFrame** | 查询执行计划 | Physical Plan、Scan 行数 |
| **Environment** | Spark 配置 | 确认配置生效 |

### 常见性能问题诊断

| 现象 | 可能原因 | 解决方案 |
|------|----------|----------|
| 个别 Task 特别慢 | **数据倾斜 (Data Skew)** | AQE Skew Join、Salting |
| Shuffle 数据量巨大 | Join 策略不当 | Broadcast Join、减少 Shuffle |
| OOM 错误 | 数据量超内存 | 增加内存、减少分区大小、避免 collect() |
| 大量小文件 | 输出文件过多 | Optimize Write、Auto Compaction、OPTIMIZE |
| Stage 间等待长 | Shuffle 分区过多 | 减少 `spark.sql.shuffle.partitions` |

### 数据倾斜解决方案

```python
# 方案 1：启用 AQE Skew Join（推荐）
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 方案 2：Salting（手动打散）
from pyspark.sql.functions import rand, concat, lit

# 给倾斜键添加随机后缀
salt_range = 10
salted_df = skewed_df.withColumn("salted_key",
    concat(col("key"), lit("_"), (rand() * salt_range).cast("int"))
)

# 对应的另一侧也要展开
from pyspark.sql.functions import explode, array
other_df_exploded = other_df.select(
    "*",
    explode(array([lit(i) for i in range(salt_range)])).alias("salt")
).withColumn("salted_key", concat(col("key"), lit("_"), col("salt")))

result = salted_df.join(other_df_exploded, "salted_key")
```

---

## 5.2 System Tables（系统表）

| 系统表 | 用途 | 示例查询 |
|--------|------|----------|
| `system.billing.usage` | 计费和资源使用 | 成本分析、预算监控 |
| `system.access.audit` | 审计日志 | 安全审计、合规 |
| `system.compute.clusters` | 集群信息 | 集群利用率分析 |
| `system.lakeflow.events` | Pipeline 事件 | Pipeline 健康监控 |
| `system.information_schema.*` | 元数据 | 数据目录查询 |

```sql
-- 查看过去 7 天 DBU 消费
SELECT
    usage_date,
    sku_name,
    sum(usage_quantity) as total_dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 7 DAYS
GROUP BY usage_date, sku_name
ORDER BY usage_date DESC

-- 查看某用户的审计日志
SELECT
    event_time,
    action_name,
    request_params,
    response.status_code
FROM system.access.audit
WHERE user_identity.email = 'user@company.com'
    AND event_date >= current_date() - INTERVAL 1 DAY
ORDER BY event_time DESC

-- 查看表的存储信息
SELECT
    table_catalog,
    table_schema,
    table_name,
    data_source_format
FROM system.information_schema.tables
WHERE table_schema = 'my_schema'
```

---

## 5.3 DLT Pipeline Event Log

```sql
-- 查看 Pipeline 事件日志
SELECT
    timestamp,
    details:flow_definition.output_dataset as dataset,
    details:flow_progress.metrics.num_output_rows as output_rows,
    details:flow_progress.data_quality.expectations as quality
FROM event_log(TABLE(my_pipeline))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC

-- 查看数据质量结果
SELECT
    timestamp,
    details:flow_progress.data_quality.expectations[0].name as expectation,
    details:flow_progress.data_quality.expectations[0].passed_records as passed,
    details:flow_progress.data_quality.expectations[0].failed_records as failed
FROM event_log(TABLE(my_pipeline))
WHERE event_type = 'flow_progress'
    AND details:flow_progress.data_quality IS NOT NULL
```

---

## 5.4 SQL Alerts

```sql
-- 创建监控数据质量的查询
-- 然后在 Databricks SQL 中设置 Alert

-- 示例：检测空值率超标
SELECT
    count(*) as total_rows,
    count(CASE WHEN id IS NULL THEN 1 END) as null_count,
    count(CASE WHEN id IS NULL THEN 1 END) / count(*) * 100 as null_percentage
FROM catalog.schema.table
WHERE ingestion_date = current_date()

-- 在 SQL Alert 中配置：
-- Trigger when: null_percentage > 5
-- Notification: email/Slack/webhook
```

---

## 5.5 Query Profile UI

用于分析 SQL 查询性能的可视化工具：

| 功能 | 说明 |
|------|------|
| **Query Plan** | 可视化执行计划 |
| **Time Distribution** | 各阶段耗时占比 |
| **Rows Processed** | 各算子处理行数 |
| **Spill to Disk** | 是否有内存溢出到磁盘 |

**考点**：如何判断查询是否有性能问题？→ 查看 Query Profile 中是否有 spill to disk、Shuffle 数据量是否异常大、Scan 行数是否与 Filter 后行数差距过大（说明缺少分区剪裁或 Data Skipping）

---

# 域 6：Testing and Deployment（10%）

## 6.1 测试策略

### 测试金字塔

```
        ┌──────────┐
        │  E2E     │  少量端到端测试
        ├──────────┤
        │Integration│  中等数量集成测试
        ├──────────┤
        │  Unit    │  大量单元测试
        └──────────┘
```

### 单元测试

```python
# tests/unit/test_transform.py
import pytest
from pyspark.sql import SparkSession
from src.silver.transform import clean_data, deduplicate

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").getOrCreate()

def test_clean_data_removes_nulls(spark):
    input_data = [
        (1, "Alice", "alice@email.com"),
        (None, "Bob", "bob@email.com"),
        (3, None, "charlie@email.com")
    ]
    input_df = spark.createDataFrame(input_data, ["id", "name", "email"])

    result = clean_data(input_df)

    assert result.count() == 1  # 只保留完整行
    assert result.first().id == 1

def test_deduplicate_keeps_latest(spark):
    input_data = [
        (1, "v1", "2024-01-01"),
        (1, "v2", "2024-01-02"),  # 更新版本
        (2, "v1", "2024-01-01")
    ]
    input_df = spark.createDataFrame(input_data, ["id", "value", "updated_at"])

    result = deduplicate(input_df, "id", "updated_at")

    assert result.count() == 2
    row_1 = result.filter("id = 1").first()
    assert row_1.value == "v2"
```

### 集成测试

```python
# tests/integration/test_pipeline.py
def test_bronze_to_silver_pipeline(spark):
    """测试 Bronze → Silver 端到端流程"""
    # Arrange: 准备测试数据
    test_data = [...]
    spark.createDataFrame(test_data).write.format("delta").save("/tmp/test_bronze")

    # Act: 运行转换
    from src.silver.transform import run_silver_pipeline
    run_silver_pipeline(spark, "/tmp/test_bronze", "/tmp/test_silver")

    # Assert: 验证输出
    result = spark.read.format("delta").load("/tmp/test_silver")
    assert result.count() > 0
    assert "cleaned_column" in result.columns
```

### 在 Databricks 中运行测试

```bash
# 使用 Databricks CLI 运行
databricks bundle run -t development test_job

# 或在 CI/CD 中
pip install databricks-sdk pytest
pytest tests/ -v
```

---

## 6.2 CI/CD 工作流

### Git 工作流

```
main (生产) ←── release/* ←── develop ←── feature/*
                                           ↑
                                     hotfix/* ──→ main
```

### CI/CD Pipeline 示例

```yaml
# .github/workflows/deploy.yml (GitHub Actions 示例)
name: Deploy Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install dependencies
        run: pip install -r requirements.txt
      - name: Run tests
        run: pytest tests/ -v

  deploy-staging:
    needs: test
    if: github.ref == 'refs/heads/develop'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to staging
        run: |
          pip install databricks-cli
          databricks bundle deploy -t staging
        env:
          DATABRICKS_HOST: ${{ secrets.STAGING_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.STAGING_TOKEN }}

  deploy-production:
    needs: test
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Deploy to production
        run: |
          pip install databricks-cli
          databricks bundle deploy -t production
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
```

### Databricks Git Folders (Repos)

```
Workspace
└── Repos/
    └── user@company.com/
        └── my-project/        # Git 仓库克隆
            ├── src/
            ├── tests/
            └── databricks.yml
```

- 支持 GitHub、GitLab、Azure DevOps、Bitbucket
- 可以直接在 Workspace 中 commit/push/pull
- 支持 branch 切换
- **不能**直接编辑 Workspace 外的文件

---

## 6.3 环境隔离策略

| 环境 | Catalog | Compute | 用途 |
|------|---------|---------|------|
| **Dev** | `dev_catalog` | 小集群/Serverless | 开发调试 |
| **Staging** | `staging_catalog` | 中等集群 | 集成测试、UAT |
| **Production** | `prod_catalog` | 生产集群/Job Cluster | 生产运行 |

```python
# 环境感知代码
import os

env = os.getenv("ENVIRONMENT", "dev")
catalog = f"{env}_catalog"
spark.sql(f"USE CATALOG {catalog}")

# 或使用 DABs variables
# catalog = dbutils.widgets.get("catalog")
```

---

## 6.4 Job 编排模式

### Fan-out 模式

```yaml
tasks:
  - task_key: prepare
    notebook_task: { notebook_path: ./prepare.py }
  - task_key: process_region_us
    depends_on: [{ task_key: prepare }]
    notebook_task:
      notebook_path: ./process.py
      base_parameters: { region: "us" }
  - task_key: process_region_eu
    depends_on: [{ task_key: prepare }]
    notebook_task:
      notebook_path: ./process.py
      base_parameters: { region: "eu" }
  - task_key: process_region_apac
    depends_on: [{ task_key: prepare }]
    notebook_task:
      notebook_path: ./process.py
      base_parameters: { region: "apac" }
```

### For Each Task（动态并行）

```yaml
tasks:
  - task_key: get_regions
    notebook_task:
      notebook_path: ./get_regions.py
  - task_key: process_each_region
    depends_on: [{ task_key: get_regions }]
    for_each_task:
      inputs: "{{tasks.get_regions.values.regions}}"
      task:
        task_key: process_region
        notebook_task:
          notebook_path: ./process.py
          base_parameters:
            region: "{{input}}"
```

---

# 高频考题场景总结

## 场景 1：性能优化

**题目模式**："管道运行缓慢，如何优化？"

**排查清单**：
1. 检查是否有数据倾斜 → Spark UI Stage 详情
2. 检查 Join 策略是否合理 → Broadcast 小表
3. 检查文件布局 → Z-Order / Liquid Clustering
4. 检查是否有小文件问题 → OPTIMIZE / Auto Compaction
5. 检查是否启用 AQE → `spark.sql.adaptive.enabled`
6. 检查 Shuffle 分区数 → `spark.sql.shuffle.partitions`

## 场景 2：数据质量

**题目模式**："如何确保数据质量？"

**方案选择**：
- DLT Expectations → 声明式、自动化
- Schema Enforcement → 防止脏数据写入
- Data Quality Checks in SQL → 自定义规则
- Unit Tests → 代码级保障

## 场景 3：安全与合规

**题目模式**："如何控制数据访问？"

**方案选择**：
- Unity Catalog GRANT → 表级/列级
- Dynamic Views → 行级安全 + 列掩码
- Column Masking → Unity Catalog 原生
- Row Filters → Unity Catalog 原生

## 场景 4：流处理故障

**题目模式**："流处理出了问题怎么办？"

**常见问题**：
| 问题 | 解决 |
|------|------|
| Checkpoint 损坏 | 清除 checkpoint 目录，从头重新处理（注意幂等性） |
| Schema 变化 | Auto Loader + Schema Evolution |
| 数据延迟 | 检查 Trigger 设置、集群资源 |
| 重复数据 | 使用 dropDuplicates 或 MERGE |
| 内存不足 | 增大 Watermark 阈值（减少状态保留） |

## 场景 5：CI/CD 部署

**题目模式**："如何安全地部署到生产？"

**最佳实践**：
1. 使用 DABs 管理多环境
2. Service Principal 运行生产 Job
3. Git-based CI/CD（PR → Test → Deploy）
4. 环境隔离（不同 Catalog）
5. 蓝绿部署或金丝雀发布

---

# 考试技巧

1. **通过分可能是 80%** — 有人 70% 未通过。模拟考目标 **85%+** 再去考
2. **场景优先** — 先理解场景，再选最佳方案
3. **注意关键词** — "most efficient"、"least cost"、"best practice"、"minimize latency"
4. **排除法** — 明显错误选项先排除
5. **时间管理** — 每题约 2 分钟，先易后难，标记不确定的题
6. **Lakeflow 新名称** — DLT → Lakeflow Declarative Pipelines, Workflows → Lakeflow Jobs
7. **代码审读** — 能读懂 Python 和 SQL 代码片段并判断对错

---

# 推荐学习资源

| 资源 | 用途 |
|------|------|
| [Databricks Academy](https://www.databricks.com/learn/training) | 官方课程（重点: Advanced Data Engineering） |
| [官方考试页面](https://www.databricks.com/learn/certification/data-engineer-professional) | 下载最新 Exam Guide |
| [Databricks Documentation](https://docs.databricks.com) | 查漏补缺 |
| Udemy 模拟题 | 练习 + 看解释 |
| Databricks Community | 考试经验分享 |

---

*祝考试顺利！*
