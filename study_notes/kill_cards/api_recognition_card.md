# API Recognition Card — 考试必识别 API/配置项速查

> 从 3 套 Udemy 模拟题（179 题）中提取的所有正确答案涉及的 API。
> 目的不是背，而是**至少见过一次**，考场上能区分真假。

---

## 1. Structured Streaming

```python
# 读取流
df = spark.readStream.format("delta").load("/path")
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("/path")

# 写入流 — 只有这几种 outputMode
df.writeStream.outputMode("append")       # 只插入
df.writeStream.outputMode("complete")     # 全量输出（聚合场景）
df.writeStream.outputMode("update")       # 只输出变化行

# 触发模式
.trigger(once=True)                       # 跑一次就停（已弃用，用 availableNow）
.trigger(availableNow=True)               # 处理完当前所有可用数据就停
.trigger(processingTime="5 seconds")      # 定时触发

# Upsert/MERGE — 唯一方式是 foreachBatch
df.writeStream.foreachBatch(upsert_fn).start()

# 流内去重
df.withWatermark("ts", "1 hour").dropDuplicates(["key"])

# 读 Change Data Feed
spark.readStream.option("readChangeFeed", "true").option("startingVersion", 0).table("t")

# ❌ 不存在的
.option("updateMode", "merge")            # 虚构
.option("ignoreDuplicates", "true")       # 虚构
writer.mode("merge")                      # 虚构
```

## 2. Auto Loader (cloudFiles)

```python
spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")           # 源文件格式
  .option("cloudFiles.schemaLocation", "/path")  # schema 推断存储位置
  .option("cloudFiles.useNotifications", "true") # Event Notification 模式（近实时）
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # 自动加新列
  .option("cloudFiles.maxBytesPerTrigger", "1g") # 限流
  .option("cloudFiles.rescuedDataColumn", "_rescue")  # 异常数据救援列
  .option("cloudFiles.format", "binaryFile")     # 读二进制文件（PDF/图片）
  .load("/path")

# ❌ 不存在的
.option("cloudFiles.allowDuplicates", "false")   # 虚构，默认就去重
```

## 3. DLT / Lakeflow Declarative Pipeline

```python
# 定义 Streaming Table（追加型，Bronze）
@dlt.table
def bronze():
    return spark.readStream.format("cloudFiles")...

# 定义 Materialized View（状态/聚合型，Silver/Gold）
@dlt.table
def silver():
    return dlt.read("bronze").groupBy(...).agg(...)

# 定义 Live View（虚拟视图，不存数据）
@dlt.view
def temp_view():
    return dlt.read("bronze").filter(...)

# 流式读取 + 去重
dlt.read_stream("source").withWatermark("ts", "1 hour").dropDuplicates(["key"])

# 数据质量约束
@dlt.expect("valid_price", "price > 0")                  # 警告但不丢
@dlt.expect_or_drop("valid_price", "price > 0")          # 不合格就丢弃
@dlt.expect_or_fail("valid_price", "price > 0")          # 不合格就停管道

# CDC / APPLY CHANGES INTO
dlt.apply_changes(
    target = "customers_silver",
    source = "customers_cdc",
    keys = ["customer_id"],
    sequence_by = col("change_timestamp"),        # ← 必须有，冲突解决
    apply_as_deletes = expr("op = 'DELETE'"),     # 删除条件
    stored_as_scd_type = 1                        # 或 2
)

# SQL 版本
# APPLY CHANGES INTO LIVE.target
# FROM STREAM(LIVE.source)
# KEYS (id)
# SEQUENCE BY timestamp

# 管道模式
# Development — 快速迭代，不保留状态
# Production  — 保留状态，生产环境

# ❌ 不存在的
# foreachBatch 在 DLT 内 — 不存在
# MERGE INTO 在 DLT 内 — 不存在
# ignoreDuplicates 在 dlt.read_stream — 不存在
```

## 4. Delta Lake 表操作

```sql
-- OPTIMIZE + ZORDER
OPTIMIZE my_table;
OPTIMIZE my_table ZORDER BY (col);
OPTIMIZE my_table WHERE date = '2026-01-01';
OPTIMIZE my_table WHERE date = '2026-01-01' ZORDER BY (col);

-- VACUUM
VACUUM my_table;
VACUUM my_table RETAIN 168 HOURS;    -- 默认 7 天
VACUUM my_table RETAIN 0 HOURS;      -- 需关闭安全检查

-- Liquid Clustering（替代 PARTITION + ZORDER）
CREATE TABLE t (...) CLUSTER BY (col1, col2);
ALTER TABLE t CLUSTER BY (new_col);   -- 可以改，不用重建表

-- 表属性（持久化到 Delta Log）
ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 15 days');
ALTER TABLE t SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true');

-- Tags
ALTER TABLE t SET TAGS ('key' = 'value');
ALTER TABLE t UNSET TAGS ('key');

-- DEEP CLONE（完整复制，含历史）
CREATE TABLE target DEEP CLONE source;
CREATE TABLE target DEEP CLONE source VERSION AS OF 5;

-- Time Travel
SELECT * FROM t VERSION AS OF 5;
SELECT * FROM t TIMESTAMP AS OF '2026-01-01';
RESTORE TABLE t TO VERSION AS OF 5;

-- Change Data Feed
ALTER TABLE t SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- ❌ 不存在的
OPTIMIZE t PARTITION BY (col);        -- 虚构，用 WHERE
ALTER TABLE t ADD PARTITION (col);    -- Hive 语法，Delta 不用
```

## 5. Unity Catalog 权限

```sql
-- 常考权限组合
GRANT CREATE TABLE ON SCHEMA s TO group;
GRANT SELECT ON SCHEMA s TO group;           -- 继承到所有子表
GRANT MODIFY ON TABLE t TO group;            -- INSERT/UPDATE/DELETE
GRANT USE CATALOG ON CATALOG c TO group;     -- 遍历权限，不给数据
GRANT USE SCHEMA ON SCHEMA s TO group;       -- 遍历权限，不给数据
GRANT EXECUTE ON FUNCTION f TO group;

-- Tags 语法
ALTER TABLE t SET TAGS ('key' = 'value');

-- Row Filter & Column Mask
ALTER TABLE t SET ROW FILTER fn ON (col);
ALTER TABLE t ALTER COLUMN ssn SET MASK mask_fn;

-- ❌ 不存在的
GRANT CREATE ON SCHEMA ...     -- 没有单独的 CREATE
GRANT DELETE ON SCHEMA ...     -- 没有 DELETE，用 MODIFY
SET TAGS FOR table AS (...)    -- 虚构
```

## 6. Lakehouse Federation

```sql
-- 两步走
CREATE CONNECTION my_conn TYPE SNOWFLAKE OPTIONS (host '...', user '...', password '...');
CREATE FOREIGN CATALOG my_catalog USING CONNECTION my_conn;

-- 之后直接查询
SELECT * FROM my_catalog.schema.table;
```

## 7. Delta Sharing

```sql
-- Provider 端
CREATE SHARE my_share;
ALTER SHARE my_share ADD TABLE catalog.schema.table;
CREATE RECIPIENT partner WITH TOKEN;    -- 生成激活链接

-- Databricks-to-Databricks 共享不需要 token
```

## 8. Databricks Asset Bundles (DABs)

```yaml
# 三步命令
databricks bundle validate --target staging
databricks bundle deploy --target staging
databricks bundle run main_job --target staging

# 绑定已有资源
databricks bundle deployment bind <resource-key> <existing-id>

# 生成已有 job 的 YAML
databricks bundle generate job --existing-job-id <id>

# YAML 结构
variables:
  table_name:
    default: "dev_table"
targets:
  staging:
    variables:
      table_name: "staging_table"
  prod:
    variables:
      table_name: "prod_table"
```

## 9. System Tables

```sql
-- 计费
SELECT DATE(usage_start_time) AS day, sku_name, SUM(usage_quantity)
FROM system.billing.usage GROUP BY 1, 2;

-- 审计日志
SELECT * FROM system.access.audit
WHERE service_name = 'unityCatalog' AND action_name LIKE '%permission%';

-- 查询历史
SELECT * FROM system.query.history
WHERE warehouse_id = '...' AND start_time > '...';

-- Schema 信息
SELECT * FROM system.information_schema.columns;
```

## 10. Compute / Cluster

```python
# Secrets（不在代码里硬编码密码）
password = dbutils.secrets.get(scope="my_scope", key="db_pass")

# Task 间传值
dbutils.jobs.taskValues.set(key="metric", value=0.95)    # 上游 task
val = dbutils.jobs.taskValues.get(taskKey="train", key="metric")  # 下游 task

# Spark 配置（会话级，不持久化）
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "20MB")

# Compute Policy — 替代 init scripts
# 在 JSON 中定义 spark_conf, env_vars, libraries → 集群自动继承
```

## 11. PySpark 常考 API

```python
# DataFrame transform 链（模块化测试）
df.transform(normalize_email).transform(filter_active)

# Window 函数
w = Window.partitionBy("id").orderBy("ts")
F.row_number().over(w)
F.lag("col", 1).over(w)
F.lead("col", 1).over(w)
w.rowsBetween(Window.unboundedPreceding, Window.currentRow)  # 累计
w.rangeBetween(-300, Window.currentRow)                       # 时间范围

# 条件聚合
F.sum(F.when(F.col("status") == "done", F.col("amt")).otherwise(0))

# 高阶函数（SQL）
transform(array, x -> x * 2)
filter(array, x -> x > 0)

# 数据倾斜处理
df.hint("skew", "customer_id")                # Skew Join Hint
broadcast(small_df)                            # 广播小表

# 性能层级
# Native Column Expr > Pandas UDF > Python UDF > rdd.map()
```
