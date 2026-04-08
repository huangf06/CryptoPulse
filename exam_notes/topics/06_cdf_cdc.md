# Change Data Feed / CDC

> 6 题全错 (0/6)，正确率 0%
> 涉及题号：Q28, Q30, Q113, Q147, Q248, Q296

---

## 概念框架

### 1. Change Data Feed (CDF) 是什么

CDF 是 Delta Lake 的一项功能，在表上启用后，Delta 会额外记录行级变更元数据。消费者可以通过 `table_changes()` (SQL) 或 `readChangeFeed` (DataFrame API) 读取这些变更记录，而不是每次全量扫描整张表。

**启用方式：**
```sql
-- 建表时启用
CREATE TABLE my_table (...) TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- 对已有表启用
ALTER TABLE my_table SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### 2. _change_type 列的四种值

| _change_type | 含义 | 触发操作 |
|---|---|---|
| `insert` | 新插入的行 | INSERT、MERGE (matched insert) |
| `update_preimage` | 更新前的旧值 | UPDATE、MERGE (matched update) |
| `update_postimage` | 更新后的新值 | UPDATE、MERGE (matched update) |
| `delete` | 被删除的行 | DELETE、MERGE (matched delete) |

CDF 输出还包含两个额外元数据列：
- `_commit_version`：变更发生的 Delta 版本号
- `_commit_timestamp`：变更发生的时间戳

### 3. 读取 CDF 的两种方式

**Batch 读取（一次性）：**
```python
# DataFrame API
spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 5) \
    .table("my_table")

# SQL
SELECT * FROM table_changes('my_table', 5)
SELECT * FROM table_changes('my_table', '2024-01-01')
```

**Streaming 读取（增量）：**
```python
spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("my_table")
```

关键区别：Streaming 读取有 checkpoint 机制，自动追踪已处理位置；Batch 读取无状态，每次都从指定版本开始读取。

### 4. CDF + MERGE INTO 构建下游表

CDF 输出不能直接 append 到目标表——必须用 MERGE INTO 将变更正确应用：
```python
changes = spark.read.option("readChangeFeed", "true") \
    .option("startingVersion", last_version) \
    .table("source")

# 过滤出 insert 和 update_postimage（当前值）
upserts = changes.filter(col("_change_type").isin(["insert", "update_postimage"]))
deletes = changes.filter(col("_change_type") == "delete")

# MERGE 到目标表
upserts.createOrReplaceTempView("upserts")
spark.sql("""
    MERGE INTO target t
    USING upserts s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")
```

### 5. readStream.table() vs readChangeFeed

| 场景 | 推荐方式 | 原因 |
|---|---|---|
| 源表只有 append 操作 | `readStream.table("source")` | 无需 CDF，streaming 自动追踪新增行 |
| 源表有 UPDATE/DELETE | `readStream.option("readChangeFeed", "true")` | 需要 CDF 捕获行级变更 |
| 表覆盖写入后比较差异 | Time Travel (`VERSION AS OF`) | CDF 可能未启用，time travel 始终可用 |

### 6. CDF 在 Lakeflow Declarative Pipelines 中的应用

当上游 streaming table 包含 UPDATE/DELETE 操作时，下游 pipeline 不能直接用 `readStream.table()` 读取（会报错或只看到 append）。正确做法：
1. 上游表启用 CDF
2. 下游用 `readStream.option("readChangeFeed", "true")` 读取 CDF
3. 用 `apply_changes()` 函数将变更正确应用到目标 streaming table

---

## 错题精析

### Q28 -- Change Data Feed + append 模式重复数据

**原题：**
A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1 table representing all of the values that have ever been valid for all rows in a bronze table created with the property delta.enableChangeDataFeed = true. They plan to execute the following code as a daily job:

```python
from pyspark.sql.functions import col

(spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("bronze")
    .filter(col("_change_type").isin(["update_postimage", "insert"]))
    .write
    .mode("append")
    .table("bronze_history_type1")
)
```

Which statement describes the execution and results of running the above query multiple times?

A. Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.
B. Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.
C. Each time the job is executed, the target table will be overwritten using the entire history of inserted or updated records, giving the desired result.
D. Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.
E. Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.

**我的答案：** E | **正确答案：** B

**解析：**
代码中有两个致命组合：
1. **`startingVersion = 0`** -- 每次执行都从版本 0 开始读取全部变更历史
2. **`spark.read`（batch read）** -- 无状态，不维护 checkpoint，不记录上次读到哪个版本

因此每次运行都会读取从版本 0 到当前版本的所有 insert 和 update_postimage 记录，再用 `.mode("append")` 追加到目标表，导致大量重复。

**我的错误根因：** 误以为 `readChangeFeed` 自带增量追踪能力（类似 streaming），实际上 batch read 完全无状态。选 E 描述的是 streaming + checkpoint 的行为。

**正确做法（如果要增量）：**
- 用 `readStream` + checkpoint，或
- 手动记录 `last_processed_version`，每次从该版本开始读取

**标签：** `CDF` `batch vs streaming` `startingVersion` `无状态读取` `append重复`

---

### Q30 -- readStream.table() 增量处理

**原题：**
A nightly job ingests data into a Delta Lake table using the following code:

```python
def ingest_daily_batch(time_col: Column, year:int, month:int, day:int):
    (spark.read
        .format("parquet")
        .load(f"/mnt/daily_batch/{year}/{month}/{day}")
        .select("*",
            time_col.alias("ingest_time"),
            input_file_name().alias("source_file")
        )
        .write
        .mode("append")
        .saveAsTable("bronze")
    )
```

The next step in the pipeline requires a function that returns an object that can be used to manipulate new records that have not yet been processed to the next table in the pipeline. Which code snippet completes this function definition?

def new_records():

A. return spark.readStream.table("bronze")
B. return spark.readStream.load("bronze")
C. return (spark.read.table("bronze").filter(col("ingest_time") == current_timestamp()))
D. return spark.read.option("readChangeFeed", "true").table("bronze")
E. return (spark.read.table("bronze").filter(col("source_file") == f"/mnt/daily_batch/{year}/{month}/{day}"))

**我的答案：** D | **正确答案：** A

**解析：**
需求是"返回尚未处理到下一张表的新记录"——这正是 Structured Streaming 的 checkpoint 机制要解决的问题。

- **A 正确**：`readStream.table("bronze")` 返回 streaming DataFrame，通过 checkpoint 自动追踪已处理位置，每次只处理新增数据
- **D 错误**：`readChangeFeed` 需要表预先启用 CDF 属性，且这里 bronze 表只有 append 操作（每晚批量写入），用 CDF 是过度设计。更关键的是，batch read + CDF 没有增量追踪能力

**我的错误根因：** 混淆了"追踪新记录"的两种机制：
- **Streaming checkpoint**：追踪"处理到哪里了"，适用于 append-only 场景
- **CDF**：追踪"行发生了什么变更"，适用于有 UPDATE/DELETE 的场景

这里 bronze 表只有 append，所以 streaming 是正确选择。

**标签：** `readStream.table()` `checkpoint` `增量处理` `CDF vs Streaming`

---

### Q113 -- 表覆盖写入后获取版本差异

**原题：**
A Delta Lake table named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.

Immediately after each update succeeds, the data engineering team would like to determine the difference between the new version and the previous version of the table.

Given the current implementation, which method can be used?

A. Execute a query to calculate the difference between the new version and the previous version using Delta Lake's built-in versioning and time travel functionality.
B. Parse the Delta Lake transaction log to identify all newly written data files.
C. Parse the Spark event logs to identify those rows that were updated, inserted, or deleted.
D. Execute DESCRIBE HISTORY customer_churn_params to obtain the full operation metrics for the update, including a log of all records that have been added or modified.
E. Use Delta Lake's change data feed to identify those records that have been updated, inserted, or deleted.

**我的答案：** E | **正确答案：** A

**解析：**
关键词是 "Given the current implementation"——题目描述的是现有实现（nightly overwrite），没有提到 CDF 已启用。

- **A 正确**：Delta Lake time travel 无需任何预先配置，始终可用。覆盖写入后可以直接用 `SELECT * FROM t VERSION AS OF (current_version - 1) EXCEPT SELECT * FROM t` 计算差异
- **E 错误**：CDF 需要预先设置 `delta.enableChangeDataFeed = true`，题目没有说已启用。而且对于 overwrite 操作，CDF 的行为与增量更新不同

**我的错误根因：** 一看到"版本差异"就条件反射选 CDF，没有注意前提条件——CDF 是可选功能，需要显式启用；Time Travel 是 Delta Lake 的内建能力，始终可用。

**决策树：**
- 需要行级变更记录 + CDF 已启用 → 用 CDF
- 需要版本差异 + 不确定 CDF 是否启用 → 用 Time Travel
- `DESCRIBE HISTORY` → 只有操作元数据，没有行级内容

**标签：** `Time Travel` `CDF前提条件` `版本差异` `DESCRIBE HISTORY`

---

### Q147 -- CDF 无增量状态导致全量重复追加

**原题：**
A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1 table representing all of the values that have ever been valid for all rows in a bronze table created with the property delta.enableChangeDataFeed = true. They plan to execute the following code as a daily job:

(与 Q28 本质相同的题目，使用 `table_changes()` 函数但没有传入正确的起始版本参数)

Which statement describes the execution and results of running the above query multiple times?

A. Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.
B. Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries.
C. Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.
D. Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.

**我的答案：** C | **正确答案：** B

**解析：**
与 Q28 同一考点。代码使用 `table_changes()` 但没有动态记录上次处理的版本号，每次都从版本 0 读取全量变更历史，再 append 到目标表，造成大量重复。

**同一个坑踩两次的根因：** 核心误解是认为 CDF / `table_changes()` 自带增量追踪。实际上：
- `table_changes()` 是无状态的 SQL 函数，返回指定版本范围内的所有变更
- 如果不手动管理 `startingVersion`，每次都读全量
- 只有 `readStream` 才有 checkpoint 自动追踪

**标签：** `CDF` `table_changes()` `无状态` `全量重复` `Type 1 SCD`

---

### Q248 -- Deletion Vectors 减少文件重写

**原题：**
A company stores account transactions in a Delta Lake table. The company needs to apply frequent account-level correlations (e.g., UPDATE statements) but wants to avoid rewriting entire Parquet files for each change to reduce file churn and improve write performance.

Which Delta Lake feature should they enable?

A. Enable automatic file compaction on writes
B. Enable change data feed on the Delta table
C. Partition the Delta table by account_id
D. Enable deletion vectors on the Delta table

**我的答案：** B | **正确答案：** D

**解析：**
- **D 正确**：Deletion Vectors 允许 Delta Lake 用软删除标记（bitmap）标记哪些行被删除/更新，而不重写整个 Parquet 文件。这直接解决了"减少文件重写"的需求
- **B 错误**：CDF 是用于**捕获变更数据供下游消费**的功能，它不会改变写入行为，不能减少文件重写开销

**我的错误根因：** 混淆了 CDF 和 Deletion Vectors 的功能边界：
- **CDF** = 变更数据的**消费者侧**功能（记录变更供下游读取）
- **Deletion Vectors** = 变更数据的**写入侧**优化（减少物理文件重写）

**标签：** `Deletion Vectors` `CDF` `文件重写优化` `功能边界混淆`

---

### Q296 -- CDF + apply_changes() 实现近实时增量丰富

**原题：**
A company has a task management system that tracks the most recent status of tasks. The system takes task events as input and processes events in near real-time using Lakeflow Spark Declarative Pipelines. A new task event is ingested into the system when a task is created or the status is changed. Lakeflow Spark Declarative Pipelines provides a streaming table (table name: tasks_status) for the BI users to query. The table represents the latest status of all tasks and includes 5 columns: task_id (unique for each task), task_name, task_owner, task_status, task_event_time. The table enables 3 properties: deletion vectors, row tracking, and change data feed.

A data engineer is asked to create a new Lakeflow Spark Declarative Pipelines to enrich the "task_status" table in near real-time by adding one additional column representing task_owner's department, which can be looked up from a static dimension table (table name: employee).

How should this enrichment be implemented?

A. Create a new Lakeflow Spark Declarative Pipelines: use readStream() function with option readChangeFeed to read tasks_status table CDF; enrich with the employee table; create a new streaming table as the result table and use apply_changes() function to process the changes from the enriched CDF.
B. Create a new Lakeflow Spark Declarative pipeline: use the readStream() function to read tasks_status table, enrich with the employee table; store the result in a new streaming table.
C. Create a new Lakeflow Spark Declarative Pipeline: use the readStream() function with the option skipChangeCommits to read the tasks_status table; enrich with the employee table; store the result in a new streaming table.
D. Create a new Lakeflow Spark Declarative Pipeline: use the read() function to read tasks_status table; enrich with employee table; store the result in a materialized view.

**我的答案：** B | **正确答案：** A

**解析：**
`tasks_status` 表是一个维护最新状态的 streaming table，包含 UPDATE 操作（任务状态变更时更新已有行）。这意味着：

- **B 错误**：直接用 `readStream()` 读取包含 UPDATE/DELETE 的 Delta 表，默认 append-only 模式会报错（因为检测到非 append 的变更）
- **C 错误**：`skipChangeCommits` 跳过包含变更的 commit，会丢失更新数据
- **D 错误**：`read()` + materialized view 是全量重算，不满足"近实时"需求
- **A 正确**：
  1. 用 `readStream().option("readChangeFeed", "true")` 读取 CDF，获取行级变更事件
  2. 用 employee 表 enrich（join 添加 department 列）
  3. 用 `apply_changes()` 将变更正确应用到目标 streaming table（处理 insert/update/delete 语义）

**核心逻辑链：** 上游有 UPDATE → 不能直接 readStream（会报错）→ 必须读 CDF → CDF 包含 _change_type → 需要 apply_changes() 正确处理变更语义

**标签：** `CDF` `apply_changes()` `Lakeflow` `streaming table` `readChangeFeed` `skipChangeCommits`

---

## 核心对比表

### CDF _change_type 值与含义

| _change_type | 含义 | 何时产生 | 在 MERGE 中如何使用 |
|---|---|---|---|
| `insert` | 新行 | INSERT, MERGE INSERT | 直接 INSERT 到目标 |
| `update_preimage` | 更新前旧值 | UPDATE, MERGE UPDATE | 通常忽略（除非需要审计） |
| `update_postimage` | 更新后新值 | UPDATE, MERGE UPDATE | UPDATE 目标表对应行 |
| `delete` | 被删除的行 | DELETE, MERGE DELETE | DELETE 目标表对应行 |

### CDF 读取方式对比

| 维度 | Batch Read (spark.read) | Streaming Read (readStream) |
|---|---|---|
| 状态管理 | 无状态，每次从指定版本开始 | 有 checkpoint，自动追踪进度 |
| 重复数据风险 | 高（如果 startingVersion 固定） | 无（checkpoint 保证 exactly-once） |
| 适用场景 | 一次性查询、回填 | 持续增量处理 |
| API | `spark.read.option("readChangeFeed", "true")` | `spark.readStream.option("readChangeFeed", "true")` |
| SQL 等价 | `table_changes('t', start, end)` | 无直接等价 |

### CDF vs 相关功能对比

| 功能 | 作用 | 解决的问题 | 需要显式启用？ |
|---|---|---|---|
| **CDF** | 记录行级变更供下游消费 | 增量 ETL、审计、CDC | 是 (`enableChangeDataFeed`) |
| **Time Travel** | 按版本/时间戳查询历史数据 | 数据回溯、版本对比 | 否（Delta 内建） |
| **Deletion Vectors** | 软删除标记，避免文件重写 | 减少写入开销 | 是 (`enableDeletionVectors`) |
| **DESCRIBE HISTORY** | 查看操作元数据 | 审计操作记录 | 否（Delta 内建） |
| **readStream.table()** | 增量读取 append-only 变更 | 流式增量处理 | 否（Delta + Streaming 内建） |

### 读取有 UPDATE/DELETE 的 Delta 表的方式选择

| 方法 | 行为 | 结果 |
|---|---|---|
| `readStream.table()` | 默认 append-only 模式 | 报错（检测到非 append 变更） |
| `readStream.option("skipChangeCommits", "true")` | 跳过包含变更的 commit | 丢失更新数据 |
| `readStream.option("readChangeFeed", "true")` | 读取 CDF 变更记录 | 正确获取所有变更（需配合 apply_changes 或 MERGE） |
| `spark.read` + Time Travel | 全量读取指定版本 | 可用，但无增量能力 |

---

## 错误模式总结

这 6 题全错，暴露了三个系统性误解：

1. **误以为 CDF batch read 有增量追踪能力**（Q28, Q147）：`spark.read` + `readChangeFeed` 是无状态的，每次从 `startingVersion` 读取全量。只有 `readStream` 才有 checkpoint。

2. **CDF 和 Streaming checkpoint 的适用场景混淆**（Q30）：append-only 表用 `readStream.table()` 即可，不需要 CDF。CDF 是为有 UPDATE/DELETE 的表设计的。

3. **CDF 功能边界不清**（Q113, Q248）：CDF 是消费者侧功能（供下游读取变更），不是写入侧优化（那是 Deletion Vectors）。CDF 需要显式启用，而 Time Travel 始终可用。

---

## 自测清单

- [ ] CDF 的四种 `_change_type` 值分别是什么？各在什么操作时产生？
- [ ] `spark.read.option("readChangeFeed", "true")` 和 `spark.readStream.option("readChangeFeed", "true")` 的关键区别是什么？
- [ ] 为什么 batch read CDF + `startingVersion=0` + `mode("append")` 会产生重复数据？
- [ ] 对于 append-only 的 Delta 表，追踪新记录应该用什么方法？为什么不需要 CDF？
- [ ] 表覆盖写入后想比较版本差异，CDF 和 Time Travel 哪个更可靠？为什么？
- [ ] CDF 和 Deletion Vectors 分别解决什么问题？
- [ ] 上游 streaming table 有 UPDATE 操作时，下游 pipeline 应该如何读取？直接 `readStream.table()` 会怎样？
- [ ] `apply_changes()` 函数的作用是什么？为什么 CDF 输出不能直接 append 到目标表？
- [ ] `table_changes('my_table', 5)` 和 `table_changes('my_table', 5, 10)` 分别返回什么？
- [ ] `skipChangeCommits` 选项的作用是什么？为什么它不是处理有变更的表的正确方案？
