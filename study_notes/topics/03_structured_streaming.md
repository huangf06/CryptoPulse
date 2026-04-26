# Structured Streaming 深度复习

> 13 道错题 | 全部答错 | 正确率 0/13 = 0%
> 这是 P1 优先级主题，与 Delta Lake、Auto Loader、DLT 深度耦合

---

## 一、概念框架

### 1.1 微批处理执行循环模型（Microbatch Execution Loop）

Structured Streaming 的本质是一个**无限循环**，每次循环处理一个微批次（microbatch）：

```
while True:
    wait_for_trigger_interval()          # Trigger 控制等待时间
    new_data = check_source_for_new()    # 检查源是否有新数据
    if new_data:
        result = process(new_data)       # 处理新数据（一个独立的 Spark job）
        write_to_sink(result)            # 写入目标
        update_checkpoint(offset)        # 记录处理到哪里了
```

关键认知：
- **每个 microbatch 是一个独立的 Spark job**，不会并行执行
- 上一个 microbatch 没处理完，下一个不会启动
- 如果处理时间 > trigger interval，下一个 batch 立即开始（不等待）
- 如果处理时间 < trigger interval，等待剩余时间后再启动

### 1.2 Checkpoint 机制

Checkpoint 是 Structured Streaming 保证 exactly-once 语义的核心机制：

- **每个 streaming query 必须有独立的 checkpoint 目录**（不能共享）
- Checkpoint 记录三类信息：
  - **Source offset**：源中处理到哪个位置了
  - **Sink commit log**：已写入目标的批次
  - **State**：有状态操作（如聚合、窗口）的中间状态
- 两个 stream 共享 checkpoint = 两个人共用一个书签，互相覆盖进度
- **修改查询逻辑（如添加聚合字段）后，旧 checkpoint 不兼容，必须指定新的 checkpointLocation**

### 1.3 Trigger 类型

| Trigger 类型 | 语法 | 行为 | 适用场景 |
|-------------|------|------|---------|
| **Default (processingTime="0")** | 不指定或 `trigger(processingTime="0s")` | 尽可能快地连续处理，一个 batch 结束立刻开始下一个 | 低延迟实时处理 |
| **Fixed interval** | `trigger(processingTime="10 seconds")` | 每 N 秒检查一次新数据 | 需要控制处理频率 |
| **Once** | `trigger(once=True)` | 处理一个 batch 后停止（已废弃） | 已被 availableNow 替代 |
| **AvailableNow** | `trigger(availableNow=True)` | 处理所有当前可用数据后停止 | 配合 Job 调度实现"批处理语义 + 流处理能力" |

关键区分：
- `processingTime` 模式下，stream **持续运行**，占用集群资源
- `once` / `availableNow` 模式下，stream **处理完即停止**，适合配合 Databricks Job 调度
- 空 microbatch 也有开销（检查源、写 checkpoint），default trigger 会产生大量空批次

### 1.4 readStream 语义

- `spark.readStream.table("delta_table")` 默认只读取**新增的行**（append-only）
- 通过 Delta Log 版本号追踪"上次读到哪里"
- 如果源表有 UPDATE/DELETE，默认会**报错**（append-only 模式不支持）
- 要读取变更，需启用 CDF：`readStream.option("readChangeFeed", "true").table(...)`
- `readStream.table()` 返回的是一个可以用于增量处理的 streaming DataFrame

---

## 二、错题精析

### A. Checkpoint 相关（3 题）

---

#### Q20 -- Structured Streaming 共享 Checkpoint 目录

**QUESTION 20**

A data architect has designed a system in which two Structured Streaming jobs will concurrently write to a single bronze Delta table. Each job is subscribing to a different topic from an Apache Kafka source, but they will write data with the same schema. To keep the directory structure simple, a data engineer has decided to nest a checkpoint directory to be shared by both streams.

The proposed directory structure is displayed below:
```
/bronze
├── _checkpoint
├── _delta_log
├── year_week=2020_01
└── year_week=2020_02
```

Which statement describes whether this checkpoint directory structure is valid for the given scenario and why?

- A. No; Delta Lake manages streaming checkpoints in the transaction log.
- B. Yes; both of the streams can share a single checkpoint directory.
- C. No; only one stream can write to a Delta Lake table.
- D. Yes; Delta Lake supports infinite concurrent writers.
- **E. No; each of the streams needs to have its own checkpoint directory.** (correct)

**我的答案：** D | **正确答案：** E

**错因分析：** 混淆了 Delta Lake 的并发写入能力和 checkpoint 的独立性要求。Delta Lake 确实支持多个 stream 并发写入同一张表（通过乐观并发控制），但这不意味着 checkpoint 可以共享。Checkpoint 记录的是每个 stream 独立的进度信息，共享会导致互相覆盖 offset，破坏 exactly-once 语义。

**核心知识点：** 每个 streaming query 必须有独立的 checkpoint 目录。多个 stream 可以写同一张 Delta 表，但 checkpoint 必须分开。

---

#### Q72 -- Structured Streaming schema 变更需要新 checkpoint

**QUESTION 72**

A data team's Structured Streaming job is configured to calculate running aggregates for item sales to update a downstream marketing dashboard. The marketing team has introduced a new promotion, and they would like to add a new field to track the number of times this promotion code is used for each item. A junior data engineer suggests updating the existing query as follows. Note that proposed changes are in bold.

Which step must also be completed to put the proposed query into production?

- **A. Specify a new checkpointLocation** (correct)
- B. Increase the shuffle partitions to account for additional aggregates
- C. Run REFRESH TABLE delta.'/item_agg'
- D. Register the data in the "/item_agg" directory to the Hive metastore
- E. Remove .option('mergeSchema', 'true') from the streaming write

**我的答案：** E | **正确答案：** A

**错因分析：** 以为 `mergeSchema` 是问题所在，但实际上 `mergeSchema` 只处理输出表的 schema 变化。真正的问题是：修改聚合逻辑后，checkpoint 中存储的旧状态（old state）与新查询不兼容。必须指定新的 checkpointLocation，从零开始构建状态。

**核心知识点：** 修改 streaming 查询的聚合逻辑 = 改变了有状态操作的结构，旧 checkpoint 中的 state 无法被新查询使用。必须用新 checkpoint 重新开始。

---

#### Q320 -- 流处理进度追踪配置

**QUESTION 320**

A data engineer is designing an append-only pipeline that needs to handle both batch and streaming data in Delta Lake. The team wants to ensure that the streaming component can efficiently track which data has already been processed.

Which configuration should be set to enable this?

- **A. checkpointLocation** (correct)
- B. overwriteSchema
- C. mergeSchema
- D. partitionBy

**我的答案：** B | **正确答案：** A

**错因分析：** 基本概念错误。overwriteSchema 用于覆盖表 schema，与流处理进度追踪完全无关。checkpointLocation 是 Structured Streaming 的核心配置，用于持久化 offset 和 state，实现断点续传和 exactly-once 语义。

**核心知识点：** checkpointLocation 是 Structured Streaming 追踪处理进度的唯一正确配置。

---

### B. Trigger 类型与调优（2 题）

---

#### Q21 -- Trigger Interval 调优

**QUESTION 21**

A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.

Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

- A. Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
- B. Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.
- C. The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.
- D. Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.
- **E. Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill.** (correct)

**我的答案：** A | **正确答案：** E

**错因分析：** A 和 E 都说减小到 5 秒，但理由不同。A 说"idle executors 处理下一个 batch"——错误，**Structured Streaming 不会在上一个 batch 未完成时启动下一个 batch**（microbatch 是串行的）。E 说"防止数据积压和大 batch 导致 spill"——正确，更频繁触发意味着每个 batch 数据量更小，处理更快，避免恶性循环。

**核心知识点：**
- Microbatch 是串行执行的，不会并行
- 数据积压的恶性循环：trigger interval 长 -> 每个 batch 数据量大 -> 处理变慢 -> 积压更多
- 减小 trigger interval 可以打断这个恶性循环

---

#### Q139 -- Trigger Once 优化存储成本

**QUESTION 139**

A Structured Streaming job deployed to production has been resulting in higher than expected cloud storage costs. At present, during normal execution, each microbatch of data is processed in less than 3s; at least 12 times per minute, a microbatch is processed that contains 0 records. The streaming write was configured using the default trigger settings. The production job is currently scheduled alongside many other Databricks jobs in a workspace with instance pools provisioned to reduce start-up time for jobs with batch execution.

Holding all other variables constant and assuming records need to be processed in less than 10 minutes, which adjustment will meet the requirement?

- A. Set the trigger interval to 3 seconds; the default trigger interval is consuming too many records per batch, resulting in spill to disk that can increase volume costs.
- **B. Use the trigger once option and configure a Databricks job to execute the query every 10 minutes; this approach minimizes costs for both compute and storage.** (correct)
- C. Set the trigger interval to 10 minutes; each batch calls APIs in the source storage account, so decreasing trigger frequency to maximum allowable threshold should minimize this cost.
- D. Set the trigger interval to 500 milliseconds; setting a small but non-zero trigger interval ensures that the source is not queried too frequently.

**我的答案：** C | **正确答案：** B

**错因分析：** 选 C 认为增大 trigger interval 即可减少成本，但忽略了关键区别：processingTime 模式下 stream **持续运行**，集群不会释放，计算成本持续产生。trigger once 模式下处理完就停止，集群可以关闭（或被 instance pool 回收），同时消除空 microbatch 的 checkpoint 写入开销。

**核心知识点：**
- Default trigger（processingTime="0"）会产生大量空 microbatch，每次都有 checkpoint 和 API 调用开销
- trigger once + Job 调度 = 批处理语义 + 流处理增量能力，是延迟容忍度 > 分钟级场景的最优方案
- processingTime 长间隔 vs trigger once 的关键区别：前者集群持续运行，后者处理完释放资源

---

### C. readStream 与增量处理语义（2 题）

---

#### Q106 -- 批量去重传播最小化计算成本

**QUESTION 106**

A nightly batch job is configured to ingest all data files from a cloud object storage container where records are stored in a nested directory structure YYYY/MM/DD. Each entry represents a user review of a product with schema: user_id STRING, review_id BIGINT, product_id BIGINT, review_timestamp TIMESTAMP, review_text STRING.

The ingestion job appends all data for the previous date to reviews_raw. The next step propagates all new records to a fully deduplicated, validated, enriched table.

Which solution minimizes the compute costs to propagate this batch of data?

- **A. Perform a batch read on the reviews_raw table and perform an insert-only merge using the natural composite key user_id, review_id, product_id, review_timestamp.** (correct)
- B. Configure a Structured Streaming read against the reviews_raw table using the trigger once execution mode to process new records as a batch job.
- C. Use Delta Lake version history to get the difference between the latest version of reviews_raw and one version prior, then write these records to the next table.
- D. Filter all records in the reviews_raw table based on the review_timestamp; batch append those records produced in the last 48 hours.
- E. Reprocess all records in reviews_raw and overwrite the next table in the pipeline.

**我的答案：** C | **正确答案：** A

**错因分析：** 选 C 以为 Delta version history diff 是最优方案，但 `VERSION AS OF` 只能获取特定版本的完整快照，不能直接得到"增量行"。而且延迟数据（moderator approval）可能在旧版本中已存在，diff 不可靠。insert-only merge 使用自然复合主键，只插入目标表中不存在的记录，既保证去重又避免全表扫描。

**核心知识点：** Insert-only merge（`MERGE ... WHEN NOT MATCHED THEN INSERT`）是去重写入的标准模式，比 Structured Streaming trigger once（有额外 checkpoint 开销）更轻量。

---

#### Q233 -- Liquid Clustering 不支持 INSERT INTO 的 cluster on write

**QUESTION 233**

A 'transactions' table has been liquid clustered on the columns 'product_id', 'user_id' and 'event_date'.

Which operation lacks support for cluster on write?

- A. CTAS and RTAS statements
- B. spark.writeStream.format('delta').mode('append')
- C. spark.write.format('delta').mode('append')
- **D. INSERT INTO operations** (correct)

**我的答案：** B | **正确答案：** D

**错因分析：** 以为 writeStream 不支持 cluster on write，但实际上 Spark 的 `.write` 和 `.writeStream` 在 append 模式下都支持 cluster on write。唯一不支持的是 SQL 的 `INSERT INTO`，它只做简单追加，不会按聚簇列重新组织数据。

**核心知识点：** Liquid Clustering 的 cluster on write 支持 CTAS/RTAS、`.write.mode('append')`、`.writeStream.mode('append')`，但**不支持 INSERT INTO**。

---

### D. SQL Alert 与监控（1 题）

---

#### Q4 -- SQL Alert 触发条件（GROUP BY 语义）

**QUESTION 4**

The data engineering team has configured a Databricks SQL query and alert to monitor the values in a Delta Lake table. The recent_sensor_recordings table contains an identifying sensor_id alongside the timestamp and temperature for the most recent 5 minutes of recordings.

The below query is used to create the alert:
```sql
SELECT MEAN(temperature), MAX(temperature), MIN(temperature)
FROM recent_sensor_recordings
GROUP BY sensor_id
```

The query is set to refresh each minute and always completes in less than 10 seconds. The alert is set to trigger when mean(temperature) > 120. Notifications are triggered to be sent at most every 1 minute.

If this alert raises notifications for 3 consecutive minutes and then stops, which statement must be true?

- A. The total average temperature across all sensors exceeded 120 on three consecutive executions of the query
- B. The recent_sensor_recordings table was unresponsive for three consecutive runs of the query
- C. The source query failed to update properly for three consecutive minutes and then restarted
- D. The maximum temperature recording for at least one sensor exceeded 120 on three consecutive executions of the query
- **E. The average temperature recordings for at least one sensor exceeded 120 on three consecutive executions of the query** (correct)

**我的答案：** A | **正确答案：** E

**错因分析：** 忽略了 `GROUP BY sensor_id` 的语义。查询结果是多行（每个 sensor 一行），不是一个总聚合值。Alert 逐行检查条件，**任意一行**满足 `mean(temperature) > 120` 就触发。所以是"至少一个 sensor"而不是"所有 sensor 的总平均"。

**核心知识点：** Databricks SQL Alert 对 GROUP BY 查询的行为：逐行检查条件，任意一行满足即触发。

---

### E. 性能测试与生产实践（3 题）

---

#### Q49 -- 性能测试最佳实践（与 Q219 为同源题）

**QUESTION 49**

A user new to Databricks is trying to troubleshoot long execution times for some pipeline logic they are working on. Presently, the user is executing code cell-by-cell, using display() calls to confirm code is producing the logically correct results as new transformations are added to an operation. To get a measure of average time to execute, the user is running each cell multiple times interactively.

Which of the following adjustments will get a more accurate measure of how code is likely to perform in production?

- A. Scala is the only language that can be accurately tested using interactive notebooks; because the best performance is achieved by using Scala code compiled to JARs, all PySpark and Spark SQL logic should be refactored.
- **B. The only way to meaningfully troubleshoot code execution times in development notebooks is to use production-sized data and production-sized clusters with Run All execution.** (correct)
- C. Production code development should only be done using an IDE; executing code against a local build of open source Spark and Delta Lake will provide the most accurate benchmarks for how code will perform in production.
- D. Calling display() forces a job to trigger, while many transformations will only add to the logical query plan; because of caching, repeated execution of the same logic does not provide meaningful results.
- E. The Jobs UI should be leveraged to occasionally run the notebook as a job and track execution time during incremental code development because Photon can only be enabled on clusters launched for scheduled jobs.

**我的答案：** C | **正确答案：** B

**错因分析：** 认为本地 IDE + 开源 Spark 可以准确模拟生产环境，但 Databricks 的性能特性（Photon、优化的存储连接、集群配置）在本地环境中完全不可复现。只有在生产规模数据 + 生产规模集群 + Run All 执行模式下才能得到有意义的性能基准。

---

#### Q219 -- display() 与缓存对性能测量的影响（Q49 的变体）

**QUESTION 219**

(Same scenario as Q49) Which adjustment will get a more accurate measure of how code is likely to perform in production and what is the limitation of this approach?

- A. The Jobs UI should be leveraged to occasionally run the notebook as a job and track execution time during incremental code development because Photon can only be enabled on clusters launched for scheduled jobs.
- B. The only way to meaningfully troubleshoot code execution times in development notebooks is to use production-sized data and production-sized clusters with Run All execution.
- C. Production code development should only be done using an IDE; executing code against a local build of open source Spark and Delta Lake will provide the most accurate benchmarks for how code will perform in production.
- **D. Calling display() forces a job to trigger, while many transformations will only add to the logical query plan; because of caching, repeated execution of the same logic does not provide meaningful results.** (correct)

**我的答案：** B | **正确答案：** D

**错因分析：** Q49 和 Q219 是同一道题的两个版本，但正确答案不同。Q49 问"what adjustment"（侧重解决方案），答案是 B（用生产数据+集群）。Q219 问"what adjustment **and what is the limitation**"（侧重问题诊断），答案是 D（解释了为什么重复执行不准确——因为缓存）。审题不够细致，没注意到题目措辞的差异。

**核心知识点：**
- Spark 的懒执行：transformations 只构建查询计划，actions（display/count/collect）才触发执行
- 重复执行同一逻辑时，Spark 缓存中间结果，后续执行比首次快，计时不准确
- 两个维度的解决方案：(1) 理解问题本质（D: 缓存导致不准确），(2) 实践方案（B: 生产规模测试）

---

#### Q110 -- 高并发高吞吐场景方案选择

**QUESTION 110**

A large company seeks to implement a near real-time solution involving hundreds of pipelines with parallel updates of many tables with extremely high volume and high velocity data.

Which of the following solutions would you implement to achieve this requirement?

- **A. Use Databricks High Concurrency clusters, which leverage optimized cloud storage connections to maximize data throughput.** (correct)
- B. Partition ingestion tables by a small time duration to allow for many data files to be written in parallel.
- C. Configure Databricks to save all data to attached SSD volumes instead of object storage, increasing file I/O significantly.
- D. Isolate Delta Lake tables in their own storage containers to avoid API limits imposed by cloud vendors.
- E. Store all tables in a single database to ensure that the Databricks Catalyst Metastore can load balance overall throughput.

**我的答案：** C | **正确答案：** A

**错因分析：** 以为本地 SSD 能提升 I/O 性能，但 Databricks 是云原生架构，计算与存储分离，不依赖本地磁盘做持久化。High Concurrency 集群专为多用户/多任务并发设计，内置了对云存储的优化连接。

**核心知识点：** Databricks 是计算存储分离的云原生架构，High Concurrency 集群是高并发高吞吐场景的标准方案。

---

### F. Jobs API 与编排（2 题）

---

#### Q117 -- REST API creator_user_name 字段含义

**QUESTION 117**

A data engineer, User A, has promoted a pipeline to production by using the REST API to programmatically create several jobs. A DevOps engineer, User B, has configured an external orchestration tool to trigger job runs through the REST API. Both users authorized the REST API calls using their personal access tokens.

A workspace admin, User C, inherits responsibility for managing this pipeline. User C uses the Databricks Jobs UI to take "Owner" privileges of each job. Jobs continue to be triggered using the credentials and tooling configured by User B.

An application has been configured to collect and parse run information returned by the REST API. Which statement describes the value returned in the creator_user_name field?

- A. Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User A's email address will appear in this field.
- **B. User B's email address will always appear in this field, as their credentials are always used to trigger the run.** (correct)
- C. User A's email address will always appear in this field, as they still own the underlying notebooks.
- D. Once User C takes "Owner" privileges, their email address will appear in this field; prior to this, User B's email address will appear in this field.
- E. User C will only ever appear in this field if they manually trigger the job, otherwise it will indicate User B.

**我的答案：** A | **正确答案：** B

**错因分析：** 混淆了 job owner（谁拥有 job）和 run creator（谁触发了运行）。`creator_user_name` 记录的是**触发这次 run 的用户**（即使用其 token 发起 API 调用的人），不是 job 的创建者或 owner。User C 接管 Owner 权限只影响 job 管理权，不影响 run 的触发者记录。

**核心知识点：** job owner vs run creator 是两个独立的概念。`creator_user_name` 始终反映触发 run 的 token 所属用户。

---

#### Q325 -- 多任务 ETL 流水线自动化触发（多选题，未作答）

**QUESTION 325**

A data team is automating a daily multitask ETL pipeline in Databricks. The pipeline includes a notebook for ingesting raw data, a Python wheel task for data transformation, and a SQL query to update aggregates. They want to trigger the pipeline programmatically and see previous runs in the GUI. They need to ensure tasks are retried on failure and stakeholders are notified by email if any task fails.

Which two approaches will meet these requirements? (Choose two.)

- **A. Trigger the job programmatically using the Databricks Jobs REST API (/jobs/run-now), the CLI (databricks jobs run-now), or one of the Databricks SDKs.** (correct)
- B. Use Databricks Asset Bundles (DABs) to deploy the workflow, then trigger individual tasks directly by referencing each task's notebook or script path in the workspace.
- **C. Create a multi-task job using the UI, DABs, or the Jobs REST API (/jobs/create) with notebook, Python wheel, and SQL tasks. Configure task-level retries and email notifications in the job definition.** (correct)
- D. Use the REST API endpoint /jobs/runs/submit to trigger each task individually as separate job runs and implement retries using custom logic in the orchestrator.
- E. Create a single notebook that uses dbutils.notebook.run() to call each step in sequence. Define a job on this orchestrator notebook and configure retries and notifications at the notebook level.

**我的答案：** X（未答） | **正确答案：** AC

**错因分析：** 多选题直接放弃。A 正确：`/jobs/run-now` 触发已有 Job，运行记录自动显示在 GUI。C 正确：多任务 Job 支持 notebook、Python wheel、SQL 三种任务类型，可在定义中配置任务级重试和邮件通知。D 的 `/jobs/runs/submit` 是一次性提交不关联已有 Job。E 的 `dbutils.notebook.run()` 串行编排无法实现任务级重试和通知。

**核心知识点：**
- `/jobs/run-now`：触发已有 Job，运行历史在 GUI 可见
- `/jobs/runs/submit`：一次性提交，不关联 Job 定义
- 多任务 Job 原生支持 task-level retries 和 email notifications

---

## 三、核心对比表

### 3.1 Trigger 类型对比

| 维度 | processingTime (default) | processingTime (interval) | once | availableNow |
|------|-------------------------|--------------------------|------|-------------|
| 集群状态 | 持续运行 | 持续运行 | 处理完停止 | 处理完停止 |
| 处理频率 | 尽可能快 | 每 N 秒 | 一次 | 一次（处理所有积压） |
| 空 batch 开销 | 高（频繁产生空 batch） | 中等 | 无 | 无 |
| 适用场景 | 亚秒级延迟 | 可控延迟 | 已废弃 | 配合 Job 调度，分钟级延迟 |
| 成本 | 最高（集群不释放） | 较高（集群不释放） | 低 | 低 |
| 备注 | 12次/分钟空batch = Q139的问题 | Q21 中减小interval防积压 | 用 availableNow 替代 | 最佳实践：延迟容忍 > 1min |

### 3.2 Checkpoint 操作对比

| 场景 | 是否需要新 checkpoint | 原因 |
|------|---------------------|------|
| 添加新的聚合字段 | 是 | 有状态操作的 state schema 变了 |
| 修改 filter 条件 | 否 | 无状态操作，不影响 checkpoint |
| 修改 trigger interval | 否 | Trigger 不影响 checkpoint 兼容性 |
| 更换数据源 | 是 | Source offset 不兼容 |
| 添加新列到 SELECT | 视情况 | 无状态操作可以，有状态操作需要新 checkpoint |

### 3.3 readStream vs read 对比

| 维度 | spark.readStream.table() | spark.read.table() |
|------|--------------------------|-------------------|
| 模式 | 增量（只读新增行） | 全量（读取整张表） |
| 追踪进度 | 通过 checkpoint | 不追踪 |
| 对 UPDATE/DELETE 的反应 | 默认报错 | 正常读取 |
| 配合 CDF | `option("readChangeFeed", "true")` | `option("readChangeFeed", "true").option("startingVersion", N)` |
| 输出类型 | Streaming DataFrame | Static DataFrame |

### 3.4 性能测试方法对比

| 方法 | 问题 | 适用场景 |
|------|------|---------|
| 逐 cell 执行 + display() | 缓存导致后续执行偏快，计时不准 | 逻辑验证，非性能测试 |
| 重复执行同一 cell | Spark 缓存中间结果 | 不适用 |
| Run All + 生产规模数据 | 最接近生产行为 | 性能基准测试 |
| 本地 IDE + 开源 Spark | 缺少 Databricks 优化 | 不适用 |

---

## 四、错误模式总结

| 错误模式 | 涉及题目 | 根因 |
|---------|---------|------|
| Checkpoint 独立性不理解 | Q20, Q72, Q320 | 缺少"每个 query 独占 checkpoint"的心智模型 |
| Trigger 类型选择 | Q21, Q139 | 不理解 processingTime vs trigger once 的成本差异 |
| Microbatch 串行执行 | Q21 | 误以为 batch 可以并行 |
| 审题不细致 | Q49/Q219 | 同源题因措辞差异答案不同 |
| 本地 vs 云原生思维 | Q49, Q110 | 习惯性选择"本地/SSD"方案 |
| 多选题放弃 | Q325 | 缺少策略，应至少排除明显错项后猜测 |

---

## 五、自测清单

### Checkpoint
- [ ] 两个 stream 写同一张 Delta 表，checkpoint 可以共享吗？（不可以，每个 query 独立）
- [ ] 修改 streaming 查询的聚合逻辑后，需要做什么？（指定新的 checkpointLocation）
- [ ] checkpointLocation 配置的作用是什么？（持久化 offset 和 state，实现 exactly-once 和断点续传）
- [ ] Checkpoint 记录哪三类信息？（source offset、sink commit log、state）

### Trigger
- [ ] processingTime 模式下，上一个 microbatch 处理时间 > trigger interval 会怎样？（下一个 batch 立即开始，不等待）
- [ ] 大量空 microbatch 的成本问题如何解决？（trigger once + Job 调度）
- [ ] 高峰期 microbatch 处理变慢，应该增大还是减小 trigger interval？（减小，防止数据积压的恶性循环）
- [ ] trigger once 和 availableNow 的区别？（once 只处理一个 batch，availableNow 处理所有可用数据）

### readStream
- [ ] `readStream.table("delta_table")` 默认读取什么数据？（只读取新增行，append-only）
- [ ] 源表有 UPDATE/DELETE 时 readStream 默认什么行为？（报错）
- [ ] 如何读取 Delta 表的变更记录？（`readStream.option("readChangeFeed", "true")`）

### 生产实践
- [ ] 为什么重复执行同一 cell 的计时不准确？（Spark 缓存中间结果）
- [ ] display() 在 Spark 中属于什么类型的操作？（Action，触发实际执行）
- [ ] High Concurrency 集群的核心优势？（优化的云存储连接，适合多用户/多任务并发）

### Jobs API
- [ ] `creator_user_name` 字段反映的是谁？（触发 run 的 token 所属用户，不是 job owner 或 creator）
- [ ] `/jobs/run-now` vs `/jobs/runs/submit` 的区别？（前者触发已有 Job，后者一次性提交）
- [ ] 多任务 Job 支持哪些任务类型？（notebook、Python wheel、SQL 等）

### Liquid Clustering
- [ ] 哪种写入操作不支持 cluster on write？（INSERT INTO）
- [ ] writeStream append 模式支持 cluster on write 吗？（支持）
