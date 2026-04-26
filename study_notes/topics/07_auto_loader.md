# Auto Loader

**题目数量:** 7 | **全错:** 7/7 (0%) | **题号:** Q14, Q108, Q242, Q257, Q268, Q289, Q298

这是所有专题中正确率最低的之一。错误模式高度集中：混淆 Auto Loader 的默认行为、适用场景，以及与 LDP/Structured Streaming 的协作关系。

---

## 概念框架

### 1. cloudFiles 数据源

Auto Loader 通过 `cloudFiles` 格式在 Structured Streaming 中使用：

```python
spark.readStream.format("cloudFiles")
  .option("cloudFiles.format", "json")       # 源文件格式
  .option("cloudFiles.schemaLocation", path)  # schema 推断结果存储位置
  .load("/path/to/source/")
```

核心特性：
- **增量处理**: 自动追踪已处理文件，只处理新到达的文件
- **幂等性**: 通过 checkpoint 保证 exactly-once 语义
- **Schema 推断与演化**: 自动推断 schema，支持新列自动添加
- **Rescue data column**: 不匹配 schema 的数据不会丢失，被捕获到 `_rescued_data` 列

### 2. 文件发现模式: Directory Listing vs File Notification

| 维度 | Directory Listing (默认) | File Notification |
|------|------------------------|-------------------|
| 启用方式 | 默认，无需配置 | `cloudFiles.useNotifications = true` |
| 原理 | 定期列举源目录，对比已处理文件列表 | 配置云厂商事件通知（S3 Events / Event Grid / Pub/Sub） |
| 适用规模 | 中小规模（百万级文件内） | 超大规模（十亿级文件） |
| 基础设施 | 零额外配置 | 需要云端队列/通知服务权限 |
| 延迟 | 略高（取决于列举频率） | 更低（事件驱动） |
| 成本 | 列举 API 调用有成本 | 通知服务有成本，但避免了列举开销 |

**Q108 考点**: 默认是 directory listing + 增量幂等加载到 Delta Lake，不是 file notification。

### 3. Schema 推断与演化

- **Schema inference**: Auto Loader 首次运行时推断 schema，存储到 `schemaLocation`
- **Schema evolution**: 通过 `cloudFiles.schemaEvolutionMode` 控制
  - `addNewColumns` (默认在 LDP 中): 新列自动添加
  - `rescue`: 不匹配的数据进入 `_rescued_data` 列
  - `failOnNewColumns`: 遇到新列时报错停止
  - `none`: 忽略新列
- **Schema enforcement**: 即使 JSON 格式正确，字段类型不匹配、额外字段、缺失字段都会导致记录被隔离

**Q257 考点**: 合法 JSON 被隔离的原因是 schema 不匹配，不是格式错误。

### 4. Rescue Data Column (`_rescued_data`)

当记录不完全匹配定义的 schema 时：
- 类型不匹配的字段值
- schema 中不存在的额外字段
- 这些数据被序列化为 JSON 字符串存入 `_rescued_data` 列
- 确保数据不会静默丢失

### 5. Auto Loader vs COPY INTO

| 维度 | Auto Loader | COPY INTO |
|------|------------|-----------|
| 执行模式 | Streaming（持续/trigger once） | Batch（SQL 命令） |
| 文件追踪 | 自动追踪（RocksDB checkpoint） | 通过文件元数据去重 |
| 性能 | 文件数量增长时性能稳定 | 文件数量多时性能下降 |
| Schema 演化 | 原生支持 | 不支持自动 schema 演化 |
| 适用场景 | 持续增量摄取 | 一次性/低频批量加载 |
| 推荐度 | Databricks 首选方案 | 仅在简单一次性加载时使用 |

### 6. Auto Loader + LDP 协作

Auto Loader 在 LDP (Lakeflow Declarative Pipelines) 中是推荐的摄取方式：

```python
@dlt.table
def bronze_raw():
    return (
        spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load("/path/to/source/")
    )
```

- LDP 自动管理 checkpoint、重试、多阶段编排
- Auto Loader 负责增量文件发现和摄取
- 组合后提供 exactly-once + schema evolution + 零运维

**Q289 考点**: LDP + Auto Loader + `schemaEvolutionMode=addNewColumns` 是满足"增量、schema 演化、exactly-once、最小运维"需求的最佳方案。

---

## 错题精析

### Q14 -- Type 1 表高效更新

**QUESTION 14**

An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema:

user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT

New records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.

Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

- A. Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.
- B. Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
- C. Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_login by user_id; write a merge statement to update or insert the most recent value for each user_id.
- D. Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.
- E. Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.

**我的答案:** A | **正确答案:** C

**错因分析:**
- 误以为 Auto Loader 适合这个场景，但 account_history 是一张 Delta 表，不是文件目录。Auto Loader 只能监听云存储中的文件（JSON/CSV/Parquet 等），不能"订阅"一张已有的 Delta 表的变更
- 正确思路：已知增量范围（最近一小时），直接过滤 + 按 user_id 取最新 + MERGE INTO，避免全表扫描
- 选项 B 每次 overwrite 百万级表，计算浪费；选项 E 按 username 去重而非 user_id（唯一键），逻辑错误

**核心教训:** Auto Loader 的输入源是云存储文件，不是 Delta 表。对 Delta 表的增量读取用 `readStream.table()` 或 CDF。

---

### Q108 -- Auto Loader 默认执行模式

**QUESTION 108**

Which statement describes the default execution mode for Databricks Auto Loader?

- A. Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; the target table is materialized by directly querying all valid files in the source directory.
- B. New files are identified by listing the input directory; the target table is materialized by directly querying all valid files in the source directory.
- C. Webhooks trigger a Databricks job to run anytime new data arrives in a source directory; new data are automatically merged into target tables using rules inferred from the data.
- D. New files are identified by listing the input directory; new files are incrementally and idempotently loaded into the target Delta Lake table.
- E. Cloud vendor-specific queue storage and notification services are configured to track newly arriving files; new files are incrementally and idempotently loaded into the target Delta Lake table.

**我的答案:** E | **正确答案:** D

**错因分析:**
- 选了 E 因为知道 Auto Loader 支持 file notification 模式且增量加载，但忽略了题目问的是"默认"模式
- 默认模式是 **directory listing**（目录列举），不需要云厂商的队列/通知服务
- File notification 模式需要显式设置 `cloudFiles.useNotifications = true`

**核心教训:** "默认"二字是关键。Auto Loader 默认 = directory listing + 增量幂等加载。

---

### Q242 -- LDP 声明式管道 vs Auto Loader + Structured Streaming

**QUESTION 242**

A data engineer is evaluating tools to build a production-grade data pipeline. The team must process change data from cloud object storage, filter out or isolate invalid records, and ensure the timely delivery of clean data to downstream consumers. The team is small, under tight deadlines, and wants to minimize operational overhead while keeping pipelines auditable and maintainable.

Which approach should the data engineer implement?

- A. Ingest data directly into Delta tables via Spark jobs, apply data quality filters using UDFs, and use LDP for creating Materialized Views.
- B. Use a hybrid approach: Ingest with Auto Loader into Bronze tables, then process using SQL queries in Databricks Workflows to generate cleaned Silver and Gold tables on a schedule.
- C. Implement ingestion using Auto Loader with Structured Streaming, and manage invalid data handling and table updates using checkpointing and merge logic.
- D. Use LDP to build declarative pipelines with Streaming Tables and Materialized Views, leveraging built-in support for data expectations and incremental processing.

**我的答案:** B | **正确答案:** D

**错因分析:**
- 选 B 是因为觉得 Auto Loader + Workflows 是更"实际"的方案，但忽略了题目强调"小团队、紧迫、最小运维"
- LDP 的核心价值正是声明式 + 自动编排 + 内置数据质量 expectations，运维开销最小
- 选项 C 虽然也用了 Auto Loader，但手动管理 checkpointing 和 merge logic 增加了运维复杂度
- 选项 B 的 SQL in Workflows 本质上是手动编排，与"最小运维"矛盾

**核心教训:** 当题目强调"最小运维 + 数据质量 + 可审计"时，答案几乎一定是 LDP。

---

### Q257 -- Auto Loader Schema 不匹配导致记录被隔离

**QUESTION 257**

A data engineer is using Auto Loader to read in JSON data as it arrives. They have configured Auto Loader to quarantine invalid JSON records. They are noticing that over time, some records are being quarantined even though they are well-formed JSON.

What is the cause of the missing data?

- A. The source data is valid JSON, but doesn't conform to their defined schema in some way.
- B. The badRecordsPath location is accumulating many small files.
- C. The engineer forgot to set the option "cloudFiles.quarantineMode", "rescue".
- D. At some point, the upstream data provider switched everything to multi-line JSON.

**我的答案:** C | **正确答案:** A

**错因分析:**
- 误以为存在一个 `cloudFiles.quarantineMode` 选项需要设置为 `rescue`，但这个选项并不存在
- Auto Loader 的隔离机制：不仅检查 JSON 格式是否合法，还检查数据是否符合定义的 schema
- 常见触发场景：上游新增字段、字段类型变更（如 string 变 int）、字段缺失

**核心教训:** "well-formed JSON" 不等于 "schema-conformant"。Auto Loader 按 schema 严格过滤，schema 不匹配的合法 JSON 照样被隔离。解决方案是用 `_rescued_data` 列或调整 schema evolution 策略。

---

### Q268 -- Streaming Table vs Materialized View 选择

**QUESTION 268**

A data engineer is designing a system leveraging Lakeflow Declarative Pipeline technology to process real-time truck telemetry data ingested from JSON files in S3 using Auto Loader. The data includes truck_id, timestamp, location, speed, and fuel_level. The system must support two use cases:
1. Near-real-time monitoring of the latest location, speed, and fuel_level per truck_id for the operations team.
2. Daily aggregated reports of total distance traveled and average fuel efficiency per truck_id for the management team.

Which approach should the data engineer use for streaming tables and materialized views in the Lakeflow Declarative Pipeline to meet these requirements?

- A. Streaming table for raw data, streaming table for daily aggregation, materialized view for real-time monitoring.
- B. Streaming table for raw data, materialized view for real-time monitoring, materialized view for daily aggregation.
- C. Streaming table for raw data, streaming table for real-time monitoring (incremental), materialized view for daily aggregation.
- D. Materialized view for raw data, streaming table for real-time monitoring, materialized view for daily aggregation.

**我的答案:** D | **正确答案:** C

**错因分析:**
- 选 D 因为错误地认为 materialized view 可以用于原始数据摄取
- 原始流数据摄取必须用 streaming table（Auto Loader 是流式源）
- 近实时监控需要增量更新 -> streaming table（因为数据持续到达，需要低延迟）
- 每日聚合报告 -> materialized view（定期批量重算即可，不需要持续流式处理）

**核心教训:**
- **Streaming table**: 适用于持续增量数据摄取、需要低延迟更新的场景
- **Materialized view**: 适用于定期批量聚合、可以完全重算的场景
- 原始数据从流式源（Auto Loader/Kafka）摄取 -> 永远用 streaming table

---

### Q289 -- LDP + Auto Loader + Schema Evolution

**QUESTION 289**

A data engineer is building a streaming data pipeline to ingest JSON files from cloud storage into a Delta Lake table. The pipeline must process files incrementally, handle schema evolution automatically, ensure exactly-once processing, and minimize manual infrastructure management.

How should the data engineer fulfill these requirements?

- A. Use Lakeflow Spark Declarative Pipelines with a static DataFrame read, merge schema with `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")`
- B. Use Auto Loader in batch mode with a daily job to overwrite the Delta table.
- C. Use Lakeflow Spark Declarative Pipelines with Auto Loader and enabling schema inference with `"cloudFiles.schemaEvolutionMode" = "addNewColumns"`
- D. Use traditional Spark Structured Streaming with Auto Loader, manually configuring checkpoints location and enabling schema inference with `"mergeSchema" = "true"`

**我的答案:** A | **正确答案:** C

**错因分析:**
- 选 A 使用 static DataFrame read，无法实现增量处理（streaming），违反核心需求
- 选 B 的 batch overwrite 既不增量也不 exactly-once
- 选 D 虽然可行，但"手动配置 checkpoint"与"最小化基础设施管理"矛盾
- 选 C 组合了 LDP（自动编排 + checkpoint 管理）+ Auto Loader（增量文件发现）+ `schemaEvolutionMode=addNewColumns`（自动 schema 演化），完美匹配所有需求

**核心教训:** 当需求同时包含"增量 + schema 演化 + exactly-once + 最小运维"时，LDP + Auto Loader 是标准答案。`schemaEvolutionMode=addNewColumns` 是 Auto Loader 的 schema 演化配置项。

---

### Q298 -- Auto Loader 摄取二进制文件（图片）

**QUESTION 298** (多选题)

A data engineer wants to ingest a large collection of image files (JPEG and PNG) from cloud object storage into Unity Catalog-managed table for further analysis and visualization.

Which two configurations and practices are recommended to incrementally ingest these images into the table? (Choose two.)

- A. Use Auto Loader and set cloudFiles.format to "TEXT"
- B. Use Auto Loader and set cloudFiles.format to "BINARYFILE"
- C. Use Auto Loader and set cloudFiles.format to "IMAGE"
- D. Use the pathGlobFilter option to select only image files (e.g., "*.jpg, *.png")
- E. Move files to a volume and read with SQL editor

**我的答案:** A | **正确答案:** B, D

**错因分析:**
- 选 A 用 TEXT 格式读取图片文件毫无意义，图片是二进制数据不是文本
- `BINARYFILE` 格式专门用于读取二进制文件（图片、PDF、模型文件等），会保留文件路径、修改时间、文件长度和二进制内容
- `IMAGE` 格式在 Auto Loader 中不存在
- `pathGlobFilter` 用于过滤特定文件扩展名，避免摄取非目标文件

**核心教训:** Auto Loader 支持的格式包括 json, csv, parquet, avro, text, binaryFile, orc。摄取非结构化二进制文件用 `BINARYFILE`，配合 `pathGlobFilter` 过滤文件类型。

---

## 核心对比表

### Auto Loader 文件发现模式

| 特性 | Directory Listing | File Notification |
|------|-------------------|-------------------|
| 默认? | 是 | 否 |
| 配置复杂度 | 零 | 需要云端权限和队列配置 |
| 扩展性上限 | 百万级文件 | 十亿级文件 |
| 成本模型 | 列举 API 按次收费 | 事件通知服务费用 |
| 延迟 | 列举间隔决定 | 近实时 |

### Streaming Table vs Materialized View (LDP 语境)

| 特性 | Streaming Table | Materialized View |
|------|----------------|-------------------|
| 数据源 | 流式源（Auto Loader, Kafka） | 任意表/视图 |
| 更新方式 | 增量追加 | 完全重算 |
| 延迟 | 低（近实时） | 高（定期刷新） |
| 适用场景 | 原始数据摄取、实时监控 | 聚合报告、维度表 |

### Auto Loader cloudFiles.format 支持的格式

| 格式 | 用途 | 典型场景 |
|------|------|---------|
| json | JSON 文件 | API 日志、事件数据 |
| csv | CSV 文件 | 传统数据导出 |
| parquet | Parquet 文件 | 数据湖标准格式 |
| avro | Avro 文件 | Kafka 消息存储 |
| text | 纯文本文件 | 日志文件 |
| binaryFile | 二进制文件 | 图片、PDF、模型文件 |
| orc | ORC 文件 | Hive 兼容系统 |

---

## 自测清单

- [ ] Auto Loader 的默认文件发现模式是什么？（directory listing）
- [ ] File notification 模式如何启用？（`cloudFiles.useNotifications = true`）
- [ ] Auto Loader 能监听 Delta 表的变更吗？（不能，只能监听云存储中的文件）
- [ ] 合法 JSON 被 Auto Loader 隔离的原因是什么？（schema 不匹配）
- [ ] `_rescued_data` 列的作用是什么？（捕获不匹配 schema 的字段数据）
- [ ] `cloudFiles.schemaEvolutionMode = "addNewColumns"` 做什么？（自动将新列添加到 schema）
- [ ] 用 Auto Loader 摄取图片文件应该用什么格式？（`BINARYFILE`）
- [ ] `pathGlobFilter` 选项的作用？（过滤特定文件扩展名）
- [ ] LDP 中原始流数据摄取用 streaming table 还是 materialized view？（streaming table）
- [ ] 近实时监控（持续更新）用哪个？（streaming table）
- [ ] 每日聚合报告用哪个？（materialized view）
- [ ] "增量 + schema 演化 + exactly-once + 最小运维"的标准方案是什么？（LDP + Auto Loader）
- [ ] Auto Loader vs COPY INTO，哪个在文件数量增长时性能更稳定？（Auto Loader）
- [ ] Type 1 SCD 表的增量更新最佳方式？（过滤增量 + MERGE INTO，不是 Auto Loader）
