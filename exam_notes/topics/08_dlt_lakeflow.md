# 08 - DLT / Lakeflow Declarative Pipelines

## 统计

| 指标 | 值 |
|------|-----|
| 总题数 | 6 |
| 错题数 | 6 |
| 正确率 | 0% |
| 涉及题号 | Q69, Q85, Q212, Q218, Q302, Q308 |

> 全部答错。根因：对 DLT 只有表面印象，缺乏对声明式框架执行模型和运维差异的深层理解。Q69/Q85 虽然不是纯 DLT 题，但涉及 pipeline 物化策略和集群事件监控，与 DLT pipeline 运维密切相关。

---

## 概念框架

### 1. LIVE TABLE vs STREAMING LIVE TABLE

| 维度 | LIVE TABLE | STREAMING LIVE TABLE |
|------|-----------|---------------------|
| 计算模式 | 全量重算（batch） | 增量处理（仅新数据） |
| 适用场景 | 维度表、聚合表、小表 | 事实表、日志流、大量追加数据 |
| 数据源要求 | 任意（Delta、CSV、API 等） | 必须是 append-only 或流式源 |
| 重启行为 | 完全重新计算 | 从 checkpoint 恢复，只处理未消费数据 |
| SQL 语法 | `CREATE OR REFRESH LIVE TABLE` | `CREATE OR REFRESH STREAMING LIVE TABLE` |
| Python 装饰器 | `@dlt.table()` | `@dlt.table()` + `spark.readStream` 或 `dlt.read_stream()` |

**关键判断标准**：数据源是否为 append-only？需不需要增量处理？如果是，用 STREAMING LIVE TABLE。

### 2. Expectations（数据质量约束）

DLT Expectations 是声明式数据质量规则，附加在表定义上：

| 模式 | 行为 | 适用场景 |
|------|------|---------|
| `EXPECT (expr)` / warn | 记录违规行数到 event log，不丢弃数据 | 监控阶段，了解数据质量 |
| `EXPECT (expr) ON VIOLATION DROP ROW` / drop | 丢弃不满足条件的行 | Silver 层清洗，过滤脏数据 |
| `EXPECT (expr) ON VIOLATION FAIL UPDATE` / fail | 整个 pipeline 失败 | 关键业务表，零容忍 |

**核心限制**：
- Expectations 只能引用当前表定义中的列，**不能直接跨表验证**
- 跨表验证的 workaround：创建临时表（`temporary=True`）做 left join，在该临时表上定义 expectation（Q218）
- 复用 expectations 的方式：将规则存储在 **Delta 表**中，通过 pipeline 参数传入 schema 名，动态读取（Q212）
  - DLT notebook 之间**不共享** Python 全局变量
  - DLT notebook **不支持** `%run` 或 `import` 其他 notebook

### 3. APPLY CHANGES INTO

用于 CDC（Change Data Capture）场景，自动处理 SCD Type 1 / Type 2：

```sql
APPLY CHANGES INTO LIVE.target_table
FROM STREAM(LIVE.source_cdc)
KEYS (id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY timestamp
-- SCD Type 2:
STORED AS SCD TYPE 2
```

- `KEYS`：主键，用于匹配记录
- `SEQUENCE BY`：排序列，确保 CDC 事件按正确顺序应用
- `APPLY AS DELETE WHEN`：定义删除条件
- `SCD TYPE 1`（默认）：直接覆盖旧值
- `SCD TYPE 2`：保留历史版本，自动维护 `__START_AT` / `__END_AT` 列

### 4. DLT vs Structured Streaming 运维差异（Q302 核心考点）

| 维度 | DLT / Lakeflow Declarative Pipelines | Structured Streaming |
|------|--------------------------------------|----------------------|
| 编程范式 | 声明式（定义"数据应该是什么"） | 命令式（定义"如何处理数据"） |
| Checkpoint 管理 | **自动管理** | 手动指定 `checkpointLocation` |
| 集群管理 | **自动创建/销毁** pipeline 集群 | 需要外部管理集群 |
| 多阶段编排 | **自动管理**依赖关系和执行顺序 | 需要 Jobs/Airflow 等外部编排 |
| 重试与恢复 | **自动重试**失败 task | 需要外部编排配置重试 |
| Schema evolution | 两者都可处理 | 两者都可处理 |
| 流处理能力 | 可以处理流数据 | 可以处理流数据 |
| 数据质量 | 内置 Expectations | 需要手动实现 |

**Q302 的教训**：DLT 和 Structured Streaming 的本质差异不在"能不能处理流"（两者都可以），也不在 schema evolution（两者都支持），而在**运维层面的自动化**——DLT 自动管理编排、依赖、checkpoint、集群、重试。

### 5. Event Log

DLT pipeline 的 event log 记录所有运行时信息：

- Pipeline 启动/停止事件
- 每张表的更新状态（成功/失败/跳过）
- Expectations 的违规统计（通过率、违规行数）
- 数据质量指标随时间的变化趋势
- 集群的 resize 事件

查询方式：
```sql
SELECT * FROM event_log(TABLE(my_pipeline.my_table))
```

注意区分：**Cluster Event Log**（Q85）记录集群生命周期事件（扩缩容、启动、终止），与 DLT event log 不同但概念相关。

### 6. 批处理与流处理统一（Q69 + Q308）

DLT / Lakeflow 的一个核心优势是**统一批处理和流处理**：

- Q308：PII 脱敏场景，用 Lakeflow 同时处理 batch（SFTP 文件）和 streaming（Kafka），确保一致的脱敏逻辑
- Q69：即使不用 DLT，也要理解**物化策略**的选择——对于每天刷新一次、多用户交互查询的场景，nightly batch job 预计算存表（而非 view 或实时流）是成本最优解

---

## 错题精析

### Q69 -- BI dashboard data refresh strategy

**QUESTION 69**

The business intelligence team has a dashboard configured to track various summary metrics for retail stores. This includes total sales for the previous day alongside totals and averages for a variety of time periods. The fields required to populate this dashboard have the following schema:

For demand forecasting, the Lakehouse contains a validated table of all itemized sales updated incrementally in near real-time. This table, named products_per_order, includes the following fields:

Because reporting on long-term sales trends is less volatile, analysts using the new dashboard only require data to be refreshed once daily. Because the dashboard will be queried interactively by many users throughout a normal business day, it should return results quickly and reduce total compute associated with each materialization.

Which solution meets the expectations of the end users while controlling and limiting possible costs?

- A. Populate the dashboard by configuring a nightly batch job to save the required values as a table overwritten with each update.
- B. Use Structured Streaming to configure a live dashboard against the products_per_order table within a Databricks notebook.
- C. Configure a webhook to execute an incremental read against products_per_order each time the dashboard is refreshed.
- D. Use the Delta Cache to persist the products_per_order table in memory to quickly update the dashboard with each query.
- E. Define a view against the products_per_order table and define the dashboard against this view.

**我的答案：E** | **正确答案：A**

**解析：**
- 需求三要素：(1) 每天刷新一次 (2) 交互查询要快 (3) 控制成本
- **A 正确**：nightly batch job 预计算聚合结果，存为物化表。查询时直接读预计算结果，速度快、成本低
- **E 错误（我选的）**：View 是逻辑定义，每次查询都触发完整的聚合计算。多用户交互查询时，每个用户每次查询都要重算，计算成本高、响应慢
- B 错误：Structured Streaming 持续运行，计算成本高，不符合"每天一次"的需求
- D 错误：Delta Cache 只缓存数据文件，不缓存聚合结果，仍需每次重算聚合

**根因**：混淆了 view（逻辑定义，惰性求值）和 materialized table（物理存储，预计算）。View 适合避免数据冗余和保证一致性，但不适合高频交互查询场景。

---

### Q85 -- Cluster resizing event timeline location

**QUESTION 85**

A distributed team of data analysts share computing resources on an interactive cluster with autoscaling configured. In order to better manage costs and query throughput, the workspace administrator is hoping to evaluate whether cluster upscaling is caused by many concurrent users or resource-intensive queries.

In which location can one review the timeline for cluster resizing events?

- A. Workspace audit logs
- B. Driver's log file
- C. Ganglia
- D. Cluster Event Log
- E. Executor's log file

**我的答案：B** | **正确答案：D**

**解析：**
- **Cluster Event Log** 专门记录集群基础设施层面的生命周期事件：upscaling、downscaling、启动、终止，带时间戳
- **Driver log**（我选的）记录的是 Spark 应用程序的 stdout/stderr 日志，不记录集群 resize 事件
- Ganglia 是实时性能监控（CPU、内存、网络），不保存历史 resize 事件
- Workspace audit logs 记录用户操作审计（谁登录了、谁创建了什么），不是集群 resize 来源
- Executor log 记录单个 executor 的运行日志

**根因**：没有区分"应用层日志"（Driver/Executor log）和"基础设施层日志"（Cluster Event Log）。

---

### Q212 -- Reusing DLT expectations across notebooks

**QUESTION 212**

A team of data engineers are adding tables to a DLT pipeline that contain repetitive expectations for many of the same data quality checks. One member of the team suggests reusing these data quality rules across all tables defined for this pipeline.

What approach would allow them to do this?

- A. Add data quality constraints to tables in this pipeline using an external job with access to pipeline configuration files.
- B. Use global Python variables to make expectations visible across DLT notebooks included in the same pipeline.
- C. Maintain data quality rules in a separate Databricks notebook that each DLT notebook or file can import as a library.
- D. Maintain data quality rules in a Delta table outside of this pipeline's target schema, providing the schema name as a pipeline parameter.

**我的答案：C** | **正确答案：D**

**解析：**
- **D 正确**：将规则存储在外部 Delta 表中，通过 pipeline 参数（`spark.conf.get("pipeline.schema_name")`）传入 schema 名，在 DLT notebook 中动态读取规则并应用为 expectations。这是 DLT 官方推荐的做法。
- **C 错误（我选的）**：DLT notebook 不支持 `%run` 或 `import` 其他 notebook 作为库。DLT 有自己的执行上下文，与普通 notebook 不同。这是 DLT 执行模型的核心限制。
- B 错误：DLT pipeline 中多个 notebook 不共享 Python 全局变量空间，每个 notebook 有独立的执行上下文
- A 错误：外部 job 无法直接修改 DLT pipeline 的 expectation 定义

**根因**：把 DLT notebook 当成普通 Databricks notebook 来理解。DLT 的执行模型是声明式的，不是命令式的，因此普通 notebook 的 `%run` / `import` 机制不适用。

---

### Q218 -- Cross-table validation with DLT expectations

**QUESTION 218**

A user wants to use DLT expectations to validate that a derived table report contains all records from the source, included in the table validation_copy.

The user attempts and fails to accomplish this by adding an expectation to the report table definition.

Which approach would allow using DLT expectations to validate all expected records are present in this table?

- A. Define a temporary table that performs a left outer join on validation_copy and report, and define an expectation that no report key values are null
- B. Define a SQL UDF that performs a left outer join on two tables, and check if this returns null values for report key values in a DLT expectation for the report table
- C. Define a view that performs a left outer join on validation_copy and report, and reference this view in DLT expectations for the report table
- D. Define a function that performs a left outer join on validation_copy and report, and check against the result in a DLT expectation for the report table

**我的答案：B** | **正确答案：A**

**解析：**
- **A 正确**：创建一个临时表（`@dlt.table(temporary=True)`），执行 `validation_copy LEFT JOIN report ON key`。如果 report 中缺少某条记录，则 join 结果中 report 的 key 列为 NULL。在该临时表上定义 expectation `EXPECT (report_key IS NOT NULL)`，即可检测缺失记录。临时表不会持久化到目标 schema，不产生额外存储成本。
- **B 错误（我选的）**：DLT expectations 的条件表达式不支持调用执行跨表 join 的 SQL UDF。Expectation 条件只能引用当前表定义中的列。
- C 错误：DLT view 不是 expectation 的数据源；expectation 必须定义在 table 或 streaming table 上
- D 错误：与 B 类似，expectation 条件中不能调用任意函数执行跨表操作

**核心模式**：DLT 跨表验证 = 临时表 + left join + expectation on null check。这是一个固定模式，需要记住。

---

### Q302 -- DLT vs Structured Streaming operational differences

**QUESTION 302**

How are the operational aspects of Lakeflow Spark Declarative Pipelines from Spark Structured Streaming different?

- A. Lakeflow Spark Declarative Pipelines automatically handle schema evolution, while Structured Streaming always requires manual schema management.
- B. Structured Streaming can process continuous data streams, while Lakeflow Spark Declarative Pipelines cannot.
- C. Lakeflow Spark Declarative Pipelines manage the orchestration of multi-stage pipelines automatically, while Structured Streaming requires external orchestration for complex dependencies.
- D. Lakeflow Spark Declarative Pipelines can write to Delta Lake format while structured streaming cannot.

**我的答案：B** | **正确答案：C**

**解析：**
- **C 正确**：DLT 自动管理编排（orchestration）、任务依赖（dependencies）、重试（retries）和多阶段 pipeline 执行。Structured Streaming 只负责流处理本身，多阶段 pipeline 需要 Jobs/Airflow 等外部工具编排。
- **B 错误（我选的）**：DLT **可以**处理连续数据流——STREAMING LIVE TABLE 就是用来处理流数据的。DLT 既支持批处理也支持流处理。
- A 错误：两者都支持 schema evolution（DLT 通过声明式定义，Structured Streaming 通过 `mergeSchema` 等选项）
- D 错误：Structured Streaming 完全可以写入 Delta Lake

**根因**：把"声明式"误解为"只能做批处理"。DLT 的声明式特性在于你定义数据应该长什么样，不关心执行方式；它完全可以在底层使用 Structured Streaming 来实现增量处理。

---

### Q308 -- PII masking pipeline design

**QUESTION 308**

An organization processes customer data from web and mobile applications. Data includes names, emails, phone numbers, and location history. Data arrives both as Batch files from an SFTP drop (daily) and Streaming JSON events from Kafka (real-time).

To comply with internal data privacy policies, the following requirements must be met:
- Personally identifiable information (PII) like email, phone_number, and ip_address must be masked or anonymized before storage
- Both batch and streaming pipelines must apply consistent PII handling
- Masking logic must be auditable and reproducible
- The masked data must still be usable for downstream analytics

How should the data engineer design a compliant data pipeline on Databricks that supports both batch and streaming modes, applies data masking to PII, and maintains traceability of transformations for audits?

- A. Ingest both batch and streaming data using Lakeflow Spark Declarative Pipelines, and apply masking via Unity Catalog column masks at read time to avoid modifying the data during ingestion.
- B. Load batch data with notebooks and ingest streaming data with SQL Warehouses; use Unity Catalog column masks on Silver tables to redact fields after storage.
- C. Use Lakeflow Spark Declarative Pipelines for batch and streaming ingestion, define a PII masking function, and apply it during Bronze ingestion before writing to Delta Lake.
- D. Allow PII to be stored unmasked in Bronze for lineage tracking, then apply masking logic in Gold tables used for reporting.

**我的答案：D** | **正确答案：C**

**解析：**
- **C 正确**：三个优势完美匹配题目需求：
  1. Lakeflow 统一批和流 -> "consistent PII handling"
  2. Bronze 写入前脱敏 -> PII 从不以明文存储（privacy by design）
  3. 声明式 pipeline 自动记录转换逻辑 -> "auditable and reproducible"
- **D 错误（我选的）**：PII 明文存储在 Bronze 层直接违反"masked before storage"的隐私要求。即使后续在 Gold 层脱敏，Bronze 层已经是数据泄露风险点。
- A 错误：UC column masks 是读时脱敏（read-time masking），数据本身仍以明文存储。题目要求"before storage"就脱敏。
- B 错误：批和流用不同框架处理（notebooks vs SQL Warehouses），无法保证"consistent PII handling"

**根因**：对 "privacy by design" 原则理解不够——PII 应该在写入的第一个环节就被脱敏，而不是"先存再治理"。

---

## 核心对比表

### DLT 数据质量规则复用 vs 跨表验证

| 场景 | 方案 | 关键技术 |
|------|------|---------|
| 多表复用相同 expectations | Delta 表存规则 + pipeline 参数传入 schema | `spark.conf.get()` 动态读取 |
| 跨表验证（A 包含 B 的所有记录） | 临时表 + left join + null check expectation | `temporary=True`, `LEFT JOIN` |
| 单表内数据质量 | 直接在表定义上加 `EXPECT` | warn / drop / fail |

### 物化策略选择

| 需求特征 | 方案 | 原因 |
|---------|------|------|
| 每天刷新 + 多用户交互查询 | Nightly batch job -> 物化表 | 预计算，查询快，成本低 |
| 实时更新 + 少量用户 | Structured Streaming | 实时性高 |
| 数据探索 + 不频繁查询 | View | 不占存储，总是最新 |
| 批+流统一 + 数据质量 | DLT / Lakeflow pipeline | 声明式管理，内置 expectations |

### PII 脱敏策略

| 方案 | 脱敏时机 | PII 是否明文存储 | 适用场景 |
|------|---------|----------------|---------|
| Bronze 层写入前脱敏函数 | Write-time | 否 | 严格合规，privacy by design |
| UC Column Masks | Read-time | 是（底层仍明文） | 灵活控制不同角色的可见性 |
| Gold 层才脱敏 | 下游处理时 | Bronze/Silver 明文 | 不推荐，合规风险 |

### 日志类型区分

| 日志类型 | 记录内容 | 查看位置 |
|---------|---------|---------|
| Cluster Event Log | 集群 resize、启动、终止等生命周期事件 | Cluster UI -> Event Log |
| Driver Log | Spark 应用程序 stdout/stderr | Cluster UI -> Driver Logs |
| DLT Event Log | Pipeline 运行状态、expectations 统计 | `event_log()` 函数查询 |
| Workspace Audit Log | 用户操作审计（登录、权限变更等） | Account Console / Log Delivery |
| Ganglia | 实时 CPU/内存/网络监控 | Cluster UI -> Metrics |

---

## 自测清单

### 概念理解

- [ ] 用一句话解释 LIVE TABLE 和 STREAMING LIVE TABLE 的区别
- [ ] Expectations 的三种模式（warn/drop/fail）分别在什么场景使用？
- [ ] 为什么 DLT notebook 不能用 `%run` 或 `import` 导入其他 notebook？
- [ ] DLT 如何跨多个 notebook 复用 expectations？（Delta 表 + pipeline 参数）
- [ ] DLT 如何实现跨表验证？（临时表 + left join + null check）
- [ ] APPLY CHANGES INTO 中 `SEQUENCE BY` 的作用是什么？
- [ ] DLT 和 Structured Streaming 的核心运维差异是什么？（编排 vs 只做流处理）

### 场景判断

- [ ] 每天刷新一次、多用户高频查询的 dashboard 应该用什么物化策略？（batch job -> 表）
- [ ] View 为什么不适合高频交互查询场景？（每次查询重算，成本高）
- [ ] PII 数据应该在哪个环节脱敏？为什么不能"先存后治理"？（Bronze 写入前，privacy by design）
- [ ] 如果需要统一批和流的 PII 处理逻辑，应该选什么框架？（Lakeflow）
- [ ] UC Column Masks 和写入时脱敏函数有什么本质区别？（read-time vs write-time）

### 易混淆点

- [ ] Cluster Event Log vs Driver Log vs DLT Event Log 分别记录什么？
- [ ] DLT "不能处理流数据" 这个说法对吗？（错，STREAMING LIVE TABLE 就是流处理）
- [ ] "DLT 自动处理 schema evolution 而 Structured Streaming 不能" 这个说法对吗？（错，两者都可以）
- [ ] DLT 和 Structured Streaming 的真正差异在哪？（运维自动化：编排、checkpoint、集群、重试）
