# Delta Lake 核心机制 -- 深度复习笔记

**分类题数**: 21 题 | **错误率**: 100%（21/21 全错或未答）
**重要程度**: P0 -- 几乎一半错题涉及 Delta Lake，是所有其他主题的基础

---

## 一、概念框架：Transaction Log 驱动一切

### 核心心智模型

Delta Lake 不是"更好的 Parquet"。Delta Lake 的本质是：**Transaction Log (`_delta_log/`) 是表状态的唯一真相来源（single source of truth）**。每一个 Delta 特性都是 Transaction Log 的衍生行为。

```
写操作 --> 写新 Parquet 文件 + 写 _delta_log/N.json
读操作 --> 读 _delta_log --> 确定当前有效文件列表 --> 只读这些文件
VACUUM --> 删除不在有效文件列表中且超过保留期的物理文件
Time Travel --> 读历史版本的 _delta_log --> 得到那个时间点的文件列表
Data Skipping --> 读 _delta_log 中每个文件的 min/max 统计 --> 跳过不可能包含目标数据的文件
```

### Transaction Log 结构

- 每次 commit 写一个 JSON 文件（`_delta_log/000000N.json`）
- JSON 记录了 `add`（新增文件）和 `remove`（移除文件）操作
- 每 10 次 commit 生成一个 Parquet checkpoint 文件（加速读取）
- 表的当前状态 = 重放所有 commit 的结果

### 关键推论

1. **Data Skipping 的统计信息存在 Delta Log 中**，不是 Parquet file footer
2. **VACUUM 只删除物理文件**，不修改 Delta Log
3. **Time Travel 依赖旧文件未被 VACUUM 清理**
4. **ACID 是单表级别的**，不支持跨表事务
5. **Deletion Vectors 是 Delta Log 中的元数据标记**，不重写数据文件

---

## 二、错题精析

### 2.1 Data Skipping 机制（Q10, Q36）

#### Q10 -- Delta Lake Data Skipping 机制

**原题：**
A Delta table of weather records is partitioned by date and has the below schema:
`date DATE, device_id INT, temp FLOAT, latitude FLOAT, longitude FLOAT`
To find all the records from within the Arctic Circle, you execute a query with the below filter:
`latitude > 66.3`
Which statement describes how the Delta engine identifies which files to load?

**选项：**
A. All records are cached to an operational database and then the filter is applied
B. The Parquet file footers are scanned for min and max statistics for the latitude column
C. All records are cached to attached storage and then the filter is applied
D. The Delta log is scanned for min and max statistics for the latitude column
E. The Hive metastore is scanned for min and max statistics for the latitude column

**我的答案：** B | **正确答案：** D

**解析：**

错因分析：混淆了 Delta Lake 和纯 Parquet 的统计信息存储位置。

- **纯 Parquet**：统计信息存在每个文件的 footer 中，必须逐个打开文件才能获取（慢）
- **Delta Lake**：统计信息（min/max/null count）存在 `_delta_log/` 的 JSON 文件中（快）
- Delta 引擎读 Transaction Log 就能判断哪些文件可以跳过，根本不需要打开 Parquet 文件本身
- 此题中 date 分区不起作用，因为查询条件是 latitude 不是 date
- **核心区别：Delta Log 是集中式索引，Parquet footer 是分散式索引**

---

#### Q36 -- Delta Log Data Skipping（非分区列）

**原题：**
A Delta Lake table representing metadata about content posts from users has the following schema:
`user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATE`

This table is partitioned by the date column. A query is run with the following filter:
`longitude < 20 & longitude > -20`

Which statement describes how data will be filtered?

**选项：**
A. Statistics in the Delta Log will be used to identify partitions that might include files in the filtered range.
B. No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude.
C. The Delta Engine will use row-level statistics in the transaction log to identify the files that meet the filter criteria.
D. Statistics in the Delta Log will be used to identify data files that might include records in the filtered range.
E. The Delta Engine will scan the parquet file footers to identify each row that meets the filter criteria.

**我的答案：** A | **正确答案：** D

**解析：**

错因分析：混淆了"文件级跳过"和"分区级跳过"。

- 查询条件是 longitude（非分区列），所以**分区裁剪不适用**
- Delta Log 存储的是**文件级别**的列统计信息（min/max/null count），不是分区级
- 引擎用这些统计跳过不可能包含匹配记录的**数据文件**（data files），不是分区
- **A 错在 "identify partitions"** -- 跳过的粒度是文件，不是分区
- **C 错在 "row-level statistics"** -- 统计是文件级的，不是行级的
- **E 错在 "Parquet file footers"** -- Delta 用 Transaction Log，不用 footer

**Data Skipping 规则总结：**
- 默认只对前 32 列收集统计（可通过 `dataSkippingNumIndexedCols` 调整）
- 只有 min/max 有效的类型才有效（数值、日期、短字符串）
- 分区列通过 partition pruning 处理，非分区列通过 data skipping 处理
- 跳过的粒度是整个文件，不是行

---

### 2.2 Managed vs External Table（Q34, Q64, Q79）

#### Q34 -- External Table 创建方式

**原题：**
The data architect has mandated that all tables in the Lakehouse should be configured as external Delta Lake tables.
Which approach will ensure that this requirement is met?

**选项：**
A. Whenever a database is being created, make sure that the LOCATION keyword is used.
B. When configuring an external data warehouse for all table storage, leverage Databricks for all ELT.
C. Whenever a table is being created, make sure that the LOCATION keyword is used.
D. When tables are created, make sure that the EXTERNAL keyword is used in the CREATE TABLE statement.
E. When the workspace is being configured, make sure that external cloud object storage has been mounted.

**我的答案：** D | **正确答案：** C

**解析：**

错因分析：错误地将 Hive 语法迁移到 Delta Lake。

- Delta Lake 中创建 external table 的方式：在 `CREATE TABLE` 时指定 `LOCATION`
- 指定了 LOCATION --> 数据在指定路径 --> external table
- 不指定 LOCATION --> 数据在数据库默认路径 --> managed table
- **D 的 `EXTERNAL` 关键字在 Hive 中有效，但 Delta Lake 不用它来区分 managed/external**
- A 错：数据库的 LOCATION 只设置默认存储路径，不能保证每张表都是 external。是表级别的 LOCATION，不是数据库级别的

---

#### Q79 -- External table 创建方式（变体题）

**原题：**
The data architect has mandated that all tables in the Lakehouse should be configured as external (also known as "unmanaged") Delta Lake tables.
Which approach will ensure that this requirement is met?

**选项：**
A. When a database is being created, make sure that the LOCATION keyword is used.
B. When configuring an external data warehouse for all table storage, leverage Databricks for all ELT.
C. When data is saved to a table, make sure that a full file path is specified alongside the Delta format.
D. When tables are created, make sure that the EXTERNAL keyword is used in the CREATE TABLE statement.
E. When the workspace is being configured, make sure that external cloud object storage has been mounted.

**我的答案：** A | **正确答案：** C

**解析：**

同一考点，不同表述。这里 C 的描述是 DataFrame API 写法：
```python
df.write.format("delta").save("/path/to/data")
```
指定了完整文件路径 + delta 格式 = external table。

两次都错，第一次选了 EXTERNAL 关键字（Hive 思维），第二次选了数据库级 LOCATION（粒度错误）。

**External table 创建方式（两种）：**
1. SQL: `CREATE TABLE ... USING delta LOCATION '/path/to/data'`
2. DataFrame API: `df.write.format("delta").save("/path/to/data")`

---

#### Q64 -- DROP TABLE on managed table

**原题：**
A Delta Lake table was created with the below query:
Consider the following query:
`DROP TABLE prod.sales_by_store`
If this statement is executed by a workspace admin, which result will occur?

**选项：**
A. Nothing will occur until a COMMIT command is executed.
B. The table will be removed from the catalog but the data will remain in storage.
C. The table will be removed from the catalog and the data will be deleted.
D. An error will occur because Delta Lake prevents the deletion of production data.
E. Data will be marked as deleted but still recoverable with Time Travel.

**我的答案：** E | **正确答案：** C

**解析：**

错因分析：错误地认为 Delta 有"软删除"保护机制。

- 默认创建方式（无 LOCATION）= **managed table**
- DROP managed table：删除元数据 + **物理删除数据文件**
- DROP external table：只删除元数据，数据保留
- Delta Lake 不会自动阻止删除生产数据，权限控制由 ACL 负责
- **E 错误：Time Travel 依赖物理文件存在。managed table 被 DROP 后，物理文件被删除，Time Travel 无法恢复**

| 操作 | Managed Table | External Table |
|------|---------------|----------------|
| DROP TABLE | 删除元数据 + 删除数据文件 | 只删除元数据，数据保留 |
| 数据生命周期 | 由 Databricks 管理 | 由用户管理 |
| Time Travel after DROP | 不可用（文件已删除） | 可用（文件仍在） |

---

### 2.3 ACID 事务与约束（Q81, Q96）

#### Q81 -- Delta CHECK 约束违反导致整批写入失败

**原题：**
A CHECK constraint has been successfully added to the Delta table named activity_details. A batch job is attempting to insert new records to the table, including a record where latitude = 45.50 and longitude = 212.67 (violates the constraint).
Which statement describes the outcome of this batch insert?

**选项：**
A. The write will fail when the violating record is reached; any records previously processed will be recorded to the target table.
B. The write will fail completely because of the constraint violation and no records will be inserted into the target table.
C. The write will insert all records except those that violate the table constraints; the violating records will be recorded to a quarantine table.
D. The write will include all records in the target table; any violations will be indicated in the boolean column named valid_coordinates.
E. The write will insert all records except those that violate the table constraints; the violating records will be reported in a warning log.

**我的答案：** D | **正确答案：** B

**解析：**

错因分析：将 CHECK 约束理解为"软标记"而非"硬阻断"。

- Delta Lake 的 CHECK 约束是**写时强制约束**（write-time constraint）
- 一旦任何记录违反约束，整个批次**原子性失败回滚**，不写入任何记录
- 这是 ACID 的 Atomicity 保证：要么全部成功，要么全部失败
- **没有"部分写入"、"隔离违规记录"、"标记违规列"这些行为**
- Delta 的约束行为类似 RDBMS 的 CHECK 约束，不是数据质量工具的"标记模式"

---

#### Q96 -- Delta Lake ACID 仅针对单表，不强制外键约束

**原题：**
A junior data engineer is migrating a workload from a relational database system to the Databricks Lakehouse. The source system uses a star schema, leveraging foreign key constraints and multi-table inserts to validate records on write.
Which consideration will impact the decisions made by the engineer while migrating this workload?

**选项：**
A. Databricks only allows foreign key constraints on hashed identifiers, which avoid collisions in highly-parallel writes.
B. Databricks supports Spark SQL and JDBC; all logic can be directly migrated from the source system without refactoring.
C. Committing to multiple tables simultaneously requires taking out multiple table locks and can lead to a state of deadlock.
D. All Delta Lake transactions are ACID compliant against a single table, and Databricks does not enforce foreign key constraints.
E. Foreign keys must reference a primary key field; multi-table inserts must leverage Delta Lake's upsert functionality.

**我的答案：** X（未答） | **正确答案：** D

**解析：**

- Delta Lake ACID 事务是**单表级别**的，不支持跨表原子性事务
- 外键约束在 Delta Lake 中是 **informational only**（仅元数据声明，不强制执行）
- 从 RDBMS 迁移时，依赖 FK 约束和跨表事务的逻辑**必须重新设计**
- B 错误：不能直接迁移，需要重构
- C 错误：Delta 没有传统的行锁/表锁机制，用的是乐观并发控制

---

### 2.4 数据去重与 MERGE（Q75）

#### Q75 -- Delta Lake 去重（跨批次）

**原题：**
A data engineer is configuring a pipeline that will potentially see late-arriving, duplicate records.
In addition to de-duplicating records within the batch, which of the following approaches allows the data engineer to deduplicate data against previously processed records as it is inserted into a Delta table?

**选项：**
A. Set the configuration delta.deduplicate = true.
B. VACUUM the Delta table after each batch completes.
C. Perform an insert-only merge with a matching condition on a unique key.
D. Perform a full outer join on a unique key and overwrite existing data.
E. Rely on Delta Lake schema enforcement to prevent duplicate records.

**我的答案：** A | **正确答案：** C

**解析：**

错因分析：期望有一个简单开关来解决去重问题。

- 跨批次去重的标准做法：**MERGE INTO**（insert-only merge）
- 语法：`WHEN NOT MATCHED THEN INSERT`，只插入表中不存在的记录
- Matching condition 基于 unique key，已存在的记录被跳过
- **`delta.deduplicate` 配置不存在**（编造的选项）
- VACUUM 是清理旧文件，不是去重
- Schema enforcement 只检查列类型和名称，不检查值重复

```sql
MERGE INTO target t
USING source s
ON t.id = s.id
WHEN NOT MATCHED THEN INSERT *
```

---

### 2.5 Kafka 数据分区与 PII 管理（Q83, Q164）

#### Q83 / Q164 -- PII 数据隔离与保留策略（重复题）

**原题：**
All records from an Apache Kafka producer are being ingested into a single Delta Lake table with the following schema:
`key BINARY, value BINARY, topic STRING, partition LONG, offset LONG, timestamp LONG`
There are 5 unique topics being ingested. Only the "registration" topic contains PII. The company wishes to restrict access to PII and only retain PII records for 14 days, while retaining non-PII records indefinitely.

**Q83 我的答案：** B | **Q164 我的答案：** D
**正确答案：** E (Q83) / C (Q164)（同一答案，选项编号不同）

**解析：**

两次都错在同一个考点上。

- 正确做法：**按 `topic` 字段分区**
- 分区后，"registration" topic 的数据存储在独立的分区目录中
- 可以对该分区目录设置 ACL 限制访问
- 可以用 `DELETE WHERE topic='registration' AND timestamp < ...` 高效删除过期 PII 数据（利用分区裁剪）
- **B 错（两题都有类似选项）：`registration` 不是 schema 中的字段名**，schema 中只有 key/value/topic/partition/offset/timestamp
- **D 错（Q164）：不需要分离到不同存储容器**，分区已经实现了目录级隔离

**Kafka + Delta Lake PII 处理模式：**
1. 按 topic 分区 --> 物理隔离 PII 数据
2. ACL 设置在分区目录级别 --> 权限隔离
3. DELETE + partition pruning --> 高效定期清理 PII

---

### 2.6 Deletion Vectors（Q327）

#### Q327 -- Deletion Vectors 删除行为

**原题：**
A data engineer has a delta table order with deletion vectors enabled for it. The engineer is attempting to execute the below code:
`DELETE FROM orders WHERE status = 'cancelled'`
What should be the behaviour of deletion vectors when the command is executed?

**选项：**
A. Files are physically rewritten without the deleted row.
B. Rows are marked as deleted both in metadata and in files.
C. Rows are marked as deleted in metadata, not in files.
D. Delta automatically removes all cancelled orders permanently.

**我的答案：** X（未答） | **正确答案：** C

**解析：**

- 启用 Deletion Vectors 后，DELETE/UPDATE **不会重写底层 Parquet 文件**
- 而是在 Delta Log（元数据）中记录一个"删除向量"，标记哪些行被删除
- 读取时，Spark 自动过滤掉被标记的行（读时过滤）
- 原始文件保持不变 --> 大幅减少写放大
- 物理清理需要运行 `REORG TABLE ... APPLY (PURGE)` 或等待 Predictive Optimization

| 场景 | 无 Deletion Vectors | 有 Deletion Vectors |
|------|---------------------|---------------------|
| DELETE 操作 | 重写整个受影响的 Parquet 文件 | 只在元数据中标记删除行 |
| 写入开销 | 高（copy-on-write） | 低（只写元数据） |
| 读取开销 | 无额外开销 | 略有开销（需过滤标记行） |
| 物理清理 | 不需要 | 需要 REORG PURGE 或 OPTIMIZE |

---

### 2.7 VACUUM 与数据保留（Q294）

#### Q294 -- deletedFileRetentionDuration 设置方式（pending 题）

**原题：**
A data engineer is tasked with ensuring that a Delta table in Databricks continuously retains deleted files for 15 days (instead of the default 7 days), in order to permanently comply with the organization's data retention policy.
Which code snippet correctly sets this retention period for deleted files?

**选项：**
A. `spark.sql("ALTER TABLE my_table SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 15 days')")`
B. (未完整显示)
C. `spark.sql("VACUUM my_table RETAIN HOURS")`
D. `spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", "15 days")`

**我的答案：** D | **正确答案：** A

**解析：**

错因分析：混淆了 session 级配置和表级属性。

- `delta.deletedFileRetentionDuration` 是**表级属性**（table property），应通过 `ALTER TABLE SET TBLPROPERTIES` 设置
- 设置在表上可以**持久化**，确保"continuously"和"permanently"符合策略要求
- **D 错误：`spark.conf.set` 是 session 级配置**，只在当前 Spark session 有效，重启后失效，不满足"permanently comply"的要求
- C 错误：VACUUM 是手动执行的清理操作，不是设置保留策略
- 默认保留期是 7 天，这里需要延长到 15 天

**VACUUM 相关配置总结：**

| 配置 | 作用域 | 用途 |
|------|--------|------|
| `delta.deletedFileRetentionDuration` | 表属性 | 控制已删除文件保留多久（默认 7 天） |
| `delta.logRetentionDuration` | 表属性 | 控制 Delta Log 保留多久（默认 30 天） |
| `VACUUM table RETAIN n HOURS` | 手动命令 | 执行一次性清理 |

---

### 2.8 CTAS 行为（Q16）

#### Q16 -- CREATE TABLE AS SELECT (CTAS) 行为

**原题：**
A table is registered with the following code:
```sql
CREATE TABLE recent_orders AS (
  SELECT a.user_id, a.email, b.order_id, b.order_date
  FROM (SELECT user_id, email FROM users) a
  INNER JOIN (SELECT user_id, order_id, order_date FROM orders
    WHERE order_date >= (current_date() - 7)) b
  ON a.user_id = b.user_id
)
```
Both users and orders are Delta Lake tables. Which statement describes the results of querying recent_orders?

**选项：**
A. All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
B. All logic will execute when the table is defined and store the result of joining tables to the DBFS; this stored data will be returned when the table is queried.
C. Results will be computed and cached when the table is defined; these cached results will incrementally update as new records are inserted into source tables.
D. All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.
E. The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.

**我的答案：** D | **正确答案：** B

**解析：**

错因分析：混淆了 TABLE 和 VIEW 的行为。

- `CREATE TABLE ... AS SELECT` (CTAS) 创建的是**物化表**（managed table），不是视图
- CTAS 在定义时就执行查询，**将结果物理存储到 DBFS**
- 之后查询 recent_orders 返回的是**存储的静态数据**，不会重新执行 JOIN
- **D 描述的是 VIEW 的行为**，不是 TABLE 的行为
- 如果想要每次查询都重新计算，应该用 `CREATE VIEW`

| 类型 | 执行时机 | 数据存储 | 源表变更影响 |
|------|---------|---------|-------------|
| `CREATE TABLE AS SELECT` | 定义时执行一次 | 物理存储到 DBFS | 不受影响（快照） |
| `CREATE VIEW` | 每次查询时执行 | 不存储 | 实时反映源表变更 |
| `CREATE MATERIALIZED VIEW` (DLT) | 定义时 + 自动刷新 | 物理存储 | 自动增量更新 |

---

### 2.9 Delta 与 Iceberg 格式互操作（Q228）

#### Q228 -- Delta 转 Iceberg 格式

**原题：**
A data engineer has created a 'transactions' Delta table on Databricks that should be used by the analytics team. The analytics team wants to use the table with another tool which requires Apache Iceberg format.
What should the data engineer do?

**选项：**
A. Require the analytics team to use a tool which supports Delta table.
B. Create an Iceberg copy of the 'transactions' Delta table which can be used by the analytics team.
C. Convert the 'transactions' Delta to Iceberg and enable uniform so that the table can be read as a Delta table.
D. Enable uniform on the transactions table to 'iceberg' so that the table can be read as an Iceberg table.

**我的答案：** D | **正确答案：** B

**解析：**

错因分析：过度依赖 UniForm 特性，忽略了实际考试答案要求。

- 正确做法：创建一个独立的 Iceberg 格式副本，不影响原始 Delta 表的现有工作流
- D 看似合理（UniForm 确实可以让 Delta 表以 Iceberg 格式被读取），但本题的标准答案是创建独立副本
- 这道题的争议性较大，但考试中需要注意：**当题目没有提到 UniForm 作为前提时，倾向于选择"创建副本"**
- UniForm 是较新的功能，考试可能基于 UniForm 尚不成熟时的知识点

---

### 2.10 写入模式与表注册（Q7, Q187）

#### Q7 / Q187 -- MLflow 预测结果追加写入 Delta 表（重复题）

**原题：**
MLflow production model outputs preds DataFrame (`customer_id LONG, predictions DOUBLE, date DATE`). Save to Delta Lake table to compare all predictions across time. Churn predictions will be made at most once per day. Minimize compute costs.

**选项：**
A. `preds.write.mode("append").saveAsTable("churn_preds")`
B. `preds.write.format("delta").save("/preds/churn_preds")`

**我的答案：** B（两次） | **正确答案：** A（两次）

**解析：**

两次考同一题，两次都选错。

- 需求："保留所有历史预测以便跨时间比较" --> **append 模式**
- `saveAsTable` 将数据注册到 Hive metastore，便于后续 SQL 查询
- `save` 只写到文件路径，不注册元数据表
- **B 错误的两个原因：**
  1. `save()` 不注册 metastore 表
  2. 没有指定 mode，默认 `ErrorIfExists`，第二次运行会报错
- append 模式每天追加一次，计算成本最低（无需 merge 的额外扫描开销）

| 方法 | 注册 Metastore | 默认格式 | 默认写入模式 |
|------|---------------|---------|-------------|
| `saveAsTable("name")` | 是 | Delta (Databricks 默认) | ErrorIfExists |
| `save("/path")` | 否 | Parquet (开源 Spark 默认) | ErrorIfExists |

---

### 2.11 Databricks Repos / Git 协作（Q5, Q270）

#### Q5 -- Databricks Repos 分支不可见

**原题：**
A junior developer is using a personal branch with old logic. The desired branch `dev-2.3.9` is not available from the branch selection dropdown.

**我的答案：** C | **正确答案：** B

**解析：**
- 分支在 dropdown 不可见 --> 本地 repo 没有同步远程仓库的最新信息
- **先 pull 远程仓库**（同步分支列表），然后就能在 dropdown 里看到并选择 dev-2.3.9
- C 错：看不到的分支无法 checkout，且 "auto-resolve conflicts" 不是标准操作

---

#### Q270 -- Databricks Git Folder 团队协作最佳实践

**原题：**
A data engineering team needs to develop and test code independently before merging. They want to avoid accidental overwrites or branch switching issues while ensuring version control and CI/CD integration.

**选项：**
A. Each team member creates their own Databricks Git folder, mapped to the same remote Git repository, and works in their own development branch within their personal folder.
B. All team members work in the same Databricks Git folder.
C. Team members edit notebooks directly in the workspace's shared folder and periodically copy changes into a Git folder.
D. Team members use the Databricks CLI to clone the Git repository from a cluster's web terminal.

**我的答案：** C | **正确答案：** A

**解析：**
- 每人创建独立的 Git folder，映射到同一远程仓库，各自在自己的 branch 工作
- 避免了分支切换冲突和意外覆盖
- C 错：直接在 shared folder 编辑然后"定期复制"不是版本控制最佳实践

---

### 2.12 dbutils.secrets（Q6）

#### Q6 -- dbutils.secrets 行为

**原题：**
Security team tests `dbutils.secrets.get(scope="db_creds", key="jdbc_password")` for JDBC connection. They also `print(password)`.

**我的答案：** B | **正确答案：** E

**解析：**
- `dbutils.secrets.get()` 返回密码的**真实值**，JDBC 连接正常工作
- 但 Databricks 安全机制：secrets 值在 notebook 输出中会被替换为 `[REDACTED]`
- **值本身是正确的，只是显示时被遮蔽**
- 不会弹出交互输入框（B 的描述完全错误）

---

### 2.13 其他分类题目（Q221, Q260）

这两道题虽然被分类在 Delta Lake 下，但核心考点分别是 Databricks CLI 和 Job foreach task，此处简要记录。

#### Q221 -- Databricks CLI 复制 pipeline 配置

正确做法：`get` 现有 pipeline 配置 --> 移除 `pipeline_id` + 重命名 --> `create` 新 pipeline。不是 `list`。

#### Q260 -- foreach task 实现可扩展 REST API 下载

正确做法：使用 foreach task，以 report type 列表为输入，自动并行执行、独立追踪每个下载任务的成功/失败、支持部分重试。不是用 Delta 表或 Pandas UDF。

---

## 三、核心对比表

### 3.1 Delta Lake vs 纯 Parquet

| 特性 | 纯 Parquet | Delta Lake |
|------|-----------|------------|
| 事务支持 | 无 | ACID（单表级别） |
| 统计信息位置 | 每个文件的 footer | Transaction Log（集中式） |
| Data Skipping | 需逐文件读 footer | 读 Delta Log 即可 |
| Schema Enforcement | 无 | 写入时强制 |
| Time Travel | 不支持 | 支持（依赖保留文件） |
| 并发写入 | 可能覆盖/损坏 | 乐观并发控制 |

### 3.2 Managed vs External Table

| 特性 | Managed Table | External Table |
|------|---------------|----------------|
| 创建方式 | 不指定 LOCATION | 指定 LOCATION |
| 数据存储 | Databricks 管理的默认路径 | 用户指定的路径 |
| DROP TABLE | 删除元数据 + 删除数据文件 | 只删除元数据 |
| 适用场景 | 内部数据，生命周期由 Databricks 管理 | 共享数据，外部系统也需要访问 |
| 注意 | 不需要 EXTERNAL 关键字 | 不需要 EXTERNAL 关键字 |

### 3.3 Data Skipping vs Partition Pruning

| 特性 | Data Skipping | Partition Pruning |
|------|--------------|-------------------|
| 作用对象 | 非分区列 | 分区列 |
| 统计来源 | Delta Log 中的文件级 min/max | 目录结构 |
| 跳过粒度 | 数据文件 | 整个分区目录 |
| 适用列数 | 默认前 32 列 | 所有分区列 |
| 适用类型 | 数值、日期、短字符串 | 任意（但推荐低基数） |

### 3.4 Auto Compaction vs Optimized Writes vs OPTIMIZE

| 特性 | Optimized Writes | Auto Compaction | OPTIMIZE |
|------|-----------------|-----------------|----------|
| 触发时机 | 写入时 | 写入后（异步） | 手动执行 |
| 目标文件大小 | 自适应 | 128 MB | 1 GB |
| 行为 | 写入前重新平衡分区内数据 | 合并小文件到 128MB | 合并所有小文件到 ~1GB |
| Z-ORDER 支持 | 否 | 否 | 是 |
| 可同时启用 | 是 | 是 | 是 |

### 3.5 CTAS vs VIEW vs Materialized View

| 类型 | 执行时机 | 存储 | 源表变更 | 适用场景 |
|------|---------|------|---------|---------|
| `CREATE TABLE AS SELECT` | 定义时一次 | 物理存储 | 不跟踪 | 一次性快照 |
| `CREATE VIEW` | 每次查询时 | 不存储 | 实时反映 | 实时查询 |
| `CREATE MATERIALIZED VIEW` | 定义时 + 自动刷新 | 物理存储 | 增量更新 | DLT pipeline |

### 3.6 saveAsTable vs save

| 方法 | 注册 Metastore | 后续 SQL 查询 | 默认格式 |
|------|---------------|-------------|---------|
| `saveAsTable("name")` | 是 | `SELECT * FROM name` | Delta |
| `save("/path")` | 否 | 需手动 `CREATE TABLE USING delta LOCATION` | Parquet |

---

## 四、自测清单

完成复习后，不看笔记回答以下问题。每题必须能在 10 秒内给出准确答案：

### Transaction Log 基础
- [ ] Delta Lake 的每次写操作具体做了哪两件事？
- [ ] 表的当前状态如何从 Transaction Log 推导出来？
- [ ] Delta Log 的 checkpoint 文件的作用和生成频率？

### Data Skipping
- [ ] Data Skipping 的统计信息存在哪里？（不是 Parquet footer！）
- [ ] Data Skipping 跳过的粒度是什么？（文件，不是行，不是分区）
- [ ] 默认对多少列收集统计？哪些类型有效？
- [ ] 分区列的过滤走什么机制？非分区列呢？

### Managed vs External Table
- [ ] 创建 external table 的关键字是什么？（LOCATION，不是 EXTERNAL）
- [ ] 是表级别还是数据库级别？
- [ ] DROP managed table 和 DROP external table 的区别？
- [ ] Time Travel 在 DROP 之后还能用吗？（取决于 managed 还是 external）

### ACID 与约束
- [ ] Delta Lake 的 ACID 事务作用域是什么？（单表）
- [ ] 外键约束在 Delta Lake 中是否被强制执行？（不是，informational only）
- [ ] CHECK 约束违反时，batch 写入的结果是什么？（整批失败，原子性）

### Deletion Vectors
- [ ] 启用 Deletion Vectors 后，DELETE 操作会重写文件吗？（不会）
- [ ] 被删除的行在哪里标记？（元数据/Delta Log）
- [ ] 物理清理用什么命令？（REORG TABLE APPLY PURGE）

### VACUUM
- [ ] `delta.deletedFileRetentionDuration` 应该设置在哪里？（表属性，不是 spark.conf）
- [ ] 为什么 session 级配置不适合持久化保留策略？

### 写入模式
- [ ] `saveAsTable` 和 `save` 的核心区别？
- [ ] 跨批次去重的标准做法？（insert-only MERGE）
- [ ] CTAS 创建的是 TABLE 还是 VIEW？查询时会重新执行吗？

### 分区策略
- [ ] Kafka 多 topic 数据中隔离 PII 应按哪个字段分区？
- [ ] 分区后如何实现权限隔离和定期清理？
