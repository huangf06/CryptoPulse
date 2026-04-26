# 04 - 性能优化与 Spark 调优

## 统计

| 指标 | 值 |
|------|-----|
| 总题数 | 14 |
| 错题数 | 14（含 1 道未答） |
| 正确率 | 0% |
| 涉及题号 | Q26, Q45, Q51, Q70, Q97, Q101, Q220, Q238, Q244, Q288, Q309, Q311, Q319, Q322 |

**诊断结论**：这是所有主题中正确率最低的领域。错误模式高度集中：(1) Spark UI 各 tab 功能混淆（反复选 A/Jobs tab 而非 SQL tab），(2) 数据布局优化技术（Z-Order vs Liquid Clustering vs Partition）选择错误，(3) 缺乏集群配置和内存管理的实战经验。

---

## 概念框架

### 1. Spark UI 各 Tab 功能

Spark UI 是诊断性能问题的核心工具。**必须记住每个 tab 看什么**：

| Tab | 看什么 | 典型诊断场景 |
|-----|--------|------------|
| **Jobs** | 所有 job 的执行时间和状态 | 找到最慢的 job；查看 job 整体执行概览 |
| **Stages** | 每个 stage 的 task 分布、shuffle read/write、spill 指标 | 诊断数据倾斜（task duration 分布不均）、shuffle 过大、disk spill |
| **SQL/DataFrame** | **查询执行计划（Physical Plan）** | 诊断 predicate push-down、join 策略、scan 范围 |
| **Storage** | 缓存的 RDD/DataFrame 的大小、存储级别 | 诊断缓存效率（Size on Disk vs Size in Memory） |
| **Executors** | 每个 executor 的 CPU、内存、GC、shuffle 使用 | 诊断内存不足、GC 过长、executor 挂掉 |

**核心记忆点**：
- 要看 **Physical Plan** --> **SQL/DataFrame tab**（不是 Jobs tab，不是 Executor log）
- 要看 **disk spill** --> **Stages tab**（Spill (Memory) 和 Spill (Disk) 列）
- 要看 **缓存状态** --> **Storage tab**
- 要看 **wall-clock 总耗时** --> **Query Profiler**（不是 Spark UI）

### 2. Query Profiler vs Spark UI

| 工具 | 用途 | 关键指标 |
|------|------|---------|
| **Spark UI** | 底层执行细节：stages、tasks、shuffle、spill、Physical Plan | Task Duration, Shuffle Read/Write, Spill (Memory/Disk) |
| **Query Profiler** | 高层查询分析：总耗时、查询来源 | **Total wall-clock duration**（真实经过时间）, Aggregated task time（所有 task 累加时间，因并行化通常远大于 wall-clock） |

### 3. Predicate Push-Down 诊断

- **在哪看**：Spark UI --> SQL/DataFrame tab --> 点击具体 query --> Physical Plan
- **怎么看**：在 Scan 节点中查找 `PushedFilters`
  - 如果 `PushedFilters: [IsNotNull(col), GreaterThan(col, value)]` --> push-down 生效
  - 如果 `PushedFilters: []` 或 filter 在 Scan 之后 --> push-down 未生效
- **不在哪看**：不在 Executor log（没有 push-down 信息）、不在 Stage 的 Input Size（间接指标）、不在 Delta transaction log（文件级统计，不是执行计划）

### 4. Disk Spill 诊断与解决

**根因**：executor 内存不足以容纳 shuffle/join/aggregation 的中间数据。

**解决方案（双管齐下）**：
1. **增加每个 core 的内存**：选择 memory-optimized 实例（更高的 core:memory 比，如 1:4 或 1:8，而非 1:2）
2. **减小每个 task 处理的数据量**：减小 `spark.sql.files.maxPartitionBytes`，使每个分区更小

**不是解决方案**：
- 增加磁盘空间（只是让 spill 有更多地方放，不减少 spill）
- 增加网络带宽（与 spill 无关）

### 5. 数据布局优化技术

| 技术 | 适用场景 | 高基数列 | 增量优化 | 可演进 | 缺点 |
|------|---------|---------|---------|--------|------|
| **Hive-Style Partition** | 低基数列（日期、地区） | 不适合（小文件问题） | 否（需全量重分区） | 差 | 高基数列导致过多小文件 |
| **Z-Order** | 中高基数列，已知查询模式 | 适合 | 部分（每次 OPTIMIZE 需重写涉及数据） | 中等 | 每次 OPTIMIZE 开销较大 |
| **Liquid Clustering** | 高基数列，查询模式变化 | **最佳** | **是（增量）** | **最佳** | 较新特性，需要 DBR 13.3+ |

**选择原则**：
- 题目说"incremental, easy to maintain, evolvable" --> **Liquid Clustering**
- 题目说"immediate data skipping" + 没有强调增量/演进 --> **Z-Order**
- 题目说低基数列 --> **Hive-Style Partition**

### 6. 集群配置原则

**Wide Transformation（shuffle）场景**：
- 更多小节点 > 少量大节点
- 原因：(1) JVM 管理小堆内存更高效，GC 压力小；(2) 更多节点 = 更多并行 shuffle 通道
- 但也不是越多越好，需要平衡 shuffle 网络开销

### 7. 文件大小控制

**无 shuffle 场景**：
- 用 `spark.sql.files.maxPartitionBytes` 控制读取时的分区大小
- 只执行 narrow transformation --> 分区数不变 --> 写出文件大小由此参数控制

**有 shuffle 场景**：
- 用 `spark.sql.shuffle.partitions` 或 AQE 的 `advisoryPartitionSizeInBytes` 控制 shuffle 后分区大小
- `repartition()` 触发 shuffle，`coalesce()` 不触发 shuffle 但只能减少分区数

---

## 错题精析

### A. Spark UI 与性能诊断（Q51, Q220, Q244, Q101, Q309）

#### Q51 -- Spark UI 诊断 predicate push-down

**原题：**
> Where in the Spark UI can one diagnose a performance problem induced by not leveraging predicate push-down?

**选项：**
- A. In the Executor's log file, by grepping for "predicate push-down"
- B. In the Stage's Detail screen, in the Completed Stages table, by noting the size of data read from the Input column
- C. In the Storage Detail screen, by noting which RDDs are not stored on disk
- D. In the Delta Lake transaction log, by noting the column statistics
- E. In the Query Detail screen, by interpreting the Physical Plan

**我的答案：** A | **正确答案：** E

**解析：**
- Predicate push-down 是否生效需要查看 **Physical Plan**，位于 Spark UI 的 **SQL/DataFrame tab** -> Query Detail 页面。
- Physical Plan 中 Scan 节点的 `PushedFilters` 字段直接显示哪些过滤条件被下推到数据源。
- Executor log 不包含 push-down 诊断信息（选 A 是臆测）。
- 这道题我两次都选了 A，说明对 Spark UI 结构完全不熟悉，纯靠猜测。

**错因根源**：没有实际使用过 Spark UI，不知道各 tab 的功能划分。

---

#### Q220 -- Spark UI 中 Physical Plan 诊断 predicate pushdown 缺失

**原题：**
> Where in the Spark UI can one diagnose a performance problem induced by not leveraging predicate push-down?

**选项：**
- A. In the Executor's log file, by grepping for "predicate push-down"
- B. In the Stage's Detail screen, in the Completed Stages table, by noting the size of data read from the Input column
- C. In the Query Detail screen, by interpreting the Physical Plan
- D. In the Delta Lake transaction log, by noting the column statistics

**我的答案：** A | **正确答案：** C

**解析：**
- 与 Q51 是同一道题的变体（4 选项 vs 5 选项），两次都错选了 A。
- **记忆锚点**：Physical Plan = SQL/DataFrame tab = Query Detail screen。三个词是同一个位置的不同表述。
- Executor log 是看报错和运行日志的，不是看执行计划的。

---

#### Q244 -- Spark UI 查询计划可视化

**原题：**
> When monitoring a complex workload, being able to see the query plan is critical to understanding what the workload is doing. Where can the visualization of the query plan be found?

**选项：**
- A. In the Spark UI, under the Jobs tab
- B. In the Query Profiler, under Query Source
- C. In the Spark UI, under the SQL/DataFrame tab
- D. In the Query Profiler, under the Stages tab

**我的答案：** A | **正确答案：** C

**解析：**
- 又选了 Jobs tab。Jobs tab 显示的是 job 列表和执行时间，不是查询计划。
- SQL/DataFrame tab 才展示逻辑和物理查询计划的可视化。
- **三次同类题，三次选错同一个方向**（A/Jobs/Executor），说明脑中有一个顽固的错误关联：query plan = Jobs tab。必须强制覆盖这个记忆。

**记忆强化**：query plan 只在 **SQL** tab 看，绝不在 Jobs tab。

---

#### Q101 -- MEMORY_ONLY 缓存异常信号

**原题：**
> Which indicators would you look for in the Spark UI's Storage tab to signal that a cached table is not performing optimally? Assume you are using Spark's MEMORY_ONLY storage level.

**选项：**
- A. Size on Disk is < Size in Memory
- B. The RDD Block Name includes the "*" annotation signaling a failure to cache
- C. Size on Disk is > 0
- D. The number of Cached Partitions > the number of Spark Partitions
- E. On Heap Memory Usage is within 75% of Off Heap Memory Usage

**我的答案：** X（未答） | **正确答案：** C

**解析：**
- **MEMORY_ONLY** 存储级别意味着数据应该完全在内存中，磁盘上不应有任何数据。
- 如果 Storage tab 显示 "Size on Disk > 0"，说明缓存行为异常 -- 可能是内存不足导致部分分区被驱逐或溢写。
- 选 B 错误：Spark UI 没有 "*" 注解机制。
- 这道题的关键是理解存储级别的语义：MEMORY_ONLY = 只在内存 = disk 应该为 0。

---

#### Q309 -- 查询执行总耗时的查看方法

**原题：**
> Which method can be used to determine the total wall-clock time it took to execute a query?

**选项：**
- A. In the Spark UI, take the job duration of the longest-running job associated with that query
- B. In the Spark UI, take the sum of all task durations that ran across all stages/jobs
- C. Open the Query Profiler associated with that query and use the Total wall-clock duration metric
- D. Open the Query Profiler associated with that query and use the Aggregated task time metric

**我的答案：** D | **正确答案：** C

**解析：**
- **Total wall-clock duration** = 从查询开始到完成的真实经过时间。
- **Aggregated task time** = 所有 task 执行时间的总和。由于 task 是并行执行的，这个值通常远大于 wall-clock 时间，不能代表查询实际耗时。
- 选 A 也不对：一个 query 可能包含多个 job，最长 job 的 duration 不等于整个 query 的 wall-clock 时间。
- **记忆点**：wall-clock time 用 wall-clock duration 指标，不是 aggregated task time。名字就是答案。

---

### B. 数据布局优化（Q238, Q319, Q288）

#### Q238 -- Z-Order 优化高基数列查询

**原题：**
> A data team is working to optimize an existing large, fast-growing table 'orders' with high cardinality columns, which experiences significant data skew and requires frequent concurrent writes. The columns 'user_id', 'event_timestamp' and 'product_id' are heavily used in analytical queries and filters, although those keys may be subject to change in the future.
> Which partitioning strategy should the team choose to optimize the table for immediate data skipping, incremental management over time, and flexibility?

**选项：**
- A. ALTER TABLE orders PARTITION BY user_id, product_id, event_timestamp
- B. OPTIMIZE orders ZORDER BY (user_id, product_id) WHERE event_timestamp = current date () - 1 DAY
- C. ALTER TABLE orders CLUSTER BY user_id, product_id, event_timestamp
- D. OPTIMIZE orders ZORDER BY (user_id, product_id, event_timestamp)

**我的答案：** C | **正确答案：** D

**解析：**
- Z-Order 通过 OPTIMIZE 命令实现 data skipping 优化，适合高基数列，且不需要物理重分区。
- 选 C 的 Liquid Clustering（CLUSTER BY）理论上也可以，但这道题的正确答案是 D。关键区别：题目说 "data skew" + "frequent concurrent writes"，Z-Order 在这种场景下更灵活，避免了 Liquid Clustering 可能引入的写入冲突。
- 选 A 错误：Hive-Style Partition 对高基数列（user_id, product_id）会产生大量小文件。
- 选 B 错误：只 Z-Order 了两个列，遗漏了 event_timestamp。

**注意**：Q238 和 Q319 是同一类型题但答案不同。差异在于题目的侧重点：Q238 强调 "immediate data skipping"，Q319 强调 "incremental, easy to maintain, evolvable"。

---

#### Q319 -- 高基数列的数据布局优化

**原题：**
> Business users query a managed Delta table with high-cardinality column filters via SQL Serverless Warehouse. The engineer needs an incremental, easy-to-maintain, evolvable data layout optimization technique. Which command?

**选项：**
- A. Hive Style Partitions + Z-ORDER with periodic OPTIMIZE
- B. Hive Style Partitions with periodic OPTIMIZE
- C. Z-ORDER with periodic OPTIMIZE
- D. Liquid Clustering with periodic OPTIMIZE

**我的答案：** A | **正确答案：** D

**解析：**
- 题目关键词：**incremental（增量）、easy to maintain（易维护）、evolvable（可演进）** --> 直指 Liquid Clustering。
- Liquid Clustering 的三大优势正好对应题目要求：
  - 增量优化：每次 OPTIMIZE 只处理新数据和需要重新聚簇的数据
  - 易维护：不需要手动管理分区或重建
  - 可演进：`ALTER TABLE ... CLUSTER BY` 可以随时更换聚簇列
- 选 A 错误：Hive-Style Partition 对高基数列会产生小文件问题，且分区列一旦设定就很难更改（不 evolvable）。
- 选 C 的 Z-Order 虽然支持高基数，但每次 OPTIMIZE 需要重写大量数据（不够 incremental），且切换 Z-Order 列需要全表重写。

---

#### Q288 -- MERGE 操作性能优化（多选）

**原题：**
> A data engineer is optimizing a MERGE operation on an 800GB UC-managed table that experiences frequent updates and deletions. Which two actions should the engineer prioritize to improve MERGE performance? (Choose two.)

**选项：**
- A. Apply liquid clustering using the merge join keys
- B. Enable deletion vectors on the table if not already enabled
- C. Partition the table by date
- D. Use ZORDER on high-cardinality columns
- E. Overwrite the table instead of Merge

**我的答案：** X（未答） | **正确答案：** AB

**解析：**
- **A. Liquid Clustering on merge join keys**：使 merge key 相同的数据在物理上紧密存储，减少 MERGE 扫描范围。对于频繁 update/delete 的表，Liquid Clustering 比 Z-Order 更适合（增量优化，不需要全表重写）。
- **B. Deletion Vectors**：启用后，update/delete 操作不需要重写整个 Parquet 文件，只需标记被删除的行，大幅减少 MERGE 的 I/O 开销。这是 UC-managed table 的重要优化手段。
- 选 C 错误：按日期分区对 MERGE 性能帮助有限，且 MERGE 的 key 通常不是日期。
- 选 D 错误：Z-Order 对 MERGE 操作的优化不如 Liquid Clustering 直接。
- 选 E 错误：全量覆盖失去了 MERGE 的增量更新语义。

---

### C. 集群配置与内存管理（Q26, Q311, Q70）

#### Q26 -- 集群配置与 Wide Transformation 性能

**原题：**
> Each configuration below is identical to the extent that each cluster has 400 GB total of RAM, 160 total cores and only one Executor per VM. Given a job with at least one wide transformation, which of the following cluster configurations will result in maximum performance?

**选项：**
- A. Total VMs: 1, 400 GB per Executor, 160 Cores/Executor
- B. Total VMs: 8, 50 GB per Executor, 20 Cores/Executor
- C. Total VMs: 16, 25 GB per Executor, 10 Cores/Executor
- D. Total VMs: 4, 100 GB per Executor, 40 Cores/Executor
- E. Total VMs: 2, 200 GB per Executor, 80 Cores/Executor

**我的答案：** A | **正确答案：** C（社区投票 C 37%, A 28%, B 26%）

**解析：**
- Wide transformation 需要 shuffle，数据在节点间传输。
- 1 台 VM（A）：所有 shuffle 在单机内完成，但 JVM 管理 400GB 堆内存的 GC 压力极大，效率极低。
- 16 台 VM（C）：每台 25GB/10 cores，JVM 堆内存管理高效，GC 暂停短，shuffle 可以在更多节点间并行进行。
- **原则**：分布式计算中，总资源相同时，更多小节点 > 少量大节点（尤其是有 shuffle 的场景）。但要平衡网络开销，不是无限细分。

---

#### Q311 -- 减少磁盘溢出的优化方法（多选）

**原题：**
> A query has significant disk spill with a 1:2 core-to-memory ratio instance. What two steps should minimize spillage? (Choose two.)

**选项：**
- A. Increase spark.sql.files.maxPartitionBytes
- B. Choose a compute instance with more disk space
- C. Choose a compute instance with a higher core-to-memory ratio
- D. Reduce spark.sql.files.maxPartitionBytes
- E. Choose a compute instance with more network bandwidth

**我的答案：** C（只选了一个） | **正确答案：** CD

**解析：**
- **C. 更高 core-to-memory 比**：当前 1:2（每核 2GB），切换到 1:4 或 1:8 的 memory-optimized 实例，每个 core 分到更多内存，减少 spill。
- **D. 减小 maxPartitionBytes**：每个分区更小 --> 每个 task 处理的数据更少 --> 峰值内存需求降低 --> 减少 spill。
- 选 A（增大 maxPartitionBytes）会让每个 task 处理更多数据，加剧 spill。
- 选 B（更多磁盘空间）只是给 spill 更多空间，不减少 spill。
- **教训**：多选题必须选够数量。我只选了 C，漏选了 D。

---

#### Q70 -- Parquet 文件大小控制（无 shuffle）

**原题：**
> A data ingestion task requires a one-TB JSON dataset to be written out to Parquet with a target part-file size of 512 MB. Because Parquet is being used instead of Delta Lake, built-in file-sizing features cannot be used. Which strategy will yield the best performance without shuffling data?

**选项：**
- A. Set spark.sql.files.maxPartitionBytes to 512 MB, ingest the data, execute the narrow transformations, and then write to parquet.
- B. Set spark.sql.shuffle.partitions to 2,048, ingest, narrow transforms, sort (which repartitions), write.
- C. Set spark.sql.adaptive.advisoryPartitionSizeInBytes to 512 MB, ingest, narrow transforms, coalesce to 2,048, write.
- D. Ingest, narrow transforms, repartition to 2,048, write.
- E. Set spark.sql.shuffle.partitions to 512, ingest, narrow transforms, write.

**我的答案：** C | **正确答案：** A

**解析：**
- 题目要求**不 shuffle**。
- `spark.sql.files.maxPartitionBytes` 控制**读取时**每个 partition 的最大字节数。设为 512MB，则读取 1TB 数据时产生约 2048 个 partition，每个 ~512MB。
- 只执行 narrow transformation（不触发 shuffle），partition 数量和大小保持不变，写出的 Parquet 文件大小就是 ~512MB。
- 选 B：`sort()` 触发 shuffle -- 违反要求。
- 选 C：`coalesce()` 虽不 shuffle，但 `advisoryPartitionSizeInBytes` 是 AQE 参数，用于 shuffle 后的优化，在无 shuffle 场景下不生效。而且 coalesce 只能减少分区数，不能增加。
- 选 D：`repartition()` 触发 shuffle -- 违反要求。

**知识点辨析**：
| 参数 | 作用阶段 | 是否 shuffle |
|------|---------|-------------|
| `spark.sql.files.maxPartitionBytes` | 读取时 | 否 |
| `spark.sql.shuffle.partitions` | shuffle 后 | 是（设置 shuffle 后的分区数） |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | AQE shuffle 后 | 是（AQE 优化 shuffle 后分区大小） |

---

### D. Delta Lake 性能相关（Q97, Q45）

#### Q97 -- Delta time travel 不适合长期版本化方案

**原题：**
> Architect wants Type 1 table + Delta time travel for long-term auditing. Engineer prefers Type 2. Which information is critical to this decision?

**选项：**
- A. Data corruption can occur if a query fails in a partially completed state with Type 2 tables.
- B. Shallow clones combined with Type 1 tables can accelerate historic queries.
- C. Delta Lake time travel cannot be used to query previous versions because Type 1 changes modify data files in place.
- D. Delta Lake time travel does not scale well in cost or latency to provide a long-term versioning solution.
- E. Delta Lake only supports Type 0 tables.

**我的答案：** C | **正确答案：** D

**解析：**
- Delta time travel 依赖保留历史版本的数据文件（旧 Parquet files + transaction log entries）。长期保留意味着：
  - 存储成本线性增长（旧文件不能 VACUUM）
  - 查询历史版本的延迟随版本数增加而增加
- 选 C 错误：Delta 的 MVCC 机制会保留旧版本文件，即使 Type 1（overwrite/update）修改了数据，time travel 仍然可以查询历史版本。Type 1 不是 in-place 修改文件，而是写入新文件 + 更新 log。
- **结论**：长期审计应使用 Type 2 SCD（保留历史行作为普通记录），而非依赖 Delta time travel。

---

#### Q45 -- Managed table with custom location

**原题：**
> An external storage container is mounted to /mnt/finance_eda_bucket. A database is created with LOCATION pointing to this mount. A team member creates a table without specifying LOCATION or EXTERNAL. How will the table be created?

**选项：**
- A. A logical table (query plan) in Hive Metastore
- B. An external table in the mounted storage
- C. A logical table (physical plan) in Hive Metastore
- D. A managed table in the mounted storage
- E. A managed table in DBFS root

**我的答案：** B | **正确答案：** D

**解析：**
- 当 database 用 `LOCATION` 指定了存储路径时，该 database 下创建的 managed table 会存储在该路径。
- 没有 `EXTERNAL` 关键字 = managed table（Databricks 管理元数据和数据）。
- 选 B 错误：没有 `LOCATION` 关键字在 CREATE TABLE 语句中，不会创建 external table。table 类型由是否使用 EXTERNAL/LOCATION 在 CREATE TABLE 中决定，不是由 database 的 LOCATION 决定。
- **关键区别**：database LOCATION 决定数据存储位置，CREATE TABLE 的 EXTERNAL/LOCATION 决定表类型。

---

### E. Pandas UDF（Q322）

#### Q322 -- Pandas UDF 分组状态处理选择

**原题：**
> A data engineer needs to design a Pandas UDF to process financial time series data with complex calculations requiring state across rows within each stock symbol group. Which approach has minimum overhead while preserving data integrity?

**选项：**
- A. SCALAR_ITER Pandas UDF with state persisted to Delta tables between batches
- B. GROUPED_AGG UDF with state via broadcast variables between UDF calls
- C. SCALAR Pandas UDF with global variables shared across executors
- D. ApplyInPandas method receiving all rows per group as a pandas DataFrame, with local state variables

**我的答案：** A | **正确答案：** D

**解析：**
- `applyInPandas`（grouped map）将同一 group 的所有行作为一个完整的 pandas DataFrame 传入函数，可以在函数内自然维护局部状态变量。
- 选 A 错误：用 Delta 表做持久化状态管理引入不必要的 I/O 开销。
- 选 B 错误：broadcast 变量是只读的，不适合逐组更新状态。
- 选 C 错误：全局变量在多个 executor 之间共享是不安全的（executor 是独立 JVM 进程）。
- **记忆点**：分组内有状态计算 = `applyInPandas`（grouped map）。

---

## 核心对比表

### 1. Spark UI Tab 速查

| 我想诊断... | 去哪个 Tab | 看什么指标 |
|------------|-----------|-----------|
| predicate push-down 是否生效 | **SQL/DataFrame** | Physical Plan 中的 PushedFilters |
| 查询计划可视化 | **SQL/DataFrame** | Physical Plan DAG |
| disk spill | **Stages** | Spill (Memory), Spill (Disk) |
| 缓存是否正常 | **Storage** | Size on Disk（MEMORY_ONLY 下应为 0） |
| GC 问题 | **Executors** | GC Time |
| 最慢的 job | **Jobs** | Duration |
| wall-clock 总耗时 | **Query Profiler**（非 Spark UI） | Total wall-clock duration |

### 2. Memory-Optimized vs Compute-Optimized 实例

| 特征 | Memory-Optimized | Compute-Optimized |
|------|-----------------|-------------------|
| Core:Memory 比 | 1:8 或更高 | 1:2 或 1:4 |
| 适用场景 | 大量 shuffle/join/聚合、容易 disk spill | CPU 密集型计算、ML 训练 |
| Disk Spill 风险 | 低 | 高 |
| 典型实例 | r5.xlarge (4 core, 32GB) | c5.xlarge (4 core, 8GB) |

### 3. 数据布局优化技术对比

| 特征 | Hive Partition | Z-Order | Liquid Clustering |
|------|---------------|---------|-------------------|
| 适用基数 | 低（<1000 值） | 中高 | 高 |
| 增量优化 | 否 | 部分 | 是 |
| 列可变更 | 不可（需重建表） | 可（下次 OPTIMIZE 时生效） | 可（ALTER TABLE CLUSTER BY） |
| MERGE 优化 | 差 | 中等 | 好 |
| 写入影响 | 大（高基数 = 小文件） | 无（读取优化） | 无 |
| 语法 | PARTITION BY | OPTIMIZE ... ZORDER BY | ALTER TABLE ... CLUSTER BY + OPTIMIZE |

### 4. 文件大小控制参数对比

| 参数 | 作用阶段 | 触发 Shuffle | 用途 |
|------|---------|-------------|------|
| `spark.sql.files.maxPartitionBytes` | 读取时 | 否 | 控制读取分区大小 |
| `spark.sql.shuffle.partitions` | shuffle 后 | 是 | 设置 shuffle 后分区数 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | AQE shuffle 后 | 是 | AQE 优化 shuffle 后分区大小 |
| `repartition(n)` | 手动调用 | 是 | 强制重新分区 |
| `coalesce(n)` | 手动调用 | 否（只减不增） | 合并分区（不增加） |

### 5. Pandas UDF 类型对比

| 类型 | 输入 | 输出 | 适用场景 |
|------|------|------|---------|
| SCALAR | pd.Series | pd.Series | 逐列变换（如 UDF 对每行计算） |
| SCALAR_ITER | Iterator[pd.Series] | Iterator[pd.Series] | 逐列变换 + 初始化开销优化 |
| GROUPED_AGG | pd.Series | scalar | 分组聚合（如 custom mean） |
| GROUPED_MAP (applyInPandas) | pd.DataFrame | pd.DataFrame | **分组内有状态计算** |

---

## 自测清单

### Spark UI（必须全部答对）
- [ ] predicate push-down 在 Spark UI 哪个 tab 诊断？--> SQL/DataFrame tab, Physical Plan
- [ ] 查询计划可视化在哪看？--> SQL/DataFrame tab（不是 Jobs tab）
- [ ] disk spill 在哪看？--> Stages tab
- [ ] MEMORY_ONLY 缓存异常的信号是什么？--> Size on Disk > 0
- [ ] wall-clock 总耗时怎么看？--> Query Profiler 的 Total wall-clock duration
- [ ] Aggregated task time 为什么不等于 wall-clock time？--> 因为 task 是并行执行的

### 数据布局（必须能区分三种技术）
- [ ] 高基数列 + "incremental, evolvable" --> 选哪个？--> Liquid Clustering
- [ ] 高基数列 + "immediate data skipping" --> 选哪个？--> Z-Order
- [ ] MERGE 性能优化应该怎么做？--> Liquid Clustering on merge key + Deletion Vectors
- [ ] Hive-Style Partition 对高基数列的问题是什么？--> 小文件问题

### 集群与内存
- [ ] wide transformation 场景：多小节点 vs 少大节点？--> 多小节点
- [ ] disk spill 的两个解决方向？--> (1) 更高 core:memory 比的实例 (2) 减小 maxPartitionBytes
- [ ] maxPartitionBytes 影响什么？--> 读取时的分区大小（无 shuffle）

### 文件大小控制
- [ ] 无 shuffle 场景控制输出文件大小用什么参数？--> spark.sql.files.maxPartitionBytes
- [ ] advisoryPartitionSizeInBytes 在什么场景下生效？--> AQE shuffle 后
- [ ] repartition vs coalesce 的区别？--> repartition 触发 shuffle，coalesce 不触发但只能减少分区

### Delta Lake 性能
- [ ] Delta time travel 为什么不适合长期版本化？--> 存储成本和查询延迟随版本数线性增长
- [ ] database LOCATION vs CREATE TABLE LOCATION 的区别？--> 前者决定存储位置，后者决定表类型（external）

### Pandas UDF
- [ ] 分组内有状态计算用哪种 UDF？--> applyInPandas（grouped map）
- [ ] 为什么不用 SCALAR_ITER + Delta 持久化？--> 不必要的 I/O 开销
- [ ] 为什么不用全局变量跨 executor 共享状态？--> executor 是独立进程，全局变量不安全
