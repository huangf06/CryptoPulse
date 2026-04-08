# Delta Lake -- 深度复习辅导材料

**错题数**: 21 题 | **涵盖子主题**: 13 个 | **优先级**: P0

---

## 一、错误模式诊断

### 1. "Parquet 思维"残留 -- 把 Delta Log 和 Parquet file footer 混淆

Q10、Q36 两次都把 Data Skipping 的统计信息来源选成 Parquet file footer。根本原因：心智模型中没有把"Delta Lake = Transaction Log 驱动一切"这条主线建立起来。仍然用纯 Parquet 的认知来理解 Delta 的行为。统计信息存在 Delta Log 中（集中式索引），不是 Parquet footer（分散式索引），这是 Delta Lake 相比纯 Parquet 的核心架构升级。

### 2. "文件级"和"分区级"跳过粒度混淆

Q36 选了"identify partitions"而非"identify data files"。Data Skipping 的粒度是文件，不是分区。分区裁剪（Partition Pruning）才是分区粒度的。两者作用于不同的列类型（分区列 vs 非分区列），用不同的统计来源（目录结构 vs Delta Log min/max），跳过不同的单位。这个混淆说明没有把 Data Skipping 和 Partition Pruning 作为两个独立机制来理解。

### 3. Hive 语法迁移到 Delta Lake -- 把 EXTERNAL 关键字当作创建 external table 的方式

Q34 选了 `EXTERNAL` 关键字，Q79 选了数据库级 `LOCATION`。两次考同一个点，两次都错。Delta Lake 中创建 external table 的唯一标志是**表级别的 LOCATION 指定**，没有 `EXTERNAL` 关键字。第二次错在粒度上（数据库级 vs 表级）。说明没有把"LOCATION 就是 external table 的充分必要条件"这条规则内化。

### 4. 把 CTAS（物化表）当成 VIEW -- TABLE vs VIEW 行为混淆

Q16 选了"每次查询时重新执行"，这是 VIEW 的行为。`CREATE TABLE AS SELECT` 在定义时执行一次查询并物理存储结果，之后查询返回的是静态快照。混淆根因：没有建立 TABLE / VIEW / Materialized View 三者的对比心智模型。

### 5. 把 CHECK 约束理解为"软标记"而非"硬阻断"

Q81 选了"违规记录标记在 boolean 列中"。Delta Lake 的 CHECK 约束是写时强制的，违反即整批原子性失败。这暴露了一个错误假设：以为 Delta Lake 有数据质量工具的"标记模式"。实际上 CHECK 约束的行为和 RDBMS 完全一致 -- 硬阻断。

### 6. 不知道 Delta Lake 外键约束是 informational only

Q96 未答。从 RDBMS 迁移到 Delta Lake 时，外键约束不被强制执行，ACID 事务是单表级别的。这两条限制直接决定了迁移时需要重构原有的跨表事务和引用完整性逻辑。

### 7. 期望"简单开关"解决复杂问题

Q75 选了不存在的 `delta.deduplicate = true` 配置。跨批次去重的标准做法是 insert-only MERGE。这个错误暴露了"希望有魔法配置"的思维惯性。

### 8. saveAsTable vs save 的区别不清晰

Q7/Q187 两次选 `save()` 而非 `saveAsTable()`。`save()` 不注册 metastore 表，且默认写入模式是 ErrorIfExists。需要跨时间比较历史预测 --> 需要 append 模式 + metastore 注册。

### 9. session 级配置 vs 表属性的持久化差异

Q294 选了 `spark.conf.set` 而非 `ALTER TABLE SET TBLPROPERTIES`。当题目要求"permanently comply"时，必须用表级属性（持久化），不能用 session 级配置（重启失效）。

### 10. Deletion Vectors 心智模型缺失

Q327 未答。启用 Deletion Vectors 后 DELETE 不重写文件，而是在元数据中标记。读时过滤。物理清理需要 REORG PURGE。这是一个需要从零建立的知识点。

---

## 二、子主题分解与学习方法

### 子主题依赖关系图

```
Transaction Log 基础（必须最先掌握）
  |
  +---> Data Skipping 机制（依赖 Transaction Log）
  +---> VACUUM 与数据保留（依赖 Transaction Log）
  +---> Deletion Vectors（依赖 Transaction Log）
  +---> Time Travel（依赖 Transaction Log + VACUUM）
  |
Managed vs External Table（独立）
ACID 事务与约束（独立，但需理解 Transaction Log 的原子性保证）
CTAS vs VIEW vs Materialized View（独立）
写入模式（saveAsTable vs save）（独立）
MERGE 去重（依赖写入模式基础）
分区策略与 PII 管理（依赖 Partition Pruning 理解）
Databricks Repos / Git 协作（独立，非 Delta 核心）
dbutils.secrets（独立，非 Delta 核心）
Delta-Iceberg 互操作（独立）
```

### 2.1 Transaction Log 基础

- **知识类型**: 架构性
- **学习方法**: 场景推演 + What-if 推理。从"一次写入操作到底发生了什么"出发，逐步推导出所有 Delta 特性。
- **实验步骤**:
  1. 创建一个 Delta 表，写入几行数据
  2. 浏览 `_delta_log/` 目录，查看 JSON 文件内容
  3. 执行一次 UPDATE，观察新增的 JSON 文件中的 `add` 和 `remove` 操作
  4. 写入 10 次，观察 checkpoint 文件的生成
  5. 预期：每次 commit 一个 JSON，每 10 次一个 Parquet checkpoint

### 2.2 Data Skipping vs Partition Pruning

- **知识类型**: 辨析性
- **学习方法**: 对比表 + 场景判断题。核心辨析点：统计来源、跳过粒度、适用列类型。
- **对比表见原笔记 3.3 节，必须能默写。**

### 2.3 Managed vs External Table

- **知识类型**: 辨析性
- **学习方法**: 对比表 + 快速问答。两个创建方式（SQL LOCATION / DataFrame save path），两种 DROP 行为。
- **关键规则**: LOCATION = external。没有 EXTERNAL 关键字。表级别，不是数据库级别。

### 2.4 CTAS vs VIEW vs Materialized View

- **知识类型**: 辨析性
- **学习方法**: 对比表。执行时机、数据存储、源表变更影响三个维度。

### 2.5 ACID 事务与约束

- **知识类型**: 概念性 + 辨析性
- **学习方法**: 苏格拉底对话。需要理解：ACID 是单表级别；CHECK 约束是硬阻断；FK 是 informational only。
- **与 RDBMS 的对比是关键 -- 哪些行为保留了，哪些没有。**

### 2.6 Deletion Vectors

- **知识类型**: 概念性
- **学习方法**: What-if 推理。理解"标记删除 vs 物理重写"的权衡：写入开销 vs 读取开销。
- **实验步骤**:
  1. 创建一个启用 Deletion Vectors 的 Delta 表
  2. 执行 DELETE，观察 `_delta_log/` 中的变化（有 deletion vector 文件，无新 Parquet 文件）
  3. 执行 REORG TABLE APPLY (PURGE)，观察文件重写

### 2.7 VACUUM 与保留配置

- **知识类型**: 程序性
- **学习方法**: 快速问答。表属性 vs session 配置 vs 手动命令三种方式的区别。
- **核心规则**: 持久化需求 --> 表属性（ALTER TABLE SET TBLPROPERTIES）。

### 2.8 写入模式与表注册

- **知识类型**: 辨析性
- **学习方法**: 对比表 + 场景判断题。saveAsTable vs save；append vs overwrite vs errorIfExists。

### 2.9 MERGE 去重

- **知识类型**: 程序性
- **学习方法**: 写代码。insert-only MERGE 的模板必须能默写。

### 2.10 分区策略与 PII 管理

- **知识类型**: 诊断性
- **学习方法**: Case Study。给定 Kafka schema 和 PII 需求，推导出分区策略 --> ACL --> 定期清理的完整方案。

### 2.11 Databricks Repos / Git 协作

- **知识类型**: 程序性
- **学习方法**: 快速问答。分支不可见 --> pull 远程同步。每人独立 Git folder + 独立 branch。

### 2.12 dbutils.secrets

- **知识类型**: 概念性
- **学习方法**: 快速问答。值是真实的（JDBC 正常），显示时被 REDACTED。

### 2.13 Delta-Iceberg 互操作

- **知识类型**: 概念性
- **学习方法**: 记忆。考试倾向"创建独立 Iceberg 副本"而非 UniForm。注意审题。

---

## 三、苏格拉底式教学问题集

### 3.1 Transaction Log

1. Delta Lake 的每次写操作写了几个文件？分别是什么？如果只写了 Parquet 数据文件而没有写 JSON commit 文件，会发生什么？
2. 一个 Delta 表经历了 25 次 commit。读取当前状态时，Spark 最少需要读多少个 log 文件？（提示：考虑 checkpoint）
3. 如果两个 Spark job 同时尝试写入同一个 Delta 表，Transaction Log 如何保证 ACID？如果是两个不同的 Delta 表呢？
4. Transaction Log 是"单一真相来源"。那么 Hive Metastore 存了什么？两者的信息有重叠吗？谁是权威的？

### 3.2 Data Skipping

1. 为什么 Delta Lake 不直接用 Parquet file footer 的统计信息，而要把它复制到 Transaction Log 中？性能差异的根因是什么？
2. 一个 Delta 表有 1000 个文件，查询条件是 `WHERE age > 60`。假设 age 列是第 35 列（超过默认 32 列限制），会发生什么？Data Skipping 还有效吗？
3. Data Skipping 对 STRING 类型的列有效吗？如果有效，有什么限制？如果一个 STRING 列的值平均长度是 500 字符呢？
4. 如果一个文件的 min(latitude) = 10, max(latitude) = 80，查询条件是 `latitude > 66.3`，这个文件会被跳过吗？为什么？
5. Data Skipping 和 Partition Pruning 能同时对同一个查询生效吗？举一个具体例子。

### 3.3 Managed vs External Table

1. 你用 `CREATE TABLE t1 USING delta LOCATION '/data/t1'` 创建了一个表。然后 `DROP TABLE t1`。数据还在吗？为什么？
2. 你用 `CREATE TABLE t2 AS SELECT * FROM source` 创建了一个表。然后 `DROP TABLE t2`。数据还在吗？为什么？和上一个有什么区别？
3. 有人说"只要在 CREATE DATABASE 时指定 LOCATION，所有在这个 database 下创建的表就都是 external table"。这个说法对吗？为什么？
4. 如果用 DataFrame API 的 `df.write.format("delta").save("/my/path")`，这创建的是 managed 还是 external table？能直接用 SQL 查询吗？

### 3.4 ACID 与约束

1. 一个 batch 包含 10000 条记录，其中第 9999 条违反了 CHECK 约束。前 9998 条记录会被写入吗？为什么？
2. 你在 Delta 表 A 上定义了 `FOREIGN KEY (user_id) REFERENCES users(id)`。如果你插入一条 user_id = 999 的记录，而 users 表里没有 id = 999，会报错吗？
3. Delta Lake 不支持跨表事务。那么在一个需要同时更新 fact 表和 dimension 表的 ETL pipeline 中，你怎么保证数据一致性？

### 3.5 Deletion Vectors

1. 启用 Deletion Vectors 后，DELETE 操作的写入开销降低了，但读取开销增加了。为什么？具体增加了什么？
2. 一个文件有 100 万行，其中 1 行被标记删除。没有 Deletion Vectors 时会怎样？有了之后呢？哪种情况重写的数据量更大？
3. Deletion Vectors 的物理清理用 `REORG TABLE APPLY (PURGE)`。这个命令和 VACUUM 有什么区别？

### 3.6 VACUUM 与保留配置

1. `delta.deletedFileRetentionDuration` 设置在 `spark.conf.set` 和 `ALTER TABLE SET TBLPROPERTIES` 中有什么区别？哪个满足"permanently comply"的要求？
2. VACUUM 删除的是什么文件？它会修改 Delta Log 吗？VACUUM 之后 Time Travel 还能用吗？
3. `delta.deletedFileRetentionDuration`（默认 7 天）和 `delta.logRetentionDuration`（默认 30 天）分别控制什么？它们独立吗？

### 3.7 写入模式与 MERGE

1. `saveAsTable("name")` 和 `save("/path")` 创建的表有什么根本区别？如果你用 `save` 写了数据，能直接用 `SELECT * FROM name` 查询吗？
2. 跨批次去重为什么不能用 `dropDuplicates()`？它和 insert-only MERGE 的区别在哪？
3. 写出一个 insert-only MERGE 的完整 SQL 模板。如果 source 中有重复的 key 会怎样？

### 3.8 CTAS vs VIEW

1. `CREATE TABLE t AS SELECT * FROM source` 之后，source 表插入了新数据。`SELECT * FROM t` 能看到新数据吗？为什么？
2. 如果把上面的 TABLE 换成 VIEW，结果有什么不同？
3. DLT 的 Materialized View 和普通 VIEW 有什么区别？它和 CTAS 又有什么区别？

---

## 四、场景判断题

### 4.1 Data Skipping 机制

**题目 1**: 一个 Delta 表有如下 schema: `order_id LONG, customer_id LONG, amount DOUBLE, category STRING, order_date DATE`。表按 `order_date` 分区。执行查询 `SELECT * FROM orders WHERE amount > 10000 AND order_date = '2024-01-15'`。以下哪个描述正确？

A. Delta Engine 先用 Partition Pruning 定位到 `order_date = '2024-01-15'` 的分区，然后用 Data Skipping（Delta Log 中的 min/max 统计）在该分区内跳过 amount 不可能 > 10000 的文件。
B. Delta Engine 用 Data Skipping 同时处理 order_date 和 amount 两个条件，跳过不符合的文件。
C. Delta Engine 先扫描所有 Parquet file footer 获取 amount 的统计信息，再应用分区过滤。
D. Delta Engine 只使用 Partition Pruning，amount 条件在读取数据后才应用。

**正确答案**: A

**解析**: 分区列（order_date）通过 Partition Pruning 处理，基于目录结构直接定位分区。非分区列（amount）通过 Data Skipping 处理，基于 Delta Log 中的文件级 min/max 统计跳过文件。两者可以协同工作。B 错：分区列不走 Data Skipping，走 Partition Pruning。C 错：Delta 不用 Parquet footer。D 错：amount 也会利用 Data Skipping，不是全表扫描后过滤。

---

**题目 2**: 一个 Delta 表的 schema 有 50 列，未做任何配置调整。执行 `SELECT * FROM t WHERE col_40 = 100`。Data Skipping 对这个查询有效吗？

A. 有效，Delta 对所有列都收集统计信息。
B. 无效，Delta 默认只对前 32 列收集统计信息，col_40 超出范围。
C. 有效，但只对数值类型有效。
D. 无效，Data Skipping 只对分区列有效。

**正确答案**: B

**解析**: Delta Lake 默认只对前 32 列收集 min/max/null count 统计（由 `dataSkippingNumIndexedCols` 控制）。col_40 是第 40 列，超出默认范围，不会收集统计，Data Skipping 无效。解决方案：调大 `dataSkippingNumIndexedCols`，或将高频过滤列调整到 schema 前部。D 错：Data Skipping 恰恰是针对非分区列的。

---

**题目 3**: 一个 Delta 表存储日志数据，`message STRING` 列的值平均长度 2000 字符，且该列是 schema 的第 5 列。执行 `SELECT * FROM logs WHERE message = 'ERROR: timeout'`。Data Skipping 的效果如何？

A. 非常有效，因为 message 在前 32 列范围内。
B. 无效，因为 Delta 不对 STRING 类型收集统计。
C. 效果差，虽然收集了统计但长字符串的 min/max 辨识度低，大多数文件无法被跳过。
D. 报错，因为 STRING 类型不支持 min/max 比较。

**正确答案**: C

**解析**: Delta 对 STRING 类型确实收集 min/max 统计，但只截取前缀（通常前 32 字节）。对于长字符串，不同值的前缀可能相同，导致 min/max 区间很宽，几乎无法跳过任何文件。Data Skipping 对短字符串（如状态码、类别）效果好，对长文本效果差。

---

### 4.2 Managed vs External Table

**题目 1**: 一个数据工程师执行了以下代码：
```sql
CREATE DATABASE analytics LOCATION '/mnt/lake/analytics';
CREATE TABLE analytics.daily_report AS SELECT * FROM raw_data;
```
然后执行 `DROP TABLE analytics.daily_report`。数据文件会被删除吗？

A. 不会，因为 database 指定了 LOCATION，所以所有表都是 external 的。
B. 会，因为 CREATE TABLE AS SELECT 没有指定表级别的 LOCATION，这是 managed table。
C. 不会，因为 Delta Lake 不会物理删除任何数据文件。
D. 会，但可以通过 Time Travel 恢复。

**正确答案**: B

**解析**: 判断 managed vs external 的唯一标准是**表创建时是否指定了 LOCATION**。数据库的 LOCATION 只决定表数据的默认存储路径，不改变表的管理类型。CTAS 没有指定表级 LOCATION --> managed table --> DROP 删除元数据和数据文件。A 是常见误解（Q79 的错因）。D 错：managed table DROP 后物理文件被删除，Time Travel 依赖物理文件，无法恢复。

---

**题目 2**: 数据工程师用两种方式分别创建了表：
```python
# 方式 1
df.write.format("delta").mode("overwrite").save("/data/warehouse/sales")
# 方式 2
df.write.format("delta").mode("overwrite").saveAsTable("sales")
```
关于这两种方式，以下哪个说法正确？

A. 两者都创建了 managed table。
B. 方式 1 创建 external table，方式 2 创建 managed table。
C. 方式 1 只写了数据文件未注册 metastore，方式 2 注册了 metastore 且创建了 managed table。
D. 两者功能完全等价。

**正确答案**: C

**解析**: `save("/path")` 只将数据写到指定路径，不在 Hive metastore 中注册表 -- 之后不能直接用 `SELECT * FROM sales` 查询，需要手动 `CREATE TABLE USING delta LOCATION` 注册。`saveAsTable("name")` 在 Databricks 默认路径存储数据并注册到 metastore，创建 managed table。B 不精确：`save()` 严格来说没有创建"表"（没有 metastore 记录）。

---

### 4.3 ACID 约束行为

**题目 1**: 一个 Delta 表 `employees` 有 CHECK 约束 `salary > 0`。以下代码执行后会发生什么？
```python
data = [(1, "Alice", 50000), (2, "Bob", -100), (3, "Carol", 75000)]
df = spark.createDataFrame(data, ["id", "name", "salary"])
df.write.mode("append").saveAsTable("employees")
```

A. Alice 和 Carol 的记录被写入，Bob 的记录被丢弃，日志中记录一条 warning。
B. 全部 3 条记录都不会被写入，整个写入操作失败并抛出异常。
C. 全部 3 条记录被写入，Bob 的记录被标记为 invalid。
D. Alice 的记录被写入（在 Bob 之前），Bob 和 Carol 的记录因异常终止而丢失。

**正确答案**: B

**解析**: CHECK 约束是写时强制的硬约束。一旦批次中任何记录违反约束，整个批次原子性失败回滚（Atomicity）。不存在"部分写入"（D）、"跳过违规记录"（A）、"标记违规"（C）的行为。

---

**题目 2**: 一个从 Oracle 迁移到 Databricks 的 ETL pipeline 原来依赖以下逻辑：在一个事务中同时更新 `orders` 表和 `inventory` 表，并通过外键约束保证 `orders.product_id` 必须存在于 `products.id` 中。迁移后以下哪个说法正确？

A. Databricks 支持跨表事务，可以直接迁移原有逻辑。
B. Databricks 支持外键约束的强制执行，但不支持跨表事务。
C. Databricks 的外键约束是 informational only（不强制执行），且 ACID 事务仅限单表。原有的跨表事务和引用完整性逻辑需要重新设计。
D. Databricks 用乐观并发控制替代了跨表事务，效果等价。

**正确答案**: C

**解析**: Delta Lake 的两个关键限制：(1) ACID 事务是单表级别的，不支持跨表原子性操作；(2) 外键约束是 informational only，只在元数据中声明关系，不在写入时检查。从 RDBMS 迁移时，依赖这两个特性的逻辑必须用应用层代码重构。D 错：乐观并发控制解决的是同一表的并发写入冲突，不是跨表事务。

---

### 4.4 Deletion Vectors

**题目 1**: 一个 Delta 表 `transactions` 启用了 Deletion Vectors，包含 500 个数据文件。执行 `DELETE FROM transactions WHERE status = 'fraud'`，其中有 3 个文件包含匹配行。操作完成后，存储中的 Parquet 文件数量发生了什么变化？

A. 减少 3 个（包含 fraud 记录的文件被删除）。
B. 不变（原文件保留，删除信息记录在元数据中）。
C. 增加 3 个（重写了 3 个不含 fraud 记录的新文件，原文件保留等待 VACUUM）。
D. 增加 500 个（所有文件都被重写以应用删除）。

**正确答案**: B

**解析**: Deletion Vectors 的核心价值：DELETE 不重写任何 Parquet 文件。删除信息以 deletion vector 的形式记录在 Delta Log 元数据中。读取时 Spark 自动过滤被标记的行。物理清理需要手动运行 `REORG TABLE APPLY (PURGE)`。C 描述的是没有 Deletion Vectors 时的 copy-on-write 行为。

---

**题目 2**: 在上题的基础上，如果不执行任何清理操作，长期积累大量 deletion vectors 会带来什么问题？应该如何处理？

A. 读取性能下降，因为每次查询都需要过滤被标记的行。应定期运行 REORG TABLE APPLY (PURGE)。
B. 存储成本增加，因为 deletion vectors 文件很大。应运行 VACUUM。
C. 写入性能下降，因为每次写入都需要检查 deletion vectors。应禁用 Deletion Vectors。
D. 不会有任何问题，Databricks 自动处理。

**正确答案**: A

**解析**: Deletion Vectors 用写入开销换取了读取开销。长期积累后，每次查询都需要在读取数据时过滤掉被标记的行，增加 CPU 和内存开销。REORG TABLE APPLY (PURGE) 会重写包含 deletion vectors 的文件，生成干净的新文件。VACUUM 只删除已经不被引用的旧文件，不处理 deletion vectors。Predictive Optimization 也可以自动处理。

---

### 4.5 VACUUM 与保留配置

**题目 1**: 一个数据工程师需要确保 Delta 表 `audit_log` 的已删除文件保留 30 天以满足合规要求。以下哪种方式最合适？

A. 在 notebook 开头添加 `spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", "30 days")`。
B. 执行 `ALTER TABLE audit_log SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')`。
C. 设置集群策略，将 `delta.deletedFileRetentionDuration` 固定为 30 天。
D. 在每次运行 VACUUM 前手动指定 `VACUUM audit_log RETAIN 720 HOURS`。

**正确答案**: B

**解析**: 合规要求意味着必须"持久化"且"不依赖人工执行"。A 是 session 级配置，重启后失效，不满足持久化要求。D 依赖每次手动指定，容易人为失误。C 是集群级别的，不是表级别的，且换集群后可能失效。B 是表属性，持久化存储在表的元数据中，任何 session、任何集群读取时都会生效。

---

### 4.6 写入模式

**题目 1**: 一个 ML 模型每天生成一次预测结果 DataFrame，schema 为 `customer_id LONG, churn_score DOUBLE, prediction_date DATE`。业务要求保留所有历史预测结果以便纵向比较。以下哪种写入方式最合适？

A. `preds.write.format("delta").mode("overwrite").save("/models/churn_preds")`
B. `preds.write.mode("append").saveAsTable("churn_preds")`
C. `preds.write.format("delta").save("/models/churn_preds")`
D. `preds.write.format("delta").mode("append").option("mergeSchema", "true").save("/models/churn_preds")`

**正确答案**: B

**解析**: 需求拆解：(1) 保留历史 --> append 模式；(2) 便于后续查询比较 --> 注册到 metastore（saveAsTable）。A 错：overwrite 每天覆盖，丢失历史。C 错：save 默认 errorIfExists，第二天运行就报错；且不注册 metastore。D 错：虽然 append 了，但 save 不注册 metastore，后续 SQL 查询不方便。B 使用 append 模式保留历史，saveAsTable 注册 metastore，Databricks 默认格式即为 Delta。

---

### 4.7 MERGE 去重

**题目 1**: 一个流处理 pipeline 从 Kafka 消费消息，可能收到重复消息。目标 Delta 表 `events` 以 `event_id` 为唯一标识。以下哪种方式可以在写入时实现跨批次去重？

A. 在写入前对每个 micro-batch 调用 `dropDuplicates("event_id")`。
B. 使用 `MERGE INTO events USING batch ON events.event_id = batch.event_id WHEN NOT MATCHED THEN INSERT *`。
C. 启用 `delta.autoOptimize.autoCompact` 自动去重。
D. 对 `event_id` 列添加 UNIQUE 约束。

**正确答案**: B

**解析**: A 只能去除同一 batch 内的重复，不能去除与历史已写入记录的重复（"跨批次"去重）。B 的 insert-only MERGE 会检查目标表中是否已存在相同 event_id，只插入不存在的记录，实现跨批次去重。C 是文件压缩，与去重无关。D 不存在 -- Delta Lake 没有 UNIQUE 约束（只有 CHECK 约束和 NOT NULL 约束）。

---

### 4.8 CTAS vs VIEW

**题目 1**: 以下两段代码分别创建了什么？

```sql
-- 代码 A
CREATE TABLE summary AS SELECT region, SUM(revenue) FROM sales GROUP BY region;
-- 代码 B
CREATE VIEW summary_view AS SELECT region, SUM(revenue) FROM sales GROUP BY region;
```

一小时后 sales 表新增了大量数据。分别查询 summary 和 summary_view，结果有什么区别？

A. 两者都返回包含新数据的最新结果。
B. summary 返回创建时的旧结果；summary_view 返回包含新数据的最新结果。
C. summary 返回最新结果；summary_view 返回创建时的旧结果。
D. 两者都返回创建时的旧结果。

**正确答案**: B

**解析**: CTAS 在创建时执行一次查询并物化结果，之后查询返回静态快照，不反映源表变更。VIEW 不存储数据，每次查询时重新执行底层 SQL，实时反映源表的最新状态。这是 TABLE 和 VIEW 的根本区别。

---

## 五、关键知识网络

### 子主题之间的关联

1. **Transaction Log 是所有 Delta 特性的根基**。Data Skipping、Deletion Vectors、Time Travel、VACUUM 都是 Transaction Log 的衍生功能。理解了 Transaction Log 的结构（JSON commit + Parquet checkpoint），其他特性自然能推导出来。

2. **Data Skipping 和 Partition Pruning 是同一目标（减少 I/O）的两种机制**。前者作用于非分区列，后者作用于分区列。前者粒度是文件，后者粒度是分区目录。两者可以在同一个查询中协同工作。

3. **Managed vs External 的区别，本质是数据生命周期的所有权问题**。这和 VACUUM、DROP TABLE 的行为直接相关：你能不能安全 DROP 一个表，取决于它是 managed 还是 external。

4. **CHECK 约束的原子性失败，是 ACID Atomicity 的具体体现**。理解 CHECK 约束行为不需要单独记忆，只需从 ACID 原则推导。

5. **Deletion Vectors 是 "copy-on-write vs merge-on-read" 权衡在 Delta Lake 中的实现**。与 Iceberg 的 position delete files 是同一思路。

6. **saveAsTable vs save 的区别，与 Managed vs External 的概念直接相关**。saveAsTable 注册 metastore = managed table；save 到路径 = 需要手动注册 = external table。

7. **CTAS vs VIEW 的区别，与流处理中 batch vs streaming 的语义呼应**。CTAS 是一次性快照（类似 batch），VIEW 是实时计算（类似 streaming）。

### 与其他 Topic 的关联

- **Topic 02 (ELT / Spark SQL)**: MERGE 语法、writeStream 的 foreachBatch 中使用 MERGE 去重。复习 MERGE 去重时记得回顾 Structured Streaming 的 exactly-once 语义。

- **Topic 03 (Structured Streaming)**: Auto Loader 读取文件进 Delta 表、流式写入的 checkpoint 机制。Data Skipping 对流式读取同样有效。

- **Topic 04 (Delta Live Tables)**: Materialized View 是 DLT 中的概念，与 CTAS / VIEW 构成三级对比。DLT 的 expectations 与 CHECK 约束的区别（expectations 支持 warn/drop/fail 三种模式，CHECK 约束只有 fail）。

- **Topic 05 (Security & Governance)**: Managed vs External table 的权限模型差异。Unity Catalog 对 external table 的 LOCATION 管理。dbutils.secrets 的 REDACTED 行为。

- **Topic 06 (Performance Optimization)**: Data Skipping + Z-ORDER 配合使用。OPTIMIZE 与 Auto Compaction 的关系。Deletion Vectors 对读取性能的影响。

---

## 六、Anki 卡片（Q/A 格式）

**Q1**: Delta Lake 的 Data Skipping 统计信息存储在哪里？
**A1**: 存储在 `_delta_log/` 的 JSON commit 文件中（文件级 min/max/null count），不是 Parquet file footer。这是 Delta 相比纯 Parquet 的核心架构升级 -- 集中式索引 vs 分散式索引。

**Q2**: Data Skipping 跳过的粒度是什么？默认对多少列收集统计？
**A2**: 跳过粒度是整个数据文件（不是行，不是分区）。默认对前 32 列收集统计（`dataSkippingNumIndexedCols`）。对数值、日期、短字符串有效。

**Q3**: Delta Lake 中创建 external table 的方式是什么？需要 EXTERNAL 关键字吗？
**A3**: 在 CREATE TABLE 时指定 `LOCATION`（表级别）即为 external table。不需要也不使用 `EXTERNAL` 关键字。DataFrame API 中 `df.write.format("delta").save("/path")` 等价（但不注册 metastore）。

**Q4**: DROP managed table 和 DROP external table 分别发生什么？
**A4**: Managed: 删除 metastore 元数据 + 物理删除数据文件（Time Travel 不可用）。External: 只删除 metastore 元数据，数据文件保留（Time Travel 仍可用）。

**Q5**: Delta Lake 的 CHECK 约束被违反时，batch 写入的结果是什么？
**A5**: 整个 batch 原子性失败回滚，不写入任何记录。不存在"部分写入"或"标记违规记录"的行为。这是 ACID Atomicity 的体现。

**Q6**: Delta Lake 的外键约束是否被强制执行？
**A6**: 不是。Delta Lake 的外键约束是 informational only -- 只在元数据中声明关系，写入时不检查引用完整性。从 RDBMS 迁移时必须在应用层重构引用完整性逻辑。

**Q7**: 启用 Deletion Vectors 后，DELETE 操作的行为是什么？
**A7**: 不重写 Parquet 文件。在 Delta Log 元数据中记录 deletion vector，标记哪些行被删除。读取时自动过滤。物理清理需要 `REORG TABLE APPLY (PURGE)`。

**Q8**: `delta.deletedFileRetentionDuration` 应设置在哪里？为什么？
**A8**: 应通过 `ALTER TABLE SET TBLPROPERTIES` 设置为表属性（持久化）。不应用 `spark.conf.set`（session 级，重启失效）。当需求是"permanently comply"时，只有表属性满足要求。

**Q9**: `saveAsTable("name")` 和 `save("/path")` 的核心区别？
**A9**: saveAsTable 注册 Hive metastore，可直接 SQL 查询，Databricks 默认 Delta 格式。save 只写文件不注册 metastore，不能直接 SQL 查询，开源 Spark 默认 Parquet 格式。

**Q10**: 跨批次去重（deduplication against previously processed records）的标准做法？
**A10**: Insert-only MERGE: `MERGE INTO target USING source ON target.key = source.key WHEN NOT MATCHED THEN INSERT *`。只插入目标表中不存在的记录。`dropDuplicates` 只能去除同一 batch 内的重复。

**Q11**: CTAS（CREATE TABLE AS SELECT）和 CREATE VIEW 的核心区别？
**A11**: CTAS 在定义时执行一次查询并物化结果，之后查询返回静态快照，不反映源表变更。VIEW 不存储数据，每次查询时重新执行 SQL，实时反映源表最新状态。

**Q12**: Kafka 多 topic 数据中如何隔离 PII 记录并实现 14 天保留策略？
**A12**: 按 `topic` 字段分区。PII topic（如 registration）的数据存储在独立分区目录中，可设置目录级 ACL 限制访问，用 `DELETE WHERE topic='registration' AND timestamp < ...` 利用分区裁剪高效清理过期数据。

---

## 七、掌握度自测

以下 8 道题覆盖所有子主题。不看笔记，独立完成。

**题 1**: 一个 Delta 表有 100 个 Parquet 文件。查询 `WHERE price > 500` 时，Delta Engine 从哪里获取每个文件的 price 列统计信息来决定是否跳过该文件？
A. 每个 Parquet 文件的 footer
B. Hive metastore
C. `_delta_log/` 中的 JSON commit 文件
D. 内存缓存

**题 2**: 以下代码创建的是什么类型的表？`DROP TABLE` 后数据文件是否保留？
```sql
CREATE TABLE reports.monthly USING delta LOCATION '/mnt/data/monthly'
AS SELECT * FROM raw_data;
```
A. Managed table，数据文件被删除
B. External table，数据文件保留
C. External table，数据文件被删除
D. Managed table，数据文件保留

**题 3**: 一个 batch 包含 5000 条记录要写入有 CHECK 约束的 Delta 表。第 4999 条违反约束。最终写入了多少条？
A. 4998
B. 4999
C. 5000
D. 0

**题 4**: 以下哪个是 Delta Lake 中不被强制执行的约束类型？
A. NOT NULL
B. CHECK
C. FOREIGN KEY
D. PRIMARY KEY（在 Unity Catalog 中）

**题 5**: 启用 Deletion Vectors 的 Delta 表执行 `DELETE FROM t WHERE id = 100` 后，包含 id=100 的原始 Parquet 文件发生了什么？
A. 被重写为不含 id=100 的新文件
B. 被物理删除
C. 保持不变，删除信息记录在 Delta Log 元数据中
D. 被移动到回收站目录

**题 6**: 哪种方式将 `delta.deletedFileRetentionDuration` 设置为 15 天且满足"永久合规"要求？
A. `spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", "15 days")`
B. `ALTER TABLE t SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 15 days')`
C. `VACUUM t RETAIN 360 HOURS`
D. 在集群环境变量中设置

**题 7**: ML 模型每天输出预测 DataFrame，需要保留所有历史预测便于跨时间比较。以下哪种写入方式最合适？
A. `preds.write.format("delta").save("/preds")`
B. `preds.write.mode("overwrite").saveAsTable("preds")`
C. `preds.write.mode("append").saveAsTable("preds")`
D. `preds.write.format("delta").mode("append").save("/preds")`

**题 8**: `CREATE TABLE t AS SELECT * FROM source` 之后 source 新增了数据。查询 `SELECT * FROM t` 能看到新数据吗？如果把 TABLE 换成 VIEW 呢？
A. TABLE 能看到，VIEW 不能
B. 两者都能看到
C. TABLE 不能看到，VIEW 能看到
D. 两者都不能看到

---

### 自测答案

1. **C** -- Delta Log 中的 JSON commit 文件存储文件级 min/max 统计。
2. **B** -- 指定了 LOCATION = external table，DROP 后只删元数据，数据保留。
3. **D** -- CHECK 约束违反导致整个 batch 原子性失败，0 条写入。
4. **C** -- FOREIGN KEY 在 Delta Lake 中是 informational only，不强制执行。NOT NULL 和 CHECK 是强制执行的。
5. **C** -- Deletion Vectors 不重写文件，只在元数据中标记。
6. **B** -- 表属性持久化，满足"永久合规"。A 是 session 级，重启失效。
7. **C** -- append 保留历史，saveAsTable 注册 metastore 便于 SQL 查询。
8. **C** -- CTAS 是静态快照，不跟踪源表变更。VIEW 每次查询重新执行，能看到新数据。
