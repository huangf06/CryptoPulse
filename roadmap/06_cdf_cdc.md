# 06 - Change Data Feed / CDC 深度复习辅导

> 6 题全错 (0/6)，正确率 0%
> 涉及题号：Q28, Q30, Q113, Q147, Q248, Q296

---

## 一、错误模式诊断

6 道题全错，暴露了三个系统性认知缺陷，按严重程度排列：

### 缺陷 1：CDF batch read 的无状态本质未内化（Q28, Q147 -- 同一个坑踩两次）

**症状：** 看到 `readChangeFeed` 就认为它"自带增量追踪"，误以为每次只返回"新变更"。

**根因：** 把 CDF 的能力和 Structured Streaming checkpoint 的能力混为一谈。CDF 只是一个**数据源**（"发生了什么变更"），不是一个**进度追踪器**（"处理到哪里了"）。`spark.read` 是无状态的批量读取，无论读什么数据源都没有 checkpoint。

**修正模型：**
- `spark.read` + `readChangeFeed` = 每次从 `startingVersion` 读到最新版本的全量变更，无记忆
- `spark.readStream` + `readChangeFeed` = 有 checkpoint，自动追踪已处理位置
- `table_changes('t', start)` = SQL 版的 batch read，同样无状态

### 缺陷 2：CDF 与相邻功能的边界模糊（Q113, Q248）

**症状：** 
- Q113：需要版本差异比较时条件反射选 CDF，忽略了 CDF 需要显式启用的前提
- Q248：需要减少文件重写时选 CDF，混淆了消费侧功能和写入侧优化

**根因：** 没有建立清晰的功能分类框架。CDF、Time Travel、Deletion Vectors、DESCRIBE HISTORY 各自解决不同层面的问题，但在脑中都被笼统归类为"Delta Lake 的版本/变更相关功能"。

**修正模型：**
| 维度 | CDF | Time Travel | Deletion Vectors | DESCRIBE HISTORY |
|---|---|---|---|---|
| **解决什么** | 下游消费行级变更 | 查询历史版本数据 | 减少写入时文件重写 | 查看操作元数据 |
| **面向谁** | 下游 ETL consumer | 数据分析师/调试者 | 写入性能优化 | 审计/运维 |
| **需要显式启用？** | 是 | 否 | 是 | 否 |
| **提供行级数据？** | 是（含 _change_type） | 是（全量快照） | 否 | 否（只有操作统计） |

### 缺陷 3：append-only vs 有变更的表的处理路径混淆（Q30, Q296）

**症状：**
- Q30：append-only 表选了 CDF 而非 streaming
- Q296：有 UPDATE 的表选了直接 readStream 而非 CDF + apply_changes()

**根因：** 没有形成**决策树**——根据源表的写入模式选择正确的下游读取方式。

**修正决策树：**
```
源表有 UPDATE/DELETE 吗？
  |
  +-- 否（append-only）--> readStream.table("source")，无需 CDF
  |
  +-- 是 --> CDF 已启用吗？
        |
        +-- 是 --> readStream.option("readChangeFeed", "true") + apply_changes() / MERGE
        |
        +-- 否 --> Time Travel 比较版本差异，或先启用 CDF
```

---

## 二、子主题分解与学习方法

### 子主题 A：CDF 机制基础

**核心知识：**
1. CDF 的启用方式：`TBLPROPERTIES (delta.enableChangeDataFeed = true)`
2. 四种 `_change_type`：insert, update_preimage, update_postimage, delete
3. 两个元数据列：`_commit_version`, `_commit_timestamp`
4. CDF 只记录启用后的变更，不追溯历史

**学习方法：** 把 CDF 理解为 Delta 表上的一个"变更日志 sidecar"——它不改变表本身的写入行为，只是在旁边额外记录"发生了什么"。类比数据库的 WAL/binlog。

### 子主题 B：Batch read vs Streaming read CDF

**核心知识：**
1. `spark.read` + `readChangeFeed`：无状态，每次从指定版本读全量
2. `spark.readStream` + `readChangeFeed`：有 checkpoint，增量处理
3. `table_changes()` SQL 函数：等价于 batch read
4. `startingVersion` 和 `startingTimestamp` 参数

**学习方法：** 记住一条铁律——**只有 readStream 有 checkpoint，只有 checkpoint 能防止重复处理**。无论数据源是 CDF、Delta 表还是 Kafka，`spark.read` 永远是无状态的。

### 子主题 C：CDF 输出的消费模式

**核心知识：**
1. CDF 输出不能直接 append 到目标表（会丢失 update/delete 语义）
2. 正确模式：过滤 `_change_type`，用 MERGE INTO 或 `apply_changes()` 应用到目标
3. insert + update_postimage = upsert 候选行
4. delete = 需要从目标表删除的行

**学习方法：** 把 CDF 输出想象成一个"指令流"——每条记录告诉你"对目标表执行什么操作"，而不是"直接插入这些行"。

### 子主题 D：CDF vs 相邻功能的选择

**核心知识：**
1. CDF vs Time Travel：CDF 需启用，Time Travel 始终可用；CDF 给行级变更类型，Time Travel 给全量快照
2. CDF vs Deletion Vectors：CDF 是消费侧（读），Deletion Vectors 是写入侧（写）
3. CDF vs readStream.table()：append-only 用 readStream，有 UPDATE/DELETE 用 CDF
4. CDF vs DESCRIBE HISTORY：CDF 有行级数据，DESCRIBE HISTORY 只有操作元数据

**学习方法：** 做功能选择题时，先识别需求的本质——是"下游要消费变更"还是"优化写入性能"还是"查看历史版本"还是"审计操作记录"？

### 子主题 E：CDF 在 Lakeflow Declarative Pipelines 中的应用

**核心知识：**
1. 上游 streaming table 有 UPDATE → 下游不能直接 readStream（会报错）
2. 正确路径：readStream + readChangeFeed → enrich → apply_changes()
3. `skipChangeCommits` 会丢失数据，不是正确方案
4. `apply_changes()` 负责将 CDF 的 insert/update/delete 语义正确应用到目标表

**学习方法：** 记住 Lakeflow 中的信号链：上游有变更 → 必须走 CDF → CDF 输出需要 apply_changes() 处理 → 目标 streaming table 正确反映变更。

---

## 三、苏格拉底式教学问题集

### 子主题 A：CDF 机制基础

1. 如果一张 Delta 表从未启用 CDF，你对它执行 `table_changes()` 会发生什么？如果先执行了 10 次 UPDATE，然后才启用 CDF，能读到之前的变更吗？
2. 一条 MERGE INTO 语句同时 INSERT 了 3 行、UPDATE 了 2 行、DELETE 了 1 行，CDF 会产生多少条变更记录？每条的 `_change_type` 分别是什么？
3. `update_preimage` 和 `update_postimage` 为什么要分成两条记录而不是合并成一条？在什么业务场景下你需要 preimage？
4. CDF 的 `_commit_version` 和 Delta 表的版本号是什么关系？如果同一个版本内有多条变更，它们的 `_commit_version` 相同吗？

### 子主题 B：Batch read vs Streaming read

1. 如果你把 Q28 的代码从 `spark.read` 改成 `spark.readStream`，其他不变，结果会怎样？为什么？
2. 假设你必须用 batch read CDF（不能用 streaming），如何设计一个不产生重复数据的方案？需要额外维护什么状态？
3. `table_changes('my_table', 5, 10)` 返回的是版本 5 到版本 10 之间的变更。如果版本 7 是一个 OPTIMIZE 操作，CDF 会包含 OPTIMIZE 的记录吗？

### 子主题 C：CDF 输出的消费模式

1. 如果目标表是 Type 2 SCD（保留历史版本），CDF 输出的消费逻辑和 Type 1 有什么不同？你还能直接过滤 insert + update_postimage 吗？
2. CDF 输出中如果同一个 primary key 在一个版本内先被 INSERT 再被 UPDATE，你会看到几条记录？用 MERGE 处理时需要特殊处理吗？
3. 为什么 `apply_changes()` 比手动写 MERGE 更适合 Lakeflow pipeline？它额外处理了什么？

### 子主题 D：功能选择

1. 一个团队每天用 overwrite 模式重写一张表，事后想知道哪些行发生了变化。他们应该用 CDF 还是 Time Travel？如果两者都可用，哪个更高效？
2. 一张表频繁执行小批量 UPDATE，写入延迟很高。团队应该启用 CDF 还是 Deletion Vectors？为什么？
3. "CDF 需要显式启用"这个约束在考试中如何影响你的选择？当题目没有提到 CDF 已启用时，你应该怎么判断？

### 子主题 E：Lakeflow 中的 CDF

1. 为什么直接 `readStream.table()` 读取包含 UPDATE 的 Delta 表会报错？Structured Streaming 的 append-only 假设是什么？
2. `skipChangeCommits` 在什么场景下是合理的选择？（提示：考虑 OPTIMIZE、VACUUM 等维护操作）
3. 如果上游表既有 append 又有偶尔的 UPDATE，你有哪些选项？每个选项的 trade-off 是什么？

---

## 四、场景判断题

### 子主题 A：CDF 机制基础

**A1.** 一张 Delta 表在版本 3 时启用了 CDF。以下哪个查询能成功执行？

A. `SELECT * FROM table_changes('t', 1)`
B. `SELECT * FROM table_changes('t', 3)`
C. `SELECT * FROM table_changes('t', 1, 3)`
D. `SELECT * FROM table_changes('t', 0, 2)`

**答案：B**
解析：CDF 只记录启用后的变更。版本 1 和 2 没有 CDF 数据，查询包含这些版本会报错或返回空。版本 3 及之后有 CDF 记录。

---

**A2.** 执行以下 MERGE 语句后，CDF 中会产生哪些 `_change_type` 记录？

```sql
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED AND s.status = 'closed' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

假设 source 有 5 行：2 行匹配且 status='closed'，2 行匹配且 status 其他值，1 行不匹配。

A. 2 delete, 2 update_postimage, 1 insert
B. 2 delete, 4 update (pre+post), 1 insert
C. 2 delete, 2 update_preimage + 2 update_postimage, 1 insert
D. 5 insert

**答案：C**
解析：DELETE 产生 2 条 delete 记录。UPDATE 产生 2 对 update_preimage + update_postimage（共 4 条）。INSERT 产生 1 条 insert。总计 7 条 CDF 记录。

---

### 子主题 B：Batch vs Streaming

**B1.** 以下代码每天执行一次，bronze 表每天新增约 1000 行（append-only），CDF 已启用。运行 30 天后，目标表有多少行？

```python
spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("bronze") \
    .filter(col("_change_type") == "insert") \
    .write.mode("append").table("target")
```

A. 30,000 行（每天 1000 行，无重复）
B. 465,000 行（1000 + 2000 + ... + 30000 的等差数列求和）
C. 1,000 行（每次覆盖写入）
D. 取决于 checkpoint 位置

**答案：B**
解析：batch read 无状态，每次从版本 0 读取全部 insert 记录。第 1 天写入 1000 行，第 2 天写入 2000 行（版本 0 到当前的所有 insert），... 第 30 天写入 30000 行。总计 `sum(1000*k, k=1..30) = 1000 * 30 * 31 / 2 = 465,000`。

---

**B2.** 要修复 B1 中的重复问题，以下哪种改法最可靠？

A. 将 `startingVersion` 改为当前版本减 1
B. 将 `.write.mode("append")` 改为 `.write.mode("overwrite")`
C. 将 `spark.read` 改为 `spark.readStream`，配合 checkpoint
D. 在目标表上执行 `DISTINCT` 去重

**答案：C**
解析：A 可能遗漏多版本变更；B 每次全量覆盖可行但低效且不适合大表；D 是事后补救不解决根因。C 用 streaming + checkpoint 从根本上解决增量追踪问题，是最可靠的方案。

---

### 子主题 C：CDF 消费模式

**C1.** 下游表需要始终反映源表的最新状态（Type 1 SCD）。以下哪种方式正确消费 CDF？

A. 过滤 insert + update_postimage，直接 append 到目标表
B. 过滤 insert + update_postimage，用 MERGE INTO 更新目标表；过滤 delete，从目标表删除
C. 将所有 _change_type 的记录直接 append 到目标表
D. 只过滤 update_postimage，用 overwrite 模式写入目标表

**答案：B**
解析：Type 1 SCD 要求目标表始终反映最新值。A 的 append 会导致同一主键有多个版本共存。C 会把 preimage 和 delete 也写入，完全错误。D 丢失了 insert 和 delete 处理。B 用 MERGE 正确处理 upsert，同时处理 delete，是完整方案。

---

**C2.** 在 Lakeflow Declarative Pipeline 中，上游 streaming table `orders` 启用了 CDF 且包含 UPDATE 操作。以下哪个方案能正确创建下游 enriched 表？

A. `spark.readStream.table("orders")` -> join -> write to streaming table
B. `spark.readStream.option("readChangeFeed", "true").table("orders")` -> join -> `apply_changes()` -> streaming table
C. `spark.readStream.option("skipChangeCommits", "true").table("orders")` -> join -> write to streaming table
D. `spark.read.table("orders")` -> join -> write to materialized view

**答案：B**
解析：A 会因 UPDATE 操作报错（append-only 假设被违反）。C 跳过变更 commit，丢失更新数据。D 是全量批处理，不满足近实时需求。B 正确读取 CDF，用 apply_changes() 处理变更语义。

---

### 子主题 D：功能选择

**D1.** 一张 Delta 表每天用 `mode("overwrite")` 重写。团队想在每次重写后比较新旧版本的差异。以下哪个方案最可靠？

A. 启用 CDF，用 `table_changes()` 读取差异
B. 用 `SELECT * FROM t VERSION AS OF v1 EXCEPT SELECT * FROM t VERSION AS OF v2`
C. 解析 Delta transaction log 找出新写入的文件
D. 用 `DESCRIBE HISTORY t` 查看变更详情

**答案：B**
解析：A 需要 CDF 预先启用，题目未提及；且 overwrite 操作下 CDF 的行为可能不符合预期。B 用 Time Travel 直接比较两个版本的全量快照差异，始终可用且结果准确。C 过于底层且不给行级内容。D 只有操作元数据，没有行级数据。

---

**D2.** 一张交易表频繁执行小批量 UPDATE（每次更新几百行，表总量数亿行），写入延迟成为瓶颈。应该启用什么？

A. Change Data Feed
B. Deletion Vectors
C. Auto Optimize (auto compaction)
D. Liquid Clustering

**答案：B**
解析：写入延迟的根因是每次 UPDATE 都要重写整个 Parquet 文件。Deletion Vectors 用软删除标记替代物理文件重写，直接解决问题。CDF 是消费侧功能，不影响写入性能。Auto Optimize 优化小文件合并，不解决 UPDATE 重写问题。Liquid Clustering 优化数据布局，也不直接解决 UPDATE 重写。

---

## 五、关键知识网络

```
                    Delta Lake 变更相关功能图谱
                    ===========================

  [写入侧优化]                              [消费侧功能]
       |                                          |
  Deletion Vectors                               CDF
  - 软删除标记                            - 记录行级变更
  - 减少文件重写                          - _change_type 四种值
  - enableDeletionVectors                 - enableChangeDataFeed
       |                                          |
       |                              +-----------+-----------+
       |                              |                       |
                                 Batch Read              Streaming Read
                              (spark.read)             (spark.readStream)
                              - 无状态                  - 有 checkpoint
                              - table_changes()         - 增量处理
                              - 需手动管理版本          - 自动追踪进度
                                      |                       |
                                      +----------+------------+
                                                 |
                                        CDF 输出消费方式
                                      /         |          \
                                MERGE INTO  apply_changes()  手动过滤+写入
                                (SQL/DF)    (Lakeflow)       (仅 append 场景)


  [内建功能 -- 无需启用]
       |
  +----+----+
  |         |
  Time    DESCRIBE
  Travel  HISTORY
  - VERSION AS OF    - 操作元数据
  - 全量快照比较     - 无行级数据
  - 始终可用         - 始终可用


  [读取有 UPDATE/DELETE 的 Delta 表]
  
  readStream.table()         --> 报错（append-only 假设）
  readStream + skipChange    --> 丢失数据
  readStream + readChangeFeed --> 正确（需配合 apply_changes/MERGE）
  spark.read + Time Travel   --> 可用但无增量能力
```

**核心联系：**
1. CDF 是"变更日志"，Deletion Vectors 是"写入优化"——前者面向下游消费者，后者面向存储引擎
2. CDF 的 batch read 和 streaming read 的区别 = `spark.read` 和 `spark.readStream` 在**任何**数据源上的区别（有无 checkpoint）
3. append-only 表 → readStream 足够；有 UPDATE/DELETE 的表 → 必须走 CDF 路径
4. Time Travel 是 Delta 内建能力（始终可用），CDF 是可选功能（需显式启用）——题目没提到启用就不能选 CDF

---

## 六、Anki 卡片（Q/A 格式）

**卡片 1**
Q: CDF 的四种 `_change_type` 值是什么？
A: `insert`（新行）、`update_preimage`（更新前旧值）、`update_postimage`（更新后新值）、`delete`（被删除的行）

**卡片 2**
Q: `spark.read.option("readChangeFeed", "true")` 和 `spark.readStream.option("readChangeFeed", "true")` 的核心区别是什么？
A: spark.read 是无状态的 batch read，每次从 startingVersion 重新读取全部变更；spark.readStream 有 checkpoint 机制，自动追踪已处理位置，实现增量处理。

**卡片 3**
Q: 为什么 batch read CDF + `startingVersion=0` + `mode("append")` 每天执行会产生大量重复？
A: 因为 batch read 无状态，每次都从版本 0 读取全部变更历史再 append。第 N 天执行时，版本 0 到 N-1 的记录已经被前几天写入过，但 batch read 不知道，会重复写入。

**卡片 4**
Q: append-only 的 Delta 表，下游要增量处理新记录，应该用 CDF 还是 readStream.table()？
A: 用 `readStream.table()`。append-only 表没有 UPDATE/DELETE，不需要 CDF 的 _change_type 信息。readStream 的 checkpoint 自动追踪新增行，更简单高效。

**卡片 5**
Q: 题目说"表每天 overwrite 重写，想比较新旧版本差异"，但没提到 CDF 已启用。应该选 CDF 还是 Time Travel？
A: Time Travel。CDF 需要显式启用（`delta.enableChangeDataFeed = true`），题目没提到就不能假设已启用。Time Travel 是 Delta Lake 内建功能，始终可用，可以用 `VERSION AS OF` 比较两个版本的全量快照。

**卡片 6**
Q: CDF 和 Deletion Vectors 分别解决什么问题？
A: CDF 是消费侧功能——记录行级变更供下游 ETL 消费（"发生了什么变更"）。Deletion Vectors 是写入侧优化——用软删除标记替代物理文件重写，减少 UPDATE/DELETE 的写入开销。

**卡片 7**
Q: 上游 streaming table 包含 UPDATE 操作，下游直接用 `readStream.table()` 读取会怎样？
A: 会报错。Structured Streaming 默认假设数据源是 append-only，检测到非 append 的变更（UPDATE/DELETE）会抛出异常。

**卡片 8**
Q: 在 Lakeflow 中，读取有 UPDATE 的上游表并创建下游 streaming table 的正确路径是什么？
A: `readStream.option("readChangeFeed", "true")` 读取 CDF，然后用 `apply_changes()` 将变更正确应用到目标 streaming table。

**卡片 9**
Q: `skipChangeCommits` 选项的作用是什么？为什么它通常不是正确答案？
A: `skipChangeCommits` 让 readStream 跳过包含数据变更（UPDATE/DELETE）的 commit。这意味着所有更新和删除操作都会被忽略，导致数据丢失。它只适用于你确实不关心变更内容、只想处理纯 append 数据的场景。

**卡片 10**
Q: `DESCRIBE HISTORY` 能获取行级变更数据吗？
A: 不能。`DESCRIBE HISTORY` 只返回操作元数据（操作类型、时间戳、用户、操作参数等），不包含任何行级数据内容。要获取行级数据，用 CDF 或 Time Travel。

**卡片 11**
Q: CDF 输出的两个额外元数据列是什么？
A: `_commit_version`（变更发生的 Delta 版本号）和 `_commit_timestamp`（变更发生的时间戳）。

**卡片 12**
Q: 为什么 CDF 输出不能直接 append 到 Type 1 SCD 目标表？
A: CDF 输出包含 insert、update_preimage、update_postimage、delete 四种记录类型。直接 append 会把旧值（preimage）和删除标记也写入目标表，且同一主键的更新会导致多行共存。正确做法是用 MERGE INTO 或 apply_changes() 将变更语义正确应用到目标表。

---

## 七、掌握度自测

### 题 1
以下代码每小时执行一次。bronze 表启用了 CDF，每小时有 INSERT 和 UPDATE 操作。执行 24 小时后，target 表的数据质量如何？

```python
spark.read.option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("bronze") \
    .filter(col("_change_type").isin(["insert", "update_postimage"])) \
    .write.mode("append").table("target")
```

A. target 表准确反映 bronze 的最新状态
B. target 表包含大量重复数据
C. target 表只包含最后一次执行的数据
D. target 表为空（代码有语法错误）

<details><summary>答案</summary>

**B.** batch read 无状态，每次从版本 0 读取全量变更再 append，24 次执行产生大量重复。

</details>

### 题 2
以下哪个功能不需要在 Delta 表上显式启用即可使用？

A. Change Data Feed
B. Deletion Vectors
C. Time Travel
D. Row Tracking

<details><summary>答案</summary>

**C.** Time Travel 是 Delta Lake 的内建功能，只要有 transaction log（Delta 表必有），就可以用 `VERSION AS OF` 或 `TIMESTAMP AS OF` 查询历史版本。CDF、Deletion Vectors、Row Tracking 都需要通过 TBLPROPERTIES 显式启用。

</details>

### 题 3
一个数据管道的源表只有 INSERT 操作（每批追加新数据），下游需要增量处理新到达的行。以下哪种方式最合适？

A. `spark.read.option("readChangeFeed", "true").table("source")`
B. `spark.readStream.table("source")`
C. `spark.readStream.option("readChangeFeed", "true").table("source")`
D. `spark.read.table("source").filter(col("_commit_version") > last_version)`

<details><summary>答案</summary>

**B.** append-only 表不需要 CDF，`readStream.table()` 通过 checkpoint 自动追踪已处理位置，每次只处理新增行。A 是无状态 batch read。C 虽然能工作但过度设计（CDF 对纯 append 场景无额外价值）。D 的 `_commit_version` 不是普通表的列，只在 CDF 输出中存在。

</details>

### 题 4
一张 Delta 表频繁执行小批量 DELETE 操作（每次删除几十行，表总量上亿行），团队发现每次 DELETE 都触发整个 Parquet 文件的重写，导致延迟很高。应该启用什么功能？

A. Change Data Feed
B. Liquid Clustering
C. Deletion Vectors
D. Auto Optimize

<details><summary>答案</summary>

**C.** Deletion Vectors 用软删除标记（bitmap）代替物理文件重写，DELETE 操作只需更新标记文件而不重写数据文件，大幅降低延迟。CDF 不影响写入行为。Liquid Clustering 优化数据布局。Auto Optimize 合并小文件。

</details>

### 题 5
在 Lakeflow Declarative Pipeline 中，上游 streaming table `events` 启用了 CDF 并包含 UPDATE 操作。以下哪种方式能正确读取变更数据？

A. `spark.readStream.table("events")`
B. `spark.readStream.option("skipChangeCommits", "true").table("events")`
C. `spark.readStream.option("readChangeFeed", "true").table("events")`
D. `spark.read.option("readChangeFeed", "true").table("events")`

<details><summary>答案</summary>

**C.** A 会因 append-only 假设被违反而报错。B 跳过变更 commit 会丢失更新数据。D 是无状态 batch read，不适合持续增量处理。C 正确读取 CDF，配合 apply_changes() 可将变更应用到下游 streaming table。

</details>

### 题 6
一个 MERGE INTO 语句对源表执行了以下操作：INSERT 10 行，UPDATE 5 行，DELETE 3 行。CDF 会产生多少条变更记录？

A. 18 条
B. 23 条
C. 13 条
D. 15 条

<details><summary>答案</summary>

**B. 23 条。** INSERT 产生 10 条 insert 记录。UPDATE 产生 5 条 update_preimage + 5 条 update_postimage = 10 条。DELETE 产生 3 条 delete 记录。总计 10 + 10 + 3 = 23 条。

</details>

### 题 7
以下关于 `table_changes()` SQL 函数的说法，哪个是正确的？

A. 它自动追踪上次读取位置，每次只返回新变更
B. 它是无状态的，每次返回指定版本范围内的所有变更记录
C. 它等价于 `spark.readStream.option("readChangeFeed", "true")`
D. 它不需要表启用 CDF 即可使用

<details><summary>答案</summary>

**B.** `table_changes()` 是 SQL 版的 CDF batch read，完全无状态，每次返回指定版本范围内的全部变更。它没有 checkpoint 机制，不追踪进度。它需要表已启用 CDF。它不等价于 readStream（readStream 有 checkpoint）。

</details>

### 题 8
`DESCRIBE HISTORY customer_table` 的输出包含以下哪些信息？

A. 每个操作涉及的行级数据变更
B. 操作类型、时间戳、用户、操作参数等元数据
C. 每个版本的完整数据快照
D. 与 CDF 相同的 _change_type 记录

<details><summary>答案</summary>

**B.** DESCRIBE HISTORY 只返回操作级别的元数据（operationType, timestamp, userName, operationParameters, operationMetrics 等），不包含任何行级数据内容。要获取行级数据用 CDF 或 Time Travel。

</details>
