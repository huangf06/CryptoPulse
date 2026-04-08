# 04 - 性能优化与 Spark 调优：深度复习辅导材料

> 基于 14 道错题（正确率 0%）的系统性诊断与强化训练。

---

## 一、错误模式诊断

### 模式 1：Spark UI Tab 功能的顽固错误关联（Q51, Q220, Q244）

**症状**：三道 Spark UI 查询计划相关题目，全部选了 Jobs tab 或 Executor log，从未选对 SQL/DataFrame tab。

**认知缺陷**：脑中存在一个未经验证的直觉 -- "查询计划 = Jobs tab"。这不是知识缺失（不知道有 SQL tab），而是**错误知识的固化** -- 一个从未被现实校验的假设被当成了事实。三次重复同一错误，说明第一次做错后没有深度纠正，只是浅层标记"记住选 C"。

**根因**：缺乏 Spark UI 的实际使用经验。所有关于 Spark UI 的知识都是文字层面的，没有视觉记忆支撑。当考试压力下需要快速决策时，直觉（错误的）压过了理性记忆。

**修复策略**：必须建立 "功能 -> Tab" 的**条件反射级映射**，而非靠考试时回忆。建议在 Databricks Community Edition 中实际打开 Spark UI，逐 tab 截图，建立视觉锚点。

### 模式 2：数据布局三技术的选择混淆（Q238, Q319, Q288）

**症状**：无法区分 Z-Order 和 Liquid Clustering 的适用场景，Q319 甚至选了 Hive Partition + Z-Order 组合。

**认知缺陷**：理解每种技术"是什么"，但不能根据**题目关键词**快速匹配到正确技术。核心问题是没有建立"关键词 -> 技术"的判断树：
- "incremental, evolvable, easy to maintain" -> Liquid Clustering
- "immediate data skipping" -> Z-Order
- "low cardinality" -> Hive Partition

**根因**：对三种技术的**差异维度**掌握不精确。比如，Z-Order 每次 OPTIMIZE 需要重写涉及的文件（非增量），Liquid Clustering 只处理新数据和需要重组的数据（增量）。这个关键区别没有内化。

### 模式 3：参数功能与作用阶段混淆（Q70, Q311）

**症状**：将 `advisoryPartitionSizeInBytes`（AQE shuffle 后参数）用于无 shuffle 场景；多选题只选一个答案。

**认知缺陷**：三个文件大小控制参数（`maxPartitionBytes`、`shuffle.partitions`、`advisoryPartitionSizeInBytes`）的**作用阶段**没有区分清楚。另外，多选题的答题策略有漏洞 -- 没有确认选够数量。

### 模式 4：Delta Lake 机制的概念错误（Q97, Q45）

**症状**：认为 Type 1 SCD 的 in-place 修改会导致 time travel 不可用（实际上 Delta 是 MVCC，不存在 in-place 修改）；混淆 database LOCATION 和 table LOCATION 的语义。

**认知缺陷**：对 Delta Lake 的存储机制理解不够底层。Delta 的写操作永远是追加新 Parquet 文件 + 更新 transaction log，不会修改已有文件。这个核心原理没有真正吃透。

### 模式 5：Pandas UDF 类型选择（Q322）

**症状**：选了 SCALAR_ITER + Delta 持久化，而非 applyInPandas。

**认知缺陷**：没有建立"分组有状态计算 = grouped map = applyInPandas"的直接映射。对四种 Pandas UDF 类型的适用场景缺乏清晰的决策框架。

---

## 二、子主题分解与学习方法

### 子主题 A：Spark UI 各 Tab 功能与诊断方法

**知识类型**：诊断性知识（给定症状，判断去哪看、看什么指标）

**推荐学习方法**：
1. **实操优先**：在 Databricks Community Edition 上跑一个包含 join + aggregation 的 query，逐 tab 浏览 Spark UI，截图标注每个 tab 的关键信息。
2. **条件-动作表记忆**：将 "诊断目标 -> Tab -> 指标" 做成条件反射表，反复默写。
3. **反面强化**：每个 tab 明确标注"这里看不到什么" -- Jobs tab 看不到查询计划，Executor tab 看不到 push-down。

**依赖关系**：需要先理解 Spark 执行模型（Job -> Stage -> Task）和查询计划（Logical Plan -> Physical Plan）的基本概念。

**Case Study 实操建议**：
```
-- 在 Databricks 中执行
CREATE TABLE test_pushdown USING delta AS SELECT * FROM range(1000000);
SELECT * FROM test_pushdown WHERE id > 999990;
-- 然后去 Spark UI -> SQL/DataFrame tab -> 点击该 query -> 查看 Physical Plan -> 找 PushedFilters
```

### 子主题 B：Query Profiler vs Spark UI

**知识类型**：概念性知识（两个工具的定位区分）

**推荐学习方法**：
1. **对比表记忆**：Spark UI = 底层执行细节（stages, tasks, shuffle），Query Profiler = 高层查询分析（wall-clock time, query source）。
2. **关键指标辨析**：wall-clock duration vs aggregated task time，前者是真实耗时，后者是所有 task 时间之和（并行 -> 远大于 wall-clock）。

**依赖关系**：需要理解并行执行的概念（为什么 aggregated task time != wall-clock time）。

### 子主题 C：数据布局优化技术（Partition / Z-Order / Liquid Clustering）

**知识类型**：决策性知识（给定场景条件，选择最优技术）

**推荐学习方法**：
1. **决策树构建**：画出判断流程图 -- 先看基数高低，再看是否需要增量/可演进，最后确定技术。
2. **关键词触发训练**：看到 "incremental" 立刻联想 Liquid Clustering，看到 "immediate data skipping" 联想 Z-Order。
3. **对比维度精记**：增量性、列可变更性、MERGE 友好度、写入影响 -- 四个维度足以区分三种技术。

**依赖关系**：需要理解 Delta Lake 的文件组织方式（Parquet files + transaction log）和 data skipping 原理（min/max statistics）。

### 子主题 D：集群配置与内存管理

**知识类型**：原理性知识 + 参数调优知识

**推荐学习方法**：
1. **因果链记忆**：disk spill 的因果链 -- executor 内存不足 -> 中间数据无法全放内存 -> 溢写到磁盘 -> 解决方案：增加内存（高 core:memory 比实例）或减少单 task 数据量（减小 maxPartitionBytes）。
2. **参数作用阶段表**：三个参数（maxPartitionBytes, shuffle.partitions, advisoryPartitionSizeInBytes）必须和作用阶段（读取时 / shuffle 后 / AQE shuffle 后）一一对应。
3. **Wide transformation 集群选择原则**：总资源相同时，更多小节点优于少量大节点（JVM GC 效率 + 并行 shuffle 通道）。

**依赖关系**：需要理解 JVM 内存管理和 GC 机制的基本概念，以及 shuffle 的网络开销。

### 子主题 E：Delta Lake 性能相关特性

**知识类型**：概念性知识 + 机制性知识

**推荐学习方法**：
1. **MVCC 核心原理**：Delta 的写操作 = 新文件 + 新 log entry，永不修改旧文件。Time travel = 读取旧版本 log 指向的旧文件。
2. **Time travel 局限性**：长期保留 = 存储成本线性增长 + 查询延迟增加。所以长期审计用 Type 2 SCD，不用 time travel。
3. **Managed vs External table 判断**：CREATE TABLE 语句中有 EXTERNAL 或 LOCATION -> external table；否则 -> managed table。Database 的 LOCATION 只决定数据存储在哪，不影响表类型。

### 子主题 F：Pandas UDF 类型选择

**知识类型**：决策性知识

**推荐学习方法**：
1. **四类型速记**：SCALAR（逐列变换）、SCALAR_ITER（逐列变换 + 初始化优化）、GROUPED_AGG（分组聚合返回标量）、GROUPED_MAP/applyInPandas（分组内有状态计算，输入输出都是 DataFrame）。
2. **关键判断**：题目说"分组 + 需要跨行状态" -> applyInPandas，没有其他选项。

---

## 三、苏格拉底式教学问题集

### A. Spark UI 与性能诊断

1. 如果你在 Spark UI 的 Jobs tab 上看到某个 job 耗时特别长，下一步应该去哪个 tab 深入诊断？你在那个 tab 上具体看什么指标？
2. Physical Plan 中的 `PushedFilters: []` 和 `PushedFilters: [IsNotNull(col)]` 分别意味着什么？如果 push-down 没有生效，可能的原因有哪些？
3. 为什么 Executor log 不是诊断 predicate push-down 的正确位置？Executor log 通常包含什么类型的信息？
4. 如果一个 MEMORY_ONLY 缓存的 DataFrame 在 Storage tab 显示 "Size on Disk = 128MB"，这说明了什么？可能的根因是什么？
5. wall-clock duration 为 5 分钟，aggregated task time 为 200 分钟。这正常吗？如果 aggregated task time 只比 wall-clock duration 大一点点（比如 6 分钟），这又说明什么？

### B. 数据布局优化

1. 一张 user_events 表，user_id 有 1000 万个不同值，日常查询按 user_id 过滤。如果用 Hive-Style Partition BY user_id，会发生什么？
2. Z-Order 和 Liquid Clustering 都支持高基数列。在什么场景下你会明确选择 Z-Order 而不是 Liquid Clustering？
3. Liquid Clustering 的"增量优化"具体是什么意思？与 Z-Order 的 OPTIMIZE 有什么本质区别？
4. 为什么 Deletion Vectors 能显著提升 MERGE 性能？不用 Deletion Vectors 时，MERGE 的 delete/update 操作在底层做了什么？

### C. 集群配置与内存管理

1. 同样 400GB RAM 和 160 cores，为什么 16 台 25GB 的机器比 1 台 400GB 的机器在 shuffle 场景下性能更好？从 JVM GC 角度解释。
2. disk spill 的根因是什么？"增加磁盘空间"为什么不是解决 disk spill 的方案？
3. `spark.sql.files.maxPartitionBytes` 设为 256MB 和 1GB 时，对 disk spill 的影响分别是什么？为什么？
4. core-to-memory ratio 1:2 和 1:8 分别适合什么类型的工作负载？

### D. 文件大小控制

1. 一个 pipeline 只做 filter + select（narrow transformations），想控制输出文件大小为 256MB。应该用哪个参数？为什么 `shuffle.partitions` 在这里无效？
2. `coalesce(100)` 和 `repartition(100)` 都能将分区数设为 100。什么情况下必须用 `repartition` 而不能用 `coalesce`？
3. AQE 的 `advisoryPartitionSizeInBytes` 在什么条件下才会生效？如果 pipeline 没有任何 shuffle 操作，设置这个参数有效果吗？

### E. Delta Lake 性能特性

1. Delta Lake 中执行 UPDATE 时，底层实际发生了什么操作？旧的 Parquet 文件被修改了吗？
2. 既然 Delta time travel 技术上可以查看任何历史版本，为什么说它"不适合长期版本化方案"？具体的 cost 和 latency 问题出在哪里？
3. 在一个 database 上设置了 `LOCATION '/mnt/data'`，然后在其中 `CREATE TABLE t1 (id INT)` -- 这张表是 managed 还是 external？数据存在哪里？

### F. Pandas UDF

1. `applyInPandas` 和 `GROUPED_AGG` UDF 都能处理分组数据。它们的根本区别是什么？什么场景下 GROUPED_AGG 不够用，必须用 applyInPandas？
2. 为什么"用全局变量在 executor 之间共享状态"是不安全的？Spark 的执行模型中，executor 之间的隔离性是怎样的？
3. SCALAR_ITER 和 SCALAR 的区别是什么？什么场景下 SCALAR_ITER 的"批量迭代"特性有实际价值？

---

## 四、场景判断题 / Case Study

### A. Spark UI 诊断

**A1.** 一个 ETL pipeline 处理 500GB 的日志数据，执行 join + aggregation 后写入 Delta table。运行时间从平时的 30 分钟变成了 3 小时。你打开 Spark UI，首先应该去哪个 tab 做初步排查？

- A. Jobs tab -- 查看哪个 job 最慢
- B. SQL/DataFrame tab -- 查看查询计划
- C. Executors tab -- 查看是否有 executor 挂掉
- D. Storage tab -- 查看缓存状态

**答案：A**
**解析**：初步排查的第一步是定位慢在哪里。Jobs tab 显示所有 job 的执行时间，能快速识别哪个 job 异常。找到最慢的 job 后，再进入 Stages tab 看具体 stage 的 task 分布、shuffle、spill 等细节。SQL/DataFrame tab 适合在定位到具体 query 后查看执行计划，不适合初步排查。

---

**A2.** 你在 Stages tab 中发现一个 stage 的 task duration 分布极不均匀：大部分 task 在 10 秒内完成，但有 3 个 task 耗时 20 分钟。最可能的根因是什么？

- A. Executor 内存不足导致 disk spill
- B. 数据倾斜（data skew）-- 少数 partition 包含大量数据
- C. 网络带宽不足导致 shuffle read 慢
- D. Predicate push-down 未生效

**答案：B**
**解析**：task duration 分布不均匀是数据倾斜的典型信号。少数 partition 包含远多于平均水平的数据，对应的 task 需要处理更多数据，因此耗时更长。如果是 disk spill，所有 task 都会变慢（因为都在溢写），而不是只有少数 task 特别慢。

---

**A3.** 你怀疑一个 query 没有利用 predicate push-down。以下哪种验证方式最直接有效？

- A. 在 Stages tab 比较有无 filter 时的 Input Size
- B. 在 SQL/DataFrame tab 查看 Physical Plan 中 Scan 节点的 PushedFilters 字段
- C. 在 Executor log 中搜索 "predicate"
- D. 检查 Delta transaction log 中的列统计信息

**答案：B**
**解析**：Physical Plan 中 Scan 节点的 PushedFilters 字段直接显示哪些谓词被下推。如果 PushedFilters 为空或缺少预期的过滤条件，则 push-down 未生效。Input Size（A）是间接指标，可能受其他因素影响；Executor log（C）不包含 push-down 信息；Delta transaction log（D）包含列统计但不直接反映执行时的 push-down 行为。

---

### B. 数据布局优化

**B1.** 一家电商平台的 orders 表有 20 亿行，product_id 有 500 万个不同值。分析师的常见查询是 `WHERE product_id = 'xxx' AND order_date BETWEEN '2025-01-01' AND '2025-03-31'`。当前没有任何优化，全表扫描耗时 40 分钟。最合适的优化策略是什么？

- A. PARTITION BY product_id
- B. PARTITION BY order_date, ZORDER BY product_id
- C. ALTER TABLE orders CLUSTER BY (product_id, order_date)
- D. OPTIMIZE orders ZORDER BY (product_id, order_date)

**答案：C**
**解析**：product_id 有 500 万个值，Hive-Style Partition 会产生海量小文件（A 排除）。B 的 PARTITION BY order_date 可以接受（日期基数低），但组合 Z-Order 不如 Liquid Clustering 灵活。C 的 Liquid Clustering 支持多列聚簇、增量优化、列可变更，是最全面的解决方案。D 的 Z-Order 也可行，但如果未来查询模式变化（如改为按 region 过滤），Liquid Clustering 更容易调整。这里选 C 是因为需要长期维护和演进能力。

---

**B2.** 一张 800GB 的 customer_profiles 表频繁执行 MERGE 操作（基于 customer_id 匹配）。当前 MERGE 耗时 2 小时。以下哪两个优化措施最有效？

- A. ALTER TABLE customer_profiles CLUSTER BY (customer_id)
- B. ALTER TABLE customer_profiles SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)
- C. PARTITION BY customer_id
- D. 增加集群节点数量
- E. OPTIMIZE customer_profiles ZORDER BY (customer_id)

**答案：AB**
**解析**：A -- Liquid Clustering on merge key 使相同 customer_id 的数据物理上紧密存储，MERGE 扫描范围大幅缩小。B -- Deletion Vectors 使 update/delete 操作只需标记行而非重写整个 Parquet 文件，大幅减少 I/O。C 错误：customer_id 基数太高，产生小文件。D 只是增加计算资源，不解决根本的 I/O 问题。E 的 Z-Order 对 MERGE 优化不如 Liquid Clustering 直接。

---

**B3.** 数据团队将一张表从 Z-Order 迁移到 Liquid Clustering。以下关于迁移的说法，哪个是正确的？

- A. 迁移需要全表重写，期间表不可用
- B. ALTER TABLE ... CLUSTER BY 只修改元数据，后续 OPTIMIZE 会逐步应用 Liquid Clustering
- C. 迁移后，之前的 Z-Order 索引仍然有效
- D. Liquid Clustering 不兼容已有的 Hive-Style Partition

**答案：B**
**解析**：`ALTER TABLE ... CLUSTER BY` 是元数据操作，不会立即重写数据。后续执行 `OPTIMIZE` 时，新数据和需要重组的数据会按 Liquid Clustering 方式组织。旧数据仍然是 Z-Order 格式，直到被 OPTIMIZE 覆盖。这体现了 Liquid Clustering 的"增量"特性。

---

### C. 集群配置与内存管理

**C1.** 一个 Spark job 在 Stages tab 中显示大量 Spill (Disk)。当前使用的实例类型是 c5.4xlarge（16 cores, 32GB RAM，core:memory = 1:2）。以下哪个调整最能有效减少 disk spill？

- A. 切换到 r5.4xlarge（16 cores, 128GB RAM，core:memory = 1:8）
- B. 增加集群节点数量（保持相同实例类型）
- C. 增大 spark.sql.shuffle.partitions
- D. 增加 EBS 卷大小

**答案：A**
**解析**：disk spill 的根因是每个 task 的可用内存不足。从 c5（1:2）切换到 r5（1:8），每个 core 从 2GB 增加到 8GB，task 可用内存大幅增加，直接减少 spill。B 增加节点数但每个节点的 core:memory 比不变，单个 task 的可用内存没有增加。C 增大 shuffle.partitions 可以间接减少每个 task 的数据量，但不如直接增加内存有效。D 只是给 spill 更多空间存储，不减少 spill 本身。

---

**C2.** 一个纯 narrow transformation pipeline（filter + select + withColumn），输入数据 2TB，需要输出约 512MB 大小的 Parquet 文件。以下哪个配置能达到目标？

- A. spark.sql.files.maxPartitionBytes = 512MB
- B. spark.sql.shuffle.partitions = 4096
- C. spark.sql.adaptive.advisoryPartitionSizeInBytes = 512MB
- D. 在写入前调用 repartition(4096)

**答案：A**
**解析**：纯 narrow transformation 不触发 shuffle，分区数和分区大小在读取时就确定了。`maxPartitionBytes = 512MB` 使读取时每个分区约 512MB，经过 narrow transformation 后分区大小基本不变，写出的文件约 512MB。B 和 C 都只在 shuffle 后生效，此 pipeline 没有 shuffle。D 会触发 shuffle，违反"无 shuffle"的隐含要求（增加不必要的开销）。

---

**C3.** 团队讨论集群配置方案。一个 ETL job 包含多次 wide transformation（join 3 张表 + groupBy + agg）。总预算允许 200 cores + 800GB RAM。以下哪个配置最优？

- A. 2 台机器，每台 100 cores / 400GB
- B. 10 台机器，每台 20 cores / 80GB
- C. 50 台机器，每台 4 cores / 16GB
- D. 1 台机器，200 cores / 800GB

**答案：B**
**解析**：wide transformation 需要 shuffle，更多节点 = 更多并行 shuffle 通道。但不是无限细分 -- C 的 50 台机器每台只有 4 cores/16GB，网络开销（shuffle 跨节点传输）可能成为瓶颈，且每台机器上 JVM 管理开销相对于可用资源过大。B 的 10 台 / 每台 20 cores + 80GB 是合理的平衡点。A 和 D 的大节点 JVM GC 压力大。

---

### D. Delta Lake 与 Pandas UDF

**D1.** 架构师提议：对 customer_360 表使用 Type 1 SCD（直接覆盖更新），同时配置 VACUUM RETAIN 365 DAYS 来保留一年的历史版本用于审计。这个方案有什么问题？

- A. Type 1 SCD 在 Delta 中不支持 time travel
- B. 保留 365 天的历史文件会导致存储成本和查询历史版本的延迟大幅增加
- C. VACUUM RETAIN 365 DAYS 会阻止所有对表的写入操作
- D. Delta transaction log 在 365 天后会自动删除

**答案：B**
**解析**：Delta time travel 技术上可行（A 错误 -- Type 1 SCD 不是 in-place 修改，Delta 的 MVCC 保留旧文件）。但 365 天不 VACUUM 意味着所有历史 Parquet 文件都要保留，存储成本线性增长。查询历史版本时，需要读取旧的 transaction log entry 并定位旧文件，版本越多延迟越大。正确做法是用 Type 2 SCD 将历史作为普通数据行保存，VACUUM 正常执行。

---

**D2.** 数据工程师需要对 stock_prices 表按 stock_symbol 分组，计算每组内的 30 日滚动平均和累积收益率。计算过程中需要在组内维护状态（前一行的收益率）。最优实现方式是什么？

- A. 用 GROUPED_AGG UDF，在 UDF 内部用 list 维护状态
- B. 用 applyInPandas，接收每组完整 DataFrame，用局部变量维护状态
- C. 用 SCALAR_ITER UDF，将中间状态写入 Delta 表
- D. 用 Window 函数 + UDF 的组合

**答案：B**
**解析**：applyInPandas 接收同一 group 的所有行作为完整 pandas DataFrame，可以在函数内自然地用局部变量维护滚动状态。A 错误：GROUPED_AGG 返回标量（单个聚合值），无法返回每行都有值的 DataFrame。C 错误：Delta 表持久化引入不必要的 I/O 开销。D 的 Window 函数可以算滚动平均，但"累积收益率"等复杂有状态计算不一定能用标准 Window 函数表达。

---

## 五、关键知识网络

```
性能优化
|
+-- 诊断工具
|   +-- Spark UI
|   |   +-- Jobs tab ---- 定位最慢 job
|   |   +-- Stages tab ---- data skew, disk spill, shuffle metrics
|   |   +-- SQL/DataFrame tab ---- Physical Plan, PushedFilters, join strategy
|   |   +-- Storage tab ---- 缓存状态 (MEMORY_ONLY: disk should = 0)
|   |   +-- Executors tab ---- GC time, memory usage
|   +-- Query Profiler
|       +-- Total wall-clock duration (真实耗时)
|       +-- Aggregated task time (所有 task 时间之和, 因并行 >> wall-clock)
|
+-- 数据布局优化
|   +-- Hive-Style Partition ---- 低基数列, 不可演进
|   +-- Z-Order ---- 中高基数, immediate data skipping, 非增量
|   +-- Liquid Clustering ---- 高基数, incremental, evolvable, MERGE 友好
|   +-- Deletion Vectors ---- 减少 MERGE/UPDATE/DELETE 的文件重写 I/O
|
+-- 集群与内存
|   +-- Wide transformation: 多小节点 > 少大节点 (GC + shuffle 并行)
|   +-- Disk spill 解决: 高 core:memory 比实例 + 减小 maxPartitionBytes
|   +-- Instance types: memory-optimized (1:8) vs compute-optimized (1:2)
|
+-- 文件大小控制
|   +-- 读取时 (无 shuffle): maxPartitionBytes
|   +-- Shuffle 后: shuffle.partitions
|   +-- AQE shuffle 后: advisoryPartitionSizeInBytes
|   +-- 手动: repartition (有 shuffle) vs coalesce (无 shuffle, 只减不增)
|
+-- Delta Lake 性能
|   +-- Time travel: MVCC, 旧文件保留, 不适合长期版本化 (成本 + 延迟)
|   +-- Managed vs External: CREATE TABLE 的 EXTERNAL/LOCATION 决定类型
|   +-- Database LOCATION 决定存储路径, 不影响表类型
|
+-- Pandas UDF
    +-- SCALAR: 逐列变换
    +-- SCALAR_ITER: 逐列 + 初始化优化
    +-- GROUPED_AGG: 分组聚合 -> 标量
    +-- GROUPED_MAP (applyInPandas): 分组内有状态计算 -> DataFrame
```

---

## 六、Anki 卡片（Q/A 格式）

**Q1:** Spark UI 中查看 Physical Plan（查询执行计划）应该去哪个 tab？
**A1:** SQL/DataFrame tab -> 点击具体 query -> Physical Plan。绝不是 Jobs tab。

**Q2:** 如何在 Spark UI 中验证 predicate push-down 是否生效？
**A2:** SQL/DataFrame tab -> Physical Plan -> Scan 节点的 PushedFilters 字段。如果 PushedFilters 包含过滤条件则生效，为空则未生效。

**Q3:** MEMORY_ONLY 存储级别下，Storage tab 中什么指标信号表示缓存异常？
**A3:** Size on Disk > 0。MEMORY_ONLY 意味着数据应全部在内存中，磁盘上有数据说明内存不足导致驱逐/溢写。

**Q4:** wall-clock duration 和 aggregated task time 的区别是什么？
**A4:** wall-clock duration = 查询从开始到完成的真实经过时间（在 Query Profiler 查看）。aggregated task time = 所有 task 执行时间之和，因为 task 并行执行，通常远大于 wall-clock time。

**Q5:** 题目出现 "incremental, easy to maintain, evolvable" 关键词，应该选择哪种数据布局优化技术？
**A5:** Liquid Clustering。它支持增量优化（只处理新数据和需要重组的数据）、ALTER TABLE CLUSTER BY 可随时更换聚簇列、无需手动管理分区。

**Q6:** Disk spill 的两个主要解决方向是什么？
**A6:** (1) 选择更高 core-to-memory ratio 的 memory-optimized 实例（如 1:8 代替 1:2），增加每个 task 可用内存。(2) 减小 `spark.sql.files.maxPartitionBytes`，使每个 task 处理更少数据。

**Q7:** 纯 narrow transformation pipeline（无 shuffle）中，控制输出文件大小应该用哪个参数？
**A7:** `spark.sql.files.maxPartitionBytes`。该参数控制读取时的分区大小，narrow transformation 不改变分区数和大小，所以输出文件大小 = 读取时的分区大小。

**Q8:** Delta Lake 中 Type 1 SCD（覆盖更新）后，time travel 还能查到旧数据吗？为什么？
**A8:** 能。Delta 使用 MVCC 机制，UPDATE 操作写入新 Parquet 文件 + 更新 transaction log，旧文件不被修改。Time travel 读取旧版本 log 指向的旧文件。但 VACUUM 后旧文件被删除则不行。

**Q9:** 分组内需要跨行维护状态的计算（如滚动平均），应该用哪种 Pandas UDF？
**A9:** applyInPandas（grouped map）。它将同一 group 的所有行作为完整 pandas DataFrame 传入函数，可以在函数内用局部变量自然维护状态。

**Q10:** `repartition()` 和 `coalesce()` 的核心区别是什么？
**A10:** `repartition()` 触发 full shuffle，可以增加或减少分区数，数据均匀分布。`coalesce()` 不触发 shuffle，只能减少分区数（合并相邻分区），可能导致数据分布不均。

**Q11:** Deletion Vectors 如何提升 MERGE 性能？
**A11:** 不启用 Deletion Vectors 时，update/delete 需要重写整个 Parquet 文件。启用后，只需在独立的 deletion vector 文件中标记被删除/修改的行，大幅减少 I/O。

**Q12:** 总资源相同时，wide transformation 场景下为什么更多小节点优于少量大节点？
**A12:** 两个原因：(1) JVM 管理小堆内存（如 25GB）比大堆内存（如 400GB）的 GC 效率高，暂停时间短；(2) 更多节点提供更多并行 shuffle 通道，提升 shuffle 效率。但需要平衡网络开销，不是无限细分。

---

## 七、掌握度自测

以下 8 道综合判断题，覆盖本主题所有子领域。

**1.** 你在 Spark UI 的 Jobs tab 中可以看到查询的 Physical Plan。(True / False)

**答案：False**
Physical Plan 在 SQL/DataFrame tab 中查看，不在 Jobs tab。Jobs tab 显示 job 列表和执行时间。

---

**2.** Aggregated task time 等于查询的实际执行时间（wall-clock time）。(True / False)

**答案：False**
Aggregated task time 是所有 task 执行时间之和。由于 task 并行执行，aggregated task time 通常远大于 wall-clock time。实际执行时间应使用 Query Profiler 的 Total wall-clock duration。

---

**3.** 对于一张高基数列（1000 万个不同值）的表，Hive-Style Partition 是合适的优化策略。(True / False)

**答案：False**
高基数列使用 Hive-Style Partition 会产生海量小文件（每个不同值一个分区目录）。应使用 Z-Order 或 Liquid Clustering。

---

**4.** `spark.sql.adaptive.advisoryPartitionSizeInBytes` 在没有 shuffle 的 pipeline 中也能控制输出文件大小。(True / False)

**答案：False**
`advisoryPartitionSizeInBytes` 是 AQE (Adaptive Query Execution) 参数，仅在 shuffle 后生效。无 shuffle 场景应使用 `spark.sql.files.maxPartitionBytes`。

---

**5.** Delta Lake 中 Type 1 SCD 的 UPDATE 操作会 in-place 修改原始 Parquet 文件。(True / False)

**答案：False**
Delta Lake 使用 MVCC 机制。UPDATE 写入新 Parquet 文件并在 transaction log 中记录新版本，旧文件保留直到 VACUUM 清理。没有任何操作会修改已有的 Parquet 文件。

---

**6.** 减小 `spark.sql.files.maxPartitionBytes` 可以帮助减少 disk spill。(True / False)

**答案：True**
减小 `maxPartitionBytes` 使每个分区更小，每个 task 处理的数据量减少，峰值内存需求降低，从而减少溢写到磁盘的可能性。

---

**7.** `applyInPandas` 接收同一分组的所有行作为一个完整的 pandas DataFrame，适合需要跨行维护状态的计算。(True / False)

**答案：True**
applyInPandas（grouped map）将同一 group 的所有行传入函数，函数可以在内部用局部变量维护任意状态，适合滚动计算、累积计算等跨行有状态场景。

---

**8.** 增加集群的磁盘空间是解决 disk spill 的有效方法。(True / False)

**答案：False**
增加磁盘空间只是给 spill 提供更多存储空间，不减少 spill 本身。spill 的性能损失来自于内存和磁盘之间的数据交换 I/O，只要 spill 存在，性能就受影响。应从根本上减少 spill：增加每个 task 可用内存（memory-optimized 实例）或减少每个 task 数据量（减小 maxPartitionBytes）。
