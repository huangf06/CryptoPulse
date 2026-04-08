# 08 - DLT / Lakeflow Declarative Pipelines -- 深度复习辅导

> 6 题全错（0%）。这不是知识点零散遗漏，而是对 DLT 声明式执行模型的系统性误解。以下材料针对错误根因逐层拆解。

---

## 一、错误模式诊断

### 1. 把 DLT 当成普通 notebook 框架

**表现**：Q212 选了 "import other notebook as library"，Q218 试图在 expectation 中调用跨表 UDF。

**根因**：没有理解 DLT 的执行模型是声明式的 -- 你定义"数据应该是什么样"，DLT 引擎决定如何执行。每个 notebook 有独立的执行上下文，不共享 Python 全局变量，不支持 `%run`/`import`。这不是"限制"，而是声明式框架的本质属性：如果允许命令式的 import 链，引擎就无法自由调度和优化执行计划。

**修正规则**：DLT notebook 之间的通信只有两条路 -- (1) 通过 Delta 表读写数据，(2) 通过 pipeline 参数传递配置。没有第三种。

### 2. 混淆"声明式"与"只能批处理"

**表现**：Q302 选了 "Structured Streaming can process continuous data streams, while DLT cannot"。

**根因**：把"声明式"等同于"静态的/批处理的"。实际上声明式描述的是编程范式（what vs how），不是处理模式。DLT 底层完全可以用 Structured Streaming 做增量处理 -- STREAMING LIVE TABLE 就是证据。

**修正规则**：DLT vs Structured Streaming 的真正差异永远在运维层面（编排、checkpoint、集群、重试的自动化），不在处理能力层面。

### 3. 物化策略选择缺乏成本思维

**表现**：Q69 选了 view 而非物化表。

**根因**：只考虑了"数据一致性"（view 总是最新），没有做成本分析 -- 多用户高频交互查询场景下，view 每次都要重算聚合，计算成本远高于预计算物化表。

**修正规则**：物化策略三要素 -- 刷新频率、查询频率、用户数量。三者综合决定方案，不能只看一个维度。

### 4. 对 "privacy by design" 原则认知不足

**表现**：Q308 选了"Bronze 存明文，Gold 再脱敏"。

**根因**：用的是"先收集再治理"的旧思路。合规要求的是 PII 在写入的第一个环节就被脱敏 -- 即使 Bronze 层也不能存明文。

**修正规则**：看到 "masked before storage" 或类似措辞，答案一定指向写入时脱敏（write-time masking），排除所有 read-time masking 和下游处理方案。

### 5. 日志体系分层不清

**表现**：Q85 选了 Driver Log 而非 Cluster Event Log。

**根因**：没有建立"应用层 vs 基础设施层"的心智模型。Driver/Executor Log 是 Spark 应用程序日志，Cluster Event Log 是集群生命周期日志，两者属于不同层次。

---

## 二、子主题分解与学习方法

### 子主题 A: LIVE TABLE vs STREAMING LIVE TABLE

**核心区分**：全量重算 vs 增量处理。判断标准是数据源是否 append-only 以及是否需要增量处理。

**学习方法**：
- 写出五个具体场景（维度表、事实表日志流、聚合报表、CDC 源、每日快照），逐个判断用哪种
- 重点理解重启行为：LIVE TABLE 完全重算，STREAMING LIVE TABLE 从 checkpoint 恢复

### 子主题 B: Expectations（数据质量约束）

**三层递进**：warn（只记录） -> drop（丢弃坏行） -> fail（中止 pipeline）

**学习方法**：
- 记住核心限制：expectation 只能引用当前表的列
- 两个固定模式必须背：
  - 复用规则 = Delta 表 + pipeline 参数
  - 跨表验证 = 临时表 + LEFT JOIN + null check

### 子主题 C: APPLY CHANGES INTO（CDC）

**关键元素**：KEYS（匹配）、SEQUENCE BY（排序）、SCD TYPE 1/2

**学习方法**：
- 手写一个完整的 APPLY CHANGES INTO 语句，确保每个子句的含义都清楚
- 理解 SCD Type 2 自动维护的 `__START_AT` / `__END_AT` 列

### 子主题 D: DLT vs Structured Streaming 运维差异

**核心论断**：差异在运维自动化，不在处理能力。

**学习方法**：
- 列出 DLT 自动管理的五件事：编排、依赖、checkpoint、集群、重试
- 对每件事想清楚：如果用 Structured Streaming，你需要自己做什么？

### 子主题 E: 物化策略与成本分析

**决策矩阵**：刷新频率 x 查询频率 x 用户数

**学习方法**：
- 对四种方案（物化表、view、streaming、DLT pipeline）各写出最佳适用场景和最差适用场景
- Q69 类型题的核心：view 是惰性求值，每次查询都重算

### 子主题 F: PII 脱敏时机

**三种方案**：write-time masking（Bronze 前脱敏）、read-time masking（UC Column Masks）、downstream masking（Gold 层处理）

**学习方法**：
- 记住判断规则：题目说 "before storage" -> write-time；题目说 "role-based visibility" -> read-time（UC Column Masks）
- "先存后治理" 永远不是合规答案

### 子主题 G: 日志体系分层

**五种日志**：Cluster Event Log、Driver Log、DLT Event Log、Workspace Audit Log、Ganglia

**学习方法**：
- 对每种日志只记一句话定位：Cluster Event Log = 集群生命周期；Driver Log = Spark 应用 stdout；DLT Event Log = pipeline 运行状态 + expectations 统计；Audit Log = 用户操作审计；Ganglia = 实时性能监控

---

## 三、苏格拉底式教学问题集

### 子主题 A: LIVE TABLE vs STREAMING LIVE TABLE

1. 如果一个 STREAMING LIVE TABLE 的数据源被 DELETE 了部分历史数据，pipeline 重启后会发生什么？为什么？
2. 为什么 STREAMING LIVE TABLE 要求数据源是 append-only？如果数据源有 update/delete 操作会怎样？
3. 如果一个维度表每天只有几条变更记录但总量很大（千万级），你会用 LIVE TABLE 还是 STREAMING LIVE TABLE？为什么？

### 子主题 B: Expectations

1. 为什么 DLT 不允许 expectation 直接跨表引用？这个限制和声明式执行模型有什么关系？
2. "临时表 + LEFT JOIN + null check" 这个跨表验证模式中，为什么用临时表而不是 view？
3. 如果你需要在 10 个表上应用同一套 expectations，但规则可能随时变更，为什么 Delta 表比 Python 变量或 notebook import 更适合？
4. EXPECT ... ON VIOLATION DROP ROW 和直接在 SQL 中写 WHERE 过滤有什么本质区别？

### 子主题 C: APPLY CHANGES INTO

1. SEQUENCE BY 为什么是必要的？如果没有 SEQUENCE BY，同一个 key 的多条 CDC 事件会怎样？
2. SCD Type 2 中，`__END_AT` 为 NULL 的记录意味着什么？
3. APPLY CHANGES INTO 和手动写 MERGE INTO 相比，DLT 帮你自动处理了哪些事情？

### 子主题 D: DLT vs Structured Streaming

1. DLT 的 "自动管理 checkpoint" 具体意味着什么？如果你用 Structured Streaming，checkpoint 管理不当会导致什么问题？
2. 如果有人说"DLT 只是 Structured Streaming 的封装"，你怎么反驳？
3. 为什么说 DLT 和 Structured Streaming 的差异不在 schema evolution？两者各自如何处理 schema evolution？
4. DLT 自动管理集群意味着什么？这对成本有什么影响？

### 子主题 E: 物化策略

1. View 的"惰性求值"在什么场景下是优势？在什么场景下是致命缺陷？
2. 如果 dashboard 查询频率从"多用户整天查"变成"一个分析师偶尔查一次"，物化策略的最优解会变吗？
3. Delta Cache 和预计算物化表的区别是什么？为什么 Delta Cache 不能替代物化表？

### 子主题 F: PII 脱敏

1. UC Column Masks（read-time masking）在什么场景下比 write-time masking 更合适？
2. 为什么 Lakeflow 特别适合"批+流统一脱敏"这个需求？用其他方案（比如 notebooks + SQL Warehouses）有什么问题？
3. "auditable and reproducible" 这个需求为什么指向声明式 pipeline 而不是手动 notebook？

---

## 四、场景判断题

### 子主题 A: LIVE TABLE vs STREAMING LIVE TABLE

**题 A1**：一个电商平台的订单事实表，每秒产生数百条新订单记录，数据从 Kafka 实时写入 Delta 表。你需要在 DLT pipeline 中创建一个下游聚合表来统计每小时订单数。应该用什么类型的表定义？

- A. LIVE TABLE，因为聚合计算需要全量数据
- B. STREAMING LIVE TABLE，因为数据源是 append-only 流式数据
- C. LIVE TABLE，因为聚合逻辑比增量处理更简单
- D. 不用 DLT，直接用 Structured Streaming 写自定义聚合

**答案：B**
解析：数据源是 append-only（新订单只增不改），且数据量大，增量处理远比全量重算高效。STREAMING LIVE TABLE 从 checkpoint 恢复，只处理新数据。A 和 C 的全量重算在大数据量下成本过高。D 放弃了 DLT 的编排和数据质量优势。

**题 A2**：一个产品维度表包含 5000 条产品记录，每周由业务人员通过 CSV 上传更新（可能修改已有记录的价格字段）。你应该如何在 DLT 中定义这个表？

- A. STREAMING LIVE TABLE，因为需要增量读取 CSV
- B. LIVE TABLE，因为数据源有 update 操作且数据量小
- C. STREAMING LIVE TABLE + APPLY CHANGES INTO，因为有记录变更
- D. 不适合用 DLT，应该用 COPY INTO

**答案：B**
解析：数据量小（5000 条），且数据源有 update（价格修改），不是 append-only，不满足 STREAMING LIVE TABLE 的要求。LIVE TABLE 全量重算的成本可忽略。C 过度设计，CSV 上传不是 CDC 流。

### 子主题 B: Expectations

**题 B1**：你的 DLT pipeline 有 15 张表需要检查 "email 格式合法" 和 "phone 不为空" 两条规则。这些规则可能随合规政策变化而调整。最佳方案是什么？

- A. 在每张表的定义中硬编码这两条 expectation
- B. 将规则存入 Delta 表，通过 pipeline 参数传入 schema 名，动态读取并应用
- C. 创建一个公共 Python 模块，让所有 DLT notebook import
- D. 用 `%run` 在每个 DLT notebook 中引入包含规则定义的 notebook

**答案：B**
解析：C 和 D 都不可行 -- DLT notebook 不支持 `import` 其他 notebook，也不支持 `%run`。A 可以工作，但规则变更时需要改 15 个地方，维护成本高。B 是 DLT 官方推荐做法，规则集中管理，变更只需更新 Delta 表。

**题 B2**：你需要验证 `orders` 表中的每条记录在 `customers` 表中都有对应的 customer_id。如何用 DLT expectations 实现？

- A. 在 `orders` 表的 expectation 中写 `EXPECT (customer_id IN (SELECT customer_id FROM customers))`
- B. 创建临时表，LEFT JOIN orders 和 customers，在临时表上定义 expectation 检查 customer_id 非空
- C. 在 `orders` 表定义中用 SQL UDF 执行跨表查询
- D. 用外部 job 定期比较两张表的 key 集合

**答案：B**
解析：A 错误 -- expectation 条件不支持子查询跨表引用。C 错误 -- expectation 不支持跨表 UDF 调用。D 放弃了 DLT 内置数据质量体系。B 是标准模式：`@dlt.table(temporary=True)` 做 LEFT JOIN，expectation 检查 join 后 customer_id IS NOT NULL。

**题 B3**：Silver 层的交易表需要确保 amount > 0，不合格的记录应该被丢弃但需要留下记录。最合适的 expectation 配置是？

- A. `EXPECT (amount > 0) ON VIOLATION FAIL UPDATE`
- B. `EXPECT (amount > 0) ON VIOLATION DROP ROW`
- C. `EXPECT (amount > 0)`（仅 warn 模式）
- D. 用 WHERE 子句在 SQL 中过滤

**答案：B**
解析：需求是"丢弃但留记录" -- DROP ROW 丢弃不合格行，同时 DLT event log 自动记录违规行数和通过率。A 会导致 pipeline 失败，过于激进。C 只记录不丢弃。D 能丢弃但不会在 event log 中记录数据质量指标。

### 子主题 D: DLT vs Structured Streaming

**题 D1**：你的团队需要构建一个三阶段的数据 pipeline（Bronze -> Silver -> Gold），每个阶段有不同的 transformation 逻辑。以下哪个是选择 DLT 而非 Structured Streaming 的最重要理由？

- A. DLT 支持写入 Delta Lake 格式，Structured Streaming 不支持
- B. DLT 自动处理 schema evolution，Structured Streaming 需要手动管理
- C. DLT 自动管理多阶段之间的编排和依赖关系
- D. DLT 可以处理流数据，Structured Streaming 只能处理批数据

**答案：C**
解析：A 完全错误 -- Structured Streaming 可以写 Delta。B 误导 -- 两者都支持 schema evolution。D 反了 -- Structured Streaming 就是做流处理的。C 是核心差异：DLT 自动管理 Bronze->Silver->Gold 的依赖顺序、失败重试、checkpoint，Structured Streaming 需要外部编排工具。

**题 D2**：以下关于 DLT 和 Structured Streaming 的陈述，哪一个是正确的？

- A. DLT 不能处理实时流数据，只能做定时批处理
- B. Structured Streaming 的 checkpoint 由框架自动管理，无需手动干预
- C. DLT 自动创建和销毁 pipeline 运行所需的集群
- D. DLT 和 Structured Streaming 都自动管理多阶段依赖

**答案：C**
解析：A 错误 -- STREAMING LIVE TABLE 就是流处理。B 错误 -- Structured Streaming 需要手动指定 `checkpointLocation`。D 错误 -- 只有 DLT 自动管理依赖，Structured Streaming 需要外部编排。C 正确 -- DLT 自动创建专用 pipeline 集群，运行结束后自动销毁。

### 子主题 E: 物化策略

**题 E1**：财务部门需要一个 dashboard 显示昨日销售汇总。Dashboard 被 30 个分析师在工作日全天交互查询，数据只需每天凌晨刷新一次。最优方案是什么？

- A. 定义一个 view 对接 dashboard，确保数据始终最新
- B. 配置 nightly batch job 将聚合结果写入物化表
- C. 用 Structured Streaming 实时更新 dashboard
- D. 用 Delta Cache 缓存原始表加速查询

**答案：B**
解析：三个需求 -- 每天一次刷新、30 人高频查询、控制成本。A 的 view 每次查询重算聚合，30 人全天查意味着上百次重复计算。C 的实时流持续运行，成本远超"每天一次"的需求。D 只缓存原始数据文件，聚合仍需每次重算。B 凌晨一次预计算，白天直接读表，成本最低。

**题 E2**：一个数据科学家偶尔（每周一两次）需要对原始日志表做探索性分析，查询模式不固定。最合适的方案是什么？

- A. 每天 batch job 预计算所有可能的聚合结果
- B. 定义 view，按需查询
- C. 配置 Structured Streaming 实时处理
- D. 创建 DLT pipeline 维护物化表

**答案：B**
解析：查询不频繁（每周一两次）且模式不固定（无法预计算）。View 在这个场景下的优势 -- 不占额外存储，查询时才计算，且总是反映最新数据。预计算方案（A/D）无法覆盖不固定的查询模式。C 过度设计。

### 子主题 F: PII 脱敏

**题 F1**：一个组织的合规要求明确写着 "PII must be masked before storage"。批数据来自 SFTP（每日），流数据来自 Kafka（实时）。最佳方案是什么？

- A. 用 UC Column Masks 在查询时动态脱敏
- B. 分别用 notebook 处理批数据、SQL Warehouse 处理流数据，各自应用脱敏逻辑
- C. 用 Lakeflow Declarative Pipelines 统一处理批和流，在 Bronze 写入前应用脱敏函数
- D. 在 Bronze 存明文以保留 lineage，在 Silver 层应用脱敏

**答案：C**
解析："before storage" 排除 A（read-time，底层仍明文）和 D（Bronze 明文存储）。B 用两个不同框架，无法保证一致性。C 三合一：Lakeflow 统一批+流、写入前脱敏满足合规、声明式 pipeline 保证 auditable。

**题 F2**：一个数据平台需要让不同角色看到不同级别的客户信息 -- 管理员看完整 email，分析师只看域名部分。PII 数据已经存储在 Delta 表中。最合适的方案是什么？

- A. 为每个角色创建不同的物化表，各自存储不同级别的脱敏数据
- B. 使用 Unity Catalog Column Masks 根据角色动态控制可见性
- C. 在 Bronze 写入前用最严格的脱敏逻辑处理
- D. 用 DLT expectations 检查 PII 字段

**答案：B**
解析：需求是"不同角色看到不同内容" -- 这是 read-time masking 的典型场景。A 数据冗余，维护成本高。C 最严格的脱敏会让管理员也无法看到完整信息。D 的 expectations 是数据质量检查，不是脱敏机制。B 的 UC Column Masks 正是为角色级读时动态脱敏设计的。

---

## 五、关键知识网络

```
DLT / Lakeflow Declarative Pipelines
|
|-- 表类型
|   |-- LIVE TABLE (batch, 全量重算)
|   |-- STREAMING LIVE TABLE (incremental, checkpoint 恢复)
|   |-- Temporary Table (跨表验证辅助, 不持久化)
|
|-- 数据质量: Expectations
|   |-- warn (记录违规, 不丢弃)
|   |-- drop (丢弃坏行, 记录统计)
|   |-- fail (中止 pipeline)
|   |-- 限制: 只能引用当前表的列
|   |-- 复用: Delta 表存规则 + pipeline 参数
|   |-- 跨表: 临时表 + LEFT JOIN + null check
|
|-- CDC: APPLY CHANGES INTO
|   |-- KEYS (主键匹配)
|   |-- SEQUENCE BY (事件排序)
|   |-- SCD Type 1 (覆盖) / Type 2 (保留历史)
|
|-- 与 Structured Streaming 的差异
|   |-- 相同: 都能处理流, 都支持 schema evolution, 都写 Delta
|   |-- DLT 独有: 自动编排, 自动 checkpoint, 自动集群, 自动重试, 内置 expectations
|
|-- Event Log
|   |-- 查询: event_log(TABLE(...))
|   |-- 内容: 运行状态, expectations 统计, resize 事件
|   |-- 区分: Cluster Event Log (集群生命周期) vs DLT Event Log (pipeline 运行)
|
|-- 应用场景
|   |-- 物化策略: 根据刷新频率 x 查询频率 x 用户数选择方案
|   |-- PII 脱敏: write-time (合规优先) vs read-time (灵活性优先)
|   |-- 批+流统一: Lakeflow 的核心优势
```

---

## 六、Anki 卡片

**Q1**: DLT 中 LIVE TABLE 和 STREAMING LIVE TABLE 的核心区别是什么？
**A1**: LIVE TABLE 全量重算（batch），STREAMING LIVE TABLE 增量处理（从 checkpoint 恢复只处理新数据）。判断标准：数据源是否 append-only、是否需要增量处理。

**Q2**: DLT Expectations 的三种违规处理模式分别是什么？
**A2**: warn -- 只记录违规行数到 event log，不丢弃数据；drop -- 丢弃不满足条件的行；fail -- 整个 pipeline 失败。

**Q3**: 如何在 DLT 中跨多个 notebook 复用相同的 expectations？
**A3**: 将规则存储在外部 Delta 表中，通过 pipeline 参数传入 schema 名，在 DLT notebook 中用 `spark.conf.get()` 动态读取规则并应用。DLT notebook 不支持 `%run` 或 `import`。

**Q4**: 如何在 DLT 中实现跨表验证（如验证表 A 包含表 B 的所有记录）？
**A4**: 标准模式：创建临时表（`temporary=True`），LEFT JOIN 两表，在临时表上定义 expectation 检查 key IS NOT NULL。Expectation 不支持子查询或跨表 UDF。

**Q5**: DLT 和 Structured Streaming 的核心运维差异是什么？
**A5**: DLT 自动管理五件事：(1) 多阶段编排和依赖 (2) checkpoint (3) 集群创建/销毁 (4) 失败重试 (5) 内置数据质量。Structured Streaming 只负责流处理本身，以上均需外部管理。

**Q6**: APPLY CHANGES INTO 中 SEQUENCE BY 的作用是什么？
**A6**: 指定排序列（通常是 timestamp），确保同一 key 的多条 CDC 事件按正确时间顺序应用，避免乱序导致数据错误。

**Q7**: "PII must be masked before storage" 这个需求排除了哪些方案？
**A7**: 排除 UC Column Masks（read-time masking，底层仍明文存储）和下游脱敏（Bronze/Silver 明文存储）。只有 write-time masking（写入前应用脱敏函数）满足此要求。

**Q8**: 每天刷新一次、多用户高频交互查询的 dashboard，为什么 view 不是好方案？
**A8**: View 是惰性求值，每次查询都触发完整的聚合计算。多用户高频查询意味着同一个聚合被重复计算上百次，计算成本高、响应慢。应该用 nightly batch job 预计算物化表。

**Q9**: Cluster Event Log 和 Driver Log 分别记录什么？
**A9**: Cluster Event Log 记录集群基础设施层事件（扩缩容、启动、终止）。Driver Log 记录 Spark 应用程序的 stdout/stderr 输出。集群 resize timeline 在 Cluster Event Log 中查看。

**Q10**: DLT "不能处理流数据" 这个说法为什么是错的？
**A10**: STREAMING LIVE TABLE 就是 DLT 处理流数据的方式，底层使用 Structured Streaming。"声明式"描述的是编程范式（定义 what 而非 how），不限制处理模式。DLT 同时支持批和流。

**Q11**: UC Column Masks 最适合什么场景？
**A11**: 不同角色需要看到同一数据的不同脱敏级别（如管理员看完整 email，分析师只看域名）。这是 read-time masking，数据底层仍完整存储，按角色动态控制可见性。

**Q12**: DLT Event Log 查询语法是什么？它记录哪些信息？
**A12**: `SELECT * FROM event_log(TABLE(my_pipeline.my_table))`。记录 pipeline 启停事件、每张表的更新状态、expectations 违规统计（通过率、违规行数）、数据质量趋势、集群 resize。

---

## 七、掌握度自测

**1.** 以下哪种场景最适合使用 STREAMING LIVE TABLE 而非 LIVE TABLE？

A. 每周全量更新的产品维度表（5000 行）
B. 从 Kafka 持续接收的用户行为日志表
C. 需要频繁 UPDATE 的客户主数据表
D. 对三张表做 JOIN 的聚合报表

<details><summary>答案</summary>B。Kafka 日志是 append-only 流式数据源，数据量大，增量处理效率远高于全量重算。A 数据量小，全量重算成本可忽略。C 有 UPDATE 操作，不是 append-only。D 是聚合计算，通常用 LIVE TABLE。</details>

**2.** 一个 DLT pipeline 包含三个 notebook。Notebook A 定义了 Python 函数 `validate_email()`，Notebook B 想调用这个函数。以下哪个方案可行？

A. 在 Notebook B 中 `%run ./NotebookA`
B. 在 Notebook B 中 `from NotebookA import validate_email`
C. 在 pipeline 配置中添加 NotebookA 作为共享库
D. 将 validate_email 逻辑存入 Delta 表或注册为 SQL UDF，在 Notebook B 中引用

<details><summary>答案</summary>D。DLT notebook 不支持 `%run`（A 错误）和 `import`（B 错误），也没有"共享库"概念（C 错误）。DLT notebook 之间的通信只能通过 Delta 表读写数据或 pipeline 参数传递配置。将逻辑注册为 SQL UDF 或存入 Delta 表是可行的间接方式。</details>

**3.** 以下关于 DLT 和 Structured Streaming 的陈述，哪些是正确的？（多选）

A. DLT 可以处理实时流数据
B. Structured Streaming 需要手动指定 checkpointLocation
C. DLT 自动处理 schema evolution，Structured Streaming 不能
D. DLT 自动管理多阶段 pipeline 的依赖和编排
E. Structured Streaming 不能写入 Delta Lake

<details><summary>答案</summary>A、B、D。C 错误 -- 两者都支持 schema evolution。E 错误 -- Structured Streaming 完全可以写 Delta Lake。</details>

**4.** 团队需要验证 `fact_sales` 表中的 `product_id` 在 `dim_products` 表中都存在。以下哪个方法正确？

A. 在 `fact_sales` 的 expectation 中写 `EXPECT (product_id IN (SELECT product_id FROM dim_products))`
B. 创建临时表 LEFT JOIN fact_sales 和 dim_products，在临时表上定义 expectation 检查 dim 侧 key 不为空
C. 定义 SQL UDF 做跨表 lookup，在 fact_sales 的 expectation 中调用
D. 用外部 Python 脚本定期比对两表

<details><summary>答案</summary>B。A 错误 -- expectation 不支持子查询。C 错误 -- expectation 不支持跨表 UDF。D 放弃了 DLT 内置数据质量体系。B 是标准模式：临时表 + LEFT JOIN + null check。</details>

**5.** 合规部门要求 "PII 数据在存储前必须脱敏"，同时需要批（SFTP 文件）和流（Kafka）保持一致的脱敏逻辑。以下哪个方案最优？

A. 用 UC Column Masks 统一控制 PII 可见性
B. 批数据用 notebook 脱敏，流数据用 SQL Warehouse 脱敏
C. 用 Lakeflow Declarative Pipelines 统一处理批和流，Bronze 写入前应用脱敏函数
D. Bronze 存明文，Silver 层统一脱敏

<details><summary>答案</summary>C。A 是 read-time masking，底层仍明文，违反 "before storage"。B 两套框架无法保证一致性。D Bronze 存明文直接违反要求。C 满足全部三个条件。</details>

**6.** 管理员想查看集群在过去一周内的扩缩容事件时间线，应该去哪里查？

A. Driver Log
B. Ganglia Metrics
C. Workspace Audit Log
D. Cluster Event Log

<details><summary>答案</summary>D。Cluster Event Log 专门记录集群生命周期事件（upscaling/downscaling/启动/终止），带时间戳。Driver Log 是应用程序日志，Ganglia 是实时性能监控，Audit Log 是用户操作审计。</details>

**7.** 以下关于 APPLY CHANGES INTO 的陈述，哪个是错误的？

A. KEYS 子句指定用于匹配记录的主键
B. SEQUENCE BY 确保 CDC 事件按正确顺序应用
C. SCD TYPE 2 会自动维护 `__START_AT` 和 `__END_AT` 列
D. APPLY CHANGES INTO 只能用于 LIVE TABLE，不能用于 STREAMING LIVE TABLE

<details><summary>答案</summary>D 是错误陈述。APPLY CHANGES INTO 通常与 STREAMING LIVE TABLE 配合使用（CDC 源通常是流式的）。A、B、C 都是正确描述。</details>

**8.** 在什么场景下，view 比物化表更适合作为 dashboard 的数据源？

A. 30 个分析师全天交互查询，数据每天刷新一次
B. 一个数据科学家每周查一两次，查询模式不固定
C. 实时交易监控，需要秒级延迟
D. 月度财务报表，每月生成一次

<details><summary>答案</summary>B。查询不频繁（每周一两次），且查询模式不固定（无法预计算覆盖所有情况），view 的惰性求值在此场景下是优势 -- 不占额外存储，按需计算，总是最新。A 需要物化表，C 需要 streaming，D 需要物化表（月度报表通常有固定格式）。</details>
