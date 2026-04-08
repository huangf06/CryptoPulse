# Structured Streaming 深度复习辅导材料

> 基于 13 道错题 | 正确率 0/13 | P1 优先级
> 子主题覆盖：Checkpoint、Trigger、readStream、SQL Alert、性能测试、Jobs API、Liquid Clustering

---

## 一、错误模式诊断

### 1. Checkpoint 独立性认知缺失（Q20, Q72, Q320）

**表现：** 三道 checkpoint 题全部答错，错误方向各不相同（Q20 选 D 认为可以共享、Q72 选 E 关注 mergeSchema、Q320 选 B 混淆 overwriteSchema）。

**根因：** 没有建立"一个 streaming query = 一个 checkpoint 目录"的基本心智模型。把 checkpoint 理解为一种可选的优化配置，而不是 exactly-once 语义的核心机制。三道题的错误方向分散说明不是记忆失误，而是概念层面的缺位。

**修复路径：** 必须理解 checkpoint 记录的三类信息（source offset、sink commit log、state）各自的作用，以及为什么共享会破坏语义。

### 2. Trigger 类型的成本模型盲区（Q21, Q139）

**表现：** Q21 选了理由错误的选项（A vs E，区别在于 microbatch 串行执行），Q139 选了 processingTime 长间隔而非 trigger once。

**根因：** 两层缺陷：(1) 不理解 microbatch 是严格串行的，不能并行；(2) 不理解 processingTime 模式下集群持续运行的成本含义，没有将 trigger once + Job 调度识别为成本优化方案。

**修复路径：** 画出 microbatch 执行的时间线图，理解串行约束。对比 processingTime vs trigger once 的集群生命周期。

### 3. 云原生思维缺失（Q49, Q110）

**表现：** Q49 选本地 IDE + 开源 Spark，Q110 选本地 SSD。

**根因：** 习惯性地将问题映射到传统部署模式，忽略了 Databricks 计算存储分离架构下本地资源不是性能瓶颈。Photon、优化的云存储连接等平台特性在本地完全不可复现。

**修复路径：** 建立"Databricks = 云原生、计算存储分离"的默认思维框架。遇到性能/架构题先问"这是不是在诱导你选本地方案"。

### 4. 审题精度不足（Q49 vs Q219）

**表现：** 同源题因问法不同（"what adjustment" vs "what adjustment and limitation"）答案不同，但未识别出差异。

**根因：** 阅读选项时关注内容正确性，忽略了选项与题目问法的匹配度。

### 5. GROUP BY 语义理解偏差（Q4）

**表现：** 把 GROUP BY 查询的 alert 行为理解为对聚合结果的总体判断，实际是逐行检查。

**根因：** 不了解 Databricks SQL Alert 的具体行为机制。

### 6. API 概念混淆（Q117, Q325）

**表现：** 混淆 job owner / job creator / run creator，放弃多选题。

**根因：** 对 Jobs REST API 的概念模型不清晰。creator_user_name 属于 run-level 属性，反映触发者而非所有者。

---

## 二、子主题分解与学习方法

### 2.1 Checkpoint 机制

| 维度 | 内容 |
|------|------|
| 知识类型 | 概念性 + 规则性 |
| 学习方法 | 先理解"为什么需要 checkpoint"（exactly-once 语义保证），再记忆三类信息，最后通过场景题巩固"何时需要新 checkpoint" |
| 依赖关系 | 依赖对 Structured Streaming 微批处理模型的理解 |
| 关键规则 | (1) 每个 query 独占一个 checkpoint 目录 (2) 有状态操作变更 = 必须新 checkpoint (3) 无状态操作变更 = 不需要 |

### 2.2 Trigger 类型与调优

| 维度 | 内容 |
|------|------|
| 知识类型 | 概念性 + 决策性 |
| 学习方法 | 建立四种 trigger 类型的对比矩阵（集群状态、成本、延迟、适用场景），用 Q21/Q139 两道真题做决策演练 |
| 依赖关系 | 依赖对 microbatch 串行执行模型的理解 |
| 关键决策 | 延迟容忍度 > 分钟级 -> trigger once/availableNow + Job 调度；需要亚秒级 -> processingTime default；需要控制频率 -> processingTime interval |

### 2.3 readStream 与增量处理语义

| 维度 | 内容 |
|------|------|
| 知识类型 | 概念性 |
| 学习方法 | 对比 readStream vs read 的行为差异，理解 append-only 约束和 CDF 的关系 |
| 依赖关系 | 与 Delta Lake CDF、Auto Loader 深度耦合 |
| 关键区分 | readStream 默认 append-only，源表有 UPDATE/DELETE 会报错，需要 CDF 读取变更 |

### 2.4 SQL Alert 机制

| 维度 | 内容 |
|------|------|
| 知识类型 | 规则性（记忆为主） |
| 学习方法 | 记住核心规则：GROUP BY 查询的 alert 逐行检查，任意一行满足即触发 |
| 依赖关系 | 独立子主题，无强依赖 |

### 2.5 性能测试与生产实践

| 维度 | 内容 |
|------|------|
| 知识类型 | 决策性 + 概念性 |
| 学习方法 | 建立"云原生 = 计算存储分离"的思维默认值，记住 Spark lazy evaluation + caching 对性能测量的影响 |
| 依赖关系 | 依赖对 Spark 执行模型（lazy evaluation、action vs transformation）的理解 |

### 2.6 Jobs API 与编排

| 维度 | 内容 |
|------|------|
| 知识类型 | 规则性 |
| 学习方法 | 区分三组概念对：job owner vs run creator、/jobs/run-now vs /jobs/runs/submit、task-level vs job-level 配置 |
| 依赖关系 | 独立子主题 |

### 2.7 Liquid Clustering

| 维度 | 内容 |
|------|------|
| 知识类型 | 规则性（记忆为主） |
| 学习方法 | 记忆支持/不支持 cluster on write 的操作列表 |
| 依赖关系 | 与 Delta Lake 主题相关 |

---

## 三、苏格拉底式教学问题集

### 3.1 Checkpoint 机制

1. Checkpoint 记录了哪三类信息？如果两个 stream 共享一个 checkpoint 目录，具体会出什么问题？（提示：从 source offset 的角度想）
2. 一个 streaming query 原来只做 filter + select，现在加了一个 GROUP BY 聚合。为什么旧 checkpoint 不兼容？（提示：checkpoint 里多存了什么东西？）
3. 如果只是修改了 filter 条件（比如从 `age > 20` 改为 `age > 25`），需要新 checkpoint 吗？为什么？
4. checkpointLocation 和 Delta Log 都记录了"处理进度"，它们之间是什么关系？是冗余的还是各管各的？
5. 如果 streaming job 崩溃后重启，checkpoint 如何保证 exactly-once 而不是 at-least-once？

### 3.2 Trigger 类型与调优

1. 如果 trigger interval 是 10 秒，但 microbatch 处理需要 30 秒，会发生什么？数据会丢失吗？
2. 为什么减小 trigger interval 可以缓解高峰期的延迟问题？（提示：每个 batch 的数据量和 trigger interval 是什么关系？）
3. processingTime="10 minutes" 和 trigger(once=True) + 每 10 分钟调度一次，在功能上有什么区别？在成本上呢？
4. availableNow 和 once 的区别是什么？为什么 once 被废弃了？
5. 一个场景：数据源每秒产生 1000 条记录，延迟容忍度 5 分钟，集群预算有限。你选什么 trigger 策略？

### 3.3 readStream 与增量处理

1. `spark.readStream.table("t")` 只读新增行，它怎么知道哪些行是"新的"？（提示：Delta Log）
2. 如果源表执行了 DELETE 操作，readStream 默认会报错。为什么 Structured Streaming 不能简单地"跳过"删除？
3. readStream + CDF 和直接 readStream 的 append-only 模式，各自适合什么场景？
4. Q106 中，为什么 insert-only merge 比 trigger once streaming 更适合去重？

### 3.4 SQL Alert

1. 一个查询没有 GROUP BY，返回一行结果。Alert 条件是 value > 100。和有 GROUP BY 返回多行的情况，alert 触发逻辑有什么不同？
2. 如果 GROUP BY 查询返回 100 行，其中 3 行满足条件，alert 触发几次？

### 3.5 性能测试

1. 为什么在 notebook 里重复执行同一个 cell 得到的执行时间越来越短？这和 Spark 的哪个机制有关？
2. display() 是 action 还是 transformation？它和 show()、collect() 在触发执行上有什么区别？
3. 为什么本地 IDE + 开源 Spark 不能作为 Databricks 性能基准？列举至少 3 个本地无法复现的平台特性。

### 3.6 Jobs API

1. User A 创建 Job，User B 用自己的 token 触发 run，User C 接管 Owner。调用 API 获取 run 信息时，creator_user_name 是谁？
2. /jobs/run-now 和 /jobs/runs/submit 的根本区别是什么？哪个的运行历史会出现在 Job UI 里？
3. 多任务 Job 中，task-level retries 和 job-level retries 有什么区别？

---

## 四、场景判断题

### 4.1 Checkpoint 机制

**题 1：** 一个 data engineering 团队有三个 streaming query，分别从同一个 Kafka topic 读取数据，写入三张不同的 Delta 表。为了简化管理，他们将三个 query 的 checkpoint 放在同一个父目录下的三个子目录中：`/checkpoints/query1/`, `/checkpoints/query2/`, `/checkpoints/query3/`。这个方案是否可行？

- A. 不可行，checkpoint 必须放在完全独立的路径中，不能共享父目录
- B. 可行，每个 query 有独立的子目录就满足了 checkpoint 隔离要求
- C. 不可行，三个 query 读取同一个 Kafka topic 时必须共享 checkpoint
- D. 可行，但只有在三个 query 的 schema 完全相同时才能这样配置

**答案：B**
**解析：** checkpoint 的隔离要求是每个 query 有自己独立的目录。子目录共享同一个父目录完全可以，关键是目录路径本身是唯一的。A 过度限制，C 和 D 的前提条件都是错误的。

**题 2：** 一个 streaming query 原来计算 `SELECT user_id, COUNT(*) as cnt FROM stream GROUP BY user_id`。现在需要改为 `SELECT user_id, COUNT(*) as cnt, SUM(amount) as total FROM stream GROUP BY user_id`。数据工程师只修改了查询代码并重启了 stream，没有更换 checkpoint。会发生什么？

- A. 查询正常运行，新增的 SUM(amount) 列从 0 开始累计
- B. 查询报错，因为 checkpoint 中的 state schema 与新查询不兼容
- C. 查询正常运行，但 mergeSchema 需要设为 true
- D. 查询正常运行，旧 state 中的 COUNT 值保留，SUM 从当前数据开始

**答案：B**
**解析：** 有状态操作（GROUP BY 聚合）的中间 state 存储在 checkpoint 中。添加 SUM(amount) 改变了 state 的 schema 结构，旧 checkpoint 中的 state 无法被反序列化为新格式，查询会报错。必须指定新的 checkpointLocation 从零开始。

**题 3：** 一个 streaming query 将 `WHERE status = 'active'` 修改为 `WHERE status = 'active' AND region = 'EU'`，其余逻辑不变（无聚合操作）。是否需要新的 checkpoint？

- A. 需要，任何查询逻辑变更都需要新 checkpoint
- B. 不需要，filter 是无状态操作，不影响 checkpoint 兼容性
- C. 需要，因为 filter 条件的变更会导致 source offset 不兼容
- D. 不需要，但需要手动清理旧 checkpoint 中的 commit log

**答案：B**
**解析：** filter 是无状态操作，不在 checkpoint 中存储 state。checkpoint 的 source offset 和 sink commit log 不受 filter 条件变更影响，所以不需要新 checkpoint。只有有状态操作（聚合、窗口、去重等）的变更才需要。

### 4.2 Trigger 类型

**题 1：** 一个 IoT 数据管道每秒接收 5000 条传感器读数，当前配置 `trigger(processingTime="30 seconds")`。运维团队报告在凌晨 2-4 点（数据量极低）期间，集群利用率不到 5% 但仍在持续运行。最合理的优化方案是？

- A. 将 trigger interval 减小到 5 秒以更快处理完并释放资源
- B. 保持当前配置，凌晨低利用率是正常的
- C. 改用 trigger(availableNow=True) 配合 Job 调度，在数据量低的时段减少运行频率
- D. 改用 trigger(processingTime="0") 以尽快处理完数据

**答案：C**
**解析：** processingTime 模式下集群持续运行，无论数据量多低都在消耗资源。如果延迟容忍度允许，使用 availableNow + Job 调度可以在低数据量时段降低调度频率（比如每 10 分钟一次），处理完即释放集群。A 和 D 不解决集群持续运行的问题。

**题 2：** 一个 streaming job 使用 default trigger（processingTime="0"），平时每个 microbatch 处理 500 条记录约 2 秒。某天数据源出现 10 分钟的延迟积压后恢复，积压期间大约累积了 300,000 条记录。恢复后第一个 microbatch 会怎样？

- A. 自动分成多个小 batch 处理，每个 batch 处理 500 条
- B. 一个 microbatch 尝试处理全部 300,000 条，可能因数据量过大而变慢甚至 OOM
- C. Structured Streaming 自动限流，每个 batch 只处理 maxOffsetsPerTrigger 默认值的数据
- D. 丢弃积压数据，从最新 offset 开始处理

**答案：B**
**解析：** 默认情况下，Structured Streaming 不限制每个 microbatch 的数据量。积压恢复后第一个 batch 会尝试处理所有可用数据，可能导致严重性能问题。要防止这种情况，需要显式设置 `maxOffsetsPerTrigger`（Kafka）或 `maxFilesPerTrigger`（文件源）。C 的前提是设置了该参数，默认不启用。

**题 3：** trigger(once=True) 和 trigger(availableNow=True) 都会在处理完后停止 stream。两者的关键区别是什么？

- A. once 处理所有可用数据分为多个 batch，availableNow 只处理一个 batch
- B. once 只处理一个 batch，availableNow 将所有可用数据分为多个 batch 处理
- C. 两者完全等价，availableNow 只是 once 的新名字
- D. once 不写 checkpoint，availableNow 写 checkpoint

**答案：B**
**解析：** trigger(once=True) 将所有可用数据塞进一个 batch 处理，如果数据量大可能导致 OOM。trigger(availableNow=True) 将可用数据分为多个 batch（受 maxFilesPerTrigger 等配置控制），处理完全部后停止。这是 once 被废弃、由 availableNow 替代的主要原因。

### 4.3 readStream 与增量处理

**题 1：** 一个 Delta 表 `orders` 每天接收新订单（INSERT），同时订单状态会更新（UPDATE）。一个下游 streaming job 使用 `spark.readStream.table("orders")` 处理数据。运行后报错。最可能的原因和修复方案是？

- A. Delta 表不支持 streaming 读取，改用 Auto Loader
- B. readStream 默认 append-only，UPDATE 操作导致报错，需启用 CDF
- C. 缺少 checkpointLocation 配置
- D. Delta 表需要启用 enableChangeDataFeed 表属性才能被 readStream 读取

**答案：B**
**解析：** readStream 默认以 append-only 模式读取 Delta 表，只追踪新增行。如果源表有 UPDATE/DELETE 操作，会违反 append-only 约束而报错。修复方案：在源表启用 CDF（`ALTER TABLE SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`），并在 readStream 中设置 `option("readChangeFeed", "true")`。D 只说了一半，没有提到 readStream 端的配置。

**题 2：** 数据工程师需要将 `raw_events` 表的新增数据传播到 `clean_events` 表，同时去重。`raw_events` 每天通过批量 append 写入一次，延迟容忍度 24 小时。以下哪种方案成本最低？

- A. readStream + trigger(availableNow=True) 配合 daily Job 调度
- B. 批量读取 raw_events，使用 INSERT-ONLY MERGE 按自然主键去重写入
- C. 使用 Delta version history 获取最新版本与上一版本的差异
- D. 每天全量重写 clean_events 表

**答案：B**
**解析：** 每天一次批量 append 的场景下，streaming 的 checkpoint 机制是额外开销。INSERT-ONLY MERGE 直接用批量读取 + 自然主键去重，比 streaming 更轻量。C 的 version history 方法获取增量行不可靠（版本快照 vs 增量行）。D 全量重写成本最高。

### 4.4 SQL Alert

**题 1：** 一个 Databricks SQL Alert 基于以下查询：
```sql
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
```
Alert 条件设为 `avg_salary > 100000`。公司有 20 个部门，其中 3 个部门的平均薪资超过 100000。Alert 的行为是？

- A. 不触发，因为 20 个部门的总体平均薪资未超过 100000
- B. 触发，因为至少一行（一个部门）满足条件
- C. 触发 3 次，每个满足条件的部门各一次
- D. 报错，因为 Alert 不支持 GROUP BY 查询

**答案：B**
**解析：** Databricks SQL Alert 对 GROUP BY 查询的结果逐行检查条件。只要任意一行满足条件，alert 就触发一次（不是每行各触发一次）。这里 3 个部门满足条件，alert 触发一次。

### 4.5 性能测试

**题 1：** 一个数据工程师在交互式 notebook 中开发了一段复杂的 ETL 逻辑。他执行了 5 次同一个 cell，发现执行时间分别是 45s、12s、8s、7s、7s。基于这个结果，他认为生产环境的执行时间约为 7-8 秒。这个结论可靠吗？

- A. 可靠，多次执行取稳定值是标准的性能测试方法
- B. 不可靠，第一次执行最接近生产行为，后续执行受 Spark 缓存影响
- C. 不可靠，应该取 5 次的平均值（约 16 秒）作为预估
- D. 可靠，但需要加上 20% 的安全系数

**答案：B**
**解析：** Spark 的缓存机制会将中间结果保留在内存中，后续执行同一逻辑时直接使用缓存，速度大幅提升。生产环境中每次 job 从冷启动开始，不存在缓存预热，所以第一次执行（45s）最接近生产行为。要获得准确基准，需要用生产规模数据 + Run All 模式。

**题 2：** 一家公司需要处理数百条并行管道、高吞吐量的近实时数据处理方案。以下哪个方案最合适？

- A. 将所有 Delta 表存储在本地 SSD 以提升 I/O 性能
- B. 使用 Databricks High Concurrency 集群
- C. 为每条管道部署独立的单节点集群
- D. 使用单个大型集群并通过 FIFO 调度处理所有管道

**答案：B**
**解析：** Databricks High Concurrency 集群专为多用户多任务并发场景设计，内置了优化的云存储连接和公平调度。A 违背云原生架构（计算存储分离），C 资源浪费严重，D 的 FIFO 调度不适合并行场景。

### 4.6 Jobs API

**题 1：** DevOps 团队使用 Service Principal 的 token 通过 REST API 每天触发一个 Job。Job 的 Owner 是数据工程师 Alice。查询 run 信息时，`creator_user_name` 字段显示什么？

- A. Alice 的邮箱，因为她是 Job Owner
- B. Service Principal 的 application ID 或名称
- C. 空值，因为 Service Principal 不是真实用户
- D. 创建 Job 时使用的用户的邮箱

**答案：B**
**解析：** `creator_user_name` 反映的是触发这次 run 的认证实体，不是 Job Owner 或 Job Creator。Service Principal 的 token 触发 run 时，该字段显示 Service Principal 的标识。Job ownership 和 run creation 是两个独立的概念。

**题 2：** 一个多任务 ETL Job 包含三个 task：ingest（notebook）、transform（Python wheel）、aggregate（SQL）。需求：每个 task 失败时最多重试 2 次，如果最终失败发送邮件通知。以下哪个方案正确？

- A. 在 Job 级别配置 max_retries=2 和 email notifications
- B. 在每个 task 定义中分别配置 max_retries=2 和 email notifications
- C. 使用 dbutils.notebook.run() 在 orchestrator notebook 中实现重试逻辑
- D. 使用 /jobs/runs/submit 分别提交每个 task 并在外部实现重试

**答案：B**
**解析：** 多任务 Job 原生支持 task-level 配置，包括重试次数和通知。task-level retries 只重试失败的 task，比 job-level retries（重新运行整个 Job）更精细。C 的 dbutils.notebook.run() 不支持 Python wheel 和 SQL task 类型。D 失去了多任务 Job 的依赖管理和 UI 整合优势。

---

## 五、关键知识网络

### 子主题间关联

```
Checkpoint 机制 <---> Trigger 类型
  |                      |
  | checkpoint 存储 state  | trigger once 不产生空 batch
  | 和 offset             | checkpoint 开销
  v                      v
readStream 语义 <---> 成本优化
  |                      |
  | append-only 约束       | 集群生命周期管理
  v                      v
CDF (Change Data Feed) <---> Jobs API 编排
                              |
                              | /jobs/run-now + trigger once
                              | = 批处理语义 + 流处理能力
```

### 与其他 Topic 的关联

| 本主题子模块 | 关联 Topic | 关联方式 |
|------------|-----------|---------|
| Checkpoint | Delta Lake | checkpoint 中的 source offset 基于 Delta Log 版本追踪 |
| readStream | Auto Loader | Auto Loader 是 readStream 的特化（cloudFiles 格式），用于文件源 |
| readStream + CDF | Delta Lake CDF | CDF 是 readStream 读取变更数据的前提配置 |
| Trigger once + Job 调度 | Jobs API | 典型生产模式：availableNow + Databricks Job 定时调度 |
| INSERT-ONLY MERGE | Delta Lake MERGE | MERGE 操作属于 Delta Lake 核心知识 |
| Liquid Clustering | Delta Lake 存储优化 | cluster on write 与写入方式的兼容性 |
| SQL Alert | Databricks SQL | Alert 的触发逻辑依赖对 SQL 查询结果结构的理解 |
| High Concurrency 集群 | 集群管理 | 属于 Databricks 平台架构知识 |
| Spark 缓存 / lazy evaluation | Spark 执行模型 | 理解 transformation vs action 是性能测试的前提 |

---

## 六、Anki 卡片（Q/A 格式）

**卡片 1**
Q: Structured Streaming 中，两个 streaming query 写入同一张 Delta 表，它们的 checkpoint 目录可以共享吗？
A: 不可以。每个 streaming query 必须有独立的 checkpoint 目录。Delta Lake 支持多 stream 并发写入同一张表（乐观并发控制），但 checkpoint 记录的是每个 query 独立的 source offset 和 state，共享会导致互相覆盖。

**卡片 2**
Q: 修改 streaming query 的聚合逻辑（如添加新的聚合字段）后，需要做什么？
A: 必须指定新的 checkpointLocation。有状态操作的 state schema 存储在 checkpoint 中，变更后旧 state 无法反序列化，会报错。

**卡片 3**
Q: Checkpoint 记录哪三类信息？
A: (1) Source offset -- 源中处理到哪个位置 (2) Sink commit log -- 已写入目标的批次 (3) State -- 有状态操作（聚合、窗口）的中间状态。

**卡片 4**
Q: processingTime 模式和 trigger once/availableNow 模式在集群资源消耗上有什么根本区别？
A: processingTime 模式下集群持续运行，即使没有数据也不释放。trigger once/availableNow 处理完数据即停止，集群可以释放或被 instance pool 回收。后者配合 Job 调度是延迟容忍度 > 分钟级场景的成本最优方案。

**卡片 5**
Q: Structured Streaming 的 microbatch 是并行执行还是串行执行？
A: 严格串行。上一个 microbatch 没处理完，下一个不会启动。如果处理时间 > trigger interval，下一个 batch 立即开始（不等待 interval），但不会并行。

**卡片 6**
Q: `spark.readStream.table("delta_table")` 默认读取什么数据？源表有 UPDATE/DELETE 时会怎样？
A: 默认只读取新增行（append-only 模式）。源表有 UPDATE/DELETE 时报错。要读取变更需启用 CDF：`readStream.option("readChangeFeed", "true")`。

**卡片 7**
Q: Databricks SQL Alert 对 GROUP BY 查询结果的检查逻辑是什么？
A: 逐行检查条件，任意一行满足即触发 alert。不是对所有行做聚合后再判断。

**卡片 8**
Q: Jobs REST API 中 `creator_user_name` 字段反映的是谁？
A: 触发这次 run 的 token 所属用户（run creator），不是 job creator 或 job owner。即使 job owner 变更，只要触发 run 的 token 不变，这个字段也不变。

**卡片 9**
Q: /jobs/run-now 和 /jobs/runs/submit 的区别是什么？
A: /jobs/run-now 触发已有的 Job 定义，运行历史在 Job UI 可见。/jobs/runs/submit 是一次性提交，不关联已有 Job 定义，相当于临时运行。

**卡片 10**
Q: Liquid Clustering 的 cluster on write 不支持哪种写入操作？
A: SQL 的 INSERT INTO。支持 CTAS/RTAS、`.write.mode('append')`、`.writeStream.mode('append')`。

**卡片 11**
Q: 为什么在 notebook 中重复执行同一 cell 得到的执行时间不能作为生产性能基准？
A: Spark 缓存中间结果，后续执行直接命中缓存，速度远快于首次执行。生产环境每次 job 冷启动，无缓存。要获得准确基准需要生产规模数据 + 生产规模集群 + Run All 执行模式。

**卡片 12**
Q: trigger(once=True) 和 trigger(availableNow=True) 的区别？
A: once 将所有可用数据塞进一个 batch，数据量大时可能 OOM。availableNow 将可用数据分为多个 batch（受 maxFilesPerTrigger 等控制），处理完全部后停止。once 已废弃，由 availableNow 替代。

---

## 七、掌握度自测

**题 1：** 一个 streaming query 使用 `trigger(processingTime="5 seconds")`，当前 microbatch 处理耗时 8 秒。下一个 microbatch 何时开始？

- A. 当前 batch 结束后等待 5 秒
- B. 当前 batch 结束后立即开始
- C. 当前 batch 开始后 5 秒（与当前 batch 并行）
- D. 当前 batch 结束后等待 3 秒（8-5=3）

**答案：B**
**解析：** 处理时间 > trigger interval 时，下一个 batch 在当前 batch 结束后立即开始，不等待。microbatch 是串行的，不会并行执行。

---

**题 2：** 以下哪个操作变更后不需要指定新的 checkpointLocation？

- A. 将 `GROUP BY user_id` 改为 `GROUP BY user_id, region`
- B. 将 `WHERE amount > 100` 改为 `WHERE amount > 200`
- C. 添加 `SUM(amount) as total` 到已有的聚合查询
- D. 更换 Kafka topic 数据源

**答案：B**
**解析：** filter 是无状态操作，修改条件不影响 checkpoint 中的 state 或 source offset。A 改变了聚合键（state schema 变化），C 改变了聚合表达式（state schema 变化），D 改变了数据源（source offset 不兼容），都需要新 checkpoint。

---

**题 3：** 一个 streaming job 使用 default trigger，监控发现每分钟有 15 个 microbatch，其中 12 个是空 batch。延迟容忍度为 30 分钟。最优的优化方案是？

- A. 设置 trigger(processingTime="30 minutes")
- B. 使用 trigger(availableNow=True) 配合每 30 分钟的 Job 调度
- C. 增加集群节点数以更快处理空 batch
- D. 设置 minOffsetsPerTrigger 以跳过空 batch

**答案：B**
**解析：** 12/15 的空 batch 说明数据量远低于处理能力，default trigger 浪费资源。延迟容忍度 30 分钟完全适合 trigger(availableNow=True) + Job 调度。A 虽然减少了空 batch 但集群持续运行。B 处理完释放集群，成本最优。

---

**题 4：** 一个 data engineer 用个人 token 创建了一个 Job（User A），然后将 Job Owner 转给 User B。User A 继续用自己的 token 触发 run。获取 run 信息时，以下哪项正确？

- A. creator_user_name = User B（因为 B 是当前 Owner）
- B. creator_user_name = User A（因为 A 的 token 触发了 run）
- C. creator_user_name = User A（因为 A 创建了 Job）
- D. creator_user_name 字段为空（Owner 已转移）

**答案：B**
**解析：** creator_user_name 反映触发 run 的 token 所属用户，与 job owner 无关。B 和 C 结果相同但理由不同：正确的理由是"A 的 token 触发了 run"，不是"A 创建了 Job"。

---

**题 5：** 以下哪种写入方式不支持 Liquid Clustering 的 cluster on write？

- A. `spark.writeStream.format("delta").mode("append")`
- B. `CREATE TABLE AS SELECT ...`
- C. `INSERT INTO table_name SELECT ...`
- D. `spark.write.format("delta").mode("append")`

**答案：C**
**解析：** SQL 的 INSERT INTO 是唯一不支持 cluster on write 的写入方式。它只做简单追加，不会按聚簇列重新组织数据。CTAS/RTAS、DataFrame 的 write 和 writeStream append 模式都支持。

---

**题 6：** 一个 streaming query 从 Delta 表 A 读取数据写入 Delta 表 B。表 A 刚刚执行了一次 `DELETE FROM A WHERE date < '2024-01-01'`。此时 streaming query 的行为是？

- A. 正常运行，自动跳过删除操作
- B. 报错，因为 readStream 默认 append-only 模式不支持源表的 DELETE
- C. 将删除操作传播到表 B
- D. 正常运行，但会重新处理所有历史数据

**答案：B**
**解析：** readStream 默认以 append-only 模式读取 Delta 表。源表的 DELETE 操作改变了 Delta Log 的结构，违反 append-only 约束，streaming query 会报错。要处理变更需要启用 CDF。

---

**题 7：** 关于 Databricks SQL Alert，以下哪项描述正确？

- A. Alert 只能基于返回单行结果的查询
- B. 对于 GROUP BY 查询，Alert 计算所有行的聚合值后判断条件
- C. 对于 GROUP BY 查询，Alert 逐行检查条件，任意一行满足即触发
- D. Alert 对每个满足条件的行分别发送通知

**答案：C**
**解析：** SQL Alert 逐行检查条件。对于 GROUP BY 查询返回多行的情况，只要任意一行满足触发条件，alert 就触发（触发一次，不是每行一次）。

---

**题 8：** 一个数据工程师在 notebook 中执行以下代码两次：
```python
df = spark.read.parquet("/data/large_table")
result = df.filter("amount > 1000").groupBy("category").count()
display(result)
```
第一次 45 秒，第二次 5 秒。哪个更接近生产环境的执行时间？

- A. 5 秒，因为生产环境有 Photon 优化
- B. 45 秒，因为生产环境每次从头执行，不存在缓存预热
- C. 两者的平均值 25 秒
- D. 都不准确，因为 notebook 不能用于性能测试

**答案：B**
**解析：** 第一次执行是冷启动，数据从存储读取并处理。第二次执行命中 Spark 缓存，跳过了数据读取和部分计算。生产环境中 Job 每次独立运行，没有缓存预热，所以第一次执行更接近真实性能。D 不正确，notebook 可以用于性能测试但需要正确方法（Run All + 生产规模数据和集群）。
