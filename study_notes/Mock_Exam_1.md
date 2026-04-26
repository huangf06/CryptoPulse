# Databricks Certified Data Engineer Professional — 模拟考试 #1

> 总题数：60 题 | 限时：120 分钟 | 通过线：90%+（54/60）
>
> 答题方式：记录你的答案（A/B/C/D），完成后对照答案文件
>
> 题型：全部为场景题/最佳实践题，模拟真实 Professional 考试风格

---

## 域 1：Databricks Tooling（12 题）

### Q1
你的团队使用 DABs 管理一个 ETL 项目。开发环境使用 `dev_catalog`，生产环境使用 `prod_catalog`。一位新加入的工程师在 `databricks.yml` 中将 production target 的 `default` 设为 `true`。这会导致什么问题？

A. 所有 `databricks bundle deploy` 命令如果不指定 `-t` 参数，将默认部署到生产环境
B. DABs 会报错，因为 `default: true` 只能设置在一个 target 上
C. 没有影响，`default` 只影响 `validate` 命令
D. DABs 会忽略 `default` 设置，总是使用第一个定义的 target

### Q2
你需要在 DLT Pipeline 中使用一个第三方 Python 库 `great_expectations`。以下哪种方式是正确的？

A. 在 DLT Notebook 顶部添加 `%pip install great_expectations`
B. 在 DLT Pipeline 配置的 `configuration` 中添加 `pipelines.libraries` 指定 PyPI 包
C. 将 `great_expectations` 安装到 Cluster Library 中
D. 在 Pipeline Settings 的 Python Libraries 中添加 PyPI 包

### Q3
一个生产 ETL Job 使用个人账号运行。当该员工离职且账号被停用后，Job 停止运行。为防止此类问题，你应该：

A. 将 Job 的 `run_as` 设置为团队 manager 的账号
B. 创建 Service Principal，将 Job 的 `run_as` 设置为该 Service Principal
C. 在 DABs 中使用 `permissions` 将 Job 共享给整个团队
D. 将 Job 配置导出为 JSON，在新账号下重新创建

### Q4
你的团队在 Notebook 中用 `%pip install pandas==2.0.0` 安装了特定版本的 pandas。安装完成后，Notebook 中的后续 cell 仍然使用旧版本。最可能的原因是什么？

A. `%pip install` 只在下次 cluster restart 时生效
B. 需要在 `%pip install` 之后调用 `dbutils.library.restartPython()`
C. Notebook scope 的库安装不支持版本指定
D. 需要先卸载旧版本再安装新版本

### Q5
你需要对一个包含 10 亿行的表执行字符串脱敏操作。以下哪种 UDF 方式性能最优？

A. Python UDF (`@udf`)
B. Pandas UDF (`@pandas_udf`)
C. SQL UDF (`CREATE FUNCTION`)
D. Scala UDF

### Q6
你在使用 DABs 部署一个 Pipeline。执行 `databricks bundle validate` 时成功，但 `databricks bundle deploy` 时失败，报错说目标 Workspace 中已存在同名 Pipeline。你应该怎么做？

A. 使用 `databricks bundle destroy` 先删除远程资源，再重新部署
B. 在 `databricks.yml` 中为该 Pipeline 设置不同的 `name`，或检查是否有其他团队成员已部署
C. 使用 `databricks bundle deploy --force` 强制覆盖
D. 在 Workspace UI 中手动删除该 Pipeline，再重新部署

### Q7
关于 Job Cluster 和 All-Purpose Cluster，以下哪个说法是**错误**的？

A. Job Cluster 的 DBU 单价比 All-Purpose Cluster 低约 50%
B. Job Cluster 在 Job 结束后自动终止
C. Job Cluster 支持交互式 Notebook 调试
D. 生产环境的 ETL Job 应优先使用 Job Cluster

### Q8
一个 Lakeflow Job 有三个 Task：Task A 生成一个日期参数，Task B 和 Task C 需要使用这个日期参数。Task A 中使用了 `dbutils.jobs.taskValues.set(key="run_date", value="2026-01-15")`。在 Task B 中，如何获取这个参数？

A. `dbutils.jobs.taskValues.get(taskKey="Task_A", key="run_date")`
B. `dbutils.widgets.get("run_date")`
C. `spark.conf.get("run_date")`
D. `dbutils.jobs.taskValues.get(key="run_date")`

### Q9
Cluster Policy 中，你想限制团队成员只能选择特定的 instance type。以下哪种策略类型最合适？

A. `regex` 验证
B. `range` 限制
C. `allowlist` 白名单
D. `unlimited` 不限制

### Q10
你的 Lakeflow Job 包含 5 个 Task，其中 Task 3 失败了。Task 4 和 Task 5 依赖 Task 3 因此也跳过了。修复 Task 3 的代码后，你执行了 Repair Run。以下哪个说法正确？

A. Repair Run 会重新运行全部 5 个 Task
B. Repair Run 只重新运行 Task 3，Task 4 和 5 仍为跳过状态
C. Repair Run 重新运行 Task 3、4、5，Task 1 和 2 保持原来的成功状态
D. Repair Run 需要先手动标记 Task 3 为"可重试"

### Q11
关于 Photon 引擎，以下哪个说法是**正确**的？

A. Photon 可以加速所有 Python UDF
B. Photon 使用 C++ 重写了 Spark 的查询引擎，主要加速 SQL 和 DataFrame 操作
C. Photon 只在 SQL Warehouse 中可用，Job Cluster 不支持
D. 启用 Photon 后，所有查询都会自动变快，无需其他优化

### Q12
你需要为一个 Lakeflow Job 配置：遍历 10 个国家的数据，每个国家执行相同的 ETL 逻辑，最多并行运行 3 个。你应该使用：

A. 手动创建 10 个 Task，每个处理一个国家
B. For Each Task，设置 `concurrency` 为 3
C. 一个 Task 内用 Python 循环处理 10 个国家
D. 10 个独立的 Job，用 Orchestrator 调度

---

## 域 2：Data Processing（18 题）

### Q13
两个并发的 Spark Job 同时操作一张 Delta 表。Job A 执行 `INSERT INTO`（Append 模式），Job B 执行 `DELETE WHERE country = 'US'`。关于 OCC（乐观并发控制），以下哪个说法正确？

A. 两个 Job 会冲突，后提交的 Job 需要重试
B. 不会冲突，因为 APPEND 只添加新文件，不读取现有文件
C. 不会冲突，因为 Delta Lake 使用行级锁
D. 会冲突，因为 DELETE 需要对整个表加独占锁

### Q14
你对一张 Delta 表执行了 `VACUUM RETAIN 0 HOURS`，但命令报错。以下哪个原因最准确？

A. `VACUUM` 不支持 0 小时的保留期
B. Delta Lake 的安全检查阻止了低于 168 小时（7 天）的保留期设置
C. 表上有活跃的 Streaming Query，不能执行 VACUUM
D. 需要先停止所有对该表的读写操作

### Q15
你在 Delta 表上执行了 `RESTORE TABLE my_table TO VERSION AS OF 5`。当前版本号为 10。RESTORE 完成后，新的版本号是？

A. 5
B. 10
C. 11
D. 0

### Q16
你创建了一个 Shallow Clone：`CREATE TABLE clone_t SHALLOW CLONE source_t`。之后在 `source_t` 上执行了 `VACUUM RETAIN 0 HOURS`（已禁用安全检查）。对 `clone_t` 执行查询会怎样？

A. 查询正常，Shallow Clone 有独立的数据副本
B. 查询可能失败，因为 Shallow Clone 引用的源数据文件可能已被 VACUUM 删除
C. 查询正常，VACUUM 不影响 Clone 引用的文件
D. Clone 自动转换为 Deep Clone

### Q17
一张启用了 CDF 的 Delta 表，执行了以下操作：
```sql
UPDATE orders SET status = 'shipped' WHERE order_id = 101;
```
这条 UPDATE 在 CDF 中会生成几行记录？其 `_change_type` 分别是什么？

A. 1 行：update
B. 2 行：update_preimage 和 update_postimage
C. 1 行：update_postimage
D. 3 行：delete、insert、update

### Q18
你正在使用 CDF 从 Bronze 表向 Silver 表同步增量数据。Silver 表出现了意外的重复数据。最可能的原因是什么？

A. CDF 本身产生了重复记录
B. Bronze 表的 OPTIMIZE 操作产生了假的 `update_preimage` / `update_postimage` 记录，这些数据被错误地当作真实变更写入了 Silver
C. Silver 表没有设置主键约束
D. Streaming Query 的 checkpoint 损坏导致重复消费

### Q19
你使用 `MERGE INTO` 进行 SCD Type 1 更新。SQL 中定义了两个 `WHEN MATCHED` 子句：
```sql
MERGE INTO target t USING source s ON t.id = s.id
WHEN MATCHED AND s.status = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
如果 source 中有一行 `id=1, status='delete'`，target 中也有 `id=1`。会执行什么操作？

A. UPDATE（因为第二个 WHEN MATCHED 没有条件，优先匹配）
B. DELETE（WHEN MATCHED 子句按**定义顺序**匹配，第一个满足条件的执行）
C. 报错，因为不能有两个 WHEN MATCHED 子句
D. 两个操作都会执行

### Q20
一个 Structured Streaming 查询从 Delta 表读取数据（默认 Append-only 模式）。有人在源 Delta 表上执行了 `DELETE` 操作。流会怎样？

A. 流会忽略 DELETE 操作，继续处理新 INSERT 的数据
B. 流会处理 DELETE，从 sink 中删除对应记录
C. 流会抛出异常并停止，因为 Delta 作为 streaming source 默认只支持 Append
D. 流会自动重启并重新处理全部数据

### Q21
你的 Structured Streaming 查询的 checkpoint 目录被意外删除。重新启动流后会发生什么？

A. 流从上次停止的 offset 继续处理
B. 流从源表的最新数据开始处理，跳过历史数据
C. 流从头开始重新处理所有数据，可能导致 sink 中出现重复数据
D. 流会报错，无法启动

### Q22
你正在使用 Auto Loader 摄取一个每天新增 100 万个小文件的 S3 目录。以下哪种 File Discovery 模式最合适？

A. Directory Listing 模式
B. File Notification 模式
C. 两种模式性能相同
D. 应该用 COPY INTO 代替 Auto Loader

### Q23
Auto Loader 配置了 `cloudFiles.schemaEvolutionMode = "addNewColumns"`。源数据中突然多了一个新字段 `phone_number`。以下哪个描述正确？

A. Auto Loader 自动将新列添加到 target schema，流继续运行
B. Auto Loader 检测到新列，**停止流**并更新 schema，需要手动重启
C. 新列数据被放入 `_rescued_data` 列
D. Auto Loader 直接报错，需要手动更新 schemaLocation

### Q24
一个 Streaming Query 包含聚合操作（`groupBy(...).count()`），**没有** Watermark。以下哪种 Output Mode 会**报错**？

A. Complete
B. Update
C. Append
D. Complete 和 Update 都会报错

### Q25
一个 Streaming Query 使用了 Watermark（阈值 10 分钟）和窗口聚合（5 分钟窗口），Output Mode 为 Append。窗口 `[12:00, 12:05)` 的聚合结果何时输出？

A. 当窗口 `[12:00, 12:05)` 的第一条数据到达时
B. 当 12:05 时刻的数据到达时
C. 当 Watermark 超过 12:05 时（即 max_event_time > 12:15）
D. 每个微批都输出该窗口的最新结果

### Q26
你有一个 Stream-Stream Join：左流是订单事件，右流是支付事件。以下哪个条件是 Stream-Stream Join 的**硬性要求**？

A. 两侧都必须有 Watermark，且 JOIN 条件中必须包含时间范围约束
B. 只需要一侧有 Watermark
C. 不需要 Watermark，但需要使用 Append Output Mode
D. 两侧都需要 Watermark，但不需要时间范围约束

### Q27
关于 Stream-Static Join，以下哪个说法是**正确**的？

A. 静态表在流启动时只读取一次，后续更新不会被看到
B. 静态表在每个微批处理时都会被重新读取，可以看到最新数据
C. Stream-Static Join 需要设置 Watermark
D. Stream-Static Join 只支持 Inner Join

### Q28
你在 DLT Pipeline 中定义了一个 Expectation：
```python
@dlt.expect_or_drop("valid_amount", "amount > 0")
```
如果一行数据的 `amount = -5`，会发生什么？

A. 该行被标记为无效但仍然写入表中
B. 该行被丢弃，不写入表中，且在 Event Log 中记录
C. Pipeline 失败并停止
D. 该行被写入一个单独的隔离表

### Q29
在 DLT 中，Bronze 层通常使用 Streaming Table，Gold 层通常使用 Materialized View。为什么 Gold 层不推荐使用 Streaming Table？

A. Gold 层的数据量太大，Streaming Table 处理不了
B. Gold 层通常需要复杂的聚合和 JOIN，这些操作需要**全量重算**以保证正确性，而 Streaming Table 是增量处理的
C. Streaming Table 不支持 SQL 查询
D. Materialized View 比 Streaming Table 性能更好

### Q30
你在 DLT Pipeline 中使用 `APPLY CHANGES INTO` 处理 CDC 数据，目标是 SCD Type 2。以下哪个字段**不会**自动生成？

A. `__START_AT`
B. `__END_AT`
C. `__IS_CURRENT`
D. `__CHANGE_TYPE`

### Q31
关于 Trigger 类型，一个数据团队需要每天凌晨 2 点运行一次 Streaming Job，处理前一天积攒的所有数据，处理完后自动停止。以下哪种配置最合适？

A. `trigger(processingTime="24 hours")`
B. `trigger(once=True)`
C. `trigger(availableNow=True)`
D. 不设置 trigger（使用默认的持续处理）

### Q32
关于 `trigger(once=True)` 和 `trigger(availableNow=True)` 的区别，以下哪个说法是正确的？

A. 两者完全等价，`availableNow` 只是 `once` 的新名字
B. `once` 在一个微批中处理所有数据；`availableNow` 将积压数据分成多个微批处理，可以更好地利用 rate limiting 和处理进度记录
C. `once` 处理所有积压数据，`availableNow` 只处理最新的一批
D. `availableNow` 只在 DLT 中可用

### Q33
你正在使用 `foreachBatch` 将流数据同时写入 Delta 表和一个外部 PostgreSQL 数据库。微批 ID 为 5 的数据处理成功写入了 Delta，但写 PostgreSQL 时失败了。流重试微批 5 时，Delta 中会出现重复数据吗？

A. 不会，Delta 的 Exactly-once 保证会自动去重
B. 会，因为 `foreachBatch` 中的操作不在 Spark 的 checkpoint 事务范围内，Delta 写入已经提交
C. 不会，Spark 会自动回滚微批 5 中 Delta 的写入
D. 取决于是否在 `foreachBatch` 中使用了 MERGE

### Q34
在 `foreachBatch` 中，为什么推荐使用 MERGE 而不是 append？

A. MERGE 的性能比 append 更好
B. MERGE 是天然幂等的（基于主键匹配），即使微批重试也不会产生重复数据
C. append 在 foreachBatch 中不被允许
D. MERGE 支持多目标写入而 append 不支持

### Q35
你的 Streaming Query 使用了 Watermark（阈值 15 分钟）。当前收到的最大 event_time 是 13:30。以下关于 Watermark 的计算，哪个是正确的？

A. Watermark = 13:30
B. Watermark = 13:15
C. Watermark = 13:45
D. Watermark = 13:00

### Q36
一张 Delta 表有严重的小文件问题（上千个小文件）。以下哪种方案最有效？

A. 使用 `OPTIMIZE` 命令合并小文件
B. 重建表（CTAS）
C. 增加 Shuffle 分区数
D. 将表转换为 Parquet 格式

### Q37
关于 AQE（Adaptive Query Execution）的三大功能，以下哪个描述是**错误**的？

A. 动态合并 Shuffle 分区（减少小分区）
B. 动态切换 Join 策略（如果运行时发现小表，自动切换为 Broadcast Join）
C. 动态优化 Skew Join（自动拆分数据倾斜的分区）
D. 动态调整 Executor 数量（自动增减计算节点）

### Q38
你有一张表经常按 `region` 和 `product_category` 两个列进行查询过滤，但查询模式不固定（有时只按 region、有时只按 category、有时两个都用）。以下哪种文件布局策略最合适？

A. 按 `region` 分区
B. 按 `region` 和 `product_category` 做 Z-Ordering
C. 使用 Liquid Clustering，设置 `CLUSTER BY (region, product_category)`
D. 按 `product_category` 分区

### Q39
一个 Streaming Query 从 Auto Loader 读取数据，写入 Delta 表。流持续运行了 6 个月，产生了大量小文件。以下哪种方式可以在**不影响流运行**的情况下治理小文件？

A. 停止流，运行 `OPTIMIZE`，再重启流
B. 在表属性中启用 `delta.autoOptimize.autoCompact = true`，流写入时自动触发后台合并
C. 增加 `trigger(processingTime=...)` 的间隔，减少写入频率
D. 使用 `VACUUM` 删除小文件

### Q40
你发现一个 Spark Job 中某个 Stage 的 200 个 Task 中，有 1 个 Task 运行了 30 分钟，其余 199 个 Task 都在 1 分钟内完成。最可能的原因是什么？

A. 该 Task 处理的数据块损坏
B. 数据倾斜（Data Skew），某个分区的数据量远大于其他分区
C. Executor 内存不足
D. 网络带宽限制

---

## 域 3：Data Modeling（12 题）

### Q41
在 Medallion 架构中，Bronze 层的主要职责是什么？

A. 数据清洗和去重
B. 原始数据的忠实保存（raw ingestion），保留原始格式和所有字段，附加摄取元数据
C. 面向业务的聚合报表
D. 数据质量校验和 Schema 标准化

### Q42
你的组织有 3 个上游系统，每个系统产生不同 schema 的事件数据。你希望用一个 Bronze 表存储所有系统的数据。这属于什么摄取模式？

A. Singleplex
B. Multiplex
C. Fan-out
D. Broadcast

### Q43
在 Lakehouse 架构中，为什么推荐使用 Star Schema 而不是 Snowflake Schema？

A. Star Schema 占用更少的存储空间
B. Lakehouse 中存储成本低，Star Schema 的冗余不是问题，而其更简单的 JOIN（只需一层）带来更好的查询性能
C. Snowflake Schema 在 Delta Lake 中不受支持
D. Star Schema 支持更多的维度

### Q44
你需要在 DLT 中实现 SCD Type 2，保留客户地址的变更历史。以下哪种 SQL 语法正确？

A. `APPLY CHANGES INTO LIVE.customer_dim FROM STREAM(LIVE.customer_cdc) KEYS (customer_id) SEQUENCE BY updated_at STORED AS SCD TYPE 2`
B. `MERGE INTO customer_dim USING customer_cdc ON ... WHEN MATCHED AND changed THEN INSERT`
C. `CREATE STREAMING TABLE customer_dim AS SELECT *, ROW_NUMBER() OVER (...) FROM STREAM(customer_cdc)`
D. `APPLY CHANGES INTO customer_dim FROM customer_cdc KEYS (customer_id) TRACK HISTORY`

### Q45
SCD Type 1 和 SCD Type 2 的核心区别是什么？

A. Type 1 保留所有历史，Type 2 只保留最新
B. Type 1 直接覆盖旧值（不保留历史），Type 2 通过新增行的方式保留变更历史
C. Type 1 用于事实表，Type 2 用于维度表
D. Type 1 更快，Type 2 更准确

### Q46
DLT Pipeline 中，以下哪种场景应该使用 Streaming Table 而不是 Materialized View？

A. Gold 层的月度销售汇总报表
B. Bronze 层从 Auto Loader 持续摄取的原始数据
C. 一个需要 JOIN 多个维度表的宽表
D. 一个需要每天全量重算的数据质量报告

### Q47
关于 DLT Pipeline 的 Development Mode 和 Production Mode，以下哪个说法是**正确**的？

A. Development Mode 使用新集群，Production Mode 复用集群
B. Development Mode 复用集群且不自动重试失败；Production Mode 创建新集群且自动重试
C. 两种模式的行为完全相同，只是名称不同
D. Development Mode 不执行 Expectations，Production Mode 才执行

### Q48
你在 DLT 中定义了三个 Expectations 在同一个表上：
```python
@dlt.expect("valid_id", "id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect_or_fail("valid_date", "order_date >= '2020-01-01'")
```
一行数据 `id=NULL, amount=5, order_date='2025-01-01'` 会怎样？

A. 该行被丢弃（因为 id IS NULL 不满足 `expect`）
B. 该行被保留并写入表中，`valid_id` 的违规被记录在 metrics 中但不阻止写入
C. Pipeline 失败（因为有一个 Expectation 不满足）
D. 只检查第一个不满足的 Expectation

### Q49
关于 Medallion Architecture 的反模式，以下哪个做法是**不推荐**的？

A. Silver 层做数据去重和 Schema 标准化
B. 直接从 Bronze 层创建面向业务的报表（跳过 Silver）
C. Gold 层做面向特定业务领域的聚合
D. Bronze 层保留原始数据的所有字段

### Q50
你有一个大型事实表 `orders`（10亿行）和一个小维度表 `products`（5000行）。在 Star Schema 中查询时，Spark 最可能使用哪种 JOIN 策略？

A. Sort Merge Join
B. Broadcast Hash Join（将 products 广播到所有 Executor）
C. Shuffle Hash Join
D. Nested Loop Join

### Q51
关于 DLT 中 `APPLY CHANGES` 和 `foreachBatch` + MERGE 的对比，以下哪个说法正确？

A. 两者功能完全相同，只是语法不同
B. `APPLY CHANGES` 是声明式的，自动处理 CDC 逻辑和 SCD；`foreachBatch` + MERGE 是命令式的，需要手动编写所有逻辑
C. `APPLY CHANGES` 只支持 SCD Type 1
D. `foreachBatch` + MERGE 性能始终优于 `APPLY CHANGES`

### Q52
在 Multiplex 摄取模式下，一个 Bronze 表包含了来自 Kafka topic 的多种事件类型。你需要为每种事件类型创建独立的 Silver 表。最佳做法是：

A. 为每种事件类型创建一个独立的 Streaming Query 从 Kafka 读取
B. 使用单一 Bronze Streaming Table 摄取所有事件，然后为每种事件类型创建 Silver Streaming Table，使用 `WHERE event_type = 'xxx'` 过滤
C. 使用 `foreachBatch` 在一个流中写入多个 Silver 表
D. 使用 COPY INTO 定时批量加载

---

## 域 4：Security and Governance（6 题）

### Q53
Unity Catalog 的三层命名空间是什么？一个用户需要查询 `prod_catalog.sales.orders` 表，至少需要哪些权限？

A. 只需要 `SELECT on prod_catalog.sales.orders`
B. `USE CATALOG on prod_catalog` + `USE SCHEMA on prod_catalog.sales` + `SELECT on prod_catalog.sales.orders`
C. `ALL PRIVILEGES on prod_catalog`
D. `SELECT on prod_catalog.sales.*`

### Q54
你需要实现列级数据脱敏：普通用户查询 `employees` 表时，`salary` 列显示 `***`，而 HR 团队可以看到真实值。最佳实现方式是：

A. 创建两张物理表，一张有真实数据，一张脱敏数据
B. 使用 Dynamic View，结合 `is_account_group_member('hr_team')` 函数实现条件显示
C. 使用 Row-Level Security
D. 在应用层处理脱敏逻辑

### Q55
关于 Managed Table 和 External Table，执行 `DROP TABLE` 后的行为区别是什么？

A. 两者都删除元数据和底层数据
B. Managed Table 删除元数据和底层数据；External Table 只删除 Unity Catalog 中的元数据，底层数据保留
C. 两者都只删除元数据
D. External Table 删除元数据和数据，Managed Table 只删除元数据

### Q56
你需要将公司内部的 Delta 表安全地共享给一个不使用 Databricks 的外部合作伙伴。最合适的方案是：

A. 导出 CSV 文件通过 SFTP 发送
B. 使用 Delta Sharing（Open sharing protocol），创建 Recipient 并生成 activation link
C. 给外部合作伙伴创建 Databricks 账号
D. 使用 Lakehouse Federation 连接外部系统

### Q57
关于 `dbutils.secrets.get(scope="my_scope", key="db_password")` 的安全机制，以下哪个说法正确？

A. Secret 值会以明文形式显示在 Notebook output 中
B. Secret 值在 Notebook output 中显示为 `[REDACTED]`，防止意外泄露
C. Secret 值只能在 SQL 中使用，不能在 Python 中使用
D. 每次调用 `secrets.get()` 都需要用户输入密码确认

### Q58
为满足 GDPR 的"被遗忘权"（Right to be Forgotten），你需要从 Delta 表中彻底删除用户数据。正确的操作流程是：

A. `DELETE WHERE user_id = 'xxx'` → 完成
B. `DELETE WHERE user_id = 'xxx'` → `VACUUM` → 保留审计记录
C. `VACUUM` → `DELETE WHERE user_id = 'xxx'`
D. 直接删除包含该用户的 Parquet 文件

---

## 域 5：Monitoring and Logging（6 题）

### Q59
在 Spark UI 中，你发现一个 Stage 的 Shuffle Read 达到了 500 GB，明显超过预期。最可能的优化方式是：

A. 增加 Executor 数量
B. 检查是否可以使用 Broadcast Join 避免 Shuffle，或检查 JOIN key 是否合理
C. 增加 Shuffle 分区数
D. 升级到更大的 instance type

### Q60
你想要查询过去 30 天所有 DLT Pipeline 运行的数据质量结果（passed/failed records）。应该查询：

A. `system.billing.usage`
B. DLT Pipeline 的 Event Log（`event_log(TABLE(pipeline))`）
C. `system.access.audit`
D. Spark UI 的 SQL tab

### Q61
你的组织需要监控 Databricks 的计费使用情况，找出哪个团队消耗了最多的 DBU。应该查询哪个 System Table？

A. `system.access.audit`
B. `system.billing.usage`
C. `system.compute.clusters`
D. `system.information_schema.tables`

### Q62
在 Spark UI 中，以下哪个指标最能说明存在数据倾斜？

A. 所有 Task 的运行时间均匀分布
B. 某些 Task 的 Shuffle Read Size / Records 远大于其他 Task（10x+），且这些 Task 运行时间远长于其他 Task
C. Executor 的 CPU 利用率低
D. Job 总耗时长

### Q63
你创建了一个 SQL Alert，当 `error_count > 100` 时发送通知到 Slack。以下哪个说法是正确的？

A. SQL Alert 是实时触发的，error_count 一超过 100 立即通知
B. SQL Alert 基于调度运行 SQL 查询，在调度触发时检查条件，满足则发送通知
C. SQL Alert 只能发送邮件，不支持 Slack
D. SQL Alert 可以自动修复检测到的问题

### Q64
你在 Query Profile 中看到某个 JOIN 操作出现了 "spill to disk"。这说明什么？

A. 磁盘空间不足
B. JOIN 操作的中间数据超过了可用内存，被迫写入磁盘，会显著降低性能
C. 数据被持久化到磁盘用于 checkpoint
D. 这是正常行为，不需要关注

---

## 域 6：Testing and Deployment（6 题）

### Q65
在 PySpark 单元测试中，以下哪种方式创建 SparkSession 最合适？

A. 连接到远程 Databricks Cluster
B. `SparkSession.builder.master("local[*]").getOrCreate()`
C. 使用 `dbutils.notebook.run()` 在 Databricks 上运行测试
D. 不需要 SparkSession，直接测试 Python 逻辑

### Q66
关于测试金字塔，以下哪个描述反映了正确的测试策略？

A. 大量 E2E 测试，少量 Unit 测试
B. 大量 Unit 测试，适量 Integration 测试，少量 E2E 测试
C. 只需要 Integration 测试，不需要 Unit 和 E2E
D. Unit、Integration、E2E 数量应该相等

### Q67
你的 CI/CD Pipeline 使用 GitHub Actions 部署 DABs。在部署到 Production 之前，应该执行什么步骤？

A. 直接 `databricks bundle deploy -t production`
B. `databricks bundle validate -t production` → 运行测试 → Code Review → `databricks bundle deploy -t production`
C. 只需要 Code Review，不需要自动化验证
D. 先部署到 Production，再运行测试验证

### Q68
关于 Repair Run 和 Job Cluster 的关系，以下哪个说法正确？

A. Repair Run 复用原来的 Job Cluster
B. Repair Run 创建新的 Job Cluster 来运行失败的 Task
C. Repair Run 只能在 All-Purpose Cluster 上执行
D. Repair Run 不需要计算资源

### Q69
你需要在 DABs 中配置三个环境的 Catalog 隔离。以下配置中，Production 环境的最佳实践是什么？

A. 使用个人账号作为 `run_as`，使用 `prod_catalog`
B. 使用 Service Principal 作为 `run_as`，使用 `prod_catalog`，限制谁可以部署
C. 不设置 `run_as`，让部署者的身份自动成为运行身份
D. 使用共享团队账号作为 `run_as`

### Q70
你的团队使用 Databricks Git Folders (Repos) 进行版本管理。以下哪个操作在 Git Folders 中**不支持**？

A. 从 Workspace UI 中 pull 最新代码
B. 创建和切换 branch
C. 直接在 Workspace UI 中解决 merge conflict
D. 查看 commit 历史

---

> **答题结束后，请将你的答案发送给我，我会对照答案逐题解析。**
>
> 格式示例：`Q1:A Q2:B Q3:C ...`
