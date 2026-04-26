# Mock Exam #1 — 答案与详细解析

> ⚠️ 请先完成答题再查看此文件！

---

## 域 1：Databricks Tooling

### Q1: A
`default: true` 意味着不指定 `-t` 参数时的默认 target。如果设在 production 上，团队成员忘记加 `-t dev` 就会直接部署到生产。这是一个常见的安全隐患，`default: true` 应该始终设在 development target 上。

### Q2: D
**DLT 不支持 `%pip install`**，这是一个高频考点。DLT Pipeline 的第三方库需要在 Pipeline Settings 的 Python Libraries 中以 PyPI 包的形式添加。选项 B 的 `pipelines.libraries` 不是正确的配置路径。

### Q3: B
Service Principal 是非人类身份，不会因员工离职而失效。这是生产环境 Job 的最佳实践。选项 A 只是换了一个人，同样的问题会再次发生。

### Q4: B
`%pip install` 安装完成后，需要调用 `dbutils.library.restartPython()` 重启 Python 解释器，新版本才会在后续 cell 中生效。

### Q5: C
SQL UDF 在 Catalyst 优化器内执行，不需要 JVM↔Python 序列化开销。Pandas UDF 次之（Arrow 批量序列化），Python UDF 最慢（逐行序列化）。对于简单的字符串操作，SQL UDF 或内置函数是首选。

### Q6: B
先检查冲突原因。可能是其他团队成员已部署了同名资源，或者你的 bundle name/pipeline name 需要调整。不应该盲目 destroy 或强制覆盖。选项 C 的 `--force` 不是 DABs 的标准选项。

### Q7: C
Job Cluster **不支持**交互式 Notebook 调试。它在 Job 启动时创建，Job 结束后自动销毁，整个生命周期只为该 Job 服务。交互式调试需要使用 All-Purpose Cluster。

### Q8: A
`dbutils.jobs.taskValues.get()` 需要指定 `taskKey`（产生该值的 Task 名称）和 `key`（值的名称）。不指定 taskKey 会导致找不到对应的 value。

### Q9: C
`allowlist` 白名单模式可以精确限制可选的 instance type 列表，是控制成本和资源的常用方式。

### Q10: C
Repair Run 的核心设计：**只重新运行失败的 Task 及其下游依赖**，成功的 Task 保持原结果不重跑。这既节省资源，又保证了数据一致性。

### Q11: B
Photon 是 C++ 重写的向量化查询引擎，加速 SQL 和 DataFrame 操作。**不加速 Python UDF**（因为 UDF 在 Python 进程中执行，不经过 Photon）。Photon 在 Job Cluster 中也可以启用。

### Q12: B
For Each Task 是专为这种"相同逻辑、不同参数"的场景设计的。通过 `concurrency` 参数控制并行度，比手动创建多个 Task 更优雅且易于维护。

---

## 域 2：Data Processing

### Q13: B
**APPEND 永不冲突**。这是 OCC 冲突矩阵的核心规则。APPEND 操作只添加新的数据文件，不读取也不修改任何现有文件，因此不可能与任何其他操作冲突。只有"读取旧文件 + 修改"的操作之间才会冲突（如 DELETE vs UPDATE）。

### Q14: B
Delta Lake 有安全检查：保留期低于 168 小时（7 天）时会报错。需要先设置 `spark.databricks.delta.retentionDurationCheck.enabled = false` 才能使用低于默认值的保留期。这是为了防止意外删除仍在被引用的文件。

### Q15: C
RESTORE 操作本身会创建一个**新的 commit**。当前版本 10 → RESTORE → 新版本号为 **11**。版本 11 的内容等同于版本 5，但版本号是递增的。RESTORE 不是"回退版本指针"。

### Q16: B
Shallow Clone 不复制数据，而是引用源表的数据文件。如果源表的 VACUUM 删除了这些文件，Shallow Clone 的查询会因为找不到文件而失败。这是 Shallow Clone 的主要风险。

### Q17: B
UPDATE 在 CDF 中产生 **2 行**：`update_preimage`（变更前的值）和 `update_postimage`（变更后的值）。这让下游可以知道"从什么变成了什么"。

### Q18: B
OPTIMIZE 操作会重写数据文件，在 CDF 中产生假的 `update_preimage` / `update_postimage` 记录。如果下游不过滤这些假记录，就会导致重复数据。解决方案是在读取 CDF 时过滤 `_commit_version` 和 `_change_type`，或在下游使用 MERGE 保证幂等。

### Q19: B
WHEN MATCHED 子句按**定义顺序**匹配。第一个满足条件的子句执行后，该行不再检查后续子句。这里 `s.status = 'delete'` 满足第一个 WHEN MATCHED 的条件，所以执行 DELETE。

### Q20: C
Delta 作为 Streaming Source 默认是 **Append-only** 模式。如果源表发生了 DELETE 或 UPDATE（非 Append 操作），流会抛出异常。如果需要处理这些变更，应使用 CDF（`readChangeFeed`）。

### Q21: C
Checkpoint 记录了流的处理进度（已处理的 offset、状态等）。删除后，流无法知道之前处理到哪里，会从头重新处理所有数据。如果 sink 不支持幂等写入，会导致重复数据。

### Q22: B
**File Notification** 模式基于云事件通知（如 S3 Event Notification + SQS），不需要扫描整个目录。对于大量文件的目录，比 Directory Listing（每次 LIST 全目录）高效得多。

### Q23: B
`addNewColumns` 模式下，Auto Loader 检测到新列后会**停止流**以更新 schema（写入 schemaLocation），然后需要重启流。这是为了确保 schema 变更被明确处理。注意：这不是静默添加，而是"检测→停止→更新→重启"。

### Q24: C
**Append 模式在有聚合但无 Watermark 时会报错**。原因：Append 要求输出"最终结果"，但没有 Watermark 时 Spark 无法判断聚合结果何时是最终的（可能还有更多数据到来改变结果）。Complete 和 Update 可以正常工作（Complete 每次输出全部结果，Update 输出变更的行）。

### Q25: C
Append 模式 + Watermark + 窗口聚合的输出时机：当 **Watermark 超过窗口结束时间**时输出。Watermark = max_event_time - threshold。窗口 `[12:00, 12:05)` 的结束时间是 12:05。Watermark 需要 > 12:05，即 max_event_time - 10min > 12:05，即 max_event_time > 12:15。

### Q26: A
Stream-Stream Join 的硬性要求：**两侧都必须有 Watermark + JOIN 条件中必须有时间范围约束**。这让 Spark 知道可以安全丢弃多旧的状态，防止无限增长。

### Q27: B
Stream-Static Join 中，静态表在**每个微批**都会被重新读取。这意味着如果静态表更新了，下一个微批就能看到最新数据。不需要 Watermark。

### Q28: B
`expect_or_drop` 行为：不满足条件的行被**丢弃**（不写入表），违规信息记录在 Pipeline Event Log 的 metrics 中。`expect`（无后缀）只记录不丢弃，`expect_or_fail` 会导致 Pipeline 失败。

### Q29: B
Gold 层通常包含复杂的聚合、JOIN、业务逻辑。这些操作需要**全量重算**才能保证结果正确（例如全部订单的月度汇总）。Streaming Table 是增量处理的，不适合这种需要全局视角的计算。Materialized View 在每次刷新时全量重算。

### Q30: D
`APPLY CHANGES INTO ... STORED AS SCD TYPE 2` 自动生成 `__START_AT`（生效开始时间）、`__END_AT`（生效结束时间，NULL 表示当前有效）。没有 `__IS_CURRENT` 字段——可以通过 `__END_AT IS NULL` 来判断是否是当前记录。也没有 `__CHANGE_TYPE`。

**更正**：实际上 DLT SCD Type 2 确实不生成 `__IS_CURRENT`，但也不生成 `__CHANGE_TYPE`。这里 D 是最佳答案，因为 `__CHANGE_TYPE` 不是 SCD Type 2 输出的一部分。

### Q31: C
`trigger(availableNow=True)` 是最佳选择：处理所有积压数据后自动停止，且将数据分成多个微批处理（可以记录中间进度）。配合 Lakeflow Jobs 的定时调度（每天凌晨 2 点触发），完美满足需求。`once=True` 已废弃且只用一个微批处理。

### Q32: B
核心区别：`once` 将所有积压数据塞进**一个**微批；`availableNow` 将积压数据分成**多个**微批，每个微批完成后更新 checkpoint。这意味着 `availableNow` 可以在中途失败后从最近的微批恢复，而 `once` 失败后必须全部重来。

### Q33: B
`foreachBatch` 中的操作不在 Spark Streaming 的事务保证范围内。第一次执行微批 5 时，Delta 写入已经提交成功。重试时 Delta 会再次写入，导致**重复数据**。这就是为什么 foreachBatch 中应使用 MERGE（幂等操作）而非 append。

### Q34: B
MERGE 基于主键匹配，是天然幂等的：重复执行同样的 MERGE，结果不变。而 append 不是幂等的：重复执行会产生重复行。在 foreachBatch 中，微批可能重试，所以幂等性至关重要。

### Q35: B
Watermark = max_event_time - threshold = 13:30 - 15min = **13:15**。不要重复减。只需要用公式计算一次。

### Q36: A
`OPTIMIZE` 命令会将小文件合并为更大的文件（默认目标大小 ~1GB）。这是 Delta Lake 原生的小文件治理方案。CTAS 也可以但更重量级，不是首选。

### Q37: D
AQE 的三大功能是：(1) 动态合并 Shuffle 分区，(2) 动态切换 Join 策略，(3) 动态优化 Skew Join。**不包括**动态调整 Executor 数量（那是 Dynamic Allocation 的功能，与 AQE 无关）。

### Q38: C
Liquid Clustering 是最佳选择：(1) 支持多列聚类，(2) 查询模式不固定时自动优化，(3) 增量聚类不需要手动 OPTIMIZE，(4) 支持高基数列。Z-Ordering 也可以但需要手动 OPTIMIZE 且不能动态修改聚类列。Partitioning 不适合多列灵活查询。

### Q39: B
`delta.autoOptimize.autoCompact = true` 会在写入操作后自动触发后台小文件合并，不需要停止流。这是流式场景下治理小文件的最佳实践。VACUUM 不合并文件，它只删除不再引用的旧文件。

### Q40: B
典型的**数据倾斜**症状：少数 Task 处理的数据量远大于其他 Task，导致运行时间极不均匀。解决方案：AQE Skew Join 或手动 Salting。

---

## 域 3：Data Modeling

### Q41: B
Bronze 层 = "原始数据保险柜"。核心职责是忠实保存原始数据，不做清洗或转换，只附加摄取元数据（如 ingestion timestamp、source 标识）。数据清洗和去重是 Silver 层的职责。

### Q42: B
**Multiplex** 模式：多个上游系统的数据写入同一个 Bronze 表，通常用 `event_type` 或 `source` 列区分来源。下游 Silver 层再按事件类型拆分。与之对比，**Singleplex** 是每个数据源一个 Bronze 表。

### Q43: B
Lakehouse 中存储成本极低（对象存储），Star Schema 的维度冗余不再是问题。而 Star Schema 的 JOIN 只需一层（事实表直接 JOIN 维度表），比 Snowflake Schema 的多层 JOIN 性能更好。

### Q44: A
`APPLY CHANGES INTO LIVE.target FROM STREAM(LIVE.source) KEYS (...) SEQUENCE BY (...) STORED AS SCD TYPE 2` 是 DLT 中实现 SCD Type 2 的声明式语法。

### Q45: B
SCD Type 1 = 直接 UPDATE 覆盖旧值，不保留历史（简单但丢失变更记录）。SCD Type 2 = 保留旧行（标记 end_date）+ 插入新行，完整保留变更历史。

### Q46: B
Bronze 层持续摄取原始数据 = 增量处理 = Streaming Table。Gold 层聚合报表 = 需要全量重算 = Materialized View。

### Q47: B
Development Mode 特点：(1) **复用集群**（快速迭代），(2) 失败不自动重试（方便调试）。Production Mode 特点：(1) **每次创建新集群**（环境干净），(2) 失败自动重试（保证可靠性）。

### Q48: B
三个 Expectations 是**独立评估**的。`expect("valid_id", ...)` 行为是"只记录，不丢弃、不失败"。所以 `id=NULL` 只会被记录为一个 violation metric，数据仍然写入表中。`valid_amount` 满足（amount=5 > 0），`valid_date` 满足（2025 >= 2020）。

### Q49: B
直接从 Bronze 跳到业务报表（跳过 Silver）是反模式。Bronze 的数据可能有重复、schema 不一致、脏数据等问题。Silver 层的清洗和标准化是必要的中间步骤。

### Q50: B
Products 表只有 5000 行，远低于默认的 Broadcast 阈值（10MB）。Spark 会自动选择 Broadcast Hash Join，将 products 表广播到所有 Executor，避免大规模 Shuffle。

### Q51: B
`APPLY CHANGES` 是 DLT 提供的声明式 CDC 处理语法，自动处理去重、排序、SCD 逻辑。`foreachBatch` + MERGE 需要手动编写所有逻辑（匹配条件、更新逻辑、幂等性保证等），灵活但更复杂。

### Q52: B
Multiplex 标准模式：一个 Bronze Streaming Table 摄取所有事件 → 多个 Silver Streaming Table 按 `event_type` 过滤。在 DLT 中，这非常自然：每个 Silver 表定义为 `SELECT * FROM STREAM(LIVE.bronze) WHERE event_type = 'xxx'`。

---

## 域 4：Security and Governance

### Q53: B
Unity Catalog 的权限模型是**三层递进**的：必须先有 `USE CATALOG`（进入 catalog 的权限）+ `USE SCHEMA`（进入 schema 的权限）+ 对象级权限（如 `SELECT`）。三者缺一不可。

### Q54: B
Dynamic View 可以使用 `CASE WHEN is_account_group_member('hr_team') THEN salary ELSE '***' END` 实现列级条件显示。也可以使用 UC 原生的 Column Mask 功能，但 Dynamic View 是更常见的考试答案。

### Q55: B
这是 UC 的核心区别：Managed Table = UC 管理数据生命周期，DROP 时数据和元数据都删除。External Table = UC 只管理元数据，DROP 时底层数据保留在原始存储位置。

### Q56: B
Delta Sharing 的 Open Sharing 协议允许与任何平台（不需要 Databricks）共享数据。创建 Recipient → 生成 activation link → 外部合作伙伴用任何支持 Delta Sharing 的客户端（如 pandas、Spark、Power BI）读取数据。

### Q57: B
Databricks 的安全机制：`dbutils.secrets.get()` 返回的值在 Notebook output 中自动显示为 `[REDACTED]`，防止 Secret 被意外打印到日志或屏幕上。这是自动的，不需要额外配置。

### Q58: B
GDPR 删除流程：(1) `DELETE` 在 Delta 的 transaction log 中标记记录为删除（逻辑删除），但物理数据文件仍然存在；(2) `VACUUM` 物理删除旧数据文件（物理删除）；(3) 保留审计记录证明合规。

---

## 域 5：Monitoring and Logging

### Q59: B
Shuffle 500GB 是异常信号。首先检查：(1) 是否有大表 JOIN 可以用 Broadcast 避免 Shuffle；(2) JOIN key 是否合理（是否触发了笛卡尔积或不必要的 Shuffle）。增加分区数或 Executor 只是治标不治本。

### Q60: B
DLT Pipeline 的数据质量结果（Expectations 的 passed/failed records）记录在 **Event Log** 中。使用 `event_log(TABLE(pipeline))` 函数查询。

### Q61: B
`system.billing.usage` 表记录了 DBU 消费明细，可以按 workspace、cluster、user 等维度分析计费。

### Q62: B
数据倾斜的典型特征：某些 Task 的 **Shuffle Read Size 或 Records 数量远大于其他 Task**（通常 10x+），导致这些 Task 的运行时间远长于其他 Task，成为整个 Stage 的瓶颈。

### Q63: B
SQL Alert 不是实时的。它基于**调度**运行 SQL 查询（如每 5 分钟、每小时），在调度触发时执行查询，如果结果满足条件则发送通知（支持邮件、Slack、Webhook 等）。

### Q64: B
"Spill to disk" 表示 JOIN（或聚合）的中间数据超过了 Executor 的可用内存，被迫写入本地磁盘。磁盘 I/O 比内存慢得多，会显著降低性能。优化方向：增加 Executor 内存、减少数据量、或优化 JOIN 策略。

---

## 域 6：Testing and Deployment

### Q65: B
本地单元测试使用 `SparkSession.builder.master("local[*]")` 创建本地 SparkSession，不需要连接远程集群。`local[*]` 使用本地所有 CPU 核心。这是 PySpark 单元测试的标准做法。

### Q66: B
测试金字塔：底层是大量**快速、廉价**的 Unit 测试 → 中间是适量 Integration 测试 → 顶层是少量**慢速、昂贵**的 E2E 测试。大部分 bug 应该被 Unit 测试捕获。

### Q67: B
正确的 CI/CD 流程：validate（语法检查）→ 运行测试（确保代码正确）→ Code Review（人工审查）→ 部署到 Production。永远不应该先部署再测试。

### Q68: B
Repair Run 会创建**新的 Job Cluster** 来运行失败的 Task。原来的 Job Cluster 在原始 run 结束后已经终止了，不可能复用。

### Q69: B
Production 环境最佳实践：使用 **Service Principal** 作为 `run_as`（避免依赖个人账号）+ 使用专用的 `prod_catalog` + 限制部署权限（只有 CI/CD 管道或特定人员可以部署）。

### Q70: C
Databricks Git Folders 支持 pull、push、创建/切换 branch、查看 commit 历史，但**不支持直接在 UI 中解决 merge conflict**。Merge conflict 需要在本地 Git 客户端或 GitHub/GitLab 上解决后再 pull。

---

## 评分标准

| 域 | 题号 | 满分 |
|----|------|------|
| 域 1: Tooling | Q1-Q12 | 12 |
| 域 2: Data Processing | Q13-Q40 | 18 (Q13-Q30 计分) |
| 域 3: Data Modeling | Q41-Q52 | 12 |
| 域 4: Security | Q53-Q58 | 6 |
| 域 5: Monitoring | Q59-Q64 | 6 |
| 域 6: Testing | Q65-Q70 | 6 |
| **总计** | | **60** |

> 目标：54/60（90%）以上
>
> 如果错题集中在某个域，说明该域需要重点强化。
