# Databricks Data Engineer Professional — 深度诊断分析报告

> 基于 327 题完整作答数据 | 生成日期：2026-03-14

---

## A. 执行摘要（Executive Diagnostic Summary）

**总分：206/327 = 63.0%** — 低于通过线（通常 70-72%），需要系统性补强。

### 关键发现

1. **成绩极度不稳定**：从 10%（Q1-Q10, Q291-Q300）到 100%（Q151-Q160, Q191-Q200, Q271-Q280），波动幅度巨大。这不是"差一点就过"的状态，而是知识结构存在严重断层。

2. **前后半程无进步**：Q1-Q160 正确率 63.1%，Q161-Q327 正确率 62.9%。刷题过程中没有产生有效的学习迁移——你在重复犯同类错误。

3. **灾难性批次暴露核心盲区**：
   - Q1-Q10（10%）：Databricks 平台基础操作（widgets、权限、secrets、Repos）几乎全军覆没
   - Q101-Q110（30%）：Spark UI、Auto Loader、Delta 优化特性混淆
   - Q291-Q300（10%）：Delta Sharing、Lakeflow、Asset Bundles 等新特性完全不熟
   - Q311-Q320（30%）：性能调优、数据治理、Git 协作、Jobs API 综合应用薄弱

4. **11 道题未作答**：多选题和新特性题直接放弃，说明对部分考试领域完全没有知识储备。

5. **错误的 121 题中，大量标记为"待补充"**：这些题的解析在批改过程中未完成补充，缺少错因分析和知识点归纳。这意味着这部分错题的学习闭环尚未形成——错了但没有复盘，下次遇到同类题仍然可能犯同样的错误。这是批改流程的遗漏，需要补完。

### 诊断结论

你的知识状态是**岛屿式的**：在某些特定主题上有扎实理解（Q121-Q160 连续高分区间），但在平台操作、性能调优、数据治理、新特性等领域存在大面积空白。这不是通过"多刷题"能解决的，需要针对性地重建知识结构。

---

## B. 按主题的表现分解（Performance Breakdown by Topic）

### 错题主题分布（按错题数量排序）

| 主题 | 错题数 | 占总错题% | 诊断级别 |
|------|--------|----------|---------|
| Delta Lake 核心机制 | 60 | 49.6% | 🔴 严重薄弱 |
| Jobs/Workflows 编排 | 57 | 47.1% | 🔴 严重薄弱 |
| Python/Notebook 操作 | 49 | 40.5% | 🔴 严重薄弱 |
| Unity Catalog/数据治理 | 48 | 39.7% | 🔴 严重薄弱 |
| 性能优化/Spark 调优 | 37 | 30.6% | 🟠 明显薄弱 |
| Structured Streaming | 34 | 28.1% | 🟠 明显薄弱 |
| 集群管理 | 28 | 23.1% | 🟡 需要加强 |
| Repos/Git 操作 | 24 | 19.8% | 🟡 需要加强 |
| 表管理（Managed/External） | 22 | 18.2% | 🟡 需要加强 |
| Change Data Feed/CDC | 19 | 15.7% | 🟡 需要加强 |
| Databricks CLI/API | 16 | 13.2% | 🟡 需要加强 |
| Auto Loader | 15 | 12.4% | 🟡 需要加强 |
| Medallion Architecture | 15 | 12.4% | 🟡 需要加强 |
| Security/Secrets | 10 | 8.3% | 🟢 局部补强 |
| DLT/Lakeflow | 9 | 7.4% | 🟢 局部补强 |
| MLflow | 7 | 5.8% | 🟢 局部补强 |
| Asset Bundles | 3 | 2.5% | 🟢 局部补强 |

### 知识状态分类

| 状态 | 主题 | 证据 |
|------|------|------|
| **真正掌握** | Structured Streaming 基础调度配置、基本 Delta 读写、简单 SQL 查询 | Q121-Q160 连续高分，Q191-Q200 满分 |
| **浅层熟悉** | Delta Lake 概念（知道名词但不理解行为）、Streaming 概念（知道 checkpoint 但不理解细节） | 简单题能对，变体题就错 |
| **不稳定知识** | Auto Compaction vs Optimized Writes、Managed vs External Table、CDF 行为 | 同类题有时对有时错（Q34/Q79 都是 External Table 题，都错） |
| **反复误解** | dbutils API 体系、权限层级、Delta Log 机制、Spark UI 诊断 | Q1(widgets)、Q6(secrets)、Q10(data skipping)、Q51/Q220(Spark UI) 反复出错 |
| **概念混淆** | CTAS vs VIEW、append vs merge 语义、streaming vs batch 处理模型 | Q16(CTAS)、Q28(CDF+append)、Q30(readStream) |
| **程序性弱点** | Jobs API 调用流程、CLI 命令、REST API endpoint 选择 | Q12、Q57、Q115、Q117、Q232、Q317 |
| **实践经验缺口** | Spark UI 性能诊断、集群配置选择、生产环境调优 | Q26、Q49、Q51、Q85、Q101、Q220、Q311 |
| **考试策略问题** | 多选题放弃、新特性题猜测、"最小权限"类题目选过度方案 | 11题未答、Q2选B(过度)、Q291-Q300 全面崩溃 |


---

## C. 错误根因分析（Root-Cause Analysis of Mistakes）

### 根因 1：Delta Lake 内部机制理解停留在表面

**涉及题目**：Q10, Q16, Q22, Q36, Q64, Q75, Q107, Q233, Q248, Q287, Q294, Q319, Q320, Q327

**你可能知道的**：Delta Lake 是基于 Parquet 的事务性存储层，支持 ACID、Time Travel、Schema Evolution。

**你可能混淆的**：
- Data Skipping 的工作原理：你在 Q10 中不理解 Delta Log 中的文件级统计信息只对前 32 列有效，且只在非分区列上通过 min/max 统计跳过文件，而不是跳过行
- Auto Compaction vs Optimized Writes vs OPTIMIZE：Q22 和 Q107 暴露你把这三个概念混为一谈。Auto Compaction 是写后异步合并小文件（目标 128MB），Optimized Writes 是写时优化分区内文件大小，OPTIMIZE 是手动命令（目标 1GB）
- Liquid Clustering 的写入行为：Q233 说明你不清楚哪些写入操作支持 cluster-on-write
- Deletion Vectors 的行为：Q327 说明你不理解 deletion vectors 是软删除标记而非物理重写

**根本问题**：你把 Delta Lake 当作一个"更好的 Parquet"来理解，而没有建立 Transaction Log 驱动一切的心智模型。Delta 的每一个特性（data skipping、time travel、CDF、compaction）都是 Transaction Log 的衍生行为，不理解 log 就无法推理这些特性。

### 根因 2：Structured Streaming 的执行模型不清晰

**涉及题目**：Q20, Q21, Q30, Q72, Q139, Q233, Q268, Q289, Q296, Q302, Q320

**你可能知道的**：Streaming 用 readStream/writeStream，有 checkpoint，有 trigger。

**你可能不理解的**：
- Checkpoint 是每个 streaming query 独占的（Q20：两个 stream 不能共享 checkpoint 目录）
- Trigger interval 不是"处理速度"，而是"检查新数据的频率"（Q21：interval 太短不会加速处理，反而增加开销）
- readStream.table() 默认读取 Delta 表的 append-only 变更，不是全量重读（Q30）
- Schema 变更对运行中的 streaming job 的影响（Q72：需要重启 stream）
- Streaming 和 batch 在 Delta 上的交互模型（Q320：enableChangeDataFeed 不是 streaming 追踪已处理数据的机制）

**根本问题**：你缺少"微批处理循环"的心智模型。每个 microbatch 是一个独立的 Spark job，checkpoint 记录的是 offset（已处理到哪里），trigger 控制的是循环频率。不理解这个循环，就无法推理 streaming 的任何行为。

### 根因 3：Unity Catalog 权限模型和数据治理体系混乱

**涉及题目**：Q2, Q33, Q45, Q64, Q82, Q84, Q98, Q100, Q230, Q259, Q264, Q291, Q303, Q315

**你可能知道的**：Unity Catalog 有 catalog > schema > table 的层级结构。

**你可能不理解的**：
- 权限继承和最小权限原则：Q2 你选了过度权限（Admin），Q303 你不清楚 MANAGE 权限的含义
- Managed vs External table 在 Unity Catalog 下的行为差异（Q45, Q64）
- USE CATALOG + USE SCHEMA 是访问的前提条件（Q230, Q259）
- Delta Sharing 的权限模型（Q264, Q299, Q300）
- 列级安全（column masking）的实现方式（Q315）

**根本问题**：你没有建立 Unity Catalog 的"权限传播"心智模型。UC 的权限是层级式的：要访问 table，必须先有 catalog 的 USE CATALOG 和 schema 的 USE SCHEMA。每一层都是独立的权限检查点。

### 根因 4：Databricks 平台操作 API 不熟悉

**涉及题目**：Q1, Q6, Q12, Q46, Q54, Q57, Q68, Q102, Q111, Q115, Q117, Q174, Q175, Q221, Q232, Q235, Q260, Q263, Q295, Q317, Q325

**根本问题**：这是纯粹的平台操作经验缺口。你可能主要通过 UI 使用 Databricks，很少用 API/CLI 进行自动化操作。考试会大量考察这些"工程师日常操作"。

### 根因 5：性能调优缺乏实战经验

**涉及题目**：Q26, Q49, Q51, Q66, Q70, Q85, Q101, Q220, Q244, Q311

**根本问题**：你没有实际做过 Spark 性能调优。这些知识无法通过阅读获得，必须在 Spark UI 中实际观察 job 执行过程。

### 根因 6：Change Data Feed / CDC 处理模式混淆

**涉及题目**：Q28, Q75, Q97, Q113, Q147, Q296

**根本问题**：你把 CDF 理解为"变更日志"，但没有理解如何正确消费这个日志来构建下游表。CDF 输出的 _change_type 列（insert, update_preimage, update_postimage, delete）需要配合 MERGE INTO 才能正确应用到目标表。

### 根因 7：新特性知识空白（Delta Sharing, Lakeflow, Asset Bundles）

**涉及题目**：Q249, Q263, Q268, Q291, Q296, Q299, Q300, Q302, Q325

Q291-Q300 的 10% 正确率是最强的信号：这些题集中在 Delta Sharing、Lakeflow Declarative Pipelines、Terraform + Service Principal 等较新的考试内容上，你几乎完全没有准备。

---

## D. 优先级排序的知识缺口清单（Ranked Priority List）

| 优先级 | 知识领域 | 错题数 | 考试权重 | 理由 |
|--------|---------|--------|---------|------|
| **P0** | Delta Lake 内部机制 | 60 | 极高 | 几乎一半错题涉及，是所有其他主题的基础 |
| **P0** | Unity Catalog 权限模型 | 48 | 极高 | 考试重点，且影响 Delta Sharing、DLT、表管理等多个领域 |
| **P1** | Structured Streaming 执行模型 | 34 | 高 | 考试必考，且与 Delta Lake、Auto Loader、DLT 深度耦合 |
| **P1** | 性能调优与 Spark UI | 37 | 高 | 考试常考，且你完全缺乏实战经验 |
| **P1** | Jobs/Workflows API | 57 | 高 | 错题最多的领域之一，但很多是记忆性知识，补起来快 |
| **P2** | Delta Sharing（新特性） | ~10 | 中高 | Q291-Q300 暴露完全空白，考试必考 |
| **P2** | DLT/Lakeflow Declarative Pipelines | 9 | 中高 | 考试重点，你只有表面了解 |
| **P2** | Auto Loader 模式与配置 | 15 | 中 | 与 Streaming 耦合，需要理解 directory listing vs notification 模式 |
| **P2** | Change Data Feed / CDC 模式 | 19 | 中 | 与 Delta Lake 和 Streaming 耦合 |
| **P3** | Databricks CLI/REST API | 16 | 中 | 记忆性知识，集中背诵即可 |
| **P3** | Managed vs External Table | 22 | 中 | 概念清晰后不难，但你反复错 |
| **P3** | Asset Bundles | 3 | 低-中 | 新特性，题量不大但必须了解基本概念 |
| **P3** | Python/Notebook 操作细节 | 49 | 中 | 很多是平台操作记忆题 |

### 每个 P0/P1 领域的必须掌握子主题

**P0 - Delta Lake 内部机制：**
- [ ] Transaction Log 结构（JSON + Parquet checkpoint）
- [ ] Data Skipping：文件级 min/max 统计、前 32 列限制、dataSkippingNumIndexedCols
- [ ] Auto Compaction（128MB 目标）vs Optimized Writes vs OPTIMIZE（1GB 目标）
- [ ] Liquid Clustering vs Z-ORDER vs Partitioning 的适用场景和限制
- [ ] Deletion Vectors：软删除标记、REORG TABLE 物理清理
- [ ] VACUUM 行为：只删除不在 transaction log 中引用的文件、默认 7 天保留
- [ ] Time Travel：版本号 vs 时间戳、与 VACUUM 的交互
- [ ] Schema Evolution：mergeSchema vs overwriteSchema
- [ ] CHECK Constraints：添加时现有数据必须合规
- [ ] Delta table properties：deletedFileRetentionDuration、logRetentionDuration

**P0 - Unity Catalog 权限模型：**
- [ ] 三级命名空间：catalog.schema.table
- [ ] 权限层级：USE CATALOG → USE SCHEMA → SELECT/MODIFY
- [ ] MANAGE vs OWNERSHIP vs ALL PRIVILEGES
- [ ] Managed vs External table 在 UC 下的行为
- [ ] External Location 和 Storage Credential
- [ ] Column Masking 和 Row Filter
- [ ] Delta Sharing：provider/recipient/share 模型
- [ ] Databricks-to-Databricks sharing vs open sharing

**P1 - Structured Streaming：**
- [ ] Microbatch 执行循环模型
- [ ] Checkpoint 机制：每个 query 独占、记录 offset
- [ ] Trigger 类型：processingTime、availableNow、once
- [ ] readStream.table() 的增量语义
- [ ] Stream-static join 行为
- [ ] Watermark 和 late data 处理
- [ ] foreachBatch 模式

**P1 - 性能调优：**
- [ ] Spark UI 各 tab 的用途：Jobs、Stages、SQL、Storage、Executors
- [ ] SQL tab 查看 query plan（Physical Plan）
- [ ] Disk spill 诊断和解决
- [ ] Shuffle 优化
- [ ] %sh 在 driver 执行的限制
- [ ] 集群配置：memory-optimized vs compute-optimized 的选择

**P1 - Jobs/Workflows：**
- [ ] Jobs API 2.0/2.1 endpoint 功能划分
- [ ] jobs/create vs jobs/run-now vs runs/submit
- [ ] Multi-task job 的 task 依赖
- [ ] Job 参数传递（dbutils.widgets）
- [ ] Repair/rerun 失败的 job run
- [ ] Job run history 保留策略（60 天）

---

## E. 每个薄弱领域的深度讲解（Detailed Explanation of Each Weak Topic）

### E1. Delta Lake Transaction Log — 一切的基础

**核心概念**：Delta Lake 的每一次写操作都不是直接修改数据文件，而是在 `_delta_log/` 目录中写入一个新的 JSON 文件（commit）。这个 JSON 文件记录了"哪些文件被添加（add）"和"哪些文件被移除（remove）"。表的当前状态 = 重放所有 commit 的结果。

**正确心智模型**：
```
写操作 → 写新 Parquet 文件 + 写 _delta_log/N.json
读操作 → 读 _delta_log → 确定当前有效文件列表 → 只读这些文件
VACUUM → 删除不在有效文件列表中且超过保留期的物理文件
Time Travel → 读历史版本的 _delta_log → 得到那个时间点的文件列表
Data Skipping → 读 _delta_log 中每个文件的 min/max 统计 → 跳过不可能包含目标数据的文件
```

**Data Skipping 详解（Q10, Q36 错误的根源）**：
- Delta Log 中为每个 Parquet 文件记录了列级统计信息（min, max, null count）
- 默认只对前 32 列收集统计（可通过 `dataSkippingNumIndexedCols` 调整）
- 查询时，Spark 用 WHERE 条件与文件统计比较，跳过整个文件（不是跳过行）
- 分区列通过分区裁剪（partition pruning）处理，不走 data skipping
- 非分区列才走 data skipping，且只有 min/max 有效的列（数值、日期、短字符串）才有效

**Auto Compaction vs Optimized Writes vs OPTIMIZE（Q22, Q107 错误的根源）**：

| 特性 | 触发时机 | 目标文件大小 | 行为 |
|------|---------|------------|------|
| **Optimized Writes** | 写入时 | 自适应 | 写入前重新平衡分区内数据，减少小文件产生 |
| **Auto Compaction** | 写入后（异步） | 128 MB | 检测小文件，合并到 128MB |
| **OPTIMIZE** | 手动执行 | 1 GB | 合并所有小文件到 ~1GB，可选 Z-ORDER |
| **Predictive Optimization** | 自动（UC managed tables） | 自适应 | 自动运行 OPTIMIZE 和 VACUUM |

关键区别：Optimized Writes 是"写时优化"，Auto Compaction 是"写后优化"，OPTIMIZE 是"手动优化"。三者可以同时启用，不冲突。

**Liquid Clustering vs Z-ORDER vs Partitioning（Q233, Q238 错误的根源）**：

| 特性 | 适用场景 | 限制 | 写入行为 |
|------|---------|------|---------|
| **Partitioning** | 低基数列（日期、地区） | 高基数会产生大量小文件 | 按分区值物理分目录 |
| **Z-ORDER** | 中高基数列 | 需要手动 OPTIMIZE，不支持 streaming | 在 OPTIMIZE 时重排数据 |
| **Liquid Clustering** | 任意基数列 | 不支持 CTAS/RTAS 的 cluster-on-write | 增量聚类，支持 streaming 写入 |

Q233 的关键：Liquid Clustering 的 cluster-on-write 不支持 CTAS（CREATE TABLE AS SELECT）和 RTAS（REPLACE TABLE AS SELECT），因为这些是一次性批量操作，不经过增量写入路径。

**Deletion Vectors（Q327 错误的根源）**：
- 启用 deletion vectors 后，DELETE/UPDATE 不会重写整个 Parquet 文件
- 而是在 Delta Log 中记录一个"删除向量"，标记哪些行被删除
- 读取时，Spark 会过滤掉被标记的行
- 物理清理需要运行 `REORG TABLE ... APPLY (PURGE)` 或等待 Predictive Optimization
- 好处：大幅减少 DELETE/UPDATE 的写放大

### E2. Structured Streaming 微批处理模型

**核心概念**：Structured Streaming 是一个无限循环，每次循环处理一个"微批次"（microbatch）。

**正确心智模型**：
```
while True:
    wait_for_trigger_interval()          # Trigger 控制等待时间
    new_data = check_source_for_new()    # 检查源是否有新数据
    if new_data:
        result = process(new_data)       # 处理新数据（一个 Spark job）
        write_to_sink(result)            # 写入目标
        update_checkpoint(offset)        # 记录处理到哪里了
```

**Checkpoint 详解（Q20 错误的根源）**：
- 每个 streaming query 必须有自己独立的 checkpoint 目录
- Checkpoint 记录：source offset（处理到哪里）、sink commit log（写了什么）、state（有状态操作的中间状态）
- 两个 stream 共享 checkpoint = 两个人共用一个书签，会互相覆盖进度
- Checkpoint 也是 exactly-once 语义的保证机制

**Trigger 类型（Q21, Q139 错误的根源）**：
- `processingTime("10 seconds")`：每 10 秒检查一次新数据。如果处理时间 > 10秒，下一个 batch 立即开始
- `availableNow`：处理所有当前可用数据，然后停止。适合"追赶"场景
- `once`：处理一个 batch 然后停止（已废弃，用 availableNow 替代）
- Q21 的关键：peak hours 时 microbatch 处理变慢，不是因为 trigger interval 太短，而是因为数据量增大。增大 trigger interval 只会让数据积压更严重。正确做法是增加集群资源或优化处理逻辑。
- Q139 的关键：trigger interval 太短导致大量空 microbatch（0 records），每个空 batch 仍有开销（检查源、写 checkpoint），浪费资源和存储成本。

**readStream.table() 的增量语义（Q30 错误的根源）**：
- `spark.readStream.table("my_delta_table")` 默认只读取新增的行（append-only）
- 它通过 Delta Log 的版本号追踪"上次读到哪里"
- 如果源表有 UPDATE/DELETE，默认会报错（因为 append-only 模式不支持）
- 要读取变更，需要启用 CDF：`readStream.option("readChangeFeed", "true").table(...)`

### E3. Unity Catalog 权限体系

**核心概念**：Unity Catalog 是 Databricks 的统一数据治理层，管理所有数据资产的访问控制。

**正确心智模型 — 权限检查链**：
```
用户请求 SELECT catalog_a.schema_b.table_c
  → 检查 1：用户是否有 catalog_a 的 USE CATALOG？ → 否则拒绝
  → 检查 2：用户是否有 schema_b 的 USE SCHEMA？ → 否则拒绝
  → 检查 3：用户是否有 table_c 的 SELECT？ → 否则拒绝
  → 全部通过 → 执行查询
```

**关键权限对比**：

| 权限 | 作用 | 适用对象 |
|------|------|---------|
| USE CATALOG | 允许访问 catalog 下的 schema | Catalog |
| USE SCHEMA | 允许访问 schema 下的对象 | Schema |
| SELECT | 读取表/视图数据 | Table/View |
| MODIFY | 写入表数据（INSERT/UPDATE/DELETE） | Table |
| CREATE TABLE/SCHEMA | 在父对象下创建子对象 | Schema/Catalog |
| ALL PRIVILEGES | 所有权限（不含 OWNERSHIP） | 任意对象 |
| MANAGE | 管理权限（可以 GRANT 给他人） | 任意对象 |
| OWNERSHIP | 完全控制（含删除、转移所有权） | 任意对象 |

**Q230/Q259 错误的根源**：新团队成员能在 default schema 创建表但不能访问其他 schema，是因为 workspace catalog 的 default schema 自动授予了 USE SCHEMA，但其他 schema 没有。需要显式 GRANT USE SCHEMA。

**Managed vs External Table（Q45, Q64 错误的根源）**：

| 特性 | Managed Table | External Table |
|------|--------------|----------------|
| 数据位置 | UC 管理的存储 | 用户指定的 External Location |
| DROP TABLE | 删除元数据 + 数据文件 | 只删除元数据，数据文件保留 |
| 创建方式 | 不指定 LOCATION | 指定 LOCATION |
| Predictive Optimization | 支持 | 不支持 |

### E4. Spark UI 性能诊断

**核心概念**：Spark UI 是诊断 Spark 作业性能问题的主要工具。

**各 Tab 用途（Q51, Q220, Q244 错误的根源）**：

| Tab | 看什么 | 典型诊断场景 |
|-----|--------|------------|
| **Jobs** | 所有 job 的执行时间和状态 | 找到最慢的 job |
| **Stages** | 每个 stage 的 task 分布、shuffle 数据量 | 诊断数据倾斜、shuffle 过大 |
| **SQL** | 查询执行计划（Physical Plan） | 诊断 predicate push-down、join 策略 |
| **Storage** | 缓存的 RDD/DataFrame | 诊断缓存效率 |
| **Executors** | 每个 executor 的资源使用 | 诊断内存不足、GC 问题 |

**Q51/Q220 的关键**：要诊断 predicate push-down 是否生效，应该看 SQL tab 的查询计划（Physical Plan），在 Scan 节点中查看 PushedFilters。不是看 Stage 的 Input Size，也不是看 Executor 日志。

**Disk Spill 诊断（Q311 错误的根源）**：
- Disk spill = 内存不够，数据溢出到磁盘
- 在 Stages tab 中可以看到 Spill (Memory) 和 Spill (Disk) 指标
- 解决方案：(1) 使用 memory-optimized 实例（更高的 core:memory 比）(2) 增加 executor 内存 (3) 减少 shuffle 分区数 (4) 优化查询减少中间数据量

### E5. Delta Sharing

**核心概念**：Delta Sharing 是一个开放协议，允许安全地共享 Delta 表数据，无需复制数据。

**两种模式**：
- **Databricks-to-Databricks**：共享方和接收方都在 Databricks 上。接收方可以直接读取共享数据，支持 time travel 和 streaming。性能最优。
- **Open Sharing**：接收方不在 Databricks 上。通过 REST API 访问，功能受限（不支持 time travel、streaming）。

**权限模型**：
```
Provider（数据提供方）
  → 创建 Share（共享单元）
  → 将 Table/Schema 添加到 Share
  → 创建 Recipient（接收方）
  → 将 Share 授权给 Recipient

Recipient（数据接收方）
  → 创建 Catalog from Share
  → 通过 catalog.schema.table 访问数据
```

**Q299 的关键**：Databricks-to-Databricks sharing 支持 time travel 和 streaming reads，但需要启用 `delta_sharing.enable_streaming` 和使用 shared catalog。

### E6. DLT / Lakeflow Declarative Pipelines

**核心概念**：DLT（现在叫 Lakeflow Declarative Pipelines）是声明式的数据管道框架。你定义"数据应该是什么样"，DLT 负责"如何实现"。

**关键概念**：
- **LIVE TABLE**：物化表，每次 pipeline 运行时全量重算
- **STREAMING LIVE TABLE**：增量表，只处理新数据
- **Expectations**：数据质量约束，可以 warn、drop、fail
- **APPLY CHANGES INTO**：CDC 处理，自动处理 SCD Type 1/2

**Q218 的关键**：DLT expectations 只能验证单表内的数据质量规则，不能跨表验证（如"report 表包含 validation_copy 的所有记录"）。跨表验证需要在 pipeline 外部实现。

**Q302 的关键**：DLT 和 Structured Streaming 的区别不是"DLT 自动处理 schema evolution"（两者都可以），而是 DLT 自动管理 checkpoint、集群、重试、依赖关系等运维层面的事情。

### E7. Jobs API 和 Databricks CLI

**关键 API Endpoint 对比（Q12, Q57, Q115, Q235, Q317 错误的根源）**：

| Endpoint | 功能 | 幂等性 |
|----------|------|--------|
| `jobs/create` | 创建 job 定义 | 不幂等！多次调用创建多个 job |
| `jobs/run-now` | 触发已有 job 的一次运行 | 每次调用创建新 run |
| `runs/submit` | 一次性运行（不创建 job 定义） | 每次调用创建新 run |
| `jobs/list` | 列出所有 job 定义 | 只读 |
| `jobs/get` | 获取 job 定义详情（含 task 配置） | 只读 |
| `runs/list` | 列出 job 的运行历史 | 只读 |
| `runs/get` | 获取单次运行详情 | 只读 |
| `runs/get-output` | 获取运行输出 | 只读 |
| `runs/repair` | 重跑失败的 task | 修改 run 状态 |

**Q12 的关键**：`jobs/create` 不是幂等的。调用 3 次会创建 3 个独立的 job 定义（即使配置完全相同）。

**Q317 的关键**：要自动化监控和恢复失败 job，正确流程是：`jobs/list` → `runs/list`（找到失败的 run）→ `runs/repair`（重跑失败的 task）。

**Databricks CLI 常用命令（Q68, Q221, Q232 错误的根源）**：
- `databricks fs cp` — 上传文件到 DBFS
- `databricks pipelines get` — 获取 DLT pipeline 配置
- `databricks clusters create` — 创建集群（需要 JSON 配置）
- `databricks secrets create-scope` / `put-secret` — 管理 secrets


---

## F. 每个薄弱领域的最佳学习方法（Best Study Method）

| 领域 | 推荐方法 | 为什么这个方法有效 |
|------|---------|------------------|
| **Delta Lake 内部机制** | 画 Transaction Log 流程图 + 在 Databricks 中实际查看 `_delta_log/` 目录内容 | 你的问题是心智模型缺失，不是记忆不足。必须亲眼看到 log 文件才能建立直觉 |
| **Unity Catalog 权限** | 制作权限层级对比表 + 在 Databricks 中实际执行 GRANT/REVOKE 并验证效果 | 权限是层级传播的，只有实际操作才能理解"缺少 USE SCHEMA 会怎样" |
| **Structured Streaming** | 写一个最小 streaming pipeline，观察 checkpoint 目录内容，尝试不同 trigger | 你的问题是不理解执行循环，必须看到 checkpoint 文件的变化 |
| **Spark UI 性能调优** | 在 Databricks 中运行有性能问题的查询，逐个 tab 分析 Spark UI | 这是纯实战技能，无法通过阅读获得 |
| **Jobs API** | 制作 API endpoint 功能对比表 + 用 curl/Postman 实际调用几个关键 endpoint | 记忆性知识，对比表最高效 |
| **Delta Sharing** | 阅读官方文档 + 画 Provider/Recipient/Share 关系图 | 概念性知识，需要建立正确的关系模型 |
| **DLT/Lakeflow** | 在 Databricks 中创建一个包含 expectations 的 DLT pipeline | 你需要理解声明式 vs 命令式的区别 |
| **Auto Loader** | 在 Databricks 中用 cloudFiles 读取一个目录，观察 directory listing vs notification 模式 | 需要理解两种模式的触发机制差异 |
| **CDF/CDC** | 写一个完整的 CDF -> MERGE INTO 目标表的 pipeline | 你需要理解 _change_type 的每种值如何映射到 MERGE 操作 |
| **CLI/平台操作** | 制作 Flashcard 卡片，每天复习 10 分钟 | 纯记忆性知识，间隔重复最高效 |
| **Managed vs External Table** | 制作对比表，在 Databricks 中分别创建两种表然后 DROP，观察数据文件是否被删除 | 一次实验胜过十次阅读 |
| **Asset Bundles** | 阅读官方文档的 Getting Started，理解 YAML 结构 | 新特性，需要基础概念建立 |

---

## G. 动手实践计划（Hands-on Databricks Practice Plan）

### 实验 1：Delta Lake Transaction Log 探索（2 小时）

**做什么**：
1. 创建一个 Delta 表，插入数据，然后用 `dbutils.fs.ls("path/_delta_log/")` 查看 log 文件
2. 读取 JSON log 文件内容，观察 add/remove 操作
3. 执行 UPDATE，观察新的 log 文件
4. 执行 VACUUM，观察哪些文件被删除
5. 尝试 Time Travel 到 VACUUM 前后的版本

**观察什么**：每次写操作产生一个新的 JSON 文件；UPDATE 实际上是 remove 旧文件 + add 新文件；VACUUM 只删除不在当前版本引用的文件；Time Travel 到被 VACUUM 清理的版本会报错。

**走出时应理解**：Delta Lake 的一切行为都是 Transaction Log 的衍生。

### 实验 2：Data Skipping 验证（1 小时）

**做什么**：
1. 创建一个有 40 列的 Delta 表
2. 在第 1 列和第 35 列上分别执行带 WHERE 的查询
3. 在 Spark UI 的 SQL tab 中查看 Physical Plan
4. 对比两个查询的 files pruned 指标

**观察什么**：第 1 列的查询会显示 files pruned > 0（data skipping 生效）；第 35 列的查询不会有 files pruned（超出前 32 列限制）。

### 实验 3：Structured Streaming Checkpoint 探索（2 小时）

**做什么**：
1. 创建一个简单的 streaming pipeline（readStream -> writeStream）
2. 查看 checkpoint 目录结构（offsets/、commits/、state/）
3. 停止 stream，添加新数据，重启 stream，观察是否从上次位置继续
4. 尝试两个 stream 共享同一个 checkpoint 目录，观察报错
5. 测试不同 trigger 类型的行为差异

### 实验 4：Unity Catalog 权限实验（1.5 小时）

**做什么**：
1. 创建一个 catalog 和 schema
2. 创建一个测试用户/组
3. 只授予 SELECT on table，不授予 USE CATALOG/USE SCHEMA，观察报错
4. 逐步添加 USE CATALOG、USE SCHEMA，观察何时能访问
5. 测试 Managed vs External table 的 DROP 行为

### 实验 5：Spark UI 性能诊断（2 小时）

**做什么**：
1. 写一个有 shuffle 的查询（大表 JOIN），在 Spark UI 中观察
2. 写一个会产生 disk spill 的查询（在小内存集群上处理大数据）
3. 在 SQL tab 中查看 query plan，找到 predicate push-down 的证据
4. 对比有无 cache 的查询在 Storage tab 中的表现

### 实验 6：CDF + MERGE INTO Pipeline（1.5 小时）

**做什么**：
1. 创建一个启用 CDF 的源表
2. 对源表执行 INSERT、UPDATE、DELETE
3. 用 table_changes() 函数读取 CDF 输出
4. 观察 _change_type、_commit_version、_commit_timestamp 列
5. 写一个 MERGE INTO 语句，将 CDF 变更正确应用到目标表

### 实验 7：DLT Pipeline with Expectations（1 小时）

**做什么**：
1. 创建一个简单的 DLT pipeline（bronze -> silver -> gold）
2. 在 silver 层添加 expectations
3. 插入一些不合规的数据，观察 warn/drop/fail 的不同行为
4. 查看 DLT 的 event log


---

## H. 14 天补救路线图（14-Day Remediation Roadmap）

### 第 1-3 天：P0 基础重建

**Day 1：Delta Lake Transaction Log + Data Skipping**
- 上午：阅读 Delta Lake 官方文档的 How Delta Lake Works 章节
- 下午：完成实验 1（Transaction Log 探索）和实验 2（Data Skipping 验证）
- 晚上：制作 Auto Compaction / Optimized Writes / OPTIMIZE 对比表
- 复习错题：Q10, Q22, Q36, Q107, Q233, Q248, Q287, Q294, Q327

**Day 2：Delta Lake 高级特性**
- 上午：学习 Liquid Clustering、Deletion Vectors、Predictive Optimization
- 下午：学习 VACUUM 行为、Time Travel、Schema Evolution
- 晚上：制作 Liquid Clustering vs Z-ORDER vs Partitioning 对比表
- 复习错题：Q16, Q64, Q75, Q319, Q320

**Day 3：Unity Catalog 权限模型**
- 上午：阅读 Unity Catalog 官方文档的权限章节
- 下午：完成实验 4（权限实验）
- 晚上：制作权限层级图和 Managed vs External 对比表
- 复习错题：Q2, Q33, Q45, Q82, Q84, Q98, Q230, Q259, Q303, Q315

### 第 4-6 天：P1 核心技能

**Day 4：Structured Streaming 执行模型**
- 上午：阅读 Structured Streaming Programming Guide
- 下午：完成实验 3（Checkpoint 探索）
- 晚上：制作 Trigger 类型对比表
- 复习错题：Q20, Q21, Q30, Q72, Q139, Q320

**Day 5：性能调优与 Spark UI**
- 上午：阅读 Spark UI 各 tab 的官方文档
- 下午：完成实验 5（Spark UI 性能诊断）
- 晚上：制作 Spark UI 诊断速查表
- 复习错题：Q26, Q49, Q51, Q66, Q85, Q101, Q220, Q244, Q311

**Day 6：Jobs API + Workflows**
- 上午：阅读 Jobs API 2.1 文档，制作 endpoint 功能对比表
- 下午：用 curl 或 Postman 实际调用 jobs/create、jobs/run-now、runs/list
- 晚上：制作 Flashcard 卡片（API endpoints + CLI commands）
- 复习错题：Q1, Q12, Q57, Q115, Q117, Q232, Q235, Q317

### 第 7-9 天：P2 扩展领域

**Day 7：Delta Sharing + DLT/Lakeflow**
- 上午：阅读 Delta Sharing 官方文档，画关系图
- 下午：完成实验 7（DLT Pipeline）
- 晚上：学习 APPLY CHANGES INTO 语法
- 复习错题：Q212, Q218, Q268, Q296, Q299, Q300, Q302

**Day 8：Auto Loader + CDF/CDC**
- 上午：阅读 Auto Loader 文档（directory listing vs notification 模式）
- 下午：完成实验 6（CDF + MERGE INTO）
- 晚上：制作 Auto Loader 模式对比表
- 复习错题：Q28, Q75, Q97, Q108, Q113, Q147, Q257, Q289

**Day 9：Databricks CLI + Asset Bundles + 平台操作**
- 上午：系统学习 Databricks CLI 命令
- 下午：阅读 Asset Bundles 文档的 Getting Started
- 晚上：Flashcard 复习所有 CLI/API 知识点
- 复习错题：Q68, Q102, Q111, Q174, Q175, Q221, Q249, Q263, Q295, Q325

### 第 10-12 天：综合练习

**Day 10：重做所有 121 道错题（第一遍）**
- 不看答案，重新作答
- 记录仍然做错的题目
- 分析二次错误的原因

**Day 11：重做二次错误的题目 + 薄弱领域专项练习**
- 针对仍然做错的题目，回到对应的知识点深入学习
- 做 Databricks 官方 Practice Exam

**Day 12：全真模拟考试**
- 从 327 题中随机抽取 60 题，限时 120 分钟
- 模拟真实考试环境
- 评估是否达到 75%+ 的目标

### 第 13-14 天：查漏补缺

**Day 13：针对模拟考试暴露的弱点进行最后补强**
- 重点复习模拟考试中做错的题目
- 确保所有 P0/P1 知识点都已掌握

**Day 14：考前冲刺**
- 快速过一遍所有对比表和 Flashcard
- 复习考试策略（见下方 checklist）
- 保持良好心态，不要临时学新内容

---

## I. 考前必须掌握清单（Must-Master Before Exam Checklist）

### Delta Lake（必须全部掌握）
- [ ] Transaction Log 驱动一切的心智模型
- [ ] Data Skipping：文件级统计、前 32 列、min/max
- [ ] Auto Compaction（128MB）vs Optimized Writes vs OPTIMIZE（1GB）
- [ ] Liquid Clustering vs Z-ORDER vs Partitioning
- [ ] Deletion Vectors：软删除、REORG TABLE PURGE
- [ ] VACUUM：默认 7 天、只删除未引用文件
- [ ] Time Travel：版本号/时间戳、与 VACUUM 的交互
- [ ] Managed vs External table 的 DROP 行为
- [ ] CHECK Constraint：添加时现有数据必须合规
- [ ] CDF：_change_type 列的四种值

### Unity Catalog（必须全部掌握）
- [ ] 三级命名空间 + 权限检查链
- [ ] USE CATALOG + USE SCHEMA 是前提
- [ ] MANAGE vs ALL PRIVILEGES vs OWNERSHIP
- [ ] Column Masking 和 Row Filter 的实现方式
- [ ] External Location 和 Storage Credential
- [ ] Delta Sharing：provider/recipient/share 模型
- [ ] Databricks-to-Databricks vs Open Sharing 的功能差异

### Structured Streaming（必须全部掌握）
- [ ] 微批处理循环模型
- [ ] Checkpoint：每个 query 独占、记录 offset
- [ ] Trigger 类型和适用场景
- [ ] readStream.table() 的增量语义
- [ ] Stream-static join：静态表在每个 batch 重新读取最新版本
- [ ] Watermark 和 late data

### 性能调优（必须全部掌握）
- [ ] Spark UI 各 tab 的用途
- [ ] SQL tab 看 query plan（predicate push-down）
- [ ] Disk spill 的诊断和解决
- [ ] %sh 在 driver 执行
- [ ] 集群配置选择原则

### Jobs/Workflows（必须全部掌握）
- [ ] jobs/create 不幂等
- [ ] jobs/run-now vs runs/submit
- [ ] runs/repair 重跑失败 task
- [ ] Job 参数通过 dbutils.widgets 传递
- [ ] Job run history 保留 60 天
- [ ] Multi-task job 的 task 依赖

### DLT/Lakeflow（必须掌握核心）
- [ ] LIVE TABLE vs STREAMING LIVE TABLE
- [ ] Expectations：warn/drop/fail
- [ ] APPLY CHANGES INTO
- [ ] DLT vs Structured Streaming 的运维差异

### 平台操作（必须记住）
- [ ] dbutils.widgets 是 Jobs API 传参机制
- [ ] dbutils.secrets.get() 返回真实值，显示 REDACTED
- [ ] %pip install vs %sh pip install（%pip 是 notebook-scoped）
- [ ] Python notebook 第一行：# Databricks notebook source
- [ ] databricks fs cp 上传文件到 DBFS
- [ ] sys.path 包含模块搜索路径

### 考试策略
- [ ] 最小权限题：选刚好满足需求的最小权限，不要选过度方案
- [ ] 多选题：不要放弃，至少选两个最有把握的
- [ ] 新特性题：Delta Sharing、Lakeflow、Asset Bundles、Liquid Clustering 是必考点
- [ ] 时间管理：每题平均 2 分钟，不要在一道题上花超过 3 分钟
- [ ] 排除法：先排除明显错误的选项，再从剩余选项中选择

---

> 本报告基于 327 题完整作答数据生成。总体评估：当前水平距离通过线约差 7-9 个百分点，通过 14 天系统性补强完全可以达到通过标准。关键是不要继续"刷题"，而是按照上述计划重建知识结构。
