# Databricks Data Engineer Professional — 10 天密集攻坚计划

> 目标：将已见题正确率从 84% 推至 90%+，确保真实考试稳定在 82-85%
> 通过线：80%（60 题最多错 12 题）
> 安全目标：85%（最多错 9 题）
> 生成日期：2026-04-24

---

## 战略框架

```
Layer 1: Kill List（Day 1-3）
  └─ 逐道攻克 25 道关键错题，每题深入到能默写规则
  └─ 目标：消灭已知失分点

Layer 2: Concept Walls（Day 4-6）
  └─ 针对 5 个根因做概念深化 + 变体题自测
  └─ 目标：能应对同概念的未见变体

Layer 3: Simulation（Day 7-8）
  └─ 两次 60 题模拟考试，严格限时 120 分钟
  └─ 目标：验证稳定在 85%+

Buffer（Day 9-10）
  └─ 根据模拟考暴露的弱点最后补强
  └─ 目标：查漏补缺 → 报名
```

---

## Layer 1: Kill List（Day 1-3）

### Day 1：Unity Catalog 权限模型（8 题）

**核心心智模型：UC 是一条权限检查链**
```
访问 catalog.schema.table →
  CHECK 1: USE CATALOG? → 否则拒绝
  CHECK 2: USE SCHEMA? → 否则拒绝
  CHECK 3: SELECT/MODIFY? → 否则拒绝
```

| # | 题号 | 一句话规则 | 错误类型 |
|---|------|-----------|---------|
| 1 | Q230 | Schema Owner 不自动继承表访问权限，但可以自行 GRANT | 顽固 |
| 2 | Q231 | 集群权限只有 CAN ATTACH TO / CAN RESTART / CAN MANAGE 三级，CAN VIEW 不存在 | 回归 |
| 3 | Q241 | Row Filter 按 region 过滤行，Column Mask 按 role 脱敏列，用 SQL UDF + ALTER TABLE 实现 | 回归 |
| 4 | Q281 | 同 Q241：Row Filter = 哪些行，Column Mask = 什么值，维度不能搞反 | 回归 |
| 5 | Q291 | 项目团队只需 USE CATALOG + USE SCHEMA，ALL PRIVILEGES/MANAGE/OWNER 都过度 | 顽固 |
| 6 | Q314 | Federation 不复制数据 → 不存在一致性问题，零拷贝消除了问题本身 | 回归 |
| 7 | Q326 | UC Managed Table + Predictive Optimization = 细粒度控制 + 最少维护 + 最佳性能 | 顽固 |
| 8 | Q187 | MLflow 推理输出是静态 DataFrame，不能用 writeStream；保留历史用 append + saveAsTable | 顽固 |

**Day 1 学习流程：**
1. 先画一张 UC 权限层级图（15 min）
2. 逐题阅读原题+解析，在旁边写下"一句话规则"（60 min）
3. 合上笔记，凭记忆默写 8 条规则（20 min）
4. 对照检查，标记默写不出的规则，再读一遍（15 min）
5. 做 Day 1 验证测试（见下方变体题）（30 min）

**Day 1 验证测试（必须全对才算过关）：**

```
V1. 一个用户有 table_x 的 SELECT 权限但没有 USE SCHEMA 权限。
    他能查询 table_x 吗？ → [答案: 不能]

V2. Schema Owner 想查看自己 schema 下某张表的数据。
    他需要额外做什么？ → [答案: 自行 GRANT SELECT，Owner 可以授权但不自动有]

V3. 集群管理员想让某用户只能查看集群的 driver logs，不能重启。
    应该给什么权限？ → [答案: CAN MANAGE 是唯一能看 driver logs 的权限，没有更小的选项]

V4. 题目说"cannot rename/delete catalog or change catalog-level permissions"。
    给 ALL PRIVILEGES 行不行？ → [答案: 不行，ALL PRIVILEGES 包含这些能力]

V5. 你需要对 PII 表做行级过滤（按 region）和列级脱敏（按 role）。
    Row Filter 用在哪个维度？Column Mask 用在哪个维度？
    → [答案: Row Filter = region（决定看哪些行），Column Mask = role（决定列显示什么值）]

V6. 为什么 Lakehouse Federation 能解决数据一致性问题？
    → [答案: 不复制数据 → 不存在副本过期/同步延迟/版本冲突]

V7. UC Managed Table vs External Table：哪个支持 Predictive Optimization？
    → [答案: 只有 Managed Table]

V8. MLflow model.predict() 返回的 DataFrame 能直接 writeStream 吗？
    → [答案: 不能，是静态 DF，用 .write.mode("append").saveAsTable()]
```

---

### Day 2：Spark 性能机制（7 题）

**核心心智模型：Driver vs Executor，逻辑计划 vs 物理计划**
```
Driver: 协调者，不处理数据（除非你 collect()）
Executor: 干活的，每个 task 处理一个 partition 的数据
Spark UI SQL tab: 看 Physical Plan → 确认优化是否生效
Stages tab: 看 task 分布、shuffle、spill
```

| # | 题号 | 一句话规则 | 错误类型 |
|---|------|-----------|---------|
| 1 | Q220 | 诊断 predicate pushdown：SQL tab → Physical Plan → FileScan 节点的 PushedFilters | 顽固 |
| 2 | Q236 | collect() 把所有数据拉到 driver → OOM 崩溃；解决：避免 collect，用 display(df) | 回归 |
| 3 | Q245 | DBSQL 慢查询诊断：Query Profile → Top Operators 面板（不是 EXPLAIN，EXPLAIN 无运行时数据） | 回归 |
| 4 | Q279 | repartition(key) 后 groupBy(key) 可做 partition-local aggregation，减少后续 shuffle | 回归 |
| 5 | Q280 | Spill = task 数据超过内存 → 溢出到磁盘；根因是 task 数据太多，解决：加内存或增加并行度 | 回归 |
| 6 | Q310 | MV = 预计算聚合给 BI dashboard；Streaming Table = 处理连续数据流 | 回归 |
| 7 | Q324 | Dynamic File Pruning：Delta 在 join 时根据条件跳过无关文件 → 读取量减少 → 可用更高效的 join 策略 | 回归 |

**Day 2 学习流程：**
1. 画 Spark 执行架构图：Driver → Executors → Tasks → Partitions（15 min）
2. 画 Spark UI 导航图：哪个 tab 看什么信息（15 min）
3. 逐题阅读原题+解析，写"一句话规则"（50 min）
4. 合上笔记默写 7 条规则（15 min）
5. 做 Day 2 验证测试（30 min）

**Day 2 验证测试：**

```
V1. 要确认 WHERE 条件是否被下推到数据源，应该看 Spark UI 的哪个 tab？
    → [答案: SQL tab → Physical Plan → FileScan 节点的 PushedFilters]

V2. display(df.collect()) 导致 driver 崩溃。是 executor 内存不足还是 driver 内存不足？
    → [答案: Driver，因为 collect() 把数据拉到 driver]

V3. EXPLAIN 和 Query Profile Top Operators 的区别是什么？
    → [答案: EXPLAIN 只给计划（静态），Query Profile 给运行时性能数据（算子耗时/数据量）]

V4. repartition("region") 会触发 shuffle 吗？那为什么反而能减少总 shuffle？
    → [答案: 会触发一次 shuffle，但后续 groupBy("region") 无需再 shuffle，净效果更少]

V5. Stages tab 显示 Spill (Disk) = 2GB。根因是什么？
    → [答案: task 处理的数据超过可用内存，溢出到磁盘]

V6. 从 ORC 迁移到 Delta 后，join 策略从 Sort Merge Join 变成 Shuffle Hash Join，为什么？
    → [答案: Dynamic File Pruning 让 Delta 跳过大量无关文件 → 数据量变小 → 可用更高效的 join]

V7. BI dashboard 需要预计算复杂聚合。用 Streaming Table 还是 Materialized View？
    → [答案: Materialized View]
```

---

### Day 3：Delta 高级特性 + DLT + 平台操作（10 题）

**核心心智模型三个：**
```
Delta 优化矩阵：
  Liquid Clustering → 查询模式常变、任意基数、自动维护
  Z-ORDER → 查询模式稳定、手动 OPTIMIZE 维护
  Deletion Vectors → 软删除，减少写放大，REORG PURGE 物理清理

DLT 对象选择：
  Streaming Table → 增量数据流（append-only 源，实时处理）
  Materialized View → 全量重算（聚合、BI、维度表）

平台操作：
  foreach task → 并行迭代执行同构任务
  Compute Policies → 配置管理；Init Scripts → 依赖安装
  Files in Repos → 标准 .py 文件 → pytest 可直接测试
```

| # | 题号 | 一句话规则 | 错误类型 |
|---|------|-----------|---------|
| 1 | Q233 | ~~PDF答案错误~~ LC cluster-on-write 支持 INSERT INTO/CTAS/RTAS/spark.write；**Streaming 默认不支持**需启用 Spark config（你的原答案 B 是对的） | 已修正 |
| 2 | Q240 | "查询模式常变" → Liquid Clustering（变更 key 无需重写全表）；Z-ORDER 变更列后需重新 OPTIMIZE 全表 | 回归 |
| 3 | Q288 | MERGE 优化两板斧：Liquid Clustering 按 merge key 聚簇（减少扫描）+ Deletion Vectors（减少写放大） | 顽固 |
| 4 | Q227 | Python 闭包捕获引用不是值；DLT for 循环中用工厂函数创建独立作用域 | 回归 |
| 5 | Q268 | 原始流数据 + 实时监控 → Streaming Table；每日聚合报告 → Materialized View | 顽固 |
| 6 | Q60 | 聚合表（7天总和/YTD/QTD）历史被审计修改时，全量重算 + overwrite 是唯一安全方案 | 回归 |
| 7 | Q73 | 延迟容忍宽松的场景：trigger(once=True) + 定时作业 << 常驻流运行成本 | 回归 |
| 8 | Q226 | Files in Repos 让函数以标准 .py 存在 → pytest/unittest 可直接 import 并测试 | 顽固 |
| 9 | Q235 | runs/get（带 include_history 参数）获取单次运行历史详情 | 顽固 |
| 10 | Q260 | 并行下载多个文件：foreach task（原生迭代任务），不是 Pandas UDF 或循环 notebook | 顽固 |
| 11 | Q292 | 生产化 Spark 应用：Compute Policies（配置管理）+ Init Scripts（依赖安装） | 顽固 |

**Day 3 学习流程：**
1. 画 Delta 优化特性对比矩阵（LC vs ZO vs Partition vs DV）（15 min）
2. 画 DLT 对象选择决策树（ST vs MV）（10 min）
3. 逐题阅读原题+解析，写"一句话规则"（70 min）
4. 合上笔记默写 11 条规则（25 min）
5. 做 Day 3 验证测试（30 min）

**Day 3 验证测试：**

```
V1. 表查询模式每季度变一次，需要避免 costly rewrites。用 Liquid Clustering 还是 Z-ORDER？
    → [答案: Liquid Clustering，变更 key 无需重写；Z-ORDER 需要重新 OPTIMIZE]

V2. Liquid Clustering 的 cluster-on-write 支持哪些写入方式？不支持哪些？
    → [答案: 支持常规写入/MERGE/streaming（需配置）；不支持 INSERT INTO、CTAS、RTAS]

V3. MERGE 在 800GB 表上很慢。选两个优化动作。
    → [答案: (1) Liquid Clustering 按 merge key 聚簇 (2) 启用 Deletion Vectors]

V4. DLT pipeline 中用 for 循环创建多个表，所有表都指向同一个数据源。为什么？
    → [答案: 闭包捕获变量引用，循环结束后所有函数引用最后一个值；用工厂函数修复]

V5. 卡车遥测数据：原始数据摄取用什么？实时位置监控用什么？每日里程聚合用什么？
    → [答案: Streaming Table / Streaming Table / Materialized View]

V6. store_sales_summary 有 7 天总和、YTD、QTD 列。历史订单被审计修改后怎么更新？
    → [答案: 全量重算 + overwrite，upsert 不会级联重算聚合窗口]

V7. 延迟可以接受 1 小时。用常驻 streaming 还是 trigger(once) + 定时？
    → [答案: trigger(once=True) + 定时作业，成本低 10 倍]

V8. 要对 notebook 中的函数做单元测试。应该怎么组织代码？
    → [答案: 把函数放在 Files in Repos 的标准 .py 文件中，pytest 直接 import 测试]

V9. 需要并行下载 10 种报告的 PDF。用什么 Jobs 功能？
    → [答案: foreach task，原生支持列表迭代、并行执行、独立重试]

V10. 生产化 Spark 应用需要管理环境变量和安装外部库。用哪两种机制？
    → [答案: Compute Policies（env vars + Spark config）+ Init Scripts（库安装）]
```

---

## Layer 2: Concept Walls（Day 4-6）

### Day 4：UC 权限 + 数据治理 概念墙

**上午（2h）：系统性权限知识重建**

1. 精读 UC 权限层级完整表（从诊断报告 E3 节开始），确保理解每个权限的精确含义
2. 重点对比容易混淆的权限对：
   - USE CATALOG vs ALL PRIVILEGES vs OWNERSHIP
   - MANAGE vs ALL PRIVILEGES（MANAGE 可以 GRANT 给他人）
   - Managed vs External table 的 DROP 行为差异
3. 理解 Delta Sharing 的三角模型：Provider → Share → Recipient
4. 理解 Column Masking / Row Filter 的实现机制（SQL UDF + ALTER TABLE）

**下午（2h）：变体题训练（10 题）**

从以下角度出变体：
- 权限不足时的报错信息是什么？
- 多层权限缺失时，第一个被拒绝的是哪个检查点？
- Delta Sharing 中 Databricks-to-Databricks vs Open Sharing 的功能差异
- External Location 和 Storage Credential 的关系
- Predictive Optimization 的前提条件（UC Managed Table only）

**晚上（1h）：Anki 卡片制作与首轮复习**

为 UC 权限领域制作 15 张 Anki 卡片（正面=问题，背面=一句话规则+记忆线索）

---

### Day 5：Spark 性能 + 执行模型 概念墙

**上午（2h）：Spark UI 完整导航图 + Physical Plan 阅读**

1. 系统学习 Spark UI 每个 tab 的用途（从诊断报告 E4 节开始）
2. 重点掌握：
   - SQL tab → Physical Plan 中 FileScan/Filter/Exchange/HashAggregate 节点含义
   - PushedFilters 的出现位置和含义
   - BroadcastHashJoin vs ShuffledHashJoin vs SortMergeJoin 的选择条件
   - Dynamic File Pruning 的触发条件（Delta + star schema join + 过滤在维度表侧）
3. Spill 诊断流程：Stages tab → Spill(Memory)/Spill(Disk) → 解决方案
4. collect() / toPandas() / display() 的 driver 内存影响

**下午（2h）：实操练习（如有 Databricks 环境）**

- 在 Databricks 中运行一个带 WHERE 条件的查询，在 SQL tab 查看 PushedFilters
- 运行一个大表 JOIN，观察 join 策略选择
- 故意触发 spill（小集群处理大数据），在 Stages tab 中观察

如果没有 Databricks 环境，用变体题替代（10 题），侧重：
- 给定 Physical Plan 片段，判断优化是否生效
- 给定症状（慢/OOM/spill），诊断根因
- 给定场景，选择 memory-optimized vs compute-optimized 集群

**晚上（1h）：Anki 卡片制作 + 复习 Day 4 卡片**

为 Spark 性能领域制作 12 张 Anki 卡片

---

### Day 6：Delta 高级特性 + DLT + 平台综合 概念墙

**上午（2h）：Delta 优化特性矩阵深化**

1. Liquid Clustering 完整知识：
   - 支持的写入操作 vs 不支持的操作
   - 与 Z-ORDER 的决策对比（查询模式稳定性是关键变量）
   - Streaming 场景需要的 Spark config
   - 与 Deletion Vectors 的组合优化 MERGE 场景
2. Deletion Vectors 完整知识：
   - 软删除机制（元数据标记，不重写文件）
   - REORG TABLE APPLY (PURGE) 物理清理
   - 对 MERGE/UPDATE/DELETE 性能的影响
3. Auto Compaction (128MB) vs Optimized Writes vs OPTIMIZE (1GB) vs Predictive Optimization

**下午（2h）：DLT + 平台操作深化**

1. DLT 三种对象的精确选择标准：
   - LIVE TABLE（全量 batch 计算）
   - STREAMING LIVE TABLE（增量 streaming）
   - Materialized View（预计算聚合 + 自动刷新）
2. DLT Expectations：warn / drop / fail 的行为差异
3. APPLY CHANGES INTO：CDC 处理（SCD Type 1 / Type 2）
4. 平台操作速查：
   - foreach task / run task / notebook task 的区别
   - Compute Policies vs Init Scripts vs Cluster Libraries
   - Files in Repos vs Notebook

**晚上（1h）：Anki 卡片制作 + 复习全部卡片**

---

## Layer 3: Simulation（Day 7-8）

### Day 7：模拟考试 #1

**上午：考试（120 min）**
- 从 327 题中随机抽 60 题（排除 16 道重复题和 2 道无效题）
- 严格限时 120 分钟
- 模拟真实考试环境：不查资料、不回看笔记

**下午：评分 + 错题深度分析**
- 计算得分，目标 ≥ 51/60（85%）
- 对每道错题：
  1. 属于 5 个根因中的哪个？
  2. 是 Kill List 中的原题还是新错？
  3. 错因是概念不懂还是审题失误？
- 如果低于 85%，标记薄弱领域进入 Day 9 补强清单

### Day 8：模拟考试 #2

**上午：考试（120 min）**
- 不同的 60 题（与 #1 不重叠）
- 同样严格限时

**下午：评分 + 综合分析**
- 比较两次模拟的成绩和错题分布
- 如果两次都 ≥ 85%：Day 9-10 轻量复习后报名
- 如果有一次 < 85%：Day 9-10 针对性强化
- 如果两次都 < 80%：需要额外延长复习周期

---

## Buffer（Day 9-10）

### Day 9：根据模拟考结果定向补强

- 分析两次模拟考的错题交集（两次都错的题 = 真正的知识盲区）
- 回到对应的概念墙章节重新学习
- 做该领域的额外变体题

### Day 10：考前冲刺

**上午：快速过一遍所有材料**
- 所有 Anki 卡片最终复习
- 25 条"一句话规则"最终默写
- Delta / UC / Spark 三张对比矩阵最终过目

**下午：考试策略确认**
- [ ] 最小权限题：选刚好满足需求的最小权限
- [ ] 多选题：不放弃，至少选两个最有把握的
- [ ] "always"/"never" 选项通常是错的
- [ ] 审题关键词："safest" ≠ "most efficient"，"minimal" = 权限尽可能小
- [ ] 时间管理：每题 2 分钟，不确定的标记后跳过，最后回来
- [ ] 新特性题（Delta Sharing、Lakeflow、Asset Bundles、Liquid Clustering）必考

**晚上：注册考试，选择 Day 11 或 Day 12 考试**

---

## 附：25 道错题速查卡

### UC 权限（7+1 题）
```
Q230: Schema Owner ≠ 自动有表权限（可自行 GRANT）
Q231: 集群权限三级：CAN ATTACH TO < CAN RESTART < CAN MANAGE（无 CAN VIEW）
Q241: Row Filter = 哪些行（region），Column Mask = 什么值（role），SQL UDF + ALTER TABLE
Q281: 同 Q241，维度不能搞反
Q291: 团队自治：USE CATALOG + USE SCHEMA 够了，ALL/MANAGE/OWNER 过度
Q314: Federation = 零拷贝 → 不存在一致性问题
Q326: UC Managed Table + Predictive Optimization = 最优组合
Q187: MLflow 输出是静态 DF → .write.mode("append").saveAsTable()
```

### Spark 性能（7 题）
```
Q220: Predicate pushdown → SQL tab → Physical Plan → PushedFilters in FileScan
Q236: collect() → 数据全到 driver → OOM；用 display(df) 替代
Q245: DBSQL 慢查询 → Query Profile Top Operators（不是 EXPLAIN）
Q279: repartition(key) → 后续 groupBy(key) 无需 shuffle
Q280: Spill = task 数据 > 内存 → 加内存或增加并行度
Q310: MV = 聚合/BI；Streaming Table = 实时数据流
Q324: Dynamic File Pruning = Delta join 时跳过无关文件 → 更高效 join 策略
```

### Delta + DLT + 平台（11 题）
```
Q233: LC cluster-on-write 不支持 INSERT INTO / CTAS；streaming 需 Spark config
Q240: 查询模式常变 → LC（变 key 无需重写）；稳定 → Z-ORDER
Q288: MERGE 优化 = LC(merge key) + Deletion Vectors
Q227: Python 闭包 → 工厂函数，不是 DLT 问题
Q268: 原始流+实时监控 → ST；每日聚合 → MV
Q60: 聚合表被审计修改 → 全量重算 + overwrite（safest）
Q73: 延迟可容忍 → trigger(once) + 定时 << 常驻流
Q226: Files in Repos = 标准 .py → pytest 可测
Q235: runs/get + include_history（不是 runs/list）
Q260: 并行下载 → foreach task（原生迭代）
Q292: 生产化 = Compute Policies(配置) + Init Scripts(依赖)
```

---

## 每日 Checklist

| Day | 主题 | 过关标准 |
|-----|------|---------|
| 1 | UC 权限 Kill List | 8 条规则默写全对 + 验证题全对 |
| 2 | Spark 性能 Kill List | 7 条规则默写全对 + 验证题全对 |
| 3 | Delta/DLT/平台 Kill List | 11 条规则默写全对 + 验证题全对 |
| 4 | UC 概念墙 | 10 道变体题 ≥ 8 对 |
| 5 | Spark 概念墙 | 10 道变体题 ≥ 8 对 |
| 6 | Delta/DLT 概念墙 | 10 道变体题 ≥ 8 对 |
| 7 | 模拟考 #1 | ≥ 51/60 (85%) |
| 8 | 模拟考 #2 | ≥ 51/60 (85%) |
| 9 | 定向补强 | 模拟考错题全部理解 |
| 10 | 考前冲刺 | 全部规则默写 + 报名考试 |
