# 3 天终极冲刺 — Databricks Data Engineer Professional

> 起点：第二轮 ~85%（含题目识别膨胀），真实概念理解 ~80%
> 目标：真实概念理解 88%+，考试稳定通过（≥80%）
> 生成日期：2026-04-25
> Go/No-Go Gate：Day 2 模拟考 ≥ 51/60 (85%) → 报名

---

## 需要攻克的 24 道错题（4 个概念模块）

### Module A: Unity Catalog 权限体系（7 题）

| 题号 | 一句话规则 | 类型 |
|------|-----------|------|
| Q230 | Schema Owner 不自动有表权限，但可自行 GRANT | 顽固 |
| Q231 | 集群权限三级：CAN ATTACH TO < CAN RESTART < CAN MANAGE（CAN VIEW 不存在） | 回归 |
| Q241 | Row Filter = 哪些行（region）；Column Mask = 显示什么值（role）；SQL UDF + ALTER TABLE | 回归 |
| Q281 | 同 Q241 — 维度不能搞反 | 回归 |
| Q291 | USE CATALOG + USE SCHEMA 够用；ALL/MANAGE/OWNER 都给了删除/改权限能力 → 过度 | 顽固 |
| Q314 | Federation 不复制 → 不存在一致性问题 | 回归 |
| Q326 | UC Managed Table + Predictive Optimization = 细粒度+最少维护+最佳性能 | 顽固 |

**统一心智模型：** UC 是一条权限检查链（USE CATALOG → USE SCHEMA → SELECT/MODIFY），每层独立检查。给权限永远选"刚好够用"。

### Module B: Spark 执行模型与性能诊断（6 题）

| 题号 | 一句话规则 | 类型 |
|------|-----------|------|
| Q220 | Predicate pushdown 验证：SQL tab → Physical Plan → FileScan 的 PushedFilters | 顽固 |
| Q236 | collect() 把全部数据拉到 driver → OOM；用 display(df) 替代 | 回归 |
| Q245 | DBSQL 慢查询：Query Profile → Top Operators（EXPLAIN 只给计划，无运行时数据） | 回归 |
| Q279 | repartition(key) 触发一次 shuffle → 后续 groupBy(key) 免 shuffle → 净效果更优 | 回归 |
| Q280 | Spill = task 数据 > 可用内存 → 溢出磁盘 → 解决：加内存或增加并行度 | 回归 |
| Q324 | Dynamic File Pruning = Delta join 时跳过无关文件 → 数据量减小 → 更高效 join 策略 | 回归 |

**统一心智模型：** Driver 是协调者不处理数据（collect 例外）；Executor 干活。诊断走两条路：Spark UI SQL tab 看 plan，Stages tab 看 shuffle/spill。DBSQL 用 Query Profile。

### Module C: Delta 高级特性 + DLT 对象选择（5 题）

| 题号 | 一句话规则 | 类型 |
|------|-----------|------|
| Q227 | Python 闭包捕获引用不是值 → DLT for 循环用工厂函数 | 回归 |
| Q240 | 查询模式常变 → Liquid Clustering（变 key 无需重写）；稳定 → Z-ORDER | 回归 |
| Q268 | 原始流+实时监控 → Streaming Table；每日聚合报告 → Materialized View | 顽固 |
| Q288 | MERGE 优化双刀：LC 按 merge key 聚簇 + Deletion Vectors 减少写放大 | 顽固 |
| Q310 | MV = 预计算聚合/BI dashboard；ST = 连续数据流/实时 | 回归 |

**统一心智模型：** Delta 优化三选一（Partition → 低基数/LC → 任意基数常变/ZO → 稳定）。DLT 对象二选一（ST → append-only 实时流/MV → 全量重算聚合）。

### Module D: 平台操作 + Streaming 策略（6 题）

| 题号 | 一句话规则 | 类型 |
|------|-----------|------|
| Q60 | 聚合表被审计修改 → 全量重算 + overwrite（safest，upsert 不级联重算） | 回归 |
| Q73 | 延迟可容忍 → trigger(once=True) + 定时 << 常驻流成本 | 回归 |
| Q187 | MLflow 推理输出 = 静态 DF → .write.mode("append").saveAsTable()（不能 writeStream） | 顽固 |
| Q226 | Files in Repos = 标准 .py → pytest/unittest 可直接 import 测试 | 顽固 |
| Q260 | 并行下载多种报告 → foreach task（原生迭代+并行+独立重试） | 顽固 |
| Q292 | 生产化 Spark 应用 = Compute Policies（配置管理）+ Init Scripts（依赖安装） | 顽固 |

---

## Day 1：概念内化（目标：24 条规则全部能脱口而出）

### 上午：Module A + B（13 题，~2h）

1. 读 Module A 的 7 条规则 + kill_card_25.md 中对应解析（30 min）
2. 合上笔记默写 7 条规则 → 标记默不出的 → 重读（15 min）
3. 读 Module B 的 6 条规则 + 解析（25 min）
4. 合上笔记默写 6 条规则（15 min）
5. Module A+B 混合验证测试（见下方，13 题必须全对）（30 min）

### 下午：Module C + D（11 题，~1.5h）

1. 读 Module C 的 5 条规则 + 解析（20 min）
2. 读 Module D 的 6 条规则 + 解析（25 min）
3. 合上笔记默写 11 条规则（15 min）
4. Module C+D 混合验证测试（11 题必须全对）（25 min）

### 晚上：全量自测（~1h）

1. 从 PDF 中抽出这 24 道原题，不看任何笔记重做
2. 必须全对。任何做错的题，当场重新理解直到能解释"为什么其他选项错"
3. 24/24 → Day 1 完成

### Day 1 验证测试

**Module A+B 验证（必须全对）：**

```
A1. 新成员有 table_x 的 SELECT 但没有 USE SCHEMA。能查询吗？
    → 不能，USE SCHEMA 是前提检查

A2. Schema Owner 想查自己 schema 下的表。需要什么？
    → 自行 GRANT SELECT（Owner 不自动有表权限）

A3. 让用户只能看 driver logs，不能重启。给什么权限？
    → CAN MANAGE（唯一能看 driver logs 的，没有更小的 CAN VIEW）

A4. Row Filter 按什么维度？Column Mask 按什么维度？
    → Row Filter = region（哪些行）；Column Mask = role（什么值）

A5. 题目说 "cannot rename/delete catalog"。给 ALL PRIVILEGES 行不行？
    → 不行，ALL PRIVILEGES 包含这些能力。USE CATALOG + USE SCHEMA 够用

A6. Federation 为什么解决一致性？
    → 不复制 → 不存在副本过期/同步延迟/版本冲突

A7. 细粒度控制 + 最少维护 + 最佳性能 = ？
    → UC Managed Table + Predictive Optimization

B1. 确认 predicate pushdown 生效，看 Spark UI 哪里？
    → SQL tab → Physical Plan → FileScan 节点的 PushedFilters

B2. display(df.collect()) 导致 driver 崩溃。问题在 driver 还是 executor？
    → Driver（collect 把数据拉到 driver）

B3. EXPLAIN 和 Query Profile Top Operators 的区别？
    → EXPLAIN = 静态计划；Query Profile = 运行时性能数据（算子耗时/数据量）

B4. repartition("region") 为什么减少总 shuffle？
    → 先 shuffle 一次按 key 分好 → 后续 groupBy 免 shuffle → 净效果更少

B5. Stages tab 显示 Spill(Disk) = 2GB。根因？
    → task 数据 > 可用内存，溢出磁盘

B6. ORC 迁移 Delta 后 join 策略变了，为什么？
    → Dynamic File Pruning 跳过无关文件 → 数据量减小 → 可用 Shuffle Hash Join
```

**Module C+D 验证（必须全对）：**

```
C1. 查询模式每季度变一次，要避免 costly rewrites。LC 还是 Z-ORDER？
    → LC（变 key 无需重写；Z-ORDER 需重新 OPTIMIZE 全表）

C2. BI dashboard 预计算聚合 → MV 还是 ST？实时 clickstream → MV 还是 ST？
    → MV；ST

C3. MERGE 在 800GB 表上很慢。选两个优化？
    → LC 按 merge key 聚簇 + Deletion Vectors

C4. DLT for 循环所有表指向同一数据源。为什么？
    → 闭包捕获引用不是值 → 用工厂函数

D1. store_sales_summary 有 7天总和/YTD/QTD，历史被修改后怎么更新？
    → 全量重算 + overwrite（upsert 不级联重算聚合窗口）

D2. 延迟可接受 1h。常驻 streaming 还是 trigger(once) + 定时？
    → trigger(once) + 定时（成本低 10 倍）

D3. MLflow model.predict() 返回的 DF 能 writeStream 吗？
    → 不能，静态 DF → .write.mode("append").saveAsTable()

D4. 对 notebook 中的函数做单元测试，怎么组织代码？
    → 函数放 Files in Repos 的标准 .py → pytest 直接 import

D5. 并行下载 10 种 PDF 报告，用什么 Jobs 功能？
    → foreach task

D6. 生产化需要管理 env vars 和安装外部库。用哪两种？
    → Compute Policies（配置）+ Init Scripts（依赖）
```

---

## Day 2：模拟考试（Go/No-Go Gate）

### 上午：60 题模拟考试（120 min 严格限时）

- 从 327 题中随机抽 60 题（用 generate_mock_exam.py）
- 不看任何笔记，模拟真实考试环境
- 每题最多 2 分钟，不确定的标记后跳过

### 下午：批改 + 决策

| 结果 | 行动 |
|------|------|
| ≥ 51/60 (85%) | Day 3 上午报名，选最近可用日期考试 |
| 48-50/60 (80-84%) | Day 3 上午补强新暴露的错题，下午报名 |
| < 48/60 (<80%) | 暂停，分析系统性弱点，延长 2-3 天 |

### 错题分析模板（如有错题）

对每道错题回答三个问题：
1. 属于 ABCD 哪个模块？（如果是模块外的新错，单独标记）
2. 错因：概念不懂 / 审题失误 / 知识遗忘？
3. 一句话修正规则

---

## Day 3：报名 + 考前收尾

### 上午（1h）

1. 24 条规则最终默写（不看笔记，纸笔或心里过一遍）
2. 如果 Day 2 有新错题，确认新规则已内化

### 下午

1. 注册考试（选 Day 4 或 Day 5）
2. 考试策略清单最终确认：
   - [ ] "minimal permissions" → 选权限最小但刚好满足的
   - [ ] "safest" ≠ "most efficient"
   - [ ] "always"/"never" 选项通常是错的
   - [ ] 多选题不放弃，至少选最有把握的两个
   - [ ] 每题 ≤ 2 min，不确定标记跳过，最后回来
   - [ ] 新特性必考：Delta Sharing、Lakeflow、Asset Bundles、Liquid Clustering

### 考前晚上

不学新内容。早睡。

---

## 附：24 条规则速查卡（打印用）

```
UC 权限（7）
  Q230: Owner 不自动有表权限 → 自行 GRANT
  Q231: 三级权限 ATTACH<RESTART<MANAGE，无 VIEW
  Q241: Row=行(region) Column=值(role) SQL-UDF+ALTER
  Q291: USE CATALOG+USE SCHEMA 够用，多了都过度
  Q314: Federation 不复制=不矛盾
  Q326: Managed+Predictive Opt=最优组合

Spark 性能（6）
  Q220: SQL tab→PhysicalPlan→PushedFilters
  Q236: collect()→Driver OOM→用 display()
  Q245: QueryProfile TopOps 有运行时数据，EXPLAIN 没有
  Q279: repartition(key)→后续 groupBy 免 shuffle
  Q280: Spill=task数据>内存→加内存/加并行度
  Q324: DFP=join时跳文件→数据少→更快join

Delta/DLT（5）
  Q227: 闭包捕获引用→工厂函数
  Q240: 常变→LC 稳定→ZO
  Q268: 实时→ST 聚合→MV
  Q288: MERGE优化=LC(key)+DV
  Q310: 同Q268

平台操作（6）
  Q60: 聚合被改→全量重算+overwrite
  Q73: 延迟容忍→trigger(once)+定时
  Q187: MLflow输出=静态DF→write.append.saveAsTable
  Q226: Files in Repos→标准.py→pytest
  Q260: 并行下载→foreach task
  Q292: 生产化=ComputePolicies+InitScripts
```
