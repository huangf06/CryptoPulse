# Databricks DEP 备考新计划 — 基于 2025-11 考纲

> 生成日期：2026-04-24
> 考试版本：November 30, 2025 blueprint
> 起点：旧版 327 题已掌握（第二轮 85%，模拟考 100%）
> 核心问题：新考纲 ~30-40% 内容在旧题库中完全空白
> 通过线：80%（59 题最多错 12 题）

---

## 形势判断

### 你的优势
- 旧版考纲覆盖的 ~60% 内容已经非常扎实
- Spark 执行模型、Delta 基础、UC 基础权限、Structured Streaming 基础 — 这些不会丢分

### 你的致命缺口
新考纲引入了大量新特性和术语变更。如果 59 题中有 18-24 题涉及以下内容，你目前的答题能力接近零：

| 新考纲内容 | 权重/出现位置 | 你的当前覆盖 | 风险等级 |
|-----------|-------------|------------|---------|
| **Databricks Asset Bundles (DABs)** | Domain 1 (22%) + Domain 9 (10%) | 零 | 极高 |
| **Lakeflow Spark Declarative Pipelines** | 全面替代 DLT 术语 | 旧 DLT 知识部分适用 | 高 |
| **System Tables** | Domain 5 (10%) — 可观测性/成本 | 零 | 高 |
| **APPLY CHANGES API** | Domain 1 (22%) — CDC | 零 | 高 |
| **Liquid Clustering** | Domain 10 (6%) + Domain 6 (13%) | 旧题有 2 题，但不深入 | 中 |
| **Row Filters & Column Masks** | Domain 7 (10%) | 旧题有 2 题 | 中 |
| **Query Profiler UI** | Domain 5 + 6 (23%) | 零 | 中 |
| **Lakehouse Federation** | Domain 4 (5%) | 旧题有 1 题 | 中 |
| **Delta Sharing (D2D + Open)** | Domain 4 (5%) | 零 | 中 |
| **Deletion Vectors** | Domain 6 (13%) | 旧题有 1 题 | 低 |

### 最坏情况估算
- 59 题中 ~36 题是旧内容 → 你拿 34/36（94%）
- 59 题中 ~23 题是新内容 → 你拿 5/23（蒙对概率）
- 总分：39/59 = 66% → **不通过**

### 目标状态
- 旧内容：维持 90%+
- 新内容：达到 75%+（18/23 对）
- 总分：52/59 = 88% → 稳定通过

---

## 学习资源（按优先级）

| 优先级 | 资源 | 用途 | 费用 |
|--------|------|------|------|
| 1 | **官方考纲 PDF + 9 道样题** | 理解考什么、怎么考 | 免费 |
| 2 | **Databricks 官方文档** | 新特性深入学习 | 免费 |
| 3 | **Databricks Academy 课程** | 结构化学习 + quiz | 部分免费 |
| 4 | **Udemy Practice Exams (118 题)** | 新题型练习 | ~$15 |
| 5 | **我生成的新特性练习题** | 覆盖考纲空白区 | 免费 |

---

## Phase 1: 考纲分析 + 新特性速通（Day 1-2）

### Day 1: 理解新考纲 + DABs 深潜

**上午（2h）：考纲对标**
1. 读官方考纲 PDF，逐条标记哪些在 327 题中有覆盖、哪些完全空白
2. 做 9 道官方样题，感受新题的提问方式和深度
3. 输出：一张"空白清单"——需要从零学习的具体知识点

**下午（3h）：DABs 深潜**

DABs 是最大盲区（可能占 10-15 题），必须优先攻克。

学习内容：
- `databricks bundle` CLI 命令：init, validate, deploy, run, destroy
- `databricks.yml` 配置结构：resources（jobs, pipelines）、targets（dev/staging/prod）
- 部署工作流：本地开发 → bundle validate → deploy to staging → promote to prod
- 与 CI/CD 集成：GitHub Actions / Azure DevOps 中调用 bundle deploy
- 权限管理：bundle 级别的权限配置
- 变量替换和环境配置：${bundle.target}, ${workspace.root_path}

学习方法：
- 读 Databricks 官方 DABs 文档（documentation site）
- 如果有 Databricks workspace，实操创建一个简单 bundle
- 如果没有，通读文档 + 我生成的练习题

**晚上（1h）：DABs 自测（10 题）**
- 我会生成 10 道 DABs 场景题，模拟考试风格

### Day 2: Lakeflow + APPLY CHANGES API

**上午（2h）：Lakeflow Spark Declarative Pipelines**

这不是全新概念——是 DLT 的品牌重命名 + 功能增强。你已有 DLT 基础，需要补的是：
- 术语映射：DLT → Lakeflow Spark Declarative Pipelines
- `CREATE OR REFRESH STREAMING TABLE` 新语法
- `CREATE OR REFRESH MATERIALIZED VIEW` 新语法
- Pipeline 配置中的新选项：serverless compute, enhanced autoscaling
- Expectations 的完整行为：constraint violation modes (warn/drop/fail)
- 与 Unity Catalog 的集成（自动注册到 UC）

**下午（2h）：APPLY CHANGES API (CDC)**

考纲 Domain 1 明确提到。学习内容：
- `APPLY CHANGES INTO target FROM source` 语法
- SCD Type 1 vs Type 2 的 API 差异
- Keys, sequence_by, stored_as_scd_type 参数
- APPLY CHANGES FROM SNAPSHOT（快照 CDC）
- 与 Streaming Tables 的关系

**晚上（1h）：Lakeflow + CDC 自测（10 题）**

---

## Phase 2: 中权重新特性（Day 3-4）

### Day 3: System Tables + Query Profiler

**上午（2h）：System Tables**

考纲 Domain 5（10%）明确考这个。学习内容：
- `system.billing.usage` — 成本分析
- `system.compute.clusters` — 集群使用情况
- `system.access.audit` — 审计日志
- `system.lakeflow.pipeline_event_log` — pipeline 监控
- `system.storage.predictive_optimization_operations_history` — PO 历史
- 查询模式：用 SQL 从 system tables 提取可观测性指标
- 告警配置：基于 system table 查询创建 SQL alerts

**下午（2h）：Query Profiler UI**

Domain 5 + 6 都涉及。你已经知道 Spark UI 的 SQL tab，但 DBSQL 的 Query Profiler 是不同的工具：
- Query Profile 面板：Top Operators, Node Details, Timeline
- 识别瓶颈：scan volume, spill, shuffle, slow operators
- 与 EXPLAIN 的区别（你已知道这个）
- Query History 看历史执行模式

**晚上（1h）：System Tables + QP 自测（10 题）**

### Day 4: Liquid Clustering 深化 + Deletion Vectors + Delta Sharing

**上午（1.5h）：Liquid Clustering 完整知识**

你有基础，需要补全：
- `CLUSTER BY` 语法：CREATE TABLE ... CLUSTER BY (col1, col2)
- `ALTER TABLE ... CLUSTER BY` 变更 clustering keys
- 与 OPTIMIZE 的关系：LC 表的 OPTIMIZE 自动使用 clustering keys
- 不需要 Z-ORDER，不需要 PARTITION BY
- 与 Deletion Vectors 的协同
- 限制：哪些写入模式支持 cluster-on-write

**下午前半（1h）：Deletion Vectors**
- 工作原理：软删除标记，不重写 Parquet 文件
- 对 MERGE/UPDATE/DELETE 的性能提升
- `REORG TABLE APPLY (PURGE)` 物理清理
- 与 Liquid Clustering 组合优化 MERGE

**下午后半（1.5h）：Delta Sharing**
- Provider / Share / Recipient 三角模型
- Databricks-to-Databricks sharing vs Open sharing protocol
- 共享的对象类型：tables, schemas, notebooks, AI models
- 安全配置：activation links, token management
- Lakehouse Federation vs Delta Sharing 的选择

**晚上（1h）：综合自测（10 题）**

---

## Phase 3: 低权重补全 + 术语统一（Day 5）

### Day 5: Row Filters/Column Masks 深化 + 其他补全

**上午（1.5h）：Row Filters & Column Masks**

你有基础（Q241, Q281），需要补全实现细节：
- `CREATE FUNCTION` 定义 row filter / column mask UDF
- `ALTER TABLE ... SET ROW FILTER` / `SET COLUMN MASK`
- Dynamic Views vs Row Filters 的比较（Row Filters 更新更优）
- 与 UC 权限的交互：filter 在权限检查之后执行

**下午（2h）：术语统一 + 交叉复习**

新考纲的术语变化是一个隐性风险——你可能概念都懂，但因为术语不认识而选错：
- Delta Live Tables → Lakeflow Spark Declarative Pipelines
- DLT Pipeline → Declarative Pipeline
- Live Table → Materialized View（在 Lakeflow 语境下）
- Streaming Live Table → Streaming Table
- Multitask Jobs → Lakeflow Jobs
- Jobs Orchestration → Lakeflow Jobs orchestration
- Auto Loader → `read_stream` with `cloudFiles` format（保持不变）

**晚上（1h）：买 Udemy 118 题 Practice Exam，做第一套（59 题）**

---

## Phase 4: 实战模拟（Day 6-7）

### Day 6: Udemy Practice Exam + 新特性混合模拟

**上午（2h）：批改 Udemy 第一套 + 分析**

对每道错题：
1. 是旧知识还是新知识？
2. 如果是新知识，属于哪个新特性？
3. 回到 Phase 1-3 对应章节补强

**下午（2h）：做 Udemy 第二套（59 题）**

**晚上（1.5h）：批改 + 查漏补缺清单**

### Day 7: 综合模拟考

**上午（2h）：综合模拟考（59 题，严格限时 120 分钟）**

题目来源：
- 从旧 327 题中随机抽 35 题
- 从我生成的新特性题中抽 24 题
- 模拟真实考试比例

**下午：评分 + Go/No-Go 决策**

| 结果 | 行动 |
|------|------|
| ≥ 50/59 (85%) | 报名考试，选 Day 9 或 Day 10 |
| 47-49/59 (80-84%) | Day 8 补强 → 报名 |
| < 47/59 (<80%) | 延长 2-3 天，针对性强化 |

---

## Phase 5: 收尾（Day 8）

### Day 8: 最终复习 + 报名

**上午（2h）：**
1. 所有新特性规则最终默写
2. 术语映射表最终确认
3. 旧 24 条规则快速过一遍（防遗忘）

**下午：**
1. 报名考试
2. 考试策略清单确认：
   - [ ] DABs 题：关注 bundle deploy 流程和 databricks.yml 配置
   - [ ] Lakeflow 题：DLT 概念 + 新术语
   - [ ] System Tables 题：知道每个 system table 的用途
   - [ ] "minimal permissions" → 选权限最小的
   - [ ] "safest" ≠ "most efficient"
   - [ ] 每题 ≤ 2 min，不确定标记跳过

**晚上：不学新内容。早睡。**

---

## 附：新特性知识清单（学完打勾）

### DABs（预计 6-10 题）
- [ ] `databricks bundle` CLI 子命令
- [ ] `databricks.yml` 配置结构（resources, targets, workspace）
- [ ] 多环境部署流程（dev → staging → prod）
- [ ] CI/CD 集成模式
- [ ] 权限和变量配置

### Lakeflow Declarative Pipelines（预计 4-6 题）
- [ ] 新术语映射（DLT → Lakeflow）
- [ ] CREATE OR REFRESH STREAMING TABLE
- [ ] CREATE OR REFRESH MATERIALIZED VIEW
- [ ] Expectations: warn / drop / fail
- [ ] Pipeline UC 集成

### APPLY CHANGES API（预计 2-3 题）
- [ ] SCD Type 1 vs Type 2 参数
- [ ] APPLY CHANGES FROM SNAPSHOT
- [ ] Keys, sequence_by 参数

### System Tables（预计 3-5 题）
- [ ] billing.usage — 成本
- [ ] compute.clusters — 集群
- [ ] access.audit — 审计
- [ ] lakeflow.pipeline_event_log — pipeline 监控
- [ ] 基于 system table 创建 alerts

### Liquid Clustering（预计 2-3 题）
- [ ] CLUSTER BY 语法
- [ ] ALTER TABLE CLUSTER BY 变更 keys
- [ ] 与 Z-ORDER/PARTITION BY 的决策对比
- [ ] cluster-on-write 限制

### Delta Sharing（预计 1-2 题）
- [ ] Provider / Share / Recipient 模型
- [ ] D2D vs Open sharing
- [ ] 可共享对象类型

### Row Filters & Column Masks（预计 1-2 题）
- [ ] UDF 定义 + ALTER TABLE 绑定
- [ ] vs Dynamic Views

### Query Profiler（预计 1-2 题）
- [ ] Top Operators / Timeline
- [ ] vs EXPLAIN vs Spark UI

---

## 每日 Checklist

| Day | 主题 | 过关标准 |
|-----|------|---------|
| 1 | 考纲对标 + DABs | 9 道样题全对 + DABs 10 题 ≥ 8 对 |
| 2 | Lakeflow + APPLY CHANGES | 10 题 ≥ 8 对 |
| 3 | System Tables + Query Profiler | 10 题 ≥ 8 对 |
| 4 | LC + DV + Delta Sharing | 10 题 ≥ 8 对 |
| 5 | Row Filters + 术语统一 + Udemy 第一套 | Udemy ≥ 80% |
| 6 | Udemy 批改 + 第二套 | Udemy ≥ 85% |
| 7 | 综合模拟考（59 题） | ≥ 50/59 (85%) |
| 8 | 最终复习 + 报名 | 全部规则默写 |

---

## 与旧计划的关系

旧计划（24 道错题 × 4 模块）仍然有效，但它解决的是旧版考纲的长尾问题。新计划的优先级更高：

- **旧 24 道错题**：穿插在 Day 1-4 的晚间复习中（每天过 6 条规则，4 天过完）
- **旧 327 题模拟考**：不再单独做，并入 Day 7 综合模拟的旧题部分
- **Anki 卡片**：扩展到新特性（每天为当天学的新特性制作 Anki 卡片）
