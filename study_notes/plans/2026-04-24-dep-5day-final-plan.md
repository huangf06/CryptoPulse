# Databricks DEP 5 天精准打击计划

> 生成日期：2026-04-24
> 考试版本：November 30, 2025 blueprint
> 起点：旧 327 题 100%（模拟考满分），新考纲覆盖 ~0%
> 核心问题：新考纲 ~30-40% 内容完全空白
> 通过线：80%（59 题最多错 12 题）
> 目标：≥ 85%（最多错 9 题）
> 题库资源：SkillCertPro 1,265 题（线上平台）

---

## 形势判断

### 优势
- 旧版考纲 ~60% 内容完全掌握（327 题 100%）
- Spark 执行模型、Delta 基础、UC 权限、Structured Streaming —— 不会丢分
- 24 条旧错题规则已总结，只需考前快速过

### 致命缺口（按风险排序）

| 新考纲内容 | 涉及 Domain（权重） | 当前覆盖 | 预计题数 |
|-----------|-------------------|---------|---------|
| **Databricks Asset Bundles (DABs)** | D1 (22%) + D9 (10%) | 零 | 6-10 |
| **System Tables** | D5 (10%) | 零 | 3-5 |
| **APPLY CHANGES API** | D1 (22%) | 零 | 2-3 |
| **Lakeflow 术语 + 新语法** | D1 (22%) + D3 (10%) | 旧 DLT 基础 | 4-6 |
| **测试框架 (assertDataFrameEqual 等)** | D1 (22%) | 零 | 1-2 |
| **Query Profiler UI** | D5 (10%) + D6 (13%) | 零 | 1-2 |
| **Delta Sharing (D2D + Open)** | D4 (5%) | 零 | 1-3 |
| **Liquid Clustering 深化** | D6 (13%) + D10 (6%) | 基础 | 2-3 |
| **匿名化技术 + Data Purging** | D7 (10%) | 零 | 1-2 |
| **Job Repairs + Parameter Overrides** | D9 (10%) | 零 | 1-2 |

### 最坏/目标估算
- 不准备新内容：~36 旧题拿 34 + ~23 新题蒙对 5 = 39/59 (66%) → 不通过
- 完成本计划后：~36 旧题拿 34 + ~23 新题拿 18 = 52/59 (88%) → 稳定通过

---

## 每天的学习方法

统一流程：
1. **Claude 直讲** — 浓缩的、带例子的 lecture，不让你自己去读文档
2. **规则提炼** — 每个知识点一句话规则（与现有 24 条规则同格式）
3. **即时验证** — 每个知识点学完 2-3 道场景题
4. **晚间自测** — SkillCertPro 上按 domain 筛选 10-15 题

---

## Day 1：DABs 深潜（4-5h）

最大盲区，横跨 Domain 1 (22%) + Domain 9 (10%)，预计 6-10 题。

### 上午（2.5h）：DABs 核心概念

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 1 | `databricks bundle` CLI：init, validate, deploy, run, destroy | 给场景选正确命令 |
| 2 | `databricks.yml` 结构：resources（jobs, pipelines, models）、targets（dev/staging/prod）、workspace | 读配置片段判断行为 |
| 3 | 多环境部署流程：local → validate → deploy staging → promote prod | 排序/选正确步骤 |
| 4 | CI/CD 集成：GitHub Actions / Azure DevOps 中调用 bundle deploy | 选正确 CI/CD 配置 |
| 5 | 权限与变量：`${bundle.target}`, `${workspace.root_path}`, permissions 块 | 配置文件填空 |
| 6 | DABs 项目结构：src/、resources/、tests/ 模块化约定 | 项目组织选择题 |
| 7 | 第三方库管理：PyPI、local wheels、source archives 在 DABs 中配置 | 依赖管理场景 |

### 下午（1.5h）：Domain 9 剩余

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 8 | Databricks Git Folders：notebook/code 部署、Git 集成 | CI/CD 方案选择 |
| 9 | Job repairs：失败 job 修复运行 + parameter overrides | 失败场景修复方式 |
| 10 | DABs vs Git Folders vs REST API 的选择 | 方案对比题 |

### 晚上（1h）
- SkillCertPro 做 DABs/Deployment 相关题 10-15 道
- 过旧规则 Module A（7 条）+ Module B 前 2 条

---

## Day 2：Lakeflow + APPLY CHANGES + 测试框架（4h）

DLT 基础已有，核心是术语映射 + 新 API + 测试。

### 上午（1.5h）：Lakeflow Spark Declarative Pipelines

| # | 知识点 | 你的基础 → 需补 |
|---|--------|----------------|
| 11 | 术语映射（6 对）内化 | 有映射表 → 题干中即时识别 |
| 12 | `CREATE OR REFRESH STREAMING TABLE` / `MATERIALIZED VIEW` 新语法 | 知 DLT 语法 → SQL 语法差异 |
| 13 | Expectations：`ON VIOLATION {WARN/DROP/FAIL}` 三种模式精确行为 | 旧基础 → 精确行为 |
| 14 | Pipeline 配置：serverless compute, enhanced autoscaling, UC 集成 | 零 → 新选项和行为 |
| 15 | ST vs MV 优劣完整对比 | Q268/Q310 → 考纲要求 "explain advantages and disadvantages" |
| 16 | Pipeline 控制流：if/else, for/each（含 Q227 闭包问题） | Q227 → 完整模式 |

### 下午前半（1h）：APPLY CHANGES API

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 17 | `APPLY CHANGES INTO target FROM source` 完整语法 | 语法选择 |
| 18 | SCD Type 1 vs Type 2：`stored_as_scd_type` 参数 | 场景选 SCD 类型 |
| 19 | Keys, sequence_by, columns 参数作用 | 参数理解 |
| 20 | `APPLY CHANGES FROM SNAPSHOT`（快照 CDC）| 与常规的区别 |

### 下午后半（1h）：测试框架（考纲新增，旧题零覆盖）

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 21 | `assertDataFrameEqual(actual, expected)` | 单元测试代码选择 |
| 22 | `assertSchemaEqual(schema1, schema2)` | schema 验证场景 |
| 23 | `DataFrame.transform(func)` 链式调用 | 代码组织 |
| 24 | Databricks 内置 debugger | 调试方法选择 |

### 晚上（0.5h）
- SkillCertPro 做 Lakeflow/CDC/Testing 相关题 10-15 道
- 过旧规则 Module B 剩余 4 条 + Module C（5 条）

---

## Day 3：System Tables + 监控全链路（4h）

Domain 5 (10%) 几乎从零开始，第二大盲区。

### 上午（2h）：System Tables

| # | System Table | 用途 | 典型查询 |
|---|-------------|------|---------|
| 25 | `system.billing.usage` | 成本分析 | 按 workspace/cluster/SKU 统计 |
| 26 | `system.compute.clusters` | 集群监控 | 闲置集群、利用率 |
| 27 | `system.access.audit` | 审计日志 | 谁/什么时候/做了什么 |
| 28 | `system.lakeflow.pipeline_event_log` | Pipeline 监控 | 失败诊断、质量事件 |
| 29 | `system.storage.predictive_optimization` | PO 历史 | 优化追踪 |

### 下午前半（1h）：Query Profiler + 诊断工具对比

| # | 知识点 | 与已知的关系 |
|---|--------|------------|
| 30 | DBSQL Query Profile：Top Operators, Node Details, Timeline | Spark UI SQL tab 的 DBSQL 等价物 |
| 31 | 瓶颈识别：scan volume, spill, shuffle, slow operators | 与 Q245/Q280 关联但工具不同 |
| 32 | Query Profile vs EXPLAIN vs Spark UI 三者对比 | 高频考点 |

### 下午后半（1h）：Event Logs + Alerting

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 33 | Lakeflow Event Logs 结构和关键事件类型 | 给错误日志选诊断方向 |
| 34 | REST API / Databricks CLI 监控 jobs 和 pipelines | 方案选择 |
| 35 | SQL Alerts：query + condition + notification | 数据质量监控方案 |
| 36 | Lakeflow Jobs UI 通知：job status + performance | 告警配置 |

### 晚上（0.5h）
- SkillCertPro 做 Monitoring/Alerting 相关题 10-15 道
- 过旧规则 Module D（6 条）

---

## Day 4：剩余缺口扫清（4h）

所有中等风险缺口一天清完。

### 上午（1.5h）：Delta Sharing + Lakehouse Federation

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 37 | Provider / Share / Recipient 三角模型 | 架构理解 |
| 38 | D2D Sharing vs D2O (Open Protocol) | 场景选择 |
| 39 | 可共享对象：tables, schemas, notebooks, AI models | 选择题 |
| 40 | 安全配置：activation links, token management | 安全题 |
| 41 | Lakehouse Federation vs Delta Sharing 选择 | 对比题（Q314 基础） |

### 下午前半（1h）：Liquid Clustering + Deletion Vectors 深化

| # | 知识点 | 已有 → 需补 |
|---|--------|------------|
| 42 | `CLUSTER BY` / `ALTER TABLE CLUSTER BY` 精确语法 | 概念 → 语法 |
| 43 | LC 表 OPTIMIZE 行为 + cluster-on-write 限制 | 模糊 → 精确 |
| 44 | LC vs Partition vs ZOrder 三选一决策树 | Q240 → 考纲 D10 要求 |
| 45 | Deletion Vectors 机制 + `REORG TABLE APPLY (PURGE)` | Q288 → 新语法 |
| 46 | CDF 解决 Streaming Table 限制 | 基础 → 考纲 D6 要求 |

### 下午后半（1.5h）：Security 补全

| # | 知识点 | 考点方向 |
|---|--------|---------|
| 47 | Row Filters & Column Masks 完整实现（CREATE FUNCTION + ALTER TABLE） | 精确语法 |
| 48 | 匿名化对比：Hashing vs Tokenization vs Suppression vs Generalisation | 给场景选技术 |
| 49 | PII masking pipeline（batch + streaming）设计模式 | 合规 pipeline 题 |
| 50 | Data purging 方案（合规删除实现） | 合规题 |

### 晚上（0.5h）
- SkillCertPro 做混合题 10-15 道（Sharing + LC + Security）
- 旧 24 条规则最终快速过一遍（全部）

---

## Day 5：综合模拟考 + Go/No-Go（3-4h）

### 上午（2h）：59 题综合模拟考

使用 SkillCertPro 的模拟考功能，严格限时 120 分钟。

### 下午：批改 + 决策

| 结果 | 行动 |
|------|------|
| ≥ 50/59 (85%) | 当天报名，选最近可用日期 |
| 47-49/59 (80-84%) | 用 1 天补强暴露弱点，然后报名 |
| < 47/59 (<80%) | 分析系统性弱点，延长 2-3 天 |

错题分析模板（如有）：
1. 是旧知识还是新知识？
2. 属于哪个 Domain / 知识点？
3. 一句话修正规则

---

## 50 个知识点总览

| Day | 数量 | 核心主题 |
|-----|------|---------|
| 1 | 10 | DABs CLI/配置/部署/CI-CD + Git Folders + Job Repairs |
| 2 | 14 | Lakeflow 术语/语法 + APPLY CHANGES + 测试框架 |
| 3 | 12 | System Tables (5) + Query Profiler + Event Logs + Alerting |
| 4 | 14 | Delta Sharing + LC/DV 深化 + 匿名化/purging |
| 5 | 0 | 模拟考（纯验证） |

## SkillCertPro 使用策略

- **不要从头刷 1,265 题**
- 每天晚间按 domain/topic 筛选做 10-15 道，5 天共 50-70 道
- Day 5 用平台模拟考功能做一套完整 59 题
- 做错的题只记规则到 review_notes.md，不搬原题
- 总共接触 ~130 道新题，足够

## 旧 24 条规则维护

穿插在 Day 1-4 晚间，每天过 6 条：
- Day 1：Module A (7) + Module B 前 2 条
- Day 2：Module B 剩余 4 条 + Module C (5)
- Day 3：Module D (6)
- Day 4：全部 24 条最终过一遍

## 与旧计划的关系

本计划替代以下所有旧计划：
- `2026-04-24-exam-attack-plan.md`（10 天攻坚 → 被替代）
- `2026-04-24-new-exam-plan.md`（8 天新考纲 → 被替代）
- `2026-04-25-3day-final-sprint.md`（3 天旧题冲刺 → 被吸收为晚间规则复习）
