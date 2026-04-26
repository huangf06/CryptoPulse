# SkillCertPro Mock Exam 2 - 分析报告

> 来源：skillcertpro.com | 60题 | 用时：01:02:00 | 日期：2026-04-24

## 总分：54/60 (90%) -- 通过 (80% 通过线)

## 错题汇总 (6题错)

---

### Q9 -- Time-series 数据建模
**题目：** Designing a data model for time-series data in a lakehouse. Which approach optimizes query performance for time-based aggregations?

**你的答案：** B. Use a flat wide table structure that pre-aggregates data at daily intervals.
**正确答案：** C. Implement a star schema with a time dimension table that includes various time hierarchies.

**解析：** Star schema 是 time-series 分析的标准建模方式。Time dimension table 包含多层时间层级（year/quarter/month/week/day/hour），支持灵活的 drill-down/roll-up 分析。flat wide table 预聚合虽然查询快，但丢失了粒度灵活性 — 如果预聚合到 daily 级别，就无法做 hourly 分析。Star schema 保留原始粒度的同时通过 dimension table 提供多层聚合能力。

**知识点：** Star schema = time-series 建模首选；time dimension table 提供层级聚合灵活性

---

### Q12 -- Multi-table join 优化
**题目：** In a multi-table join with varying data sizes, which technique minimizes shuffle?

**你的答案：** B. Enforcing a uniform repartition across all tables before joining.
**正确答案：** A. Applying broadcast hints selectively based on table sizes and existing statistics.

**解析：** "Varying data sizes" 是关键词 — 有大表有小表。对小表用 broadcast hint 可以避免 shuffle（小表广播到所有节点，大表不动）。uniform repartition 所有表 = 强制所有表做全量 shuffle，反而增加开销。正确策略是**选择性**地对小表 broadcast，而非一刀切。

**知识点：** varying sizes → broadcast hints (selective)；uniform repartition = 全量 shuffle = 浪费

---

### Q19 -- Data Exfiltration Prevention
**题目：** Prevent data exfiltration while allowing legitimate data operations?

**你的答案：** B. Utilizing Databricks runtime features for data encryption and access controls, relying on their default configurations.
**正确答案：** A. Implementing strict network security controls, including NSGs and Azure Private Link, to limit outbound traffic.

**解析：** Data exfiltration = 数据外泄，是**网络层**问题。防止数据外泄需要控制 outbound traffic（出站流量），这是 NSG 和 Private Link 的职责。Databricks 默认的加密和访问控制保护的是 data at rest/in transit 和未授权访问，但不能阻止已授权用户把数据发到外部。关键词 "exfiltration" → 网络出口控制。

**知识点：** exfiltration prevention = 网络层控制 (NSG + Private Link)，不是加密或访问控制

---

### Q23 -- Streaming job 性能实时监控
**题目：** Monitor streaming job performance in real-time?

**你的答案：** C. Configuring Azure Event Hubs to capture streaming metrics and analyze them in Azure Monitor.
**正确答案：** B. Databricks Spark UI for streaming jobs to view real-time performance metrics.

**解析：** Spark UI 有专门的 Structured Streaming tab，直接展示每个 micro-batch 的 input rate、processing rate、batch duration 等实时指标。这是 Databricks **内置**的最直接的监控方式。Event Hubs + Azure Monitor 是间接方案，增加了不必要的复杂度。题目问的是"tool or feature"，Spark UI 就是那个 feature。

**决策规则：** 监控 Spark/Streaming 性能 → 首选 Spark UI（内置、实时、零配置）

---

### Q24 -- ML-based Data Quality Testing
**题目：** Advanced testing framework using ML for data quality?

**你的答案：** D. Using Databricks MLflow for model management, automating deployment into production pipelines.
**正确答案：** C. Leveraging Azure Machine Learning to periodically retrain data quality models, deploying as web services called by Databricks jobs.

**解析：** 这题考的是**架构选择**。Azure ML 提供完整的 model retraining + web service deployment pipeline，可以被 Databricks jobs 以 REST API 调用。MLflow 是模型追踪和 registry 工具，它管理模型版本但不提供独立的 web service hosting 能力（MLflow serving 在 Databricks 中可用但不如 Azure ML 的 managed endpoint 成熟）。题目强调 "periodically retrain + deploy as web services" → Azure ML 的标准用例。

**知识点：** Azure ML = managed model training + web service endpoint；MLflow = experiment tracking + model registry

---

### Q54 -- Multi-dimensional Analytics 数据建模
**题目：** Data model for multi-dimensional analytics for a retail company?

**你的答案：** C. Normalize the data into multiple related tables to reduce redundancy.
**正确答案：** A. Implement a star schema with dimension tables for time, geography, product, and customer demographics.

**解析：** Multi-dimensional analytics = OLAP = Star Schema。这是数据仓库/lakehouse 的基本设计原则。3NF normalization 适合 OLTP（减少冗余、保证一致性），但 OLAP 场景下 normalization 导致大量 join，查询性能差。Star schema 通过适度反规范化（dimension tables）换取查询性能。

**注意：** 你在 Q9 和 Q54 犯了同类错误 — 两次都没选 star schema。这说明你对 OLAP 建模的默认答案（star schema）还不够条件反射。

**知识点：** multi-dimensional / OLAP / BI analytics → Star Schema（几乎永远是正确答案）

---

## 错题分类分析

| 分类 | 题号 | 数量 |
|------|------|------|
| 数据建模 (Star Schema) | Q9, Q54 | 2 |
| Spark 性能优化 (join strategy) | Q12 | 1 |
| Azure 安全 (exfiltration) | Q19 | 1 |
| Azure 监控工具选择 | Q23 | 1 |
| Azure ML vs MLflow | Q24 | 1 |

## 关键发现

**1. Star Schema 盲区 (2题)**
Q9 和 Q54 是同一个知识缺口：OLAP / multi-dimensional analytics / time-series → Star Schema。你两次都选了其他方案（flat table / normalization）。

**考试规则：** 只要题目出现以下关键词，答案大概率是 star schema：
- multi-dimensional analytics
- time-series with multiple hierarchies
- BI / reporting / dashboard
- fact + dimension

**2. "最直接的内置工具" 偏好 (Q23)**
Databricks/Spark 内置功能 > Azure 外部集成。监控 streaming → Spark UI，不需要 Event Hubs 绕一圈。

**3. Exfiltration ≠ Encryption (Q19)**
记住安全问题的层次：
- data at rest → encryption
- data in transit → TLS/SSL
- unauthorized access → RBAC / ACL
- data exfiltration → **network controls** (NSG, Private Link, firewall)

---

## 两套 Mock Exam 对比

| 指标 | Mock 1 | Mock 2 | 变化 |
|------|--------|--------|------|
| 总分 | 40/60 (66.7%) | 54/60 (90%) | +23.3% |
| 错题数 | 20 | 6 | -14 |
| 用时 | 01:03:33 | 01:02:00 | -1:33 |
| Azure 安全题错误 | 4 | 2 | -2 |
| Spark 性能题错误 | 2 | 1 | -1 |
| 数据建模题错误 | 0 | 2 | +2 (新暴露) |
| REST API 错误 | 3 | 0 | -3 |
| DLT/Delta 细节错误 | 4 | 0 | -4 |
| 权限/平台行为错误 | 7 | 0 | -7 |
