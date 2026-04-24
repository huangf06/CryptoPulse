# SkillCertPro Mock Exam 1 - 分析报告

> 来源：skillcertpro.com | 60题 | 用时：01:03:33 | 日期：2026-04-24

## 总分：40/60 (66.7%) -- 未达 80% 通过线

## 错题汇总 (20题错)

---

### Q1 -- 监控异常用户行为
**题目：** What strategy would you deploy to monitor for and alert on anomalous user behavior within the Databricks environment?

**正确答案：** A. Integrate Databricks with Azure Sentinel to analyze and detect unusual access patterns or data queries.

**解析：** Azure Sentinel（现 Microsoft Sentinel）是 Microsoft 的云原生 SIEM/SOAR 解决方案，专为安全分析设计。将 Databricks audit logs 接入 Sentinel 可以利用其内置的 ML 异常检测能力和自动化 playbook 来发现异常用户行为。相比之下：B（Azure Monitor + KQL）更偏基础设施监控而非安全分析；C（自定义 Spark 算法）成本高且重复造轮子；D（手动审计）不可扩展。

**知识点：** Azure Sentinel = SIEM for security analytics; Databricks audit log integration

---

### Q5 -- Notebook version history 保留时长
**题目：** A data engineer wants to see the version history of their notebook which they did not add to Git. How many days of version history will be visible?

**正确答案：** The version history can be accessed from the time the notebook was created.

**解析：** Databricks notebook 自带版本历史，从创建时起一直保留，不限天数（除非手动清除）。这与 Git 无关 — 即使不接入 Git，notebook 的每次保存都会自动创建版本快照。常见陷阱是以为有 30/60 天的限制，实际上没有。

**知识点：** Databricks notebook revision history — 不限时保留，从创建起可追溯

---

### Q6 -- Can Restart 权限不能做什么
**题目：** A Databricks admin provided Can Restart permission to data_analysts group. Which cluster action cannot be performed?

**正确答案：** Edit the cluster

**解析：** Can Restart 权限允许：attach to cluster, restart, terminate, view Spark UI, view cluster metrics。唯一不能做的是 **Edit the cluster**（修改集群配置），这需要 Can Manage 权限。权限层级：Can Attach To < Can Restart < Can Manage。

**知识点：** Cluster permissions 层级: Can Attach To (view only) → Can Restart (+ restart/terminate) → Can Manage (+ edit config)

---

### Q7 -- Job 运行权限
**题目：** Grant permission to run a job via Jobs UI but prevent accidental deletion?

**正确答案：** The junior data engineer should be given the Can Manage Run permission.

**解析：** Job 权限层级：
- **Is Owner**: 完全控制（编辑、删除、运行）
- **Can Manage**: 可编辑、删除、运行
- **Can Manage Run**: 可触发运行、查看运行结果，但**不能**编辑或删除 job
- **Can View**: 只能查看 job 定义和运行历史，**不能**触发运行

题目要求"能运行 + 不能删除"→ Can Manage Run 是精确匹配。

**知识点：** Job permissions: Can View < Can Manage Run < Can Manage < Is Owner

---

### Q8 -- Join 后 ambiguous column 错误
**题目：** Error when running code with join — possible reason?

**正确答案：** medal column exists in both the DataFrames

**解析：** 当两个 DataFrame join 后 select `medal` 列，如果两边都有同名列，Spark 无法判断你要哪个，抛出 `AnalysisException: Reference 'medal' is ambiguous`。解决方案：用 `df1["medal"]` 或 `df2["medal"]` 指定来源，或者 join 前 rename 其中一个。

**知识点：** Spark ambiguous column reference in joins; 解决方法: alias, col("table.column"), drop duplicate

---

### Q10 -- 部署前确保数据质量
**题目：** Strategy to ensure updated pipeline maintains data quality before production deployment?

**正确答案：** A: Implement unit and integration tests within Databricks notebooks that validate data outputs against a controlled set of test data, integrating these tests into your CI/CD pipeline.

**解析：** 自动化测试集成到 CI/CD 是确保数据质量的最佳实践。相比之下：B（手动验证）不可扩展；C（ADF 并行运行）增加复杂度且不是标准做法；D（MLflow 统计分析）适用于 ML 实验，不是通用数据质量保障。关键词是「before deployed to production」→ CI/CD pipeline 中的自动化测试。

**知识点：** Data quality assurance = automated tests in CI/CD pipeline, not manual validation

---

### Q12 -- Zero Trust Architecture for Databricks
**题目：** How to implement Zero Trust for Azure Databricks?

**正确答案：** A. By integrating Azure Databricks with Azure Active Directory and enforcing Conditional Access policies based on user and device risk levels

**解析：** Zero Trust 核心原则："never trust, always verify" — 不基于网络位置假设信任。AAD + Conditional Access 实现了这一点：根据用户身份、设备状态、风险等级动态决策访问。相比之下：B（VPN only）是传统网络边界思维，恰恰是 Zero Trust 要替代的模式；C（NSG）是网络层控制；D（Bastion）是跳板机方案。只有 A 实现了身份驱动的动态信任评估。

**知识点：** Zero Trust = identity-based (AAD + Conditional Access), not network-based (VPN/NSG)

---

### Q15 -- Spark Streaming stateful transformations
**题目：** Fine-tuning Spark for Low-latency Streaming — how to minimize processing time per micro-batch while ensuring complete stateful accuracy?

**你的答案：** B. Enable spark.streaming.receiver.maxRate and set a high spark.sql.streaming.metricsEnabled value.
**正确答案：** A. Implement stateful transformations using mapGroupsWithState with a low trigger interval.

**解析：** 题目要求「stateful accuracy + low latency」。`mapGroupsWithState` 是 Structured Streaming 中实现自定义有状态处理的核心 API，配合低 trigger interval 可以同时满足状态完整性和低延迟。B 选项的 `receiver.maxRate` 是老版 DStream API 的参数，`metricsEnabled` 只是开启监控指标，两者都不解决有状态处理的问题。

**知识点：** Structured Streaming stateful operations, mapGroupsWithState vs flatMapGroupsWithState

---

### Q24 -- AAD token-based access to ADLS Gen2
**题目：** Which configuration is **irrelevant** for setting up AAD token-based access to Azure Data Lake Storage Gen2?

**你的答案：** D. spark.hadoop.fs.azure.account.oauth2.client.id
**正确答案：** B. spark.hadoop.fs.azure.simple.httpclient.retry.policy.type

**解析：** AAD OAuth 认证 ADLS Gen2 需要：auth.type（设为 OAuth）、oauth.provider.type（设为 ClientCredsTokenProvider）、oauth2.client.id（应用 ID）、client.secret、client.endpoint。`retry.policy.type` 是 HTTP 客户端重试策略配置，与 OAuth 认证流程无关。client.id 是必须的，你看错了题目要求（问的是 irrelevant）。

**知识点：** Azure ADLS Gen2 OAuth configuration, Spark Hadoop Azure connector settings

---

### Q27 -- Change Data Feed table_changes() 输出
**题目：** CDF enabled table，经过 CREATE(v0) → INSERT(v1) → UPDATE(v2) → INSERT(v3) → DELETE(v4)，`SELECT max(_commit_version), max(version) FROM table_changes('versions', 2)` 的结果？

**你的答案：** max(_commit_version)=3, max(version)=5.1.0
**正确答案：** max(_commit_version)=4, max(version)=6.2.0

**解析：** `table_changes('versions', 2)` 返回从 version 2 开始的所有变更记录。
- v2 (UPDATE): 产生 pre_image (version='6.2.0') 和 post_image (version='5.1.0')
- v3 (INSERT): version='1.3.0'
- v4 (DELETE): pre_image version='1.3.0'
所有版本号 max 按字符串比较：'6.2.0' > '5.1.0' > '1.3.0'，所以 max(version) = '6.2.0'（来自 v2 的 pre_image）。max(_commit_version) = 4。

**关键点：** UPDATE 的 CDF 记录包含 pre_image（旧值），字符串比较 '6' > '5' > '1'。

**知识点：** Delta Lake Change Data Feed, table_changes(), _commit_version, pre/post image

---

### Q29 -- Alert status OK 含义
**题目：** Alert status 显示 OK，如何解读？

**你的答案：** The OK state signifies that the alert is functioning correctly without any errors.
**正确答案：** OK state means that the alert may or may not be triggered in the past but the alert condition is not met in the most recent execution.

**解析：** Databricks SQL Alert 有三种状态：OK（条件未触发）、TRIGGERED（条件触发）、UNKNOWN（未执行过）。OK 不是指"运行正常"，而是指"最近一次执行中，alert 条件不满足"。历史上可能触发过，但当前状态是未触发。

**知识点：** Databricks SQL Alerts 三种状态：OK / TRIGGERED / UNKNOWN

---

### Q33 -- jobs/reset vs jobs/update REST API
**题目：** 2.0/jobs/update 和 2.0/jobs/reset 的区别？

**你的答案：** reset 用 default settings 覆盖，update 用来 add/change/remove specific settings
**正确答案：** reset 用 JSON payload 中的 settings 覆盖所有设置，update 用来 add/change/remove specific settings

**解析：** 关键区别：reset 不是恢复默认值，而是用你提供的 JSON 完全替换所有设置（相当于 PUT）；update 是部分更新（相当于 PATCH）。你混淆了「default settings」和「JSON payload settings」。

**知识点：** Databricks REST API jobs/reset (full replace) vs jobs/update (partial update)

---

### Q34 -- Package cells
**题目：** Package cells in Databricks notebook 的正确描述？

**你的答案：** A package cell returns an executable file when executed.
**正确答案：** A package cell is compiled when executed.

**解析：** Databricks notebook 中的 Package cell（Scala）在执行时会被编译，而不是返回可执行文件。Package cell 允许你定义可被其他 cell 引用的包/类，编译是 Scala/JVM 的基本行为。

**知识点：** Databricks notebook Package cells (Scala), compilation behavior

---

### Q37 -- AQE (Adaptive Query Execution) 能力
**题目：** 哪个 data engineer 的说法 not completely correct？

**你的答案：** Data Engineer 2 - Helps in saving computing costs by combining small tasks.
**正确答案：** Data Engineer 3 - Handles skews dynamically in stream-static joins.

**解析：** AQE 三大能力：(1) 动态合并 shuffle partitions（coalescing small partitions → DE2 正确），(2) 动态转换 join 策略（sort-merge → broadcast → DE4 正确），(3) 动态处理 skew joins。但 AQE 的 skew handling 适用于 sort-merge joins，不是 stream-static joins。Streaming 查询不使用 AQE。DE3 不完全正确。

**知识点：** AQE: coalesce partitions, convert join strategies, handle skew (batch only, not streaming)

---

### Q44 -- Data skipping 错误说法
**题目：** 关于 data skipping 哪个说法 not true？

**你的答案：** All fields inside a nested column are taken as individual columns while collecting statistics.
**正确答案：** Data skipping features are not enabled by default and need to be enabled using delta.dataSkipping table property.

**解析：** Data skipping 在 Delta Lake 中是**默认启用**的，不需要手动开启。它自动收集前 32 列的 min/max 统计信息。所以选项 A 的说法是错误的（不需要手动启用）。嵌套列的字段确实会被视为独立列来收集统计信息，选项 C 实际上是正确的。

**知识点：** Delta Lake data skipping 默认启用，自动收集前 32 列统计信息

---

### Q46 -- REST API jobs/create 返回值
**题目：** 2.0/jobs/create 的有效 JSON 响应？

**你的答案：** { job_id: 13746 } （无引号的 key）
**正确答案：** { "job_id": 13746 } （标准 JSON 格式）

**解析：** 标准 JSON 要求 key 必须用双引号包裹。`job_id: 13746` 不是合法 JSON（是 JavaScript 对象字面量语法）。注意 value 是数字 13746 不是字符串 "13746"。

**知识点：** JSON 格式规范，Databricks REST API response format

---

### Q49 -- Notebook export .ipynb 后的 outputs 和 Spark UI
**题目：** .ipynb 导出（不清除 outputs）后导入，command outputs 和 Spark UI logs 的可见性？

**你的答案：** Command outputs visible, Spark UI logs visible only if attached to same cluster
**正确答案：** Command outputs visible, Spark UI logs NOT visible even if attached to same cluster

**解析：** .ipynb 文件会保存 cell outputs（因为没清除），所以导入后 outputs 可见。但 Spark UI logs 是集群运行时产生的，存储在集群的 Spark History Server 中，与 notebook 文件无关。即使挂载到同一集群，导入的 notebook 也无法关联到之前的 Spark job，所以 Spark UI logs 不可见。

**知识点：** .ipynb export preserves cell outputs but NOT Spark UI context

---

### Q52 -- REST API jobs/create JSON payload
**题目：** 创建 spark_python_task job 的正确 JSON？

**你的答案：** 使用 python_task + python_file_path + parameters（字符串格式）
**正确答案：** 使用 spark_python_task + python_file + parameters（数组格式）+ existing_cluster_id

**解析：** Databricks REST API 的正确字段：
- `existing_cluster_id`（不是 existing_cluster）
- `spark_python_task`（不是 python_task）
- `python_file`（不是 python_file_path）
- `parameters` 是 JSON 数组（不是字符串化的数组）
- `name` 字段是可选的（正确答案里没有 name 也是合法的）

**知识点：** Databricks REST API 2.0/jobs/create: spark_python_task, existing_cluster_id, python_file, parameters

---

### Q54 -- DLT events log (无 storage setting)
**题目：** 未设置 storage 时 DLT events log 的位置？

**你的答案：** /pipelines/system/events
**正确答案：** /pipelines/{pipeline-id}/system/events

**解析：** 当 DLT pipeline 没有设置 storage location 时，events log 存储在 DBFS 的 `/pipelines/{pipeline-id}/system/events`。注意路径中包含 `{pipeline-id}` 来区分不同 pipeline。对比 Q41（有 storage 设置时路径是 `{storage}/system/events`），无 storage 时使用 DBFS 默认路径。

**知识点：** DLT events log: 有 storage → {storage}/system/events; 无 storage → /pipelines/{pipeline-id}/system/events

---

### Q57 -- DLT Python 不支持的操作
**题目：** DLT with Python 不支持什么？

**你的答案：** read() function
**正确答案：** pivot() operation

**解析：** DLT Python API 不支持 `pivot()` 操作。`spark.read()` 在 DLT 中虽然推荐用 `dlt.read()` 和 `spark.readStream`，但 `spark.read` 本身是可用的。DLT 支持 import、views（通过 @dlt.view 或 @dlt.table）、Python decorators（这是 DLT Python API 的核心机制）。

**知识点：** DLT Python API limitations: pivot() not supported

---

## 错题分类分析

| 分类 | 题号 | 数量 |
|------|------|------|
| Azure Security / Zero Trust / SIEM | Q1, Q12, Q24 | 3 |
| Databricks REST API | Q33, Q46, Q52 | 3 |
| Databricks Permissions (Cluster/Job) | Q6, Q7 | 2 |
| Delta Lake 特性 (CDF, data skipping) | Q27, Q44 | 2 |
| Spark Streaming / AQE | Q15, Q37 | 2 |
| DLT (Delta Live Tables) | Q54, Q57 | 2 |
| Databricks SQL (Alerts) | Q29 | 1 |
| Databricks Notebook 特性 | Q5, Q34, Q49 | 3 |
| Spark DataFrame (ambiguous column) | Q8 | 1 |
| CI/CD & Testing | Q10 | 1 |

## 薄弱领域（按严重程度排序）

1. **Azure Security 概念题 (Q1, Q10, Q12)** — 这类"最佳实践"选择题，答案通常是最 enterprise/automated 的方案。记住：Sentinel=SIEM、AAD+Conditional Access=Zero Trust、CI/CD=数据质量。
2. **Databricks REST API (Q33, Q46, Q52)** — 需要精确记忆 API 字段名、JSON 格式、reset vs update 语义。
3. **Databricks 产品特性细节 (Q5, Q29, Q34, Q49)** — notebook revision history 不限时、alert 三态、package cell 编译、.ipynb 不含 Spark UI。
4. **Permissions 模型 (Q6, Q7)** — 记住 cluster 和 job 的权限层级。
5. **Delta Lake / DLT / Streaming (Q15, Q27, Q37, Q44, Q54, Q57)** — CDF pre_image、data skipping 默认启用、AQE 不适用 streaming、DLT pivot 不支持。

## 关键记忆点速查表

| 知识点 | 要记住的 |
|--------|---------|
| Notebook version history | 不限天数，从创建起保留 |
| Cluster permissions | Can Attach To < Can Restart < Can Manage |
| Job permissions | Can View < Can Manage Run < Can Manage < Is Owner |
| Alert states | OK / TRIGGERED / UNKNOWN |
| jobs/reset vs update | reset = PUT (全量替换), update = PATCH (部分修改) |
| jobs/create response | `{"job_id": 13746}` — 数字，有引号 key |
| spark_python_task | python_file (不是 path), parameters (数组) |
| Data skipping | 默认启用，前 32 列统计 |
| CDF table_changes | 包含 pre_image，注意字符串比较 |
| AQE | 仅 batch，不适用 streaming |
| DLT Python | pivot() 不支持 |
| DLT events log | 有 storage: {storage}/system/events; 无: /pipelines/{id}/system/events |
| .ipynb export | 保留 cell output，不保留 Spark UI |
| Zero Trust | 身份驱动 (AAD + Conditional Access)，不是网络驱动 (VPN) |
| Sentinel | SIEM/SOAR，用于安全异常检测 |
