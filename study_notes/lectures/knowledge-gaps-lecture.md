# 知识缺口补齐讲义 — SkillCertPro Mock Exam 1 错题分析

> 基于 20/60 错题提取的 5 个系统性知识缺口。每节讲透一个模型，不是罗列答案。

---

## 第一章：Databricks 权限与平台行为模型

**错题：Q5, Q6, Q7, Q8, Q29, Q34, Q49 (7题)**

这 7 题表面看题目各不相同，但背后是同一个缺口：你没有建立 Databricks 平台各组件的**行为模型**。你在用「常识猜测」代替「规则查表」。

### 1.1 权限层级 — 一套统一的心智模型

Databricks 的权限设计遵循一个通用模式：**每一级权限都完整包含下一级的所有能力**。这不是随机设计，而是 RBAC 的标准分层。

**Cluster Permissions（三级）：**

```
Can Attach To          Can Restart              Can Manage
─────────────────────────────────────────────────────────────
attach notebooks       attach notebooks         attach notebooks
view Spark UI          view Spark UI            view Spark UI
view metrics           view metrics             view metrics
                       restart cluster          restart cluster
                       terminate cluster        terminate cluster
                                                EDIT cluster config
                                                modify permissions
```

**决策规则：** "能不能做 X？" → 找到 X 在哪一级，看用户的权限是否 >= 那一级。

Q6 错在：你不确定 terminate 和 edit 分别属于哪一级。Terminate 是 Can Restart 级别的（restart 和 terminate 是同级操作），Edit 需要 Can Manage。

**Job Permissions（四级）：**

```
Can View               Can Manage Run           Can Manage            Is Owner
──────────────────────────────────────────────────────────────────────────────
view job definition    view job definition      view job definition   view job definition
view run history       view run history         view run history      view run history
                       trigger new runs         trigger new runs      trigger new runs
                       cancel runs              cancel runs           cancel runs
                                                EDIT job settings     EDIT job settings
                                                DELETE job            DELETE job
                                                modify permissions    modify permissions
                                                                      transfer ownership
```

Q7 错在：你不知道 Can Manage Run 这一级的存在。这是 Databricks 特有的中间层 — 大多数平台只有 view/manage 两级，Databricks 加了 "manage run" 来解决"让人跑 job 但不让改 job"的需求。

**Secrets Permissions（三级）：**

```
READ                   WRITE                    MANAGE
─────────────────────────────────────────────────────
read secret value      read secret value        read secret value
                       write/delete secrets     write/delete secrets
                                                change ACLs
```

注意：这里没有 EXECUTE 和 CREATE。考试爱在选项里塞不存在的权限级别。

**记忆技巧：** 所有 Databricks 权限都遵循"读 < 写/运行 < 管理"的三段式。Job 多了一个 Can Manage Run 夹在中间，是唯一的例外。

### 1.2 Notebook 行为模型

Databricks notebook 有几个容易混淆的行为：

**版本历史：** 从创建起无限期保留（除非手动清除）。没有 30 天、60 天的限制。这与 Git 无关 — notebook 自带的 revision history 是独立系统。

Q5 错因：你可能把 Databricks notebook history 和某些 SaaS 产品的"30天免费保留"混淆了。

**Package Cells（Scala only）：**

- Package cell 用 `package` 关键字声明，执行时会被**编译**（compile）
- 不是生成可执行文件，不是解释执行
- 编译后的 class/object 可以被同 notebook 的其他 cell 引用
- 这是 Scala/JVM 的基本行为：声明 package → 编译 → classloader 加载

Q34 错因：你把 "compile" 和 "produce executable" 混为一谈。编译产出的是 JVM bytecode（class 文件），不是独立可执行文件。

**.ipynb 导出/导入：**

核心模型：.ipynb 文件 = notebook 文本 + cell outputs（如果没清除）。它是一个**静态快照**。

```
.ipynb 包含的：          .ipynb 不包含的：
─────────────           ──────────────
cell source code        Spark UI logs
cell outputs            Spark job metadata
cell metadata           cluster association
markdown content        execution context
```

Spark UI logs 存储在 Spark History Server（集群级别），和 notebook 文件完全解耦。即使你把 .ipynb 挂载到原来的集群，它也是一个"新的 notebook"，不会关联到历史 Spark jobs。

Q49 错因：你以为 Spark UI 和 notebook 之间有某种绑定关系。实际上 Spark UI 绑定的是 SparkContext（集群+session），不是 notebook 文件。

### 1.3 Databricks SQL Alerts 状态模型

Alert 有且仅有三种状态：

```
UNKNOWN ──[第一次执行，条件不满足]──→ OK
UNKNOWN ──[第一次执行，条件满足]────→ TRIGGERED
OK ──────[条件满足]────────────────→ TRIGGERED
TRIGGERED ──[条件不满足]───────────→ OK
```

- **UNKNOWN** = 从未执行过（刚创建）
- **OK** = 最近一次执行中，条件**未满足**（不是"一切正常"）
- **TRIGGERED** = 最近一次执行中，条件**满足**

Q29 错因：你把 OK 理解为"系统状态正常"。OK 是对**数据条件**的判断结果，不是对**系统健康**的判断。

### 1.4 Spark Join Ambiguous Column

这不是知识缺口，更像是审题问题。但底层模型值得明确：

当两个 DataFrame 有同名列时，join 后 select 该列名会报 `AnalysisException: Reference 'X' is ambiguous`。Spark 无法确定你要哪个表的列。

解决方案（按推荐顺序）：
1. Join 时用列对象：`df1.join(df2, df1["key"] == df2["key"])`，然后 `select(df1["medal"])`
2. Alias：`df1.alias("a").join(df2.alias("b"), ...)` 然后 `col("a.medal")`
3. Join 前 rename：`df2.withColumnRenamed("medal", "medal_2")`

---

## 第二章：Databricks REST API 精确语义

**错题：Q33, Q46, Q52 (3题)**

这 3 题暴露的问题是：你从未系统看过 Databricks REST API 的 spec，而是靠直觉猜字段名和行为。

### 2.1 Jobs API 核心概念

**两个关键端点的语义差异：**

| 端点 | HTTP 类比 | 行为 |
|------|-----------|------|
| `2.0/jobs/reset` | **PUT** | 用 payload 的 `new_settings` **完全替换**所有设置。未在 payload 中出现的字段会被清除。 |
| `2.0/jobs/update` | **PATCH** | 只修改 payload 中指定的字段。未提及的字段保持不变。 |

Q33 陷阱：选项把 reset 描述成"恢复默认设置"。reset 这个词在日常英语中确实有"重置为默认"的含义，但在 Databricks API 中它的语义是"用新内容完全覆盖"。

**记忆锚点：** reset = 格式化硬盘后重装系统（用你给的东西全覆盖）。update = 打补丁（只改你指定的）。

### 2.2 jobs/create 的请求和响应

**请求 payload 的正确字段名：**

```json
{
  "name": "Get All Matches",              // 可选
  "existing_cluster_id": "1198-...",      // 不是 existing_cluster
  "spark_python_task": {                  // 不是 python_task
    "python_file": "dbfs:/fetch.py",     // 不是 python_file_path
    "parameters": ["2019", "11"]          // JSON 数组，不是字符串
  }
}
```

**常见陷阱对照：**

| 错误写法 | 正确写法 | 陷阱类型 |
|----------|---------|---------|
| `existing_cluster` | `existing_cluster_id` | 缺 `_id` 后缀 |
| `python_task` | `spark_python_task` | 缺 `spark_` 前缀 |
| `python_file_path` | `python_file` | 多了 `_path` 后缀 |
| `arguments` | `parameters` | 字段名完全不同 |
| `"parameters": "[\"a\"]"` | `"parameters": ["a"]` | 字符串 vs 数组 |

**响应格式：**

```json
{"job_id": 13746}
```

- Key 必须有双引号（JSON 规范）
- Value 是数字，不是字符串 `"13746"`
- 没有 `status`、`job_name` 等其他字段

Q46 陷阱：`{job_id: 13746}` 看起来"几乎正确"，但 key 没有引号 → 不是合法 JSON。考试就喜欢出这种"只差一个引号"的选项。

### 2.3 API 字段记忆规则

Databricks REST API 的命名有内部一致性：

1. **引用已有资源时，用 `_id` 后缀：** `existing_cluster_id`, `job_id`, `run_id`, `notebook_id`
2. **Task 类型用 `spark_` 前缀 + `_task` 后缀：** `spark_python_task`, `spark_jar_task`, `spark_submit_task`（例外：`notebook_task` 没有 `spark_` 前缀，因为 notebook 不是直接 submit 到 Spark 的）
3. **文件路径字段叫 `_file` 不叫 `_file_path`：** `python_file`, `jar_uri`
4. **命令行参数叫 `parameters`：** 在 `spark_python_task` 和 `spark_submit_task` 中都是 `parameters`（不是 `arguments`、`args`、`params`）

---

## 第三章：Azure 安全架构决策框架

**错题：Q1, Q10, Q12, Q24 (4题，含 Q24 的 irrelevant 审题错误)**

这些题不需要你深入理解 Azure 安全产品的实现细节。考试考的是**选择框架** — 给定一个安全需求，你能不能选对工具。

### 3.1 Azure 安全工具分类

把 Azure 安全工具按**职责**分成四层：

```
┌─────────────────────────────────────────────────────┐
│ Layer 4: SIEM / Threat Detection                     │
│   Azure Sentinel (Microsoft Sentinel)                │
│   → 安全分析、异常检测、自动响应                       │
│   → 关键词：anomalous behavior, threat, alert         │
├─────────────────────────────────────────────────────┤
│ Layer 3: Identity & Access                           │
│   Azure Active Directory (AAD) + Conditional Access  │
│   → 身份验证、动态信任评估                             │
│   → 关键词：Zero Trust, risk-based, identity          │
├─────────────────────────────────────────────────────┤
│ Layer 2: Network Security                            │
│   VPN, NSG, Private Link, Azure Bastion              │
│   → 网络层隔离和控制                                  │
│   → 关键词：network isolation, perimeter              │
├─────────────────────────────────────────────────────┤
│ Layer 1: Monitoring & Cost                           │
│   Azure Monitor, Azure Cost Management               │
│   → 基础设施监控、性能指标、成本优化                    │
│   → 关键词：metrics, performance, cost                │
└─────────────────────────────────────────────────────┘
```

### 3.2 选择决策树

```
题目问什么？
│
├── "anomalous user behavior" / "threat detection" / "security alerting"
│   → Layer 4: Sentinel
│   （Q1：直接命中）
│
├── "Zero Trust" / "identity-based access" / "risk-based"
│   → Layer 3: AAD + Conditional Access
│   （Q12：直接命中）
│
├── "network isolation" / "restrict access" / "private connectivity"
│   → Layer 2: VPN / NSG / Private Link
│   （如果题目说 Zero Trust，选 Layer 3 不选 Layer 2 — VPN 是网络边界思维，恰好是 Zero Trust 要替代的）
│
├── "cost optimization" / "usage monitoring" / "billing"
│   → Layer 1: Azure Cost Management + Billing
│
└── "data quality" / "pipeline testing" / "before production"
    → CI/CD + automated tests（不是 Azure 安全工具的问题，但答题逻辑相同：选最自动化的方案）
    （Q10：直接命中）
```

### 3.3 Zero Trust 的核心原则

考试中 Zero Trust 题目的解题规则：

**Zero Trust = "Never trust, always verify"**
- 核心：基于**身份和风险**做决策，而不是基于**网络位置**
- 所以 VPN（"你在内网就可信"）恰恰是 Zero Trust 要消灭的思维
- 正确答案永远指向 AAD + Conditional Access（身份 + 设备状态 + 风险评估）

Q12 错因：你可能被 VPN/NSG/Bastion 这些"听起来很安全"的选项吸引了。但这些都是网络层方案 — Zero Trust 的哲学是**网络位置不代表信任**。

### 3.4 OAuth ADLS Gen2 配置 — "哪个不相关"

AAD OAuth 访问 ADLS Gen2 的必需配置项：

```
spark.hadoop.fs.azure.account.auth.type              → "OAuth" (认证方式)
spark.hadoop.fs.azure.account.oauth.provider.type     → token provider class
spark.hadoop.fs.azure.account.oauth2.client.id        → 应用 ID
spark.hadoop.fs.azure.account.oauth2.client.secret    → 应用密钥
spark.hadoop.fs.azure.account.oauth2.client.endpoint  → AAD token endpoint URL
```

不相关的：
```
spark.hadoop.fs.azure.simple.httpclient.retry.policy.type → HTTP 重试策略，跟认证无关
```

Q24 错因：两层失误。(1) 审题失误 — 问的是 "irrelevant"，你选了 relevant 的。(2) 即使注意到了 irrelevant，你可能也不确定 `retry.policy.type` 是做什么的。关键判断：名字里有 `httpclient.retry` 的配置，显然是网络传输层的重试策略，跟 OAuth 认证流程（获取 token）无关。

---

## 第四章：Delta Lake 内部机制

**错题：Q15, Q27, Q37, Q44 (4题)**

### 4.1 Change Data Feed (CDF) 的完整数据模型

CDF 启用后，每次 DML 操作都会产生变更记录，包含以下列：

| 列名 | 含义 |
|------|------|
| `_change_type` | `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | 产生该记录的 commit 版本号 |
| `_commit_timestamp` | 产生该记录的时间戳 |

**关键模型 — UPDATE 产生两条记录：**

```
UPDATE table SET col = 'new' WHERE col = 'old'

→ 记录1: _change_type = 'update_preimage',  col = 'old', _commit_version = N
→ 记录2: _change_type = 'update_postimage', col = 'new', _commit_version = N
```

**DELETE 产生一条记录：**

```
DELETE FROM table WHERE col = 'x'

→ 记录: _change_type = 'delete', col = 'x', _commit_version = N
```

**Q27 详细推演：**

```
操作                              commit_version
CREATE TABLE                      0
INSERT ('IDE', '6.2.0')           1
UPDATE SET version='5.1.0'        2    → preimage: ('IDE','6.2.0'), postimage: ('IDE','5.1.0')
INSERT ('IDE-1', '1.3.0')         3
DELETE WHERE version='1.3.0'      4    → delete: ('IDE-1','1.3.0')

table_changes('versions', 2) 返回 version >= 2 的所有变更记录：

_commit_version | _change_type       | version
2               | update_preimage    | 6.2.0    ← 这行的 version 最大
2               | update_postimage   | 5.1.0
3               | insert             | 1.3.0
4               | delete             | 1.3.0

max(_commit_version) = 4
max(version) = '6.2.0'  (字符串比较：'6' > '5' > '1')
```

**考试陷阱：** preimage 记录包含**旧值**。如果你忘了 preimage 的存在，你会以为 max(version) = '5.1.0'（post-update 值）或 '1.3.0'（insert 值）。

### 4.2 Data Skipping — 默认行为

```
Data Skipping 在 Delta Lake 中：
✅ 默认启用（不需要任何配置）
✅ 自动收集前 32 列的 min/max/null_count 统计
✅ 嵌套字段(struct fields)被视为独立列计入 32 列限制
✅ 统计信息写入 _delta_log 的 JSON/Parquet 文件
❌ 不需要用 delta.dataSkipping 属性手动启用
```

Q44 的陷阱：选项 A 说"需要手动启用" — 这是假的，data skipping 默认就开。选项 C 说"嵌套字段算独立列" — 这是真的。你选了 C 当作 false statement，但 C 实际上是正确描述。

**相关配置（了解即可）：**
- `delta.dataSkippingNumIndexedCols` — 控制收集统计的列数，默认 32
- 长字符串列（>1KB）收集统计成本高，可以通过减少 indexed cols 来优化

### 4.3 AQE (Adaptive Query Execution) 的精确边界

AQE 的三大能力：

| 能力 | 触发条件 | 作用 |
|------|---------|------|
| Coalescing Post-Shuffle Partitions | shuffle 后发现 partition 过小 | 合并小 partition 减少 task 数 |
| Converting Sort-Merge Join to Broadcast Join | shuffle 后发现一侧数据量小于阈值 | 避免 sort-merge 的开销 |
| Optimizing Skew Joins | shuffle 后发现某 partition 数据量极大 | 拆分 skew partition 并行处理 |

**关键限制：AQE 仅适用于 batch queries。**

为什么？AQE 的核心机制是在 shuffle 完成后，根据 **runtime statistics**（实际的 partition 大小）来动态调整后续 stage 的执行计划。Streaming 的每个 micro-batch 都很小且持续到达，没有"shuffle 完成后统计"的契机。

Q37 的陷阱：DE3 说 "Handles skews dynamically in **stream-static joins**"。两层错误：(1) AQE 不适用于 streaming 查询；(2) 即使在 batch 中，AQE 的 skew handling 也是针对 sort-merge join 的 shuffle 阶段，不是所有 join 类型。

### 4.4 Structured Streaming 有状态处理 API

| API | 状态管理 | 输出 | 适用场景 |
|-----|---------|------|---------|
| `mapGroupsWithState` | 每组一个状态对象 | 每组输出恰好一条记录 | sessionization, deduplication |
| `flatMapGroupsWithState` | 每组一个状态对象 | 每组可输出零到多条记录 | alerts, complex session handling |

**两者都需要低 trigger interval 来降低延迟。**

Q15 的陷阱选项分析：
- B: `spark.streaming.receiver.maxRate` — 这是 **DStream API**（旧版 Spark Streaming）的配置，不适用于 Structured Streaming
- C: `spark.streaming.blockInterval` — 同上，DStream API
- D: `spark.sql.streaming.schemaInference` — 跟性能/状态无关，是 schema 推断配置

**决策规则：** 题目提到 "stateful" + "Structured Streaming" → 答案一定涉及 `mapGroupsWithState` 或 `flatMapGroupsWithState`。如果选项里出现 `spark.streaming.*`（不带 sql），那是 DStream 的东西，可以排除。

---

## 第五章：DLT (Lakeflow Declarative Pipelines) 运行时细节

**错题：Q54, Q57 (2题)**

### 5.1 DLT Events Log 路径规则

DLT pipeline 的 events log 路径取决于是否设置了 storage location：

```
情况 1：设置了 storage location = /teams/prod
  → events log: /teams/prod/system/events

情况 2：未设置 storage location
  → events log: /pipelines/{pipeline-id}/system/events
```

**统一模型：** events log 永远在 `{base}/system/events`。区别在于 `{base}` 是什么：
- 有 storage → base = storage location
- 无 storage → base = `/pipelines/{pipeline-id}`（DBFS 默认路径）

Q54 错因：你记住了 `system/events` 这个后缀，但搞混了前缀。无 storage 时，前缀包含 pipeline-id 来区分不同 pipeline。

### 5.2 DLT Python API 限制

DLT Python API 不支持的操作：

```
❌ pivot()           — DLT 的 DataFrame 转换不支持 pivot
❌ 交互式 display()  — DLT 在 pipeline 模式下运行，不是交互式 notebook
❌ foreach/foreachBatch — 不能用自定义 sink
```

DLT Python API 支持的：

```
✅ spark.read() / spark.readStream   — 可以读取外部数据源
✅ dlt.read() / dlt.read_stream()     — 读取 pipeline 内的其他表
✅ import statements                   — 可以导入任何 Python 包
✅ @dlt.table / @dlt.view decorators   — 这是 DLT 的核心 API
✅ Python functions, classes           — 正常 Python 代码都可以
✅ expectations (数据质量约束)          — @dlt.expect, @dlt.expect_or_drop 等
```

Q57 错因：你可能以为 `spark.read()` 在 DLT 中不可用（因为推荐用 `dlt.read()`）。但 `spark.read()` 只是不推荐，并非不支持。而 `pivot()` 是真的不支持 — DLT 的 DataFrame 转换链有特殊限制。

**记忆技巧：** DLT 不支持的都是**需要物化中间状态**或**改变 DataFrame 结构方式**的操作。`pivot()` 把行变列，改变 schema 结构，不符合 DLT 的声明式模型。

---

## 总结：考试答题元规则

从这 20 道错题中，除了具体知识点之外，可以提取出 5 条答题元规则：

### 规则 1：审题关键词匹配

考试题目中的关键词直接映射到答案类别：
- "anomalous behavior" → Sentinel
- "Zero Trust" → AAD + Conditional Access（排除所有网络层方案）
- "stateful" + "streaming" → mapGroupsWithState / flatMapGroupsWithState
- "cost optimization" → autoscaling / Azure Cost Management
- "data quality before production" → CI/CD automated tests

### 规则 2：排除法优先于猜测

当你不确定正确答案时，先排除：
- 名字里有 `spark.streaming.*`（不带 sql）→ DStream API，排除
- 选项涉及"手动"操作 → 几乎一定不是最佳实践
- JSON key 没有引号 → 不是合法 JSON
- 权限名不在已知列表中 → 虚构的

### 规则 3：注意 "NOT" / "irrelevant" / "not true" 题

Q24 和 Q44 都是反向题。你在正向知识上可能是对的，但审题失误导致选反。
- 看到 NOT / irrelevant / not true / cannot / except → 标记出来
- 先正向判断每个选项的真假，再翻转选择

### 规则 4：API 字段名用排除法

REST API 题的选项差异往往很小（多一个 `_path`、少一个 `spark_`）。不要靠记忆猜全名，而是排除明显错误的变体：
- `existing_cluster`（缺 `_id`）→ 排除
- `python_task`（缺 `spark_`）→ 排除
- `arguments`（Databricks 用 `parameters`）→ 排除

### 规则 5：安全/治理题选"最 enterprise"的方案

这类题的出题逻辑是：哪个方案最符合企业级最佳实践？
- 自动化 > 手动
- 集中化 > 分散化
- 身份驱动 > 网络驱动
- 内置工具 > 自建方案
