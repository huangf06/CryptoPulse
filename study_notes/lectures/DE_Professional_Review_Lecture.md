# Databricks Data Engineer Professional 全面串讲

> 整理时间：2026-02-10
> 用途：考前知识梳理，深入浅出串讲全部考点

---

## 先看大局：这个考试在考什么？

一句话概括：**你能不能在 Databricks 上独立设计、构建、运维一条生产级数据管道？**

想象你要建一条从"原始数据"到"业务报表"的高速公路：

```
原始数据 → 摄取 → 清洗 → 建模 → 交付给业务
```

考试的 6 个域就是围绕这条路的不同方面：

| 你要解决的问题 | 对应域 | 权重 |
|---------------|--------|------|
| 用什么工具建这条路？ | 域1: Tooling | 20% |
| 数据怎么流过这条路？ | **域2: Data Processing** | **30%** |
| 路的终点长什么样？ | 域3: Data Modeling | 20% |
| 谁能上这条路？ | 域4: Security | 10% |
| 路况怎么监控？ | 域5: Monitoring | 10% |
| 怎么安全地修路？ | 域6: Testing & Deployment | 10% |

---

## 域 1：Databricks Tooling（20%）— 你的工具箱

### 1.1 DABs（Databricks Asset Bundles）— 项目管理的核心

**类比**：DABs 就像是你项目的"蓝图文件"。一个 `databricks.yml` 文件描述了你的整个项目要部署什么、部署到哪里。

**核心概念**：

**多环境隔离**是 DABs 最重要的能力。你有一份代码，但要部署到 dev/staging/prod 三个环境：

```
targets:
  development:    → 连接 dev workspace，用 dev_catalog
  staging:        → 连接 staging workspace，用 staging_catalog
  production:     → 连接 prod workspace，用 prod_catalog
                    且用 Service Principal 运行（不是个人账号）
```

**四个命令记住顺序**：
- `validate` → 检查语法（不部署）
- `deploy` → 部署资源（不运行）
- `run` → 部署 + 运行
- `destroy` → 销毁资源

### 1.2 第三方库 — 怎么装包？

最常用的方式：**在 Notebook 顶部用 `%pip install`**。

特殊情况：**DLT Pipeline 不支持 `%pip`**，必须在 Pipeline 配置中声明依赖。这是一个常考的坑。

安装后别忘了 `dbutils.library.restartPython()` 让新装的包生效。

### 1.3 UDF — 自定义函数的性能差异

这里的核心考点就一个：**为什么 Pandas UDF 比 Python UDF 快？**

想象一个工厂流水线：
- **Python UDF**：工人一次只搬一个零件，搬一个、处理一个、送回去一个。来回跑 100 万次。
- **Pandas UDF**：工人用叉车一次搬 1 万个零件，批量处理，再用叉车送回去。来回只跑 100 次。

这个"叉车"就是 **Apache Arrow**，它做的是批量列式序列化。

性能排序：**SQL UDF > Pandas UDF > Python UDF**

### 1.4 Compute — 集群选择

记住一个省钱原则：
- **开发调试** → All-Purpose Cluster（可以交互）
- **生产 Job** → Job Cluster（按需创建销毁，**便宜约 50%**）
- **SQL 查询** → SQL Warehouse（Photon 加速）

### 1.5 Lakeflow Jobs — 任务编排

核心考点：**Task 之间怎么传数据？**

```python
# Task A 设置值
dbutils.jobs.taskValues.set(key="row_count", value=12345)

# Task B 读取值（注意要指定 taskKey）
count = dbutils.jobs.taskValues.get(taskKey="task_a", key="row_count")
```

这是轻量级的值传递。如果要传大量数据，应该写到 Delta 表里。

---

## 域 2：Data Processing（30%）— 最重要的域

这个域占了将近三分之一，是考试的重中之重。

### 2.1 Delta Lake — 一切的基础

**Delta Lake 本质上就是：Parquet 文件 + 事务日志（`_delta_log`）**

每次你对表做写操作（INSERT/UPDATE/DELETE），Delta Lake 做两件事：
1. 写新的数据文件（Parquet）
2. 在 `_delta_log/` 里写一条 JSON 记录，说明"这次操作添加了哪些文件、删除了哪些文件"

#### VACUUM — 重要考点

这是一个非常重要的区分：

```
Delta 表的组成：
├── 数据文件（Parquet）     ← VACUUM 删除的是这些
└── _delta_log/（事务日志）  ← VACUUM 永远不碰这些
```

**VACUUM 做什么**：删除超过保留期的、不再被当前版本引用的旧数据文件。

**VACUUM 的后果**：那些旧数据文件被删了，Time Travel 就找不到它们了。所以 VACUUM 之后，你只能 Time Travel 到保留期内的版本。

**默认保留期**：7 天（168 小时）

**记忆口诀**：VACUUM 删数据文件，不删日志文件；数据文件没了，Time Travel 就断了

#### OCC（乐观并发控制）— 重要考点

**类比**：想象两个人同时编辑同一个 Google Doc。

Delta Lake 用的是**乐观锁**，不是悲观锁：
- **悲观锁**：我编辑时锁住文件，别人不能编辑（Delta Lake **不是**这样）
- **乐观锁**：大家都可以编辑，提交时检查有没有冲突

**冲突规则**（必须记住）：

| 操作 A | 操作 B | 冲突？ | 为什么？ |
|--------|--------|--------|----------|
| **APPEND** | **APPEND** | **不冲突** | 各自添加新文件，互不影响 |
| APPEND | DELETE | 可能冲突 | 如果 DELETE 的条件涉及 APPEND 的文件 |
| DELETE | DELETE | 冲突 | 可能操作同一个文件 |
| UPDATE | UPDATE | 冲突 | 可能操作同一个文件 |

**核心直觉**：APPEND 只是"加新文件"，不碰已有文件，所以两个 APPEND 永远不冲突。

**记忆口诀**：APPEND + APPEND = 和平共处；Delta 用乐观锁，先写先赢，后写检查，冲突重试

#### Clone

- **Deep Clone** = 完整复制（独立副本，用于备份）
- **Shallow Clone** = 只复制元数据，数据文件指向原表（用于快速测试）

Shallow Clone 的写入是独立的——你往 clone 里写数据，原表不受影响。

### 2.2 CDC 与 CDF

**CDF（Change Data Feed）**：Delta 表的"变更日记"。启用后，你可以查询"从版本 2 到版本 5 之间发生了什么变化"。

关键字段：
- `_change_type`：`insert`、`delete`、`update_preimage`（更新前）、`update_postimage`（更新后）

**MERGE INTO**：最常用的 upsert 操作。"有就更新，没有就插入，标记删除的就删除"。

### 2.3 Structured Streaming — 流处理核心

#### Auto Loader（考试重点中的重点）

Auto Loader 就是 Databricks 的"文件监听器"，自动发现新文件并摄取。

**两种文件发现模式**：
- **Directory Listing**（默认）：定期扫描目录，像你每隔 5 分钟去信箱看看有没有新信
- **File Notification**：云服务主动通知你有新文件，像快递员按门铃通知你。**大目录用这个更高效**

**Schema Evolution 模式**（源数据结构变了怎么办）：

| 模式 | 行为 | 记忆方法 |
|------|------|----------|
| `addNewColumns` | 自动加新列 | 来者不拒 |
| `failOnNewColumns` | 发现新列就报错 | 严格把关 |
| `rescue` | 新列放到 `_rescued_data` | 先收着再说 |
| `none` | 忽略新列 | 视而不见 |

#### Trigger 类型

**重点区分 `Once` vs `AvailableNow`**：
- `Trigger.Once`：只处理**一个微批**就停。如果积压了很多数据，可能处理不完。
- `Trigger.AvailableNow`：处理**所有积压数据**（可能分多个微批），全部处理完才停。**推荐用这个替代 Once**。

#### Watermark（水印）

用于处理迟到数据。核心逻辑：

```
系统记录：已见到的最大事件时间 = 10:30
水印阈值 = 10 分钟
水印线 = 10:30 - 10分钟 = 10:20
→ 事件时间 < 10:20 的数据会被丢弃
```

#### Stream-Stream Join vs Stream-Static Join

| | Stream-Stream | Stream-Static |
|--|--------------|---------------|
| 两边都是流？ | 是 | 一边是流，一边是批量表 |
| 需要 Watermark？ | **必须** | 不需要 |
| 需要时间约束？ | **必须** | 不需要 |
| 静态表何时读取？ | N/A | **每个微批开始时重新读取最新快照** |

#### foreachBatch

当你需要在流处理中做"非标准"操作时用它：
1. 写入**多个目标表**
2. 在流中执行 **MERGE**（upsert）
3. 调用**外部 API**

### 2.4 性能优化

#### AQE（Adaptive Query Execution）

Spark 运行时的"自动驾驶"，三大能力：
1. **自动合并小分区** — 减少 Task 数量
2. **自动切换 Broadcast Join** — 运行时发现小表就广播
3. **自动处理数据倾斜** — 拆分过大的分区

Databricks 默认启用，一般不需要手动配置。

#### 文件布局优化（重要考点）

三种方式的选择逻辑：

```
新建表？ → Liquid Clustering（首选，自动化，可改列）
已有表，低基数列过滤？ → Partitioning（如按日期分区）
已有表，多列过滤？ → Z-Ordering（需手动 OPTIMIZE）
```

**Liquid Clustering 的优势**：
- 写入时自动聚类，不需要手动跑 OPTIMIZE
- 可以用 `ALTER TABLE ... CLUSTER BY` 随时改聚类列
- 支持高基数列（Partitioning 不行）

#### Photon

C++ 向量化引擎，加速 SQL 操作（Scan、Filter、Join、Aggregation）。

**但是**：Photon **不加速 Python UDF**。这是常考陷阱。

---

## 域 3：Data Modeling（20%）

### 3.1 Medallion Architecture（奖牌架构）

```
Bronze（铜）→ Silver（银）→ Gold（金）
原始数据      清洗数据      业务数据
```

| 层 | 做什么 | DLT 表类型 |
|----|--------|-----------|
| Bronze | 原样摄取，保留原始格式 | Streaming Table |
| Silver | 清洗、去重、标准化 | Materialized View |
| Gold | 聚合、维度建模 | Materialized View |

**Multiplex 模式**：多个数据源写入同一个 Bronze 表（带 source/type 标识），然后在 Silver 层按类型拆分。适合统一事件流场景。

### 3.2 SCD（缓慢变化维度）

- **SCD Type 1**：直接覆盖旧值。简单，但丢失历史。
- **SCD Type 2**：保留历史，新增行，标记有效期（`start_date`, `end_date`, `is_current`）。

在 DLT 中实现 SCD Type 2：
```sql
APPLY CHANGES INTO LIVE.customers
KEYS (customer_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 2;
```

### 3.3 DLT（Lakeflow Declarative Pipelines）

#### 数据质量 Expectations

三种违规处理（必须记住）：

| 关键词 | 违规行怎么处理 | Pipeline 继续？ |
|--------|---------------|----------------|
| `EXPECT` | **保留**，记录警告 | 继续 |
| `ON VIOLATION DROP ROW` | **丢弃** | 继续 |
| `ON VIOLATION FAIL UPDATE` | 不写入 | **停止** |

#### Development vs Production 模式

| | Development | Production |
|--|------------|------------|
| 失败重试 | 不重试 | 自动重试 |
| 集群 | 重用集群 | 每次新集群 |
| 用途 | 调试开发 | 生产运行 |

---

## 域 4：Security & Governance（10%）

### Unity Catalog 权限模型

**最重要的一点**：权限是**层级的**。要查询一个表，你需要**三层权限全部具备**：

```
USE CATALOG (catalog 级)
  + USE SCHEMA (schema 级)
    + SELECT (table 级)
```

缺任何一层都会报 Access Denied。这是高频考点。

### Dynamic Views

用于实现**行级安全**和**列级掩码**：
- `is_account_group_member('finance_team')` — 判断用户是否属于某个组
- 根据用户身份返回不同的数据

### Delta Sharing

- **D2D**（Databricks-to-Databricks）：两个 Databricks workspace 之间共享
- **D2O**（Databricks-to-Open）：向非 Databricks 平台共享，接收方用开源客户端读取

### Secrets

- `dbutils.secrets.get()` 获取密钥
- 输出中显示 `[REDACTED]`，防止泄露
- 生产环境用 **Service Principal** 运行 Job，不用个人账号

---

## 域 5：Monitoring & Logging（10%）

### Spark UI 诊断

记住这个诊断表：

| 你看到的现象 | 最可能的原因 | 解决方案 |
|-------------|-------------|---------|
| 少数 Task 特别慢 | **数据倾斜** | AQE Skew Join / Salting |
| Shuffle 数据量巨大 | Join 策略不当 | Broadcast 小表 |
| OOM 错误 | 数据超内存 | 加内存 / 减少分区大小 |
| 大量小文件 | 写入碎片化 | OPTIMIZE / Auto Compaction |

### System Tables

记住最常用的几个：
- `system.billing.usage` → 看花了多少钱
- `system.access.audit` → 看谁做了什么操作
- `system.information_schema.*` → 看表的元数据

---

## 域 6：Testing & Deployment（10%）

### 测试金字塔

```
      E2E（少）
    Integration（中）
   Unit Tests（多）
```

- **单元测试**：用 `pytest` + 本地 SparkSession 测试转换函数
- **集成测试**：测试 Bronze → Silver 端到端流程

### CI/CD 流程

```
feature branch → PR → 自动测试 → merge to develop → 部署 staging → merge to main → 部署 production
```

关键实践：
1. DABs 管理多环境
2. Service Principal 运行生产 Job
3. 不同环境用不同 Catalog 隔离

---

## 易错知识点总结

### VACUUM 与 Time Travel

```
记忆口诀：VACUUM 删数据文件，不删日志文件
         数据文件没了，Time Travel 就断了
```

### OCC 并发控制

```
记忆口诀：APPEND + APPEND = 和平共处（不冲突）
         Delta 用乐观锁，不用悲观锁
         先写先赢，后写检查，冲突重试
```

---

*祝考试顺利！*
