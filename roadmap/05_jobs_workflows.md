# 05 Jobs & Workflows -- 深度复习辅导材料

> 来源：11 题全错 (0/11, 0%)，诊断级别：严重薄弱
> 核心问题：API endpoint 语义混淆 + Delta Lake 写入优化概念交叉污染

---

## 一、错误模式诊断

### 模式 1：API endpoint 功能边界模糊（5/11 题）

你把 Jobs API 的 endpoint 当成了一个模糊的整体，没有建立 "定义层 vs 运行层" 的清晰分界。具体表现：

- **create vs run-now 混淆**：以为 `jobs/create` 会去重同名 job（Q12），实际上它不幂等，N 次调用 = N 个独立 job 定义。
- **get vs list vs runs/* 混淆**：想看 task 配置时选了 `runs/list`（Q57），但 `runs/*` 系列返回的是运行时数据，不是定义。
- **update 的功能误解**：以为 `jobs/update` 能重跑 job（Q317），但它只修改配置。
- **runs/get vs runs/list 语义**：需要"最新运行 + repair history"时选了 `runs/get`（Q235），但 `runs/get` 需要已知 run_id，而"找最新"必须先 list。

**根因**：缺少一个二维分类框架 -- 横轴是操作类型（CRUD + 执行），纵轴是作用对象（job 定义 / job 运行 / task 运行）。

### 模式 2：想当然推断不存在的 API（2/11 题）

- 以为 `dbutils.notebooks.getParam()` 存在（Q1）
- 以为 `job.start_time.is_weekday` 语法非法（Q301）

**根因**：靠"看起来合理"来判断 API 是否存在，而不是基于对 Databricks API 设计体系的理解。

### 模式 3：Delta Lake 写入优化三件套混淆（2/11 题）

- Auto Compaction 目标文件大小记成了 1 GB（Q22），实际是 128 MB
- Optimized Writes 和 Auto Compaction 的触发时机搞反（Q107）

**根因**：两个概念名称相似、功能互补，但没有用"时间轴"（写入前 vs 写入后）来锚定记忆。

### 模式 4：DataFrame 操作作用域误判（1/11 题）

- 以为 `dropDuplicates()` 会检查目标表已有数据（Q8）

**根因**：把 DataFrame transformation 的作用域想大了。DataFrame 操作只看当前内存中的数据，不可能自动扫描目标表。

---

## 二、子主题分解与学习方法

### 子主题 A：Jobs API Endpoints（权重最高，5 题）

**学习方法**：建立"操作-对象"矩阵，不要逐个记忆 endpoint，而是理解设计逻辑。

核心框架：Jobs API 分两层 --

| | Job 定义（静态） | Job/Task 运行（动态） |
|---|---|---|
| 创建 | `jobs/create` | `jobs/run-now`, `runs/submit` |
| 读取 | `jobs/get`, `jobs/list` | `runs/get`, `runs/list`, `runs/get-output` |
| 更新 | `jobs/update` | `runs/repair` |
| 删除 | `jobs/delete` | `runs/cancel` |

关键区分规则：
1. 题目问"configured / definition / task 配置" -> 定义层 -> `jobs/get` 或 `jobs/list`
2. 题目问"run history / status / output" -> 运行层 -> `runs/*`
3. 题目要"执行/触发" -> `jobs/run-now`（已有定义）或 `runs/submit`（一次性）
4. `jobs/create` 只创建定义，不执行；不幂等
5. `jobs/update` 只改配置，不执行

### 子主题 B：Job 参数传递（1 题）

**学习方法**：记住唯一正确路径，排除干扰项。

参数传递的唯一机制：`dbutils.widgets`
- 声明：`dbutils.widgets.text("param_name", "default_value")`
- 获取：`dbutils.widgets.get("param_name")`
- Jobs API 通过 JSON body 传入参数，自动覆盖 widget 默认值

不存在的 API（考试干扰项）：
- `dbutils.notebooks.getParam()` -- 不存在
- `spark.conf.get()` -- 用于 Spark 配置，不是 job 参数
- `sys.argv` -- notebook 不是命令行脚本
- `input()` -- notebook 不是交互式终端

### 子主题 C：Multi-task Job ID 体系（1 题）

**学习方法**：画出层级关系图。

```
Job 定义 (job_id = 12345, 静态)
  |
  +-- Job Run #1 (run_id = 100, 动态)
  |     +-- Task "etl" Run (run_id = 101, 动态)
  |     +-- Task "validate" Run (run_id = 102, 动态)
  |
  +-- Job Run #2 (run_id = 200, 动态)
        +-- Task "etl" Run (run_id = 201, 动态)
        +-- Task "validate" Run (run_id = 202, 动态)
```

核心：job run 和 task run 的标识符都叫 `run_id`，只是层级不同。`task_key` 是定义时的名称标签，不是运行时 ID。

### 子主题 D：Job Run History 与运维（2 题）

**学习方法**：三个数字 + 一个时区规则。

- 保留期：**60 天**（不是 30，不是 90）
- 60 天后：**删除**（不是归档）
- 导出格式：**HTML**
- `job.start_time`：基于 **UTC**，跨时区场景下 weekday 判断可能不正确

### 子主题 E：Delta Lake 写入优化（2 题）

**学习方法**：用时间轴锚定 --

```
[写入前] Optimized Writes: shuffle 合并同分区数据 -> 写出更少、更大的文件
   |
[写入中] 数据落盘
   |
[写入后] Auto Compaction: 异步检测小文件 -> 合并到 128 MB
   |
[手动]   OPTIMIZE: 主动触发 -> 合并到 1 GB
```

### 子主题 F：dropDuplicates 作用域（1 题）

**学习方法**：一句话 -- DataFrame transformation 只操作当前内存中的数据，对目标表一无所知。

- `dropDuplicates()` = 当前 DataFrame 内去重
- `append` 模式 = 纯追加，不检查已有数据
- 跨 batch 去重 = `MERGE INTO`

---

## 三、苏格拉底式教学问题集

### A. Jobs API Endpoints

1. `jobs/create` 和 `jobs/run-now` 分别做了什么？如果你只调用 `jobs/create`，job 会运行吗？如果你调用三次呢？
2. 当你需要查看一个 multi-task job 里配置了哪些 notebook 时，你需要的是"定义"还是"运行记录"？对应哪个 endpoint？
3. `runs/get` 需要 `run_id` 作为参数。如果你还不知道 run_id（比如你要找"最新的运行"），你该怎么办？
4. `jobs/update` 和 `jobs/run-now` 有什么本质区别？为什么"更新配置"不等于"重新执行"？
5. `runs/submit` 和 `jobs/create` + `jobs/run-now` 的组合有什么区别？什么场景用前者？

### B. Job 参数传递

1. 为什么 Databricks 选择用 `dbutils.widgets` 而不是 `sys.argv` 或 `spark.conf` 来接收 job 参数？notebook 的执行环境和命令行脚本有什么本质区别？
2. 如果你不调用 `dbutils.widgets.text()` 声明 widget，直接调用 `dbutils.widgets.get()`，会发生什么？
3. Jobs API 传入的参数和 widget 的默认值之间是什么关系？

### C. Multi-task Job ID 体系

1. 为什么 Databricks 给 task run 也分配 `run_id` 而不是 `task_id`？这反映了什么设计哲学？
2. 如果你用 `runs/get` 传入一个 job 级别的 `run_id`，返回的响应中是否包含各 task 的 `run_id`？
3. `task_key` 和 task 的 `run_id` 分别在什么场景下使用？

### D. Job Run History 与运维

1. 60 天保留期意味着什么？如果你需要审计 6 个月前的 job 运行记录，你该怎么办？
2. `job.start_time.is_weekday` 基于 UTC。假设一个 job 在 UTC 周六 00:30 启动，此时纽约是周五 19:30 -- 这个条件会路由到周末还是工作日 notebook？
3. 为什么 Databricks 选择 UTC 而不是 job 配置的时区来评估 `start_time`？

### E. Delta Lake 写入优化

1. Auto Compaction 的目标是 128 MB，手动 OPTIMIZE 是 1 GB。为什么自动的目标更小？这体现了什么设计权衡？
2. Optimized Writes 发生在写入前（shuffle），Auto Compaction 发生在写入后（异步合并）。如果同时启用两者，它们会冲突吗？
3. 为什么 Auto Compaction 被描述为"异步"？它在什么时间点触发，使用什么资源？

### F. dropDuplicates 作用域

1. `dropDuplicates()` 是 transformation 还是 action？它能访问目标表吗？为什么？
2. 如果上游系统在不同 batch 中产生了相同 key 的重复数据，`dropDuplicates()` + `append` 模式能解决吗？正确方案是什么？
3. `MERGE INTO` 和 `dropDuplicates()` + `append` 的根本区别是什么？

---

## 四、场景判断题

### A. Jobs API Endpoints

**A1.** 一个 data engineer 需要自动化以下流程：每小时检查所有 job 的运行状态，找到失败的 job 并重新触发。正确的 API 调用顺序是？

- A. `jobs/get` -> `runs/get` -> `jobs/update`
- B. `jobs/list` -> `runs/list` -> `jobs/run-now`
- C. `jobs/list` -> `runs/get` -> `runs/submit`
- D. `jobs/create` -> `jobs/run-now` -> `runs/get`

<details><summary>答案</summary>

**B**。流程：`jobs/list` 列出所有 job -> `runs/list` 检查每个 job 的最新运行状态 -> `jobs/run-now` 重新触发失败的 job。A 错在 `jobs/get` 只能查一个 job 且 `jobs/update` 不能执行；C 错在 `runs/get` 需要已知 run_id 且 `runs/submit` 会创建一次性运行而非基于已有定义；D 错在完全不需要重新创建 job。

</details>

**A2.** 一个团队的 CI/CD pipeline 调用 `jobs/create` 部署一个名为 "nightly_etl" 的 job。由于 retry 逻辑问题，这个 API 被调用了 5 次。结果是什么？

- A. 1 个 job 被创建，因为 Databricks 对同名 job 去重
- B. 5 个同名 job 被创建，且各执行一次
- C. 5 个同名 job 被创建，但都不会执行
- D. 报错，因为同名 job 已存在

<details><summary>答案</summary>

**C**。`jobs/create` 不幂等，每次调用创建一个独立的 job 定义，即使名称相同。Databricks 允许同名 job 共存。`jobs/create` 只创建定义，不触发执行。

</details>

**A3.** 一个 data engineer 需要获取某 job 最近一次运行的 repair history。正确的 API 调用是？

- A. `runs/get` + run_id + include_history=true
- B. `runs/list` + run_id + include_history=true
- C. `runs/list` + job_id + include_history=true
- D. `jobs/get` + job_id + include_history=true

<details><summary>答案</summary>

**C**。获取运行历史（含 repair history）用 `runs/list`，参数是 `job_id`（因为你要列出这个 job 的所有运行），加 `include_history=true` 获取修复历史。`runs/get` 只获取单次运行详情；用 run_id 的前提是你已经知道具体哪次运行，但题目说的是"最近一次"，必须先 list。

</details>

### B. Job 参数传递

**B1.** 一个 notebook 需要接收 Jobs API 传入的 `date` 参数。以下哪段代码是正确的？

- A. `date = spark.conf.get("date")`
- B. `date = dbutils.notebooks.getParam("date")`
- C. `dbutils.widgets.text("date", ""); date = dbutils.widgets.get("date")`
- D. `import sys; date = sys.argv[1]`

<details><summary>答案</summary>

**C**。Databricks 接收 job 参数的唯一机制是 `dbutils.widgets`：先声明 widget，再获取值。Jobs API 传参时自动覆盖默认值。A 用于 Spark 配置，B 的 API 不存在，D 在 notebook 环境中不适用。

</details>

**B2.** 一个 scheduled job 通过 Jobs API 传入参数 `{"env": "prod"}`。notebook 中有以下代码：

```python
dbutils.widgets.text("env", "dev")
env = dbutils.widgets.get("env")
```

`env` 的值是什么？

- A. "dev"，因为默认值优先
- B. "prod"，因为 API 参数覆盖默认值
- C. 报错，因为参数名冲突
- D. None，因为 widget 类型不匹配

<details><summary>答案</summary>

**B**。Jobs API 传入的参数会覆盖 widget 的默认值。`widgets.text()` 声明的默认值 "dev" 仅在交互式运行（手动在 notebook UI 中执行）且未传参时生效。

</details>

### C. Multi-task Job ID 体系

**C1.** 一个 multi-task job 包含 3 个 task。运行一次后，系统中共生成了多少个 `run_id`？

- A. 1 个（job 级别）
- B. 3 个（每个 task 一个）
- C. 4 个（1 个 job 级别 + 3 个 task 级别）
- D. 取决于 task 是否成功

<details><summary>答案</summary>

**C**。job 运行本身有一个 `run_id`，每个 task 运行也各有一个独立的 `run_id`。所以 1 + 3 = 4 个 `run_id`。

</details>

### D. Job Run History 与运维

**D1.** 一个合规团队需要保留所有 job 运行记录至少 1 年。以下哪个方案正确？

- A. 不需要做任何事，Databricks 默认保留 365 天
- B. 在 60 天内导出运行结果为 HTML，存储到外部系统
- C. 等待 60 天后从归档中恢复
- D. 调用 `runs/get` 获取历史记录，该 API 不受保留期限制

<details><summary>答案</summary>

**B**。Job run history 只保留 60 天，之后直接删除（不归档）。要长期保留必须在 60 天内主动导出 notebook 运行结果为 HTML 并存储到外部系统。

</details>

**D2.** 一个 job 使用 `{{job.start_time.is_weekday}}` 做条件路由，在东京（UTC+9）和伦敦（UTC+0）两个 workspace 中部署。job 在当地时间每天 08:00 触发。以下哪个说法正确？

- A. 两个 workspace 的路由结果一定相同
- B. 东京 workspace 可能出现路由错误，因为 UTC 时间比东京早 9 小时
- C. 伦敦 workspace 可能出现路由错误，因为 BST 夏令时偏移
- D. 该语法无效，job 会报错

<details><summary>答案</summary>

**C** 也对，但更根本的答案是：两者都可能出错。`job.start_time` 基于 UTC。东京 08:00 = UTC 前一天 23:00，如果当地是周一早上，UTC 则是周日晚上，路由到周末 notebook。伦敦在冬季 UTC+0 时没问题，但 BST（UTC+1）期间也会偏移。本题核心：凡涉及 `job.start_time` + 多时区，必须意识到 UTC 问题。

</details>

### E. Delta Lake 写入优化

**E1.** 以下哪项正确描述了 Auto Compaction？

- A. 写入前通过 shuffle 减少文件数，目标 1 GB
- B. 写入后异步合并小文件，目标 1 GB
- C. 写入后异步合并小文件，目标 128 MB
- D. 写入前通过 shuffle 减少文件数，目标 128 MB

<details><summary>答案</summary>

**C**。Auto Compaction 在写入完成后异步触发，检测是否有可以进一步合并的小文件，目标文件大小是 128 MB。写入前 shuffle 是 Optimized Writes 的描述；1 GB 是手动 OPTIMIZE 的目标。

</details>

**E2.** 一个 batch job 每小时从 Parquet 文件加载数据，使用 `dropDuplicates(["id"])` 去重后 `append` 到目标表。上游偶尔在不同小时的 batch 中产生相同 id 的重复记录。以下哪个说法正确？

- A. 目标表不会有重复，因为 `dropDuplicates` 会检查已有数据
- B. 目标表可能有重复，因为 `dropDuplicates` 只在当前 batch 内去重
- C. 目标表不会有重复，因为 `append` 模式会自动去重
- D. 写入会失败，因为主键冲突

<details><summary>答案</summary>

**B**。`dropDuplicates()` 只在当前 DataFrame（当前 batch）内去重，完全不知道目标表已有什么数据。`append` 模式是纯追加，不做任何检查。要跨 batch 去重必须用 `MERGE INTO`。

</details>

**E3.** 同时启用 Auto Compaction 和 Optimized Writes 后，一次写入操作的完整流程是？

- A. 先 Auto Compaction 合并文件，再 Optimized Writes 写入
- B. 先 Optimized Writes shuffle 减少文件数写入，再 Auto Compaction 异步合并剩余小文件
- C. 两者冲突，只能启用其中一个
- D. Optimized Writes 替代了 Auto Compaction 的功能

<details><summary>答案</summary>

**B**。Optimized Writes 在写入前通过 shuffle 将同分区数据集中，减少写出的文件数。写入完成后，Auto Compaction 异步检测是否仍有小文件需要合并。两者互补、独立，可以同时启用。

</details>

---

## 五、关键知识网络

```
Jobs & Workflows 知识网络
|
+-- Jobs API 体系
|   |
|   +-- 定义层 (静态)
|   |   +-- jobs/create (不幂等, 只创建不执行)
|   |   +-- jobs/get (单个 job 完整定义, 含 task 配置)
|   |   +-- jobs/list (所有 job 概要)
|   |   +-- jobs/update (修改配置, 不执行)
|   |   +-- jobs/delete
|   |
|   +-- 运行层 (动态)
|   |   +-- jobs/run-now (触发已有 job)
|   |   +-- runs/submit (一次性运行, 不创建定义)
|   |   +-- runs/list (运行历史, 支持 include_history)
|   |   +-- runs/get (单次运行详情, 需要 run_id)
|   |   +-- runs/get-output (运行输出)
|   |   +-- runs/repair (重跑失败 task)
|   |   +-- runs/cancel
|   |
|   +-- ID 体系
|       +-- job_id (静态, 定义级)
|       +-- run_id (动态, job 级和 task 级都用)
|       +-- task_key (静态, 定义级名称)
|
+-- 参数传递
|   +-- 唯一机制: dbutils.widgets
|   +-- 声明: widgets.text("name", "default")
|   +-- 获取: widgets.get("name")
|   +-- API 参数覆盖默认值
|
+-- 运维规则
|   +-- Run history: 60 天保留, 删除不归档
|   +-- 导出: HTML 格式
|   +-- job.start_time: UTC 时区
|
+-- Delta Lake 写入优化 (交叉考点)
|   +-- Optimized Writes: 写入前 shuffle
|   +-- Auto Compaction: 写入后异步, 128 MB
|   +-- 手动 OPTIMIZE: 主动触发, 1 GB
|
+-- 去重机制 (交叉考点)
    +-- dropDuplicates(): 当前 DataFrame 内
    +-- MERGE INTO: 跨 batch, 检查目标表
```

---

## 六、Anki 卡片

**Q1:** `jobs/create` 调用 3 次（相同参数）会创建几个 job？
**A1:** 3 个独立的 job。`jobs/create` 不幂等，Databricks 允许同名 job 共存，且 `jobs/create` 只创建定义不触发执行。

---

**Q2:** 查看 multi-task job 中配置了哪些 notebook，用哪个 API endpoint？
**A2:** `/jobs/get`。它返回 job 的完整定义，包含所有 task 配置和 notebook 路径。`/jobs/list` 只返回概要，`/runs/*` 返回运行时数据。

---

**Q3:** 获取某 job 的运行历史 + repair history，正确的 API 调用是什么？
**A3:** `runs/list` + `job_id` + `include_history=true`。不是 `runs/get`（需要已知 run_id，只返回单次运行详情）。

---

**Q4:** Databricks notebook 接收 Jobs API 传入参数的唯一机制是什么？
**A4:** `dbutils.widgets`。先 `dbutils.widgets.text("name", "default")` 声明，再 `dbutils.widgets.get("name")` 获取。API 参数自动覆盖默认值。

---

**Q5:** Multi-task job 运行一次（含 3 个 task），系统共生成多少个 `run_id`？
**A5:** 4 个。1 个 job 级别的 `run_id` + 3 个 task 级别的 `run_id`。job run 和 task run 的标识符都叫 `run_id`，层级不同。

---

**Q6:** Job run history 保留多久？过期后是归档还是删除？
**A6:** 保留 60 天，过期后直接删除，不归档。需要长期保留必须在 60 天内导出 notebook 结果为 HTML。

---

**Q7:** `{{job.start_time.is_weekday}}` 基于什么时区？跨时区使用有什么风险？
**A7:** 基于 UTC 时区。如果 job 在多个时区部署，UTC 的 weekday 判断可能与本地时间不一致（如 UTC 周六 = 东京周六 = 纽约周五）。

---

**Q8:** Auto Compaction 的目标文件大小是多少？手动 OPTIMIZE 呢？
**A8:** Auto Compaction: 128 MB。手动 OPTIMIZE: 1 GB。Auto Compaction 是写入后异步触发的轻量操作。

---

**Q9:** Optimized Writes 和 Auto Compaction 分别发生在写入的什么阶段？
**A9:** Optimized Writes = 写入前（通过 shuffle 减少文件数）。Auto Compaction = 写入后（异步合并小文件到 128 MB）。

---

**Q10:** `dropDuplicates()` 能跨 batch 去重吗？正确的跨 batch 去重方案是什么？
**A10:** 不能。`dropDuplicates()` 只在当前 DataFrame 内去重，不知道目标表已有什么数据。跨 batch 去重用 `MERGE INTO`（upsert）。

---

**Q11:** `jobs/update` 能重新执行 job 吗？重新执行用什么 endpoint？
**A11:** 不能。`jobs/update` 只修改 job 配置（schedule、cluster 等）。重新执行用 `jobs/run-now`。

---

**Q12:** `runs/submit` 和 `jobs/create` + `jobs/run-now` 的区别是什么？
**A12:** `runs/submit` 一次性运行，不创建持久的 job 定义。`jobs/create` + `jobs/run-now` 先创建可复用的 job 定义，再触发运行。临时测试用 `runs/submit`，生产调度用后者。

---

## 七、掌握度自测

**1.** 一个 data engineer 调用 `POST /api/2.1/jobs/create` 传入以下 JSON 两次：
```json
{"name": "daily_etl", "tasks": [{"task_key": "ingest", "notebook_task": {"notebook_path": "/prod/ingest"}}]}
```
结果是什么？

A. 创建 1 个 job 并执行 2 次
B. 创建 2 个同名 job，各执行 1 次
C. 创建 2 个同名 job，都不执行
D. 第二次调用报错

<details><summary>答案</summary>C。`jobs/create` 不幂等，每次创建独立的 job 定义。只创建不执行。</details>

---

**2.** 以下哪个 API 调用可以获取一个 multi-task job 中所有 task 的 notebook 配置？

A. `GET /api/2.1/jobs/list`
B. `GET /api/2.1/jobs/get?job_id=123`
C. `GET /api/2.1/jobs/runs/get?run_id=456`
D. `GET /api/2.1/jobs/runs/list?job_id=123`

<details><summary>答案</summary>B。`jobs/get` 返回 job 定义的完整信息，包含所有 task 的配置（notebook 路径等）。</details>

---

**3.** 一个 notebook 通过 Jobs API 接收参数 `region`。以下哪段代码正确？

A. `region = spark.conf.get("region")`
B. `region = dbutils.notebooks.getParam("region")`
C. `dbutils.widgets.text("region", "default"); region = dbutils.widgets.get("region")`
D. `import os; region = os.environ["region"]`

<details><summary>答案</summary>C。`dbutils.widgets` 是 Jobs API 向 notebook 传参的唯一机制。</details>

---

**4.** 一个 multi-task job 包含 task A, B, C（B 和 C 依赖 A）。运行一次后，task B 失败。data engineer 调用 `runs/repair` 只重跑 task B。此时系统中与这次运行相关的 `run_id` 共有几个？

A. 3 个
B. 4 个
C. 5 个
D. 取决于 repair 是否成功

<details><summary>答案</summary>C。原始运行：1 个 job run_id + 3 个 task run_id = 4 个。repair 重跑 task B 会生成一个新的 task run_id，共 5 个。</details>

---

**5.** 以下哪项正确描述了 Delta Lake Optimized Writes？

A. 写入后异步检测小文件并合并到 128 MB
B. 写入后异步检测小文件并合并到 1 GB
C. 写入前通过 shuffle 将同分区数据集中，减少写出文件数
D. 手动执行 OPTIMIZE 命令重写文件到 1 GB

<details><summary>答案</summary>C。Optimized Writes 发生在写入前，通过 shuffle 将同分区数据集中到少数 executor，减少文件数。A 是 Auto Compaction，D 是手动 OPTIMIZE。</details>

---

**6.** Job run history 的保留策略是什么？

A. 30 天，之后归档到 S3
B. 60 天，之后归档
C. 60 天，之后删除
D. 90 天，之后删除

<details><summary>答案</summary>C。保留 60 天，过期后直接删除，不归档。</details>

---

**7.** 一个 batch job 使用 `dropDuplicates(["order_id"])` 后以 `append` 模式写入目标表。上游在不同 batch 中发送了相同 order_id 的记录。目标表中会有重复吗？

A. 不会，`dropDuplicates` 会检查目标表
B. 不会，`append` 模式会自动去重
C. 会，因为 `dropDuplicates` 只在当前 batch 内去重
D. 会导致写入失败

<details><summary>答案</summary>C。`dropDuplicates()` 只在当前 DataFrame 内去重，不检查目标表。`append` 模式纯追加。跨 batch 去重需要 `MERGE INTO`。</details>

---

**8.** 自动化监控和恢复失败 job 的正确 API 流程是？

A. `jobs/get` -> `jobs/update` -> `jobs/run-now`
B. `jobs/list` -> `runs/list` -> `jobs/run-now`
C. `jobs/list` -> `runs/get` -> `runs/submit`
D. `jobs/create` -> `jobs/run-now` -> `runs/get`

<details><summary>答案</summary>B。`jobs/list` 列出所有 job -> `runs/list` 检查运行状态 -> `jobs/run-now` 重新触发失败的 job。</details>
