# Jobs & Workflows

> 11 题 | 全部答错 (0/11, 0%) | 诊断级别：严重薄弱
> 
> 这是错误率最高的专题之一。好消息是这些主要是记忆性知识，通过系统整理可以快速补强。

---

## 一、概念框架

Jobs & Workflows 在考试中覆盖五个子领域：

1. **Jobs API endpoints 及其语义**：create / run-now / runs/submit / get / list / runs/list / runs/get / runs/repair 各自的功能和幂等性
2. **Job 参数传递**：dbutils.widgets 是 Jobs API 向 notebook 传参的唯一机制
3. **Multi-task jobs 响应结构**：job run 和 task run 都用 run_id 标识，层级不同
4. **Job run history 与 repair**：60 天保留期，runs/list + include_history 获取修复历史
5. **Job 条件路由与时区**：job.start_time 基于 UTC，跨时区场景需注意

附带考点（与 Delta Lake 交叉）：
- Auto Compaction vs Optimized Writes 的区分（常与 Jobs 混合出题）
- dropDuplicates 的作用范围（batch job 中的去重陷阱）

---

## 二、错题精析

### A. Jobs API Endpoints (5题)

---

#### Q12 -- jobs/create 不幂等

**QUESTION 12**
A junior data engineer has configured a workload that posts the following JSON to the Databricks REST API endpoint 2.0/jobs/create.

```json
{
  "name": "Ingest new data",
  "existing_cluster_id": "6015-954420-peace720",
  "notebook_task": {
    "notebook_path": "/Prod/ingest.py"
  }
}
```

Which statement describes the result of executing this workload three times assuming that all configurations and referenced resources are available?

- A. Three new jobs named "Ingest new data" will be defined in the workspace, and they will each run once daily.
- B. The logic defined in the referenced notebook will be executed three times on new clusters with the configurations of the provided cluster ID.
- **C. Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed.** <<<CORRECT>>>
- D. One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.
- E. The logic defined in the referenced notebook will be executed three times on the referenced existing all purpose cluster.

**我的答案：** D | **正确答案：** C

**错因分析：**
以为 Databricks 会对同名 job 去重，只创建一个。实际上 `jobs/create` 不幂等，每次调用都创建一个独立的 job 定义，即使 name 完全相同。另外，`jobs/create` 只创建定义，不触发执行。

**核心知识点：**
- `jobs/create` = 创建 job 定义，不执行
- `jobs/create` 不幂等：调用 N 次 = 创建 N 个独立 job
- Databricks 允许同名 job 共存
- 要执行 job 需要调用 `jobs/run-now`

---

#### Q57 -- jobs/get 获取 job 定义详情

**QUESTION 57**
Which REST API call can be used to review the notebooks configured to run as tasks in a multi-task job?

- A. /jobs/runs/list
- B. /jobs/runs/get-output
- C. /jobs/runs/get
- **D. /jobs/get** <<<CORRECT>>>
- E. /jobs/list

**我的答案：** A | **正确答案：** D

**错因分析：**
混淆了 "job 定义" 和 "job 运行历史"。题目问的是 "configured to run as tasks"，即 job 的配置信息（task 列表、notebook 路径等），这属于 job 定义，要用 `/jobs/get`。而 `/jobs/runs/*` 系列返回的是运行时数据。

**核心知识点：**
- `/jobs/get` = 获取 job 定义详情（含所有 task 配置）
- `/jobs/list` = 列出所有 job，但不含详细 task 配置
- `/jobs/runs/*` = 运行历史，与 job 定义无关

---

#### Q235 -- runs/list 获取运行历史 + repair history

**QUESTION 235**
A data engineer needs to create an application that will collect information about the latest job run including the repair history.

How should the data engineer format the request?

- A. Call /api/2.1/jobs/runs/list with the run_id and include_history parameters
- B. Call /api/2.1/jobs/runs/get with the run_id and include_history parameters
- C. Call /api/2.1/jobs/runs/get with the job_id and include_history parameters
- **D. Call /api/2.1/jobs/runs/list with the job_id and include_history parameters** <<<CORRECT>>>

**我的答案：** B | **正确答案：** D

**错因分析：**
两个错误叠加：(1) 以为 `runs/get` 可以获取历史，但 `runs/get` 只获取单次运行详情；(2) 以为用 `run_id` 查询，但要获取"最新运行"就意味着你还不知道 run_id，需要按 job_id 列出所有运行。

**核心知识点：**
- `runs/list` + `job_id` + `include_history=true` = 获取 job 的运行列表含修复历史
- `runs/get` + `run_id` = 获取已知 run 的详情（不含历史列表）
- 要找"最新运行"必须先 list，不能直接 get

---

#### Q317 -- 自动化监控与恢复的 API 流程

**QUESTION 317**
A data engineer wants to automate job monitoring and recovery in Databricks using the Jobs API. They need to list all jobs, identify a failed job, and rerun it.

Which sequence of API actions should the data engineer perform?

- **A. Use the jobs list endpoint to list jobs, check job run statuses with jobs runs list, and rerun a failed job using jobs run-now.** <<<CORRECT>>>
- B. Use the jobs get endpoint to retrieve job details, then use jobs update to rerun failed jobs.
- C. Use the jobs list endpoint to list jobs, then use the jobs create endpoint to create a new job, and run the new job using jobs run-now.
- D. Use the jobs cancel endpoint to remove failed jobs, then recreate them with jobs create endpoint and run the new ones.

**我的答案：** B | **正确答案：** A

**错因分析：**
以为 `jobs/update` 可以重跑 job，但 `jobs/update` 只更新 job 配置（如修改 schedule、cluster 设置等），不能触发执行。正确流程：`jobs/list`（找到 job）-> `runs/list`（找到失败的 run）-> `jobs/run-now`（重新触发）。

**核心知识点：**
- 监控恢复流程：list -> runs/list（检查状态）-> run-now（重跑）
- `jobs/update` = 修改 job 配置，不是执行
- `jobs/run-now` = 触发已有 job 的新运行
- 不要 cancel + recreate，直接重跑即可

---

#### Q115 -- Multi-task job 的 run_id 结构

**QUESTION 115**
When using CLI or REST API to get results from jobs with multiple tasks, which statement correctly describes the response structure?

- A. Each run of a job will have a unique job_id; all tasks within this job will have a unique job_id
- B. Each run of a job will have a unique job_id; all tasks within this job will have a unique task_id
- C. Each run of a job will have a unique orchestration_id; all tasks within this job will have a unique run_id
- D. Each run of a job will have a unique run_id; all tasks within this job will have a unique task_id
- **E. Each run of a job will have a unique run_id; all tasks within this job will also have a unique run_id** <<<CORRECT>>>

**我的答案：** B | **正确答案：** E

**错因分析：**
直觉上认为 job run 用 job_id、task 用 task_id，这是想当然。实际上 Databricks 的 ID 体系：
- `job_id` = job 定义的 ID（静态，创建时分配）
- `run_id` = 每次运行的 ID（动态，job 级别）
- 每个 task 也会分配独立的 `run_id`（task 级别）
- `task_key` 是 task 的配置标识符（静态），不是运行时 ID

**核心知识点：**
- job run -> run_id（job 级别）
- task run -> 也是 run_id（task 级别，子级）
- `task_key` / `task_id` 是定义时的名称，不是运行时唯一标识

---

### B. Job 参数传递 (1题)

---

#### Q1 -- dbutils.widgets 是 Jobs API 传参机制

**QUESTION 1**
An upstream system has been configured to pass the date for a given batch of data to the Databricks Jobs API as a parameter. The notebook to be scheduled will use this parameter to load data with the following code:

`df = spark.read.format("parquet").load(f"/mnt/source/{date}")`

Which code block should be used to create the date Python variable used in the above code block?

- A. `date = spark.conf.get("date")`
- B. `input_dict = input(); date = input_dict["date"]`
- C. `import sys; date = sys.argv[1]`
- D. `date = dbutils.notebooks.getParam("date")`
- **E. `dbutils.widgets.text("date", "null"); date = dbutils.widgets.get("date")`** <<<CORRECT>>>

**我的答案：** D | **正确答案：** E

**错因分析：**
`dbutils.notebooks.getParam()` 根本不存在，是纯干扰项。Databricks 接收 Job 参数的方式是通过 `dbutils.widgets`：先声明 widget，再获取值。Jobs API 传参时自动覆盖 widget 的默认值。

**核心知识点：**
- `dbutils.widgets` 是 Job 参数传递的唯一机制
- 流程：`widgets.text("name", "default")` 声明 -> `widgets.get("name")` 获取
- Jobs API 的参数通过 JSON body 传入，自动映射到 widget
- `dbutils.notebooks.getParam()` 不存在
- `spark.conf.get()` 用于 Spark 配置，不是 job 参数

---

### C. Job Run History (1题)

---

#### Q47 -- 60 天保留期

**QUESTION 47**
What statement is true regarding the retention of job run history?

- A. It is retained until you export or delete job run logs
- B. It is retained for 30 days, during which time you can deliver job run logs to DBFS or S3
- **C. It is retained for 60 days, during which you can export notebook run results to HTML** <<<CORRECT>>>
- D. It is retained for 60 days, after which logs are archived
- E. It is retained for 90 days or until the run-id is re-used through custom run configuration

**我的答案：** D | **正确答案：** C

**错因分析：**
知道是 60 天，但以为 60 天后会被归档（archived）。实际上 60 天后直接删除，不会归档。在 60 天内可以导出 notebook 运行结果为 HTML。

**核心知识点：**
- Job run history 保留 **60 天**
- 60 天内可导出 notebook 结果为 HTML
- 60 天后**删除**，不是归档
- 需要长期保留则必须在 60 天内主动导出

---

### D. Job 条件路由与时区 (1题)

---

#### Q301 -- job.start_time 基于 UTC

**QUESTION 301**
A data engineer is creating a daily reporting Job. There are two reporting notebooks -- one for weekdays and the other for weekends. An "If/else condition" task is configured as `"{{job.start_time.is_weekday}} == true"` to route the job to either weekday or weekend notebook tasks. The same job would be used in multiple time zones.

Which action should a senior data engineer take upon reviewing the job to merge or reject the pull request?

- A. Reject. As they should use `{{job.trigger_time.is_weekday}}` instead.
- B. Reject. As the `{{job.start_time.is_weekday}}` is not a valid value reference.
- **C. Reject. As the `{{job.start_time.is_weekday}}` is for the UTC timezone.** <<<CORRECT>>>
- D. Merge. As the job configuration looks good.

**我的答案：** B | **正确答案：** C

**错因分析：**
以为 `job.start_time.is_weekday` 是无效的引用语法。实际上语法是合法的，但问题在于它基于 UTC 时区。当同一个 job 在多个时区使用时，UTC 的工作日/周末判断可能与本地时间不一致（如 UTC 周六凌晨 = 亚洲周六上午 = 美洲周五晚上）。

**核心知识点：**
- `{{job.start_time.is_weekday}}` 语法合法
- 但 `job.start_time` 基于 **UTC** 时区
- 跨时区场景下 UTC 的 weekday 判断可能不正确
- 审查要点：凡涉及时间条件路由 + 多时区，必须注意 UTC 问题

---

### E. Delta Lake 写入优化（与 Jobs 交叉，2题）

---

#### Q22 -- Auto Compaction 目标 128 MB

**QUESTION 22**
Which statement describes Delta Lake Auto Compaction?

- A. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- B. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- C. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
- D. Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.
- **E. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 128 MB.** <<<CORRECT>>>

**我的答案：** A | **正确答案：** E

**错因分析：**
选项 A 和 E 只差目标文件大小：1 GB vs 128 MB。Auto Compaction 的目标是 128 MB（轻量级、自动触发），手动 OPTIMIZE 的目标才是 1 GB。

**核心知识点：**
- Auto Compaction = 写入后异步触发，目标 **128 MB**
- 手动 OPTIMIZE = 主动执行，目标 **1 GB**
- 两者机制不同：Auto Compaction 是自动的轻量操作

---

#### Q107 -- Optimized Writes = 写入前 shuffle

**QUESTION 107**
Which statement describes Delta Lake optimized writes?

- A. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
- B. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
- C. Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.
- D. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
- **E. A shuffle occurs prior to writing to try to group similar data together resulting in fewer files instead of each executor writing multiple files based on directory partitions.** <<<CORRECT>>>

**我的答案：** B | **正确答案：** E

**错因分析：**
选了 B，这是 Auto Compaction 的描述（写入后异步检测）。Optimized Writes 发生在写入**之前**：通过 shuffle 将同分区数据集中到少数 executor，减少每个分区的文件数。

**核心知识点：**
- Optimized Writes = 写入**前** shuffle，减少小文件
- Auto Compaction = 写入**后**异步合并小文件
- 两者互补但独立，不要混淆

---

### F. Batch Job 去重陷阱 (1题)

---

#### Q8 -- dropDuplicates 只管当前 batch

**QUESTION 8**
An upstream source writes Parquet data as hourly batches to directories named with the current date. A nightly batch job runs the following code to ingest all data from the previous day as indicated by the date variable:

```python
(spark.read
    .format("parquet")
    .load(f"/mnt/raw_orders/{date}")
    .dropDuplicates(["customer_id", "order_id"])
    .write
    .mode("append")
    .saveAsTable("orders")
)
```

Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order. If the upstream system is known to occasionally produce duplicate entries for a single order hours apart, which statement is correct?

- A. Each write to the orders table will only contain unique records, and only those records without duplicates in the target table will be written.
- **B. Each write to the orders table will only contain unique records, but newly written records may have duplicates already present in the target table.** <<<CORRECT>>>
- C. Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, these records will be overwritten.
- D. Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, the operation will fail.
- E. Each write to the orders table will run deduplication over the union of new and existing records, ensuring no duplicate records are present.

**我的答案：** A | **正确答案：** B

**错因分析：**
以为 `dropDuplicates` 会检查目标表已有数据。实际上它只在当前 DataFrame（即当前 batch 加载的数据）内去重，完全不知道目标表里有什么。append 模式更不会检查重复。

**核心知识点：**
- `dropDuplicates()` = 当前 DataFrame 内去重，不跨 batch
- `append` 模式 = 纯追加，不检查已有数据
- 要跨 batch 去重必须用 `MERGE INTO`（upsert）

---

## 三、核心对比表

### Jobs API Endpoints 功能对比

| Endpoint | 功能 | 幂等性 | 参数 |
|----------|------|--------|------|
| `jobs/create` | 创建 job 定义 | **不幂等**（N次=N个job） | job config JSON |
| `jobs/run-now` | 触发已有 job 的一次运行 | 每次创建新 run | job_id |
| `runs/submit` | 一次性运行（不创建 job 定义） | 每次创建新 run | task config JSON |
| `jobs/list` | 列出所有 job 定义 | 只读 | offset, limit |
| `jobs/get` | 获取 job 定义详情（含 task 配置） | 只读 | job_id |
| `jobs/update` | 更新 job 配置 | 幂等 | job_id + new config |
| `runs/list` | 列出 job 的运行历史 | 只读 | job_id, include_history |
| `runs/get` | 获取单次运行详情 | 只读 | run_id |
| `runs/get-output` | 获取运行输出 | 只读 | run_id |
| `runs/repair` | 重跑失败的 task | 修改 run 状态 | run_id, rerun_tasks |

### "我要做X，用哪个API" 速查

| 我要做什么 | 正确 API | 常见错选 |
|-----------|---------|---------|
| 查看 multi-task job 的 task 配置 | `jobs/get` | ~~jobs/list~~, ~~runs/get~~ |
| 获取运行历史 + repair history | `runs/list` + job_id + include_history | ~~runs/get~~ |
| 重跑失败的 job | `jobs/run-now` | ~~jobs/update~~, ~~jobs/create~~ |
| 自动化监控流程 | list -> runs/list -> run-now | ~~get -> update~~ |
| 一次性运行不留定义 | `runs/submit` | ~~jobs/create + run-now~~ |

### Multi-task Job ID 体系

| 层级 | ID 字段 | 类型 | 说明 |
|------|--------|------|------|
| Job 定义 | `job_id` | 静态 | 创建 job 时分配，不变 |
| Job 运行 | `run_id` | 动态 | 每次运行生成新 ID |
| Task 定义 | `task_key` | 静态 | 配置时指定的名称 |
| Task 运行 | `run_id` | 动态 | 每个 task 运行也有独立 run_id |

注意：job run 和 task run 的标识符都叫 `run_id`，但层级不同。

### Auto Compaction vs Optimized Writes vs 手动 OPTIMIZE

| 特性 | Auto Compaction | Optimized Writes | 手动 OPTIMIZE |
|------|----------------|------------------|--------------|
| 触发时机 | 写入**后**异步 | 写入**前** | 手动执行 |
| 机制 | 合并已有小文件 | shuffle 减少写出文件数 | 重写整个表/分区的文件 |
| 目标文件大小 | **128 MB** | N/A | **1 GB** |
| 配置方式 | 表属性启用 | 表属性启用 | SQL 命令 |
| 是否需要额外资源 | 使用当前集群 | 增加 shuffle 开销 | 需要计算资源 |

---

## 四、自测清单

### Jobs API（必须全部掌握）

- [ ] `jobs/create` 调用 3 次会创建几个 job？（3个，不幂等）
- [ ] `jobs/create` 会执行 job 吗？（不会，只创建定义）
- [ ] 查看 multi-task job 的 notebook 配置用哪个 endpoint？（`jobs/get`）
- [ ] `jobs/list` 和 `jobs/get` 的区别？（list = 概要列表；get = 单个 job 的完整定义）
- [ ] 获取某 job 的运行历史 + repair history 用什么？（`runs/list` + job_id + include_history）
- [ ] `runs/get` 和 `runs/list` 的区别？（get = 单次运行详情；list = 运行列表）
- [ ] 自动化监控恢复的 API 流程？（jobs/list -> runs/list -> jobs/run-now）
- [ ] `jobs/update` 能重跑 job 吗？（不能，只修改配置）
- [ ] Multi-task job 中 task 运行的唯一标识是什么？（run_id，不是 task_id）
- [ ] job run 和 task run 的 ID 字段名分别是什么？（都叫 run_id，层级不同）

### Job 参数传递

- [ ] Jobs API 向 notebook 传参用什么？（`dbutils.widgets`）
- [ ] `dbutils.notebooks.getParam()` 存在吗？（不存在，干扰项）
- [ ] widget 声明和获取的代码？（`widgets.text("name", "default")` + `widgets.get("name")`）

### Job 运维

- [ ] Job run history 保留多久？（60 天）
- [ ] 60 天后会归档吗？（不会，直接删除）
- [ ] 如何长期保留运行结果？（60 天内导出 HTML）
- [ ] `job.start_time.is_weekday` 基于什么时区？（UTC）

### Delta Lake 写入优化（高频混淆）

- [ ] Auto Compaction 目标文件大小？（128 MB）
- [ ] 手动 OPTIMIZE 目标文件大小？（1 GB）
- [ ] Optimized Writes 发生在写入前还是后？（前，通过 shuffle）
- [ ] Auto Compaction 发生在写入前还是后？（后，异步合并）
- [ ] `dropDuplicates()` 会检查目标表已有数据吗？（不会，只在当前 DataFrame 内去重）
- [ ] 跨 batch 去重应该用什么？（`MERGE INTO`）
