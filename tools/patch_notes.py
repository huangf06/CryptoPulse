# -*- coding: utf-8 -*-
import re

replacements = {}

replacements[105] = """\
### Q105 ❌ — MLflow模型在Spark中的调用方式

**原题：**
The data science team logged a production model using MLflow. The model accepts a list of column names and returns a new column of type DOUBLE. Which code block outputs a DataFrame with schema "customer_id LONG, predictions DOUBLE"?

**选项：**
A. df.map(lambda x:model(x[columns])).select("customer_id, predictions")
B. df.select("customer_id", model(*columns).alias("predictions")) ✅
C. model.predict(df, columns)
D. df.select("customer_id", pandas_udf(model, columns).alias("predictions"))
E. df.apply(model, columns).select("customer_id, predictions")

**我的答案：** E | **正确答案：** B

**解析：**
- MLflow加载的pyfunc模型可以直接作为Spark UDF使用，调用方式是 `model(*columns)`，在 `select` 中展开列名列表并 `.alias("predictions")`，这是标准用法。
- 选项E的 `df.apply()` 是Pandas DataFrame的方法，PySpark DataFrame没有此方法，会报错。
- 选项D中 `pandas_udf(model, columns)` 语法错误，`pandas_udf` 需要函数和返回类型，不接受列名列表作为第二参数。
- 关键：MLflow pyfunc模型加载后可直接当UDF调用，`model(*columns)` 解包列名列表传入。

**知识点：** `MLflow` `pyfunc UDF` `Spark DataFrame select`\
"""

replacements[106] = """\
### Q106 ❌ — 批量去重传播最小化计算成本

**原题：**
A nightly batch job appends data to reviews_raw. The next step propagates new records to a fully deduplicated, validated, enriched table. Which solution minimizes compute costs?

**选项：**
A. Perform a batch read on reviews_raw and do an insert-only merge using composite key (user_id, review_id, product_id, review_timestamp). ✅
B. Configure Structured Streaming with trigger once against reviews_raw.
C. Use Delta Lake version history to get the diff between latest and previous version.
D. Filter records by review_timestamp for the last 48 hours.
E. Reprocess all records and overwrite the next table.

**我的答案：** C | **正确答案：** A

**解析：**
- insert-only merge 使用自然复合主键，只插入目标表中不存在的记录，既保证去重又避免全表扫描，计算成本最低。
- 选项C看似聪明，但 `VERSION AS OF` 只能获取写入的文件列表，无法直接得到"新增行"，且延迟数据（moderator approval）可能在旧版本中已存在，diff不可靠。
- 选项B的 trigger once 虽然也是批处理，但Structured Streaming有额外的checkpoint开销，成本略高于直接merge。
- 选项D按时间戳过滤48小时存在重复风险，且延迟记录可能被遗漏。

**知识点：** `insert-only merge` `Delta Lake去重` `批处理成本优化`\
"""

replacements[107] = """\
### Q107 ❌ — Delta Lake Optimized Writes原理

**原题：**
Which statement describes Delta Lake optimized writes?

**选项：**
A. Before a Jobs cluster terminates, OPTIMIZE is executed on all modified tables.
B. An async job runs after write to detect if files could be compacted further.
C. Data is queued in a messaging bus; committed in one batch once job completes.
D. Uses logical partitions instead of directory partitions; fewer small files in metadata.
E. A shuffle occurs prior to writing to group similar data, resulting in fewer files instead of each executor writing multiple files based on directory partitions. ✅

**我的答案：** B | **正确答案：** E

**解析：**
- Optimized Writes 的核心机制：在写入前对数据进行 shuffle，使同一分区的数据集中到少数 executor，从而每个分区只写出少量大文件，而不是每个 executor 各写一堆小文件。
- 选项B描述的是 Auto Compaction（自动压缩），是另一个独立功能，写完后异步触发，不是 Optimized Writes。
- 选项A描述的不存在，Databricks不会在集群终止前自动OPTIMIZE。
- 关键区分：Optimized Writes = 写入前shuffle减少文件数；Auto Compaction = 写入后异步合并小文件。

**知识点：** `Optimized Writes` `Auto Compaction` `Delta Lake小文件问题`\
"""

replacements[108] = """\
### Q108 ❌ — Auto Loader默认执行模式

**原题：**
Which statement describes the default execution mode for Databricks Auto Loader?

**选项：**
A. Cloud queue+notification services track new files; target table materialized by querying all valid files.
B. New files identified by listing input directory; target table materialized by querying all valid files.
C. Webhooks trigger a Databricks job on new data; auto-merged using inferred rules.
D. New files identified by listing input directory; incrementally and idempotently loaded into Delta Lake. ✅
E. Cloud queue+notification services track new files; incrementally and idempotently loaded into Delta Lake.

**我的答案：** E | **正确答案：** D

**解析：**
- Auto Loader 有两种文件发现模式：**默认是目录列举（directory listing）**，另一种是文件通知（file notification，需手动配置云队列）。
- 选项D正确描述了默认模式：通过列举目录发现新文件，并增量幂等地加载到Delta表。
- 选项E描述的是**文件通知模式**（`cloudFiles.useNotifications=true`），需要额外配置云存储事件通知，不是默认行为。
- 关键：默认=directory listing；file notification模式适合超大规模场景，需显式开启。

**知识点：** `Auto Loader` `directory listing` `file notification` `增量加载`\
"""

replacements[110] = """\
### Q110 ❌ — 高并发高吞吐场景方案选择

**原题：**
A large company needs near real-time solution with hundreds of pipelines, parallel updates, extremely high volume and velocity. Which solution achieves this?

**选项：**
A. Use Databricks High Concurrency clusters, which leverage optimized cloud storage connections to maximize data throughput. ✅
B. Partition ingestion tables by small time duration to allow many files written in parallel.
C. Configure Databricks to save data to attached SSD volumes instead of object storage.
D. Isolate Delta Lake tables in their own storage containers to avoid API limits.
E. Store all tables in a single database for Catalyst Metastore load balancing.

**我的答案：** C | **正确答案：** A

**解析：**
- High Concurrency 集群专为多用户/多任务并发设计，内置了对云存储的优化连接，适合高并发高吞吐场景。
- 选项C将数据存到本地SSD在Databricks架构中不可行，Databricks是云原生架构，计算与存储分离，不依赖本地磁盘持久化。
- 选项B按小时间粒度分区会产生大量小分区和小文件，反而增加元数据开销。
- 选项D/E都是错误的架构建议，不能解决高并发吞吐问题。

**知识点：** `High Concurrency集群` `计算存储分离` `云原生架构`\
"""

replacements[111] = """\
### Q111 ❌ — Notebook级别安装Python包到所有节点

**原题：**
Which describes a method of installing a Python package scoped at the notebook level to all nodes in the currently active cluster?

**选项：**
A. Run `source env/bin/activate` in a notebook setup script
B. Use `b` in a notebook cell
C. Use `%pip install` in a notebook cell ✅
D. Use `%sh pip install` in a notebook cell
E. Install libraries from PyPI using the cluster UI

**我的答案：** E | **正确答案：** C

**解析：**
- `%pip install` 是 Databricks notebook 的魔法命令，会在 driver 和所有 worker 节点上安装包，并自动重启 Python 解释器使包生效，作用域是当前 notebook session。
- 选项E通过集群UI安装是集群级别（cluster-scoped），不是notebook级别，且需要重启集群才能生效。
- 选项D `%sh pip install` 只在 driver 节点的 shell 中执行，不会安装到 worker 节点。
- 关键区分：`%pip` = notebook级别，所有节点；集群UI = 集群级别，需重启。

**知识点：** `%pip install` `notebook-scoped library` `cluster library`\
"""

replacements[113] = """\
### Q113 ❌ — 表覆盖写入后获取版本差异

**原题：**
The customer_churn_params table is overwritten nightly. Immediately after each update, the team wants to determine the difference between the new version and the previous version. Which method can be used?

**选项：**
A. Execute a query to calculate the difference between new and previous version using Delta Lake's built-in versioning and time travel. ✅
B. Parse the Delta Lake transaction log to identify newly written data files.
C. Parse Spark event logs to identify updated/inserted/deleted rows.
D. Execute DESCRIBE HISTORY to get operation metrics including a log of all records added or modified.
E. Use Delta Lake's change data feed to identify updated/inserted/deleted records.

**我的答案：** E | **正确答案：** A

**解析：**
- 使用 Delta Lake time travel，可以直接查询 `VERSION AS OF n-1` 和当前版本，用 `EXCEPT`/`MINUS` 或 join 计算差异，这是最直接的方法。
- 选项E（Change Data Feed，CDF）需要在表创建时或之前启用 `delta.enableChangeDataFeed=true`，题目说"currently the team overwrites nightly"，没有提到CDF已启用，所以不适用。
- 选项D `DESCRIBE HISTORY` 只返回操作元数据（如写入了多少文件），不包含具体行级别的变更记录。
- 关键：time travel 无需预先配置，随时可用；CDF需要提前开启。

**知识点：** `Delta Lake time travel` `Change Data Feed` `版本差异查询`\
"""

replacements[115] = """\
### Q115 ❌ — Jobs API多任务响应结构

**原题：**
When using CLI or REST API to get results from jobs with multiple tasks, which statement correctly describes the response structure?

**选项：**
A. Each run has a unique job_id; all tasks have a unique job_id
B. Each run has a unique job_id; all tasks have a unique task_id
C. Each run has a unique orchestration_id; all tasks have a unique run_id
D. Each run has a unique run_id; all tasks have a unique task_id
E. Each run of a job will have a unique run_id; all tasks within this job will also have a unique run_id ✅

**我的答案：** B | **正确答案：** E

**解析：**
- Databricks Jobs API 中，每次 job run 有一个唯一的 `run_id`（job级别）；多任务job中，每个 task 也有自己独立的 `run_id`（task级别），两者都叫 run_id 但层级不同。
- 选项B中的 `task_id` 是任务的定义标识符（配置时指定的名称），不是运行时的唯一ID，不能用于区分不同次运行的同一任务。
- 选项D混淆了 task_id（静态配置）和运行时的 run_id（动态生成）。
- 关键：job run → run_id；task run → 也是 run_id（子级），不是 task_id。

**知识点：** `Databricks Jobs API` `run_id` `multi-task job` `REST API响应结构`\
"""

replacements[117] = """\
### Q117 ❌ — REST API creator_user_name字段含义

**原题：**
User A created jobs via REST API. User B triggers runs via REST API with their token. User C takes "Owner" privileges via UI. Which value appears in the creator_user_name field?

**选项：**
A. Once User C takes Owner, their email appears; prior to this, User A's email appears.
B. User B's email always appears, as their credentials are always used to trigger the run. ✅
C. User A's email always appears, as they still own the underlying notebooks.
D. Once User C takes Owner, their email appears; prior to this, User B's email appears.
E. User C only appears if they manually trigger the job.

**我的答案：** A | **正确答案：** B

**解析：**
- `creator_user_name` 字段记录的是**触发（trigger）这次run的用户**，即使用其token发起API调用的用户，而不是job的创建者或owner。
- User B 始终使用自己的 personal access token 通过外部编排工具触发 job run，所以 `creator_user_name` 始终是 User B 的邮箱。
- User C 接管 Owner 权限只影响 job 的所有权（谁可以管理job），不影响 run 的触发者记录。
- 关键区分：job owner（谁拥有job）vs run creator（谁触发了这次运行）。

**知识点：** `Databricks Jobs API` `creator_user_name` `personal access token` `job owner vs run creator`\
"""

replacements[126] = """\
### Q126 ❌ — Python变量与Spark视图的跨语言互操作

**原题：**
A junior engineer runs two cells: Cmd 1 (Python) creates countries_af, Cmd 2 (SQL) queries countries_af. Database only has geo_lookup and sales. What is the outcome?

**选项：**
A. Both succeed; countries_af and sales_af registered as views.
B. Cmd 1 succeeds; Cmd 2 searches all accessible databases for countries_af.
C. Cmd 1 succeeds, Cmd 2 fails; countries_af is a Python variable representing a PySpark DataFrame.
D. Cmd 1 succeeds, Cmd 2 fails; countries_af is a Python variable containing a list of strings. ✅

**我的答案：** C | **正确答案：** D

**解析：**
- 根据题目描述，Cmd 1 的 Python 代码将 `countries_af` 赋值为一个包含国家名称字符串的列表（从 geo_lookup 过滤后提取），而不是 PySpark DataFrame。
- 因为 `countries_af` 是 Python 列表变量，没有注册为 Spark 临时视图，所以 Cmd 2 的 SQL 查询找不到名为 `countries_af` 的表/视图，会失败。
- 选项C错在将其描述为 PySpark DataFrame，实际是字符串列表。
- 关键：Python变量（无论是DataFrame还是list）都不会自动成为SQL可查询的视图，必须显式调用 `.createOrReplaceTempView()` 注册。

**知识点：** `notebook语言互操作` `createOrReplaceTempView` `Python变量 vs Spark视图`\
"""

replacements[139] = """\
### Q139 ❌ — Structured Streaming触发模式与存储成本优化

**原题：**
A Structured Streaming job processes microbatches in <3s, but triggers 12+ empty microbatches per minute, causing high cloud storage costs. Records must be processed within 10 minutes. Which adjustment meets the requirement?

**选项：**
A. Set trigger interval to 3 seconds; default is consuming too many records per batch causing spill.
B. Use trigger once and schedule a Databricks job every 10 minutes; minimizes both compute and storage costs. ✅
C. Set trigger interval to 10 minutes; decreasing frequency minimizes storage API calls.
D. Set trigger interval to 500ms; small non-zero interval prevents querying source too frequently.

**我的答案：** C | **正确答案：** B

**解析：**
- 问题根源：默认触发模式（processingTime="0"，即尽可能快）导致每分钟12次以上的空microbatch，每次都会写checkpoint和事务日志，产生大量小文件，推高存储成本。
- `trigger(once=True)` 每次运行处理所有积压数据后立即停止，配合每10分钟调度一次，既满足"10分钟内处理"的SLA，又大幅减少checkpoint写入次数和空批次开销。
- 选项C将触发间隔设为10分钟虽然减少了触发频率，但流式作业会持续运行占用计算资源，成本高于trigger once方案。
- 关键：trigger once = 批处理语义 + 流处理能力，适合延迟容忍度较高（分钟级）的场景，同时最小化存储和计算成本。

**知识点：** `Structured Streaming` `trigger once` `microbatch` `存储成本优化` `checkpoint`\
"""

# Read the file
with open('mock_exam/review_notes.md', 'r', encoding='utf-8') as f:
    content = f.read()

original_len = len(content)

for qn, new_block in replacements.items():
    pattern = rf'### Q{qn} ❌ — 待补充\n\n\*\*我的答案:\*\* \S+ \| \*\*正确答案:\*\* \S+\n\n\*\*知识点:\*\* 待补充\n\n---\n'
    replacement = new_block + '\n\n---\n'
    new_content, count = re.subn(pattern, replacement, content)
    if count == 1:
        content = new_content
        print(f'Q{qn}: replaced OK')
    else:
        print(f'Q{qn}: WARNING - matched {count} times, trying fallback')
        # Fallback: broader pattern
        pattern2 = rf'### Q{qn} ❌ — 待补充.*?---\n'
        new_content2, count2 = re.subn(pattern2, replacement, content, flags=re.DOTALL)
        if count2 == 1:
            content = new_content2
            print(f'Q{qn}: fallback replaced OK')
        else:
            print(f'Q{qn}: FAILED - {count2} matches')

with open('mock_exam/review_notes.md', 'w', encoding='utf-8') as f:
    f.write(content)

print(f'\nDone. File size: {original_len} -> {len(content)} chars')
