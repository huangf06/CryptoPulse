# Databricks Certified Data Engineer Professional - 错题复习笔记

> 来源：examtopics.com | 总题数：327 | 开始日期：2026-02-21

## 统计

| 批次 | 题号范围 | 得分 | 日期 |
|------|---------|------|------|
| 第1轮 Page 1 | Q1-Q10 | 1/10 | 2026-02-21 |
| 第2轮 Page 2 | Q11-Q20 | 6/10 | 2026-02-22 |
| 第3轮 Page 3 | Q21-Q30 | 5/10 | 2026-02-22 |
| 第4轮 Page 4 | Q31-Q40 | 7/10 | 2026-02-23 |
| 第5轮 Page 5 | Q41-Q50 | 6/10 | 2026-03-06 |
| 第6轮 Page 6 | Q51-Q60 | 7/10 | 2026-03-06 |
| 第7轮 Page 7 | Q61-Q70 | 5/10 | 2026-03-06 |
| 第8轮 Page 8 | Q71-Q80 | 7/10 | 2026-03-06 |
| 第9轮 Page 9 | Q81-Q90 | 5/10 | 2026-03-06 |
| 第10轮 Page 10 | Q91-Q100 | 6/10 | 2026-03-06 |
| 第11轮 Page 11 | Q101-Q110 | 3/10 | 2026-03-06 |
| 第12轮 Page 12 | Q111-Q120 | 6/10 | 2026-03-06 |
| 第13轮 Page 13 | Q121-Q130 | 9/10 | 2026-03-06 |
| 第14轮 Page 14 | Q131-Q140 | 9/10 | 2026-03-06 |
| 第15轮 Page 15 | Q141-Q150 | 9/10 | 2026-03-06 |
| 第16轮 Page 16 | Q151-Q160 | 10/10 | 2026-03-06 |
| 第17轮 Page 17 | Q161-Q170 | 8/10 | 2026-03-06 |
| 第18轮 Page 18 | Q171-Q180 | 8/10 | 2026-03-06 |
| 第19轮 Page 19 | Q181-Q190 | 7/10 | 2026-03-06 |
| 第20轮 Page 20 | Q191-Q200 | 10/10 | 2026-03-06 |
| 第21轮 Page 21 | Q201-Q210 | 9/10 | 2026-03-06 |
| 第22轮 Page 22 | Q211-Q220 | 6/10 | 2026-03-06 |
| 第23轮 Page 23 | Q221-Q230 | 6/10 | 2026-03-06 |
| 第24轮 Page 24 | Q231-Q240 | 6/10 | 2026-03-06 |
| 第25轮 Page 25 | Q241-Q250 | 5/10 | 2026-03-06 |
| 第26轮 Page 26 | Q251-Q260 | 7/10 | 2026-03-06 |
| 第27轮 Page 27 | Q261-Q270 | 4/10 | 2026-03-06 |
| 第28轮 Page 28 | Q271-Q280 | 10/10 | 2026-03-06 |
| 第29轮 Page 29 | Q281-Q290 | 7/10 | 2026-03-06 |
| 第30轮 Page 30 | Q291-Q300 | 1/10 | 2026-03-06 |
| 第31轮 Page 31 | Q301-Q310 | 5/10 | 2026-03-06 |
| 第32轮 Page 32 | Q311-Q320 | 3/10 | 2026-03-06 |
| 第33轮 Page 33 | Q321-Q327 | 3/7 | 2026-03-06 |

---

## Page 1: Q1 - Q10

---

### Q1 ❌ — Jobs API 参数传递

**原题：**
An upstream system has been configured to pass the date for a given batch of data to the Databricks Jobs API as a parameter. The notebook to be scheduled will use this parameter to load data with the following code:
`df = spark.read.format("parquet").load(f"/mnt/source/{date}")`
Which code block should be used to create the date Python variable used in the above code block?

**选项：**
A. `date = spark.conf.get("date")`
B. `input_dict = input(); date = input_dict["date"]`
C. `import sys; date = sys.argv[1]`
D. `date = dbutils.notebooks.getParam("date")`
E. `dbutils.widgets.text("date", "null"); date = dbutils.widgets.get("date")` ✅

**我的答案：** D | **正确答案：** E（社区投票 95%）

**解析：**
- `dbutils.notebooks.getParam()` 根本不存在，是干扰项
- Databricks 接收 Job 参数的唯一方式是 `dbutils.widgets`
- 先 `widgets.text()` 声明参数，再 `widgets.get()` 获取值
- Jobs API 传参时会自动覆盖 widget 默认值

**知识点：** `dbutils.widgets` `Jobs API`

---

### Q2 ❌ — 集群最小权限

**原题：**
The Databricks workspace administrator has configured interactive clusters for each of the data engineering groups. To control costs, clusters are set to terminate after 30 minutes of inactivity. Each user should be able to execute workloads against their assigned clusters at any time of the day.
Assuming users have been added to a workspace but not granted any permissions, which of the following describes the minimal permissions a user would need to start and attach to an already configured cluster.

**选项：**
A. "Can Manage" privileges on the required cluster
B. Workspace Admin privileges, cluster creation allowed, "Can Attach To" privileges on the required cluster
C. Cluster creation allowed, "Can Attach To" privileges on the required cluster
D. "Can Restart" privileges on the required cluster ✅
E. Cluster creation allowed, "Can Restart" privileges on the required cluster

**我的答案：** B | **正确答案：** D（社区投票 79% D, 17% C）

**解析：**
- Can Attach To：只能 attach 到已运行的集群（不够，集群会自动关闭）
- Can Restart：可以启动/重启集群 + 隐含 attach 权限（刚好够用）
- Can Manage：完全控制（权限过多）
- 题目关键词 "minimal permissions" → 选权限最小但刚好满足需求的

**知识点：** `集群权限` `最小权限原则`

---

### Q3 ✅ — Structured Streaming 生产调度

**原题：**
When scheduling Structured Streaming jobs for production, which configuration automatically recovers from query failures and keeps costs low?

**选项：**
A. Cluster: New Job Cluster; Retries: Unlimited; Maximum Concurrent Runs: Unlimited
B. Cluster: New Job Cluster; Retries: None; Maximum Concurrent Runs: 1
C. Cluster: Existing All-Purpose Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1
D. Cluster: New Job Cluster; Retries: Unlimited; Maximum Concurrent Runs: 1 ✅
E. Cluster: Existing All-Purpose Cluster; Retries: None; Maximum Concurrent Runs: 1

**我的答案：** D ✅ | **正确答案：** D（社区投票 100%）

**解析：**
- New Job Cluster 比 All-Purpose Cluster 便宜（成本低）
- Unlimited Retries 保证失败后自动恢复（有 checkpoint 支持断点续传）
- Max Concurrent Runs: 1 防止多个实例同时跑同一个 stream

**知识点：** `Structured Streaming` `Job调度` `成本优化`

---

### Q4 ❌ — SQL Alert 触发条件（GROUP BY）

**原题：**
The data engineering team has configured a Databricks SQL query and alert to monitor the values in a Delta Lake table. The `recent_sensor_recordings` table contains an identifying `sensor_id` alongside the `timestamp` and `temperature` for the most recent 5 minutes of recordings.

The query is:
```sql
SELECT MEAN(temperature), MAX(temperature), MIN(temperature)
FROM recent_sensor_recordings
GROUP BY sensor_id
```

The query is set to refresh each minute and always completes in less than 10 seconds. The alert is set to trigger when `mean(temperature) > 120`. Notifications are triggered to be sent at most every 1 minute.

If this alert raises notifications for 3 consecutive minutes and then stops, which statement must be true?

**选项：**
A. The total average temperature across all sensors exceeded 120 on three consecutive executions of the query
B. The recent_sensor_recordings table was unresponsive for three consecutive runs of the query
C. The source query failed to update properly for three consecutive minutes and then restarted
D. The maximum temperature recording for at least one sensor exceeded 120 on three consecutive executions of the query
E. The average temperature recordings for at least one sensor exceeded 120 on three consecutive executions of the query ✅

**我的答案：** A | **正确答案：** E（社区投票 100%）

**解析：**
- `GROUP BY sensor_id` → 结果是多行（每个 sensor 一行），不是一个总聚合值
- Alert 逐行检查条件，任意一行满足 `mean(temperature) > 120` 就触发
- 正确理解：至少有一个 sensor 的平均温度超过120，连续3次都如此

**知识点：** `SQL Alert` `GROUP BY` `聚合理解`

---

### Q5 ❌ — Databricks Repos 分支不可见

**原题：**
A junior developer complains that the code in their notebook isn't producing the correct results in the development environment. A shared screenshot reveals that while they're using a notebook versioned with Databricks Repos, they're using a personal branch that contains old logic. The desired branch named dev-2.3.9 is not available from the branch selection dropdown.

Which approach will allow this developer to review the current logic for this notebook?

**选项：**
A. Use Repos to make a pull request use the Databricks REST API to update the current branch to dev-2.3.9
B. Use Repos to pull changes from the remote Git repository and select the dev-2.3.9 branch. ✅
C. Use Repos to checkout the dev-2.3.9 branch and auto-resolve conflicts with the current branch
D. Merge all changes back to the main branch in the remote Git repository and clone the repo again
E. Use Repos to merge the current branch and the dev-2.3.9 branch, then make a pull request to sync with the remote repository

**我的答案：** C | **正确答案：** B（社区投票 100%）

**解析：**
- 分支在 dropdown 里不可见 → 本地 repo 没有同步远程仓库的最新信息
- 先 pull 远程仓库（同步分支列表），然后就能在 dropdown 里看到并选择 dev-2.3.9
- 选项 C 的问题：看不到的分支无法 checkout，且 "auto-resolve conflicts" 不是标准操作

**知识点：** `Databricks Repos` `Git操作`

---

### Q6 ❌ — dbutils.secrets 行为

**原题：**
The security team is exploring whether or not the Databricks secrets module can be leveraged for connecting to an external database. After testing the code with all Python variables being defined with strings, they upload the password to the secrets module and configure the correct permissions for the currently active user. They then modify their code to the following (leaving all other variables unchanged).

```python
password = dbutils.secrets.get(scope="db_creds", key="jdbc_password")

print(password)

df = (spark
    .read
    .format("jdbc")
    .option("url", connection)
    .option("dbtable", tablename)
    .option("user", username)
    .option("password", password)
    .load()
)
```

Which statement describes what will happen when the above code is executed?

**选项：**
A. The connection to the external table will fail; the string "REDACTED" will be printed.
B. An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the encoded password will be saved to DBFS.
C. An interactive input box will appear in the notebook; if the right password is provided, the connection will succeed and the password will be printed in plain text.
D. The connection to the external table will succeed; the string value of password will be printed in plain text.
E. The connection to the external table will succeed; the string "REDACTED" will be printed. ✅

**我的答案：** B | **正确答案：** E（社区投票 100%）

**解析：**
- `dbutils.secrets.get()` 返回密码的真实值，JDBC 连接正常工作
- 但 Databricks 安全机制：secrets 获取的值在 notebook 输出中（print/display/log）会被替换为 `[REDACTED]`
- 值本身是正确的，只是显示时被遮蔽，防止密码泄露
- 不会弹出任何交互输入框

**知识点：** `dbutils.secrets` `安全机制` `REDACTED`

---

### Q7 ❌ — MLflow 预测结果保存

**原题：**
The data science team has created and logged a production model using MLflow. The following code correctly imports and applies the production model to output the predictions as a new DataFrame named preds with the schema "customer_id LONG, predictions DOUBLE, date DATE".

```python
from pyspark.sql.functions import current_date

model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/churn/prod")
df = spark.table("customers")
columns = ["account_age", "time_since_last_seen", "app_rating"]
preds = (df.select(
    "customer_id",
    model(*columns).alias("predictions"),
    current_date().alias("date")
))
```

The data science team would like predictions saved to a Delta Lake table with the ability to compare all predictions across time. Churn predictions will be made at most once per day.

Which code block accomplishes this task while minimizing potential compute costs?

**选项：**
A. `preds.write.mode("append").saveAsTable("churn_preds")` ✅
B. `preds.write.format("delta").save("/preds/churn_preds")`
C. `(preds.writeStream.outputMode("overwrite").option("checkpointPath", "/_checkpoints/churn_preds").start("/preds/churn_preds"))`
D. `(preds.write.format("delta").mode("overwrite").saveAsTable("churn_preds"))`
E. `(preds.writeStream.outputMode("append").option("checkpointPath", "/_checkpoints/churn_preds").table("churn_preds"))`

**我的答案：** B | **正确答案：** A（社区投票 100%）

**解析：**
- 需要"跨时间对比" → 必须用 append 模式保留历史数据
- `saveAsTable` 注册到 metastore，方便查询和管理
- 选项 B 用 `save(path)` 不注册 metastore，且默认 mode 是 errorIfExists（第二天跑就报错）
- 选项 C/E 用 writeStream，但 preds 是 batch DataFrame 不是 streaming
- 选项 D 用 overwrite 会覆盖历史数据，无法跨时间对比

**知识点：** `saveAsTable vs save` `append模式` `batch vs streaming`

---

### Q8 ❌ — dropDuplicates 作用范围

**原题：**
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

Assume that the fields customer_id and order_id serve as a composite key to uniquely identify each order.
If the upstream system is known to occasionally produce duplicate entries for a single order hours apart, which statement is correct?

**选项：**
A. Each write to the orders table will only contain unique records, and only those records without duplicates in the target table will be written.
B. Each write to the orders table will only contain unique records, but newly written records may have duplicates already present in the target table. ✅
C. Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, these records will be overwritten.
D. Each write to the orders table will only contain unique records; if existing records with the same key are present in the target table, the operation will fail.
E. Each write to the orders table will run deduplication over the union of new and existing records, ensuring no duplicate records are present.

**我的答案：** A | **正确答案：** B（社区投票 100%）

**解析：**
- `dropDuplicates()` 只在当前 DataFrame（当前 batch）内去重
- 它完全不知道目标表里已有什么数据，不会跨 batch 对比
- 所以：当天 batch 内部不会有重复 ✓，但跨天可能和已有数据重复 ✗
- 如果要跨 batch 去重，需要用 `MERGE INTO`（upsert）
- ⚠️ 高频考点！

**知识点：** `dropDuplicates` `去重范围` `MERGE INTO`

---

### Q9 ❌ — Python 变量 vs SQL 引用

**原题：**
A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. The intended outcome of the below code is to register a view of all sales that occurred in countries on the continent of Africa that appear in the geo_lookup table.
Before executing the code, running SHOW TABLES on the current database indicates the database contains only two tables: geo_lookup and sales.

**Cmd 1**
```python
%python
countries_af = [x[0] for x in
    spark.table("geo_lookup").filter("continent='AF'").select("country").collect()]
```

**Cmd 2**
```sql
%sql
CREATE VIEW sales_af AS
  SELECT *
  FROM sales
  WHERE city IN countries_af
  AND CONTINENT = "AF"
```

Which statement correctly describes the outcome of executing these command cells in order in an interactive notebook?

**选项：**
A. Both commands will succeed. Executing show tables will show that countries_af and sales_af have been registered as views.
B. Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af: if this entity exists, Cmd 2 will succeed.
C. Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.
D. Both commands will fail. No new variables, tables, or views will be created.
E. Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings. ✅

**我的答案：** B | **正确答案：** E（社区投票 93%）

**解析：**
- Cmd 1：`collect()` 返回 Row 列表，list comprehension 提取第一个字段 → 得到 Python 字符串列表 ✓
- Cmd 2：`%sql` cell 中 SQL 引擎不认识 Python 变量，`countries_af` 会被当作列名或表名解析 → 报错 ✗
- 选项 C 错在说 countries_af 是 PySpark DataFrame（实际是 Python list，因为 collect() 已经把数据拉到 driver 了）
- 跨语言传值需要：`spark.sql(f"...")` 或注册临时视图

**知识点：** `语言互操作` `Python vs SQL` `collect()` `变量作用域`

---

### Q10 ❌ — Delta Lake Data Skipping 机制

**原题：**
A Delta table of weather records is partitioned by date and has the below schema: `date DATE, device_id INT, temp FLOAT, latitude FLOAT, longitude FLOAT`
To find all the records from within the Arctic Circle, you execute a query with the below filter: `latitude > 66.3`
Which statement describes how the Delta engine identifies which files to load?

**选项：**
A. All records are cached to an operational database and then the filter is applied
B. The Parquet file footers are scanned for min and max statistics for the latitude column
C. All records are cached to attached storage and then the filter is applied
D. The Delta log is scanned for min and max statistics for the latitude column ✅
E. The Hive metastore is scanned for min and max statistics for the latitude column

**我的答案：** B | **正确答案：** D（社区投票 88%）

**解析：**
- 纯 Parquet：需要打开每个文件读 footer 获取统计信息（慢）
- Delta Lake：统计信息（min/max/null count）存在 `_delta_log/` 目录的 JSON 文件里（快）
- Delta 引擎读 transaction log 就能跳过不相关文件，不需要打开 Parquet 文件本身
- 注意：date 分区在此题不起作用，因为查询条件是 latitude 不是 date
- 这是 Delta Lake 和纯 Parquet 的核心区别

**知识点：** `Delta Log` `Data Skipping` `Transaction Log vs Parquet Footer`

---

## 高频易错知识点汇总

| 知识点 | 出现题号 | 核心要记住的 |
|--------|---------|-------------|
| dbutils.widgets | Q1 | Job 参数传递的唯一方式 |
| dbutils.secrets | Q6 | 值可用但 print 显示 REDACTED |
| 集群权限层级 | Q2 | Can Attach To < Can Restart < Can Manage |
| GROUP BY + Alert | Q4 | Alert 按行检查，不是总聚合 |
| Databricks Repos | Q5 | 看不到分支 → 先 pull 远程 |
| dropDuplicates | Q8 | 只管当前 batch，不跨 batch（高频！） |
| saveAsTable vs save | Q7 | saveAsTable 注册 metastore，save 不注册 |
| batch vs streaming write | Q7 | batch 用 write，streaming 用 writeStream |
| Python vs SQL 变量 | Q9 | SQL cell 不能直接用 Python 变量 |
| Delta Log vs Parquet Footer | Q10 | Delta 用 transaction log 做 data skipping |

---

## Page 2: Q11 - Q20

---

### Q11 ✅ — Delta Lake 删除 + VACUUM + Time Travel

**原题：**
The data engineering team has configured a job to process customer requests to be forgotten (have their data deleted). All user data that needs to be deleted is stored in Delta Lake tables using default table settings.
The team has decided to process all deletions from the previous week as a batch job at 1am each Sunday. The total duration of this job is less than one hour. Every Monday at 3am, a batch job executes a series of VACUUM commands on all Delta Lake tables throughout the organization.
The compliance officer has recently learned about Delta Lake's time travel functionality. They are concerned that this might allow continued access to deleted data. Assuming all delete logic is correctly implemented, which statement correctly addresses this concern?

**选项：**
A. Because the VACUUM command permanently deletes all files containing deleted records, deleted records may be accessible with time travel for around 24 hours.
B. Because the default data retention threshold is 24 hours, data files containing deleted records will be retained until the VACUUM job is run the following day.
C. Because Delta Lake time travel provides full access to the entire history of a table, deleted records can always be recreated by users with full admin privileges.
D. Because Delta Lake's delete statements have ACID guarantees, deleted records will be permanently purged from all storage systems as soon as a delete job completes.
E. Because the default data retention threshold is 7 days, data files containing deleted records will be retained until the VACUUM job is run 8 days later. ✅

**我的答案：** E ✅ | **正确答案：** E（社区投票 60% E, 36% A）

**解析：**
- Delta Lake 默认 `delta.deletedFileRetentionDuration = 7 days`
- DELETE 只是逻辑删除（在 transaction log 中标记），物理文件仍在
- VACUUM 只删除超过 retention threshold 的文件
- 周日删除 → 下周一 VACUUM 时才过了1天，远未到7天 → 文件不会被清理
- 要到8天后的 VACUUM 才会真正物理删除这些文件

**知识点：** `VACUUM` `Time Travel` `deletedFileRetentionDuration` `逻辑删除 vs 物理删除`

---

### Q12 ❌ — Jobs API 2.0/jobs/create 重复调用

**原题：**
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
Assuming that all configurations and referenced resources are available, which statement describes the result of executing this workload three times?

**选项：**
A. Three new jobs named "Ingest new data" will be defined in the workspace, and they will each run once daily.
B. The logic defined in the referenced notebook will be executed three times on new clusters with the configurations of the provided cluster ID.
C. Three new jobs named "Ingest new data" will be defined in the workspace, but no jobs will be executed. ✅
D. One new job named "Ingest new data" will be defined in the workspace, but it will not be executed.
E. The logic defined in the referenced notebook will be executed three times on the referenced existing all purpose cluster.

**我的答案：** D | **正确答案：** C（社区投票 100%）

**解析：**
- `2.0/jobs/create` 是创建 job 定义，不是运行 job（运行要用 `jobs/run-now`）
- 每次调用 `jobs/create` 都会创建一个新的 job 定义，即使 name 相同
- Databricks 允许同名 job 存在（不会去重或覆盖）
- 所以调用3次 = 创建3个同名 job，但都不会执行
- 选项 D 错在以为只会创建1个 job

**知识点：** `Jobs API` `jobs/create vs jobs/run-now` `job 定义不去重`

---

### Q13 ✅ — CDC 日志处理 + Bronze/Silver 架构

**原题：**
An upstream system is emitting change data capture (CDC) logs that are being written to a cloud object storage directory. Each record in the log indicates the change type (insert, update, or delete) and the values for each field after the change. The source table has a primary key identified by the field pk_id.
For auditing purposes, the data governance team wishes to maintain a full record of all values that have ever been valid in the source system. For analytical purposes, only the most recent value for each record needs to be recorded. The Databricks job to ingest these records occurs once per hour, but each individual record may have changed multiple times over the course of an hour.
Which solution meets these requirements?

**选项：**
A. Create a separate history table for each pk_id resolve the current state of the table by running a union all filtering the history tables for the most recent state.
B. Use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a bronze table, then propagate all changes throughout the system.
C. Iterate through an ordered set of changes to the table, applying each in turn; rely on Delta Lake's versioning ability to create an audit log.
D. Use Delta Lake's change data feed to automatically process CDC data from an external system, propagating all changes to all dependent tables in the Lakehouse.
E. Ingest all log information into a bronze table; use MERGE INTO to insert, update, or delete the most recent entry for each pk_id into a silver table to recreate the current table state. ✅

**我的答案：** E ✅ | **正确答案：** E（社区投票 86%）

**解析：**
- 审计需求 → bronze 表保留所有原始 CDC 日志（全量历史）
- 分析需求 → silver 表只保留每个 pk_id 的最新状态
- MERGE INTO 在 silver 表上按 pk_id 做 upsert/delete，保证最新状态
- 选项 B 错在 bronze 表就做 MERGE（丢失历史记录，不满足审计需求）

**知识点：** `CDC处理` `Bronze/Silver架构` `MERGE INTO` `审计 vs 分析需求`

---

### Q14 ❌ — Type 1 表高效更新（account_current）

**原题：**
An hourly batch job is configured to ingest data files from a cloud object storage container where each batch represent all records produced by the source system in a given hour. The batch job to process these records into the Lakehouse is sufficiently delayed to ensure no late-arriving data is missed. The user_id field represents a unique key for the data, which has the following schema: user_id BIGINT, username STRING, user_utc STRING, user_region STRING, last_login BIGINT, auto_pay BOOLEAN, last_updated BIGINT
New records are all ingested into a table named account_history which maintains a full record of all data in the same schema as the source. The next table in the system is named account_current and is implemented as a Type 1 table representing the most recent value for each unique user_id.
Assuming there are millions of user accounts and tens of thousands of records processed hourly, which implementation can be used to efficiently update the described account_current table as part of each hourly batch job?

**选项：**
A. Use Auto Loader to subscribe to new files in the account_history directory; configure a Structured Streaming trigger once job to batch update newly detected files into the account_current table.
B. Overwrite the account_current table with each batch using the results of a query against the account_history table grouping by user_id and filtering for the max value of last_updated.
C. Filter records in account_history using the last_updated field and the most recent hour processed, as well as the max last_login by user_id write a merge statement to update or insert the most recent value for each user_id. ✅
D. Use Delta Lake version history to get the difference between the latest version of account_history and one version prior, then write these records to account_current.
E. Filter records in account_history using the last_updated field and the most recent hour processed, making sure to deduplicate on username; write a merge statement to update or insert the most recent value for each username.

**我的答案：** A | **正确答案：** C（社区投票 69% C, 25% B）

**解析：**
- 关键：高效更新 → 只处理最近一小时的增量数据，不要全表扫描
- 选项 C：用 last_updated 过滤最近一小时 + 按 user_id 取最新 + MERGE INTO → 增量高效
- 选项 A：Auto Loader 是 streaming 方案，题目是 batch job，且 account_history 是表不是文件目录
- 选项 B：每次全表 overwrite，百万级用户太低效
- 选项 E：按 username 去重而非 user_id，不正确（user_id 才是唯一键）

**知识点：** `Type 1 SCD` `增量更新` `MERGE INTO` `batch vs streaming`

---

### Q15 ✅ — Change Data Feed 识别变更记录

**原题：**
A table in the Lakehouse named customer_churn_params is used in churn prediction by the machine learning team. The table contains information about customers derived from a number of upstream sources. Currently, the data engineering team populates this table nightly by overwriting the table with the current valid values derived from upstream data sources.
The churn prediction model used by the ML team is fairly stable in production. The team is only interested in making predictions on records that have changed in the past 24 hours.
Which approach would simplify the identification of these changed records?

**选项：**
A. Apply the churn model to all rows in the customer_churn_params table, but implement logic to perform an upsert into the predictions table that ignores rows where predictions have not changed.
B. Convert the batch job to a Structured Streaming job using the complete output mode; configure a Structured Streaming job to read from the customer_churn_params table and incrementally predict against the churn model.
C. Calculate the difference between the previous model predictions and the current customer_churn_params on a key identifying unique customers before making new predictions; only make predictions on those customers not in the previous predictions.
D. Modify the overwrite logic to include a field populated by calling spark.sql.functions.current_timestamp() as data are being written; use this field to identify records written on a particular date.
E. Replace the current overwrite logic with a merge statement to modify only those records that have changed; write logic to make predictions on the changed records identified by the change data feed. ✅

**我的答案：** E ✅ | **正确答案：** E（社区投票 86%）

**解析：**
- 需求：识别过去24小时变更的记录
- Change Data Feed (CDF) 是 Delta Lake 内置功能，自动追踪行级变更
- 用 MERGE 替代 overwrite → 只修改真正变化的行 → CDF 能精确捕获这些变更
- 选项 D 的 timestamp 方案：overwrite 会重写所有行，timestamp 无法区分真正变化的记录

**知识点：** `Change Data Feed` `MERGE vs Overwrite` `增量预测`

---

### Q16 ❌ — CREATE TABLE AS SELECT (CTAS) 行为

**原题：**
A table is registered with the following code:
```sql
CREATE TABLE recent_orders AS (
  SELECT a.user_id, a.email, b.order_id, b.order_date
  FROM
    (SELECT user_id, email
    FROM users) a
  INNER JOIN
    (SELECT user_id, order_id, order_date
    FROM orders
    WHERE order_date >= (current_date() - 7)) b
  ON a.user_id = b.user_id
)
```
Both users and orders are Delta Lake tables. Which statement describes the results of querying recent_orders?

**选项：**
A. All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query finishes.
B. All logic will execute when the table is defined and store the result of joining tables to the DBFS; this stored data will be returned when the table is queried. ✅
C. Results will be computed and cached when the table is defined; these cached results will incrementally update as new records are inserted into source tables.
D. All logic will execute at query time and return the result of joining the valid versions of the source tables at the time the query began.
E. The versions of each source table will be stored in the table transaction log; query results will be saved to DBFS with each query.

**我的答案：** D | **正确答案：** B（社区投票 57% B, 43% D）

**解析：**
- `CREATE TABLE ... AS SELECT` (CTAS) 创建的是物化表（managed table），不是视图
- CTAS 在定义时就执行查询，将结果物理存储到 DBFS
- 之后查询 recent_orders 返回的是存储的静态数据，不会重新执行 JOIN
- 如果想要每次查询都重新计算，应该用 `CREATE VIEW`
- 选项 D 描述的是 VIEW 的行为，不是 TABLE 的行为

**知识点：** `CTAS` `TABLE vs VIEW` `物化表` `静态数据`

---

### Q17 ✅ — Auto Optimize + MERGE 导致小文件

**原题：**
A production workload incrementally applies updates from an external Change Data Capture feed to a Delta Lake table as an always-on Structured Stream job. When data was initially migrated for this table, OPTIMIZE was executed and most data files were resized to 1 GB. Auto Optimize and Auto Compaction were both turned on for the streaming production job. Recent review of data files shows that most data files are under 64 MB, although each partition in the table contains at least 1 GB of data and the total table size is over 10 TB.
Which of the following likely explains these smaller file sizes?

**选项：**
A. Databricks has autotuned to a smaller target file size to reduce duration of MERGE operations ✅
B. Z-order indices calculated on the table are preventing file compaction
C. Bloom filter indices calculated on the table are preventing file compaction
D. Databricks has autotuned to a smaller target file size based on the overall size of data in the table
E. Databricks has autotuned to a smaller target file size based on the amount of data in each partition

**我的答案：** A ✅ | **正确答案：** A（社区投票 66% A, 30% E）

**解析：**
- Auto Optimize 会根据工作负载类型自动调整目标文件大小
- 当表有频繁的 MERGE/UPDATE/DELETE 操作时，Databricks 会自动缩小目标文件大小
- 小文件 → MERGE 操作时需要重写的数据量更少 → 更快完成
- 初始 OPTIMIZE 到 1GB 是针对读优化，但 streaming CDC + MERGE 场景下自动调小

**知识点：** `Auto Optimize` `Auto Tune` `文件大小 vs MERGE性能`

---

### Q18 ✅ — Stream-Static Join 行为

**原题：**
Which statement regarding stream-static joins and static Delta tables is correct?

**选项：**
A. Each microbatch of a stream-static join will use the most recent version of the static Delta table as of each microbatch. ✅
B. Each microbatch of a stream-static join will use the most recent version of the static Delta table as of the job's initialization.
C. The checkpoint directory will be used to track state information for the unique keys present in the join.
D. Stream-static joins cannot use static Delta tables because of consistency issues.
E. The checkpoint directory will be used to track updates to the static Delta table.

**我的答案：** A ✅ | **正确答案：** A（社区投票 63% A, 37% B）

**解析：**
- Stream-static join：每个 microbatch 处理时，都会重新读取 static 表的最新版本
- 不是在 job 启动时固定版本（那是选项 B 的说法）
- Checkpoint 只追踪 streaming 端的进度，不追踪 static 表
- 这意味着 static 表的更新会自动反映在后续 microbatch 中

**知识点：** `Stream-Static Join` `microbatch` `静态表版本`

---

### Q19 ✅ — Streaming Window 聚合

**原题：**
A junior data engineer has been asked to develop a streaming data pipeline with a grouped aggregation using DataFrame df. The pipeline needs to calculate the average humidity and average temperature for each non-overlapping five-minute interval. Events are recorded once per minute per device.
Streaming DataFrame df has the following schema: "device_id INT, event_time TIMESTAMP, temp FLOAT, humidity FLOAT"
Code block:
```python
df.withWatermark("event_time", "10 minutes")
    .groupBy(
        ______,
        "device_id"
    )
    .agg(
        avg("temp").alias("avg_temp"),
        avg("humidity").alias("avg_humidity")
    )
    .writeStream
    .format("delta")
    .saveAsTable("sensor_avg")
```
Choose the response that correctly fills in the blank within the code block to complete this task.

**选项：**
A. to_interval("event_time", "5 minutes").alias("time")
B. window("event_time", "5 minutes").alias("time") ✅
C. "event_time"
D. window("event_time", "10 minutes").alias("time")
E. lag("event_time", "10 minutes").alias("time")

**我的答案：** B ✅ | **正确答案：** B（社区投票 100%）

**解析：**
- non-overlapping five-minute interval → tumbling window = `window("event_time", "5 minutes")`
- `window()` 函数创建固定时间窗口用于聚合
- watermark 是10分钟（允许迟到数据），但窗口大小是5分钟（聚合粒度）
- `to_interval` 和 `lag` 不是窗口函数

**知识点：** `window()` `tumbling window` `watermark` `streaming聚合`

---

### Q20 ❌ — Structured Streaming 共享 Checkpoint 目录

**原题：**
A data architect has designed a system in which two Structured Streaming jobs will concurrently write to a single bronze Delta table. Each job is subscribing to a different topic from an Apache Kafka source, but they will write data with the same schema. To keep the directory structure simple, a data engineer has decided to nest a checkpoint directory to be shared by both streams.
The proposed directory structure is displayed below:
```
/bronze
├── _checkpoint
├── _delta_log
├── year_week=2020_01
└── year_week=2020_02
```
Which statement describes whether this checkpoint directory structure is valid for the given scenario and why?

**选项：**
A. No; Delta Lake manages streaming checkpoints in the transaction log.
B. Yes; both of the streams can share a single checkpoint directory.
C. No; only one stream can write to a Delta Lake table.
D. Yes; Delta Lake supports infinite concurrent writers.
E. No; each of the streams needs to have its own checkpoint directory. ✅

**我的答案：** D | **正确答案：** E（社区投票 89%）

**解析：**
- 每个 Structured Streaming 查询必须有自己独立的 checkpoint 目录
- Checkpoint 记录了 stream 的 offset、状态等信息，两个 stream 共享会互相覆盖/冲突
- Delta Lake 确实支持多个 stream 并发写入同一张表（通过乐观并发控制）
- 但 checkpoint 必须分开，例如 `/bronze/_checkpoint/stream1` 和 `/bronze/_checkpoint/stream2`
- 选项 D 说"支持无限并发写入"过于绝对，且忽略了 checkpoint 问题

**知识点：** `Checkpoint目录` `并发写入` `Structured Streaming` `每个stream独立checkpoint`

---

## Page 2 高频易错知识点汇总

| 知识点 | 出现题号 | 核心要记住的 |
|--------|---------|-------------|
| VACUUM + Time Travel | Q11 | 默认保留7天，VACUUM 只删超期文件 |
| Jobs API create vs run | Q12 | create 只创建定义不执行，允许同名 |
| CDC + Bronze/Silver | Q13 | Bronze 存全量，Silver 用 MERGE 取最新 |
| Type 1 SCD 增量更新 | Q14 | 过滤增量 + MERGE，不要全表扫描 |
| Change Data Feed | Q15 | MERGE + CDF 精确追踪行级变更 |
| CTAS vs VIEW | Q16 | CTAS 是物化表（定义时执行），VIEW 是每次查询时执行 |
| Auto Tune 文件大小 | Q17 | MERGE 频繁时自动缩小目标文件 |
| Stream-Static Join | Q18 | 每个 microbatch 读 static 表最新版本 |
| window() 函数 | Q19 | tumbling window 用 window(col, interval) |
| Checkpoint 独立性 | Q20 | 每个 stream 必须有独立 checkpoint 目录 |

---

## Page 3: Q21 - Q30

---

### Q21 ❌ — Structured Streaming Trigger Interval 调优

**原题：**
A Structured Streaming job deployed to production has been experiencing delays during peak hours of the day. At present, during normal execution, each microbatch of data is processed in less than 3 seconds. During peak hours of the day, execution time for each microbatch becomes very inconsistent, sometimes exceeding 30 seconds. The streaming write is currently configured with a trigger interval of 10 seconds.
Holding all other variables constant and assuming records need to be processed in less than 10 seconds, which adjustment will meet the requirement?

**选项：**
A. Decrease the trigger interval to 5 seconds; triggering batches more frequently allows idle executors to begin processing the next batch while longer running tasks from previous batches finish.
B. Increase the trigger interval to 30 seconds; setting the trigger interval near the maximum execution time observed for each batch is always best practice to ensure no records are dropped.
C. The trigger interval cannot be modified without modifying the checkpoint directory; to maintain the current stream state, increase the number of shuffle partitions to maximize parallelism.
D. Use the trigger once option and configure a Databricks job to execute the query every 10 seconds; this ensures all backlogged records are processed with each batch.
E. Decrease the trigger interval to 5 seconds; triggering batches more frequently may prevent records from backing up and large batches from causing spill. ✅

**我的答案：** A | **正确答案：** E（社区投票 E 60%, B 29%）

**解析：**
- 问题根因：trigger interval 10秒，但高峰期处理时间超30秒 → 数据积压 → batch 越来越大 → 更慢
- 选项 A 和 E 都说减小到5秒，但理由不同：
  - A 说"idle executors 处理下一个 batch"→ 错误，Structured Streaming 不会在上一个 batch 未完成时启动下一个
  - E 说"防止数据积压和大 batch 导致 spill"→ 正确，更频繁触发 = 每个 batch 数据量更小 = 处理更快
- 减小 trigger interval 不会让 batch 并行，而是让每个 batch 处理的数据量更少

**知识点：** `Trigger Interval` `数据积压` `Spill` `microbatch不并行`

---

### Q22 ❌ — Delta Lake Auto Compaction

**原题：**
Which statement describes Delta Lake Auto Compaction?

**选项：**
A. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 1 GB.
B. Before a Jobs cluster terminates, OPTIMIZE is executed on all tables modified during the most recent job.
C. Optimized writes use logical partitions instead of directory partitions; because partition boundaries are only represented in metadata, fewer small files are written.
D. Data is queued in a messaging bus instead of committing data directly to memory; all data is committed from the messaging bus in one batch once the job is complete.
E. An asynchronous job runs after the write completes to detect if files could be further compacted; if yes, an OPTIMIZE job is executed toward a default of 128 MB. ✅

**我的答案：** A | **正确答案：** E（社区投票 E 93%）

**解析：**
- Auto Compaction 在每次写入完成后异步运行，检测是否需要合并小文件
- 关键区别：目标大小是 128 MB，不是 1 GB
  - Auto Compaction → 128 MB（轻量级，写入后自动触发）
  - 手动 OPTIMIZE → 1 GB（完整优化，需要手动执行）
- 选项 A 的描述机制正确，但目标大小错误（1 GB vs 128 MB）
- 选项 C 描述的是 Optimized Writes，不是 Auto Compaction（两者是不同功能）

**知识点：** `Auto Compaction` `128 MB` `OPTIMIZE 1GB` `Optimized Writes ≠ Auto Compaction`

---

### Q23 ✅ — Spark Structured Streaming 编程模型

**我的答案：** D ✅ | **正确答案：** D（社区投票 100%）
Structured Streaming 将数据流建模为无界表（unbounded table），新数据作为新行追加。

---

### Q24 ✅ — spark.sql.files.maxPartitionBytes

**我的答案：** A ✅ | **正确答案：** A（社区投票 100%）
`spark.sql.files.maxPartitionBytes` 直接控制数据摄入时 spark-partition 的大小。

---

### Q25 ✅ — Data Skew 诊断

**我的答案：** D ✅ | **正确答案：** D（社区投票 100%）
Min/Median 相近但 Max 是 Min 的100倍 → 典型的数据倾斜（skew），某些 partition 数据量远大于其他。

---

### Q26 ❌ — 集群配置与 Wide Transformation 性能

**原题：**
Each configuration below is identical to the extent that each cluster has 400 GB total of RAM, 160 total cores and only one Executor per VM. Given a job with at least one wide transformation, which of the following cluster configurations will result in maximum performance?

**选项：**
A. • Total VMs: 1 • 400 GB per Executor • 160 Cores / Executor
B. • Total VMs: 8 • 50 GB per Executor • 20 Cores / Executor
C. • Total VMs: 16 • 25 GB per Executor • 10 Cores/Executor ✅
D. • Total VMs: 4 • 100 GB per Executor • 40 Cores/Executor
E. • Total VMs: 2 • 200 GB per Executor • 80 Cores/Executor

**我的答案：** A | **正确答案：** C（社区投票 C 37%, A 28%, B 26%）

**解析：**
- Wide transformation（如 join、groupBy）需要 shuffle → 数据在节点间传输
- 更多 VM = 更多并行度 + 更多网络带宽用于 shuffle
- 选项 A（1台VM）：所有 shuffle 在单机内完成，但单机 160 cores 的 GC 压力巨大，JVM 管理 400GB 堆内存效率极低
- 选项 C（16台VM）：每台 25GB/10cores，JVM 管理小堆内存更高效，shuffle 可以充分并行
- 关键原则：分布式计算中，更多小节点通常优于少量大节点（尤其是有 shuffle 的场景）
- 但也不是越多越好，需要平衡 shuffle 网络开销

**知识点：** `Wide Transformation` `Shuffle` `集群配置` `JVM堆内存` `并行度`

---

### Q27 ✅ — MERGE INTO WHEN NOT MATCHED INSERT

**我的答案：** B ✅ | **正确答案：** B（社区投票 91%）
MERGE 只有 `WHEN NOT MATCHED INSERT *`，所以已存在的 event_id 会被忽略（matched 但没有对应操作）。

---

### Q28 ❌ — Change Data Feed + append 模式重复数据

**原题：**
A junior data engineer seeks to leverage Delta Lake's Change Data Feed functionality to create a Type 1 table representing all of the values that have ever been valid for all rows in a bronze table created with the property delta.enableChangeDataFeed = true. They plan to execute the following code as a daily job:

```python
from pyspark.sql.functions import col

(spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("bronze")
    .filter(col("_change_type").isin(["update_postimage", "insert"]))
    .write
    .mode("append")
    .table("bronze_history_type1")
)
```

Which statement describes the execution and results of running the above query multiple times?

**选项：**
A. Each time the job is executed, newly updated records will be merged into the target table, overwriting previous values with the same primary keys.
B. Each time the job is executed, the entire available history of inserted or updated records will be appended to the target table, resulting in many duplicate entries. ✅
C. Each time the job is executed, the target table will be overwritten using the entire history of inserted or updated records, giving the desired result.
D. Each time the job is executed, the differences between the original and current versions are calculated; this may result in duplicate entries for some records.
E. Each time the job is executed, only those records that have been inserted or updated since the last execution will be appended to the target table, giving the desired result.

**我的答案：** E | **正确答案：** B（社区投票 B 93%）

**解析：**
- 关键：`startingVersion = 0` → 每次执行都从版本0开始读取全部变更历史
- `spark.read`（batch read）不维护状态，不记得上次读到哪里
- 每次运行都读取从版本0到当前的所有 insert 和 update_postimage 记录
- `.mode("append")` → 追加到目标表 → 每次运行都追加全量 → 大量重复
- 选项 E 描述的是 streaming + checkpoint 的行为（增量读取），但这里用的是 batch read
- 如果要实现增量读取，应该用 `readStream` + checkpoint，或者记录上次处理的版本号

**知识点：** `Change Data Feed` `startingVersion` `batch vs streaming` `append重复` `无状态读取`

---

### Q29 ✅ — Bronze 表保留原始数据防止数据丢失

**我的答案：** E ✅ | **正确答案：** E（社区投票 92%）
将 Kafka 原始数据全量写入 bronze Delta 表，创建永久可重放的数据历史，避免因 Kafka retention 过期导致数据丢失。

---

### Q30 ❌ — readStream.table() 增量处理

**原题：**
A nightly job ingests data into a Delta Lake table using the following code:

```python
def ingest_daily_batch(time_col: Column, year:int, month:int, day:int):
    (spark.read
        .format("parquet")
        .load(f"/mnt/daily_batch/{year}/{month}/{day}")
        .select("*",
            time_col.alias("ingest_time"),
            input_file_name().alias("source_file")
        )
        .write
        .mode("append")
        .saveAsTable("bronze")
    )
```

The next step in the pipeline requires a function that returns an object that can be used to manipulate new records that have not yet been processed to the next table in the pipeline. Which code snippet completes this function definition?

def new_records():

**选项：**
A. return spark.readStream.table("bronze") ✅
B. return spark.readStream.load("bronze")
C. return (spark.read.table("bronze").filter(col("ingest_time") == current_timestamp()))
D. return spark.read.option("readChangeFeed", "true").table("bronze")
E. return (spark.read.table("bronze").filter(col("source_file") == f"/mnt/daily_batch/{year}/{month}/{day}"))

**我的答案：** D | **正确答案：** A（社区投票 A 47%, D 26%, E 26%）

**解析：**
- 需求：返回一个对象来操作"尚未处理到下一张表"的新记录
- `spark.readStream.table("bronze")` → 返回 streaming DataFrame，自动通过 checkpoint 追踪已处理的数据
- 每次运行只处理自上次 checkpoint 以来新增的记录 → 完美满足"未处理的新记录"需求
- 选项 D（readChangeFeed）：CDF 追踪的是行级变更（insert/update/delete），但这里 bronze 表只有 append 操作，且 CDF 需要额外配置 `delta.enableChangeDataFeed`
- 选项 B 错误：`readStream.load("bronze")` 语法不对，load 需要路径而非表名
- 选项 C/E：batch read + filter 无法可靠追踪"未处理"状态

**知识点：** `readStream.table()` `增量处理` `checkpoint追踪` `streaming vs CDF`

---

## Page 3 高频易错知识点汇总

| 知识点 | 出现题号 | 核心要记住的 |
|--------|---------|-------------|
| Trigger Interval | Q21 | 减小 interval = 减小 batch 大小，microbatch 不并行 |
| Auto Compaction | Q22 | 目标 128 MB，手动 OPTIMIZE 才是 1 GB |
| 集群配置 + Shuffle | Q26 | Wide transformation 场景下，多小节点优于少大节点 |
| CDF batch read | Q28 | startingVersion=0 + batch read = 每次全量读取，会重复 |
| readStream.table() | Q30 | streaming read + checkpoint 自动追踪增量 |

---

## Page 4: Q31 - Q40

---

### Q31 ✅ — Schema 推断 vs 手动声明

**原题：**
A junior data engineer is working to implement logic for a Lakehouse table named silver_device_recordings. The source data contains 100 unique fields in a highly nested JSON structure.

The silver_device_recordings table will be used downstream to power several production monitoring dashboards and a production model. At present, 45 of the 100 fields are being used in at least one of these applications.

The data engineer is trying to determine the best approach for dealing with schema declaration given the highly-nested structure of the data and the numerous fields.

Which of the following accurately presents information about Delta Lake and Databricks that may impact their decision-making process?

**选项：**
A. The Tungsten encoding used by Databricks is optimized for storing string data; newly-added native support for querying JSON strings means that string types are always most efficient.
B. Because Delta Lake uses Parquet for data storage, data types can be easily evolved by just modifying file footer information in place.
C. Human labor in writing code is the largest cost associated with data engineering workloads; as such, automating table declaration logic should be a priority in all migration workloads.
D. Because Databricks will infer schema using types that allow all observed data to be processed, setting types manually provides greater assurance of data quality enforcement. ✅
E. Schema inference and evolution on Databricks ensure that inferred types will always accurately match the data types used by downstream systems.

**我的答案：** D ✅ | **正确答案：** D

**解析：**
- Databricks 自动推断 schema 时使用能容纳所有数据的类型（如推断为 STRING 而非 INT）
- 手动设置类型能更严格地保证数据质量，在写入时就拦截异常数据
- 生产环境推荐手动声明 schema

**知识点：** `Schema推断` `数据质量` `手动声明优于自动推断`

---

### Q32 ✅ — DLT Live Table（非增量）JOIN 行为

**原题：**
The data engineering team maintains the following code:

Assuming that this code produces logically correct results and the data in the source tables has been de-duplicated and validated, which statement describes what will occur when this code is executed?

**选项：**
A. A batch job will update the enriched_itemized_orders_by_account table, replacing only those rows that have different values than the current version of the table, using accountID as the primary key.
B. The enriched_itemized_orders_by_account table will be overwritten using the current valid version of data in each of the three tables referenced in the join logic. ✅
C. An incremental job will leverage information in the state store to identify unjoined rows in the source tables and write these rows to the enriched_itemized_orders_by_account table.
D. An incremental job will detect if new rows have been written to any of the source tables; if new rows are detected, all results will be recalculated and used to overwrite the enriched_itemized_orders_by_account table.
E. No computation will occur until enriched_itemized_orders_by_account is queried; upon query materialization, results will be calculated using the current valid version of data in each of the three tables referenced in the join logic.

**我的答案：** B ✅ | **正确答案：** B

**解析：**
- DLT 中非增量的 live table（无 STREAMING 关键字）每次 pipeline 运行时完整重新计算
- 用源表当前有效版本的数据覆盖目标表
- 选项 C/D 描述的是 streaming live table 的增量行为
- 选项 E 描述的是 VIEW 的行为

**知识点：** `DLT Live Table` `非增量全量重写` `STREAMING关键字区分`

---

### Q33 ❌ — 数据库隔离与权限管理

**原题：**
The data engineering team is migrating an enterprise system with thousands of tables and views into the Lakehouse. They plan to implement the target architecture using a series of bronze, silver, and gold tables. Bronze tables will almost exclusively be used by production data engineering workloads, while silver tables will be used to support both data engineering and machine learning workloads. Gold tables will largely serve business intelligence and reporting purposes. While personal identifying information (PII) exists in all tiers of data, pseudonymization and anonymization rules are in place for all data at the silver and gold levels.

The organization is interested in reducing security concerns while maximizing the ability to collaborate across diverse teams.

Which statement exemplifies best practices for implementing this system?

**选项：**
A. Isolating tables in separate databases based on data quality tiers allows for easy permissions management through database ACLs and allows physical separation of default storage locations for managed tables. ✅
B. Because databases on Databricks are merely a logical construct, choices around database organization do not impact security or discoverability in the Lakehouse.
C. Storing all production tables in a single database provides a unified view of all data assets available throughout the Lakehouse, simplifying discoverability by granting all users view privileges on this database.
D. Working in the default Databricks database provides the greatest security when working with managed tables, as these will be created in the DBFS root.
E. Because all tables must live in the same storage containers used for the database they're created in, organizations should be prepared to create between dozens and thousands of databases depending on their data isolation requirements.

**我的答案：** C | **正确答案：** A

**解析：**
- 按数据质量层级（bronze/silver/gold）分库，可以通过数据库级别 ACL 统一管理权限
- 不同数据库可以指定不同的默认存储位置（LOCATION），实现物理隔离
- 选项 C 把所有表放一个库 → 权限管理困难，PII 数据暴露风险高
- 选项 B 说数据库只是逻辑构造不影响安全 → 错误，数据库 ACL 是重要的安全机制
- 选项 D 用默认数据库 + DBFS root → 安全性最差

**知识点：** `数据库ACL` `存储隔离` `Bronze/Silver/Gold分库` `权限管理`

---

### Q34 ❌ — External Table 创建方式

**原题：**
The data architect has mandated that all tables in the Lakehouse should be configured as external Delta Lake tables.

Which approach will ensure that this requirement is met?

**选项：**
A. Whenever a database is being created, make sure that the LOCATION keyword is used.
B. When configuring an external data warehouse for all table storage, leverage Databricks for all ELT.
C. Whenever a table is being created, make sure that the LOCATION keyword is used. ✅
D. When tables are created, make sure that the EXTERNAL keyword is used in the CREATE TABLE statement.
E. When the workspace is being configured, make sure that external cloud object storage has been mounted.

**我的答案：** D | **正确答案：** C

**解析：**
- Delta Lake 中创建 external table 的方式是在 `CREATE TABLE` 时指定 `LOCATION`
- 指定了 LOCATION → 数据存储在指定路径 → external table
- 不指定 LOCATION → 数据存储在数据库默认路径 → managed table
- 选项 D 的 `EXTERNAL` 关键字：在 Hive 中有效，但 Delta Lake 不使用这个关键字来区分 managed/external
- 选项 A：数据库指定 LOCATION 只是设置默认存储路径，不能保证每张表都是 external
- 关键区别：是表级别的 LOCATION，不是数据库级别的

**知识点：** `External Table` `LOCATION关键字` `Managed vs External` `不是EXTERNAL关键字`

---

### Q35 ✅ — 表重命名 + View 兼容方案

**原题：**
To reduce storage and compute costs, the data engineering team has been tasked with curating a series of aggregate tables leveraged by business intelligence dashboards, customer-facing applications, production machine learning models, and ad hoc analytical queries.

The data engineering team has been made aware of new requirements from a customer-facing application, which is the only downstream workload they manage entirely. As a result, an aggregate table used by numerous teams across the organization will need to have a number of fields renamed, and additional fields will also be added.

Which of the solutions addresses the situation while minimally interrupting other teams in the organization without increasing the number of tables that need to be managed?

**选项：**
A. Send all users notice that the schema for the table will be changing; include in the communication the logic necessary to revert the new table schema to match historic queries.
B. Configure a new table with all the requisite fields and new names and use this as the source for the customer-facing application; create a view that maintains the original data schema and table name by aliasing select fields from the new table. ✅
C. Create a new table with the required schema and new fields and use Delta Lake's deep clone functionality to sync up changes committed to one table to the corresponding table.
D. Replace the current table definition with a logical view defined with the query logic currently writing the aggregate table; create a new table to power the customer-facing application.
E. Add a table comment warning all users that the table schema and field names will be changing on a given date; overwrite the table in place to the specifications of the customer-facing application.

**我的答案：** B ✅ | **正确答案：** B

**解析：**
- 新表满足客户应用的新需求（重命名字段 + 新字段）
- View 用别名映射回旧 schema → 其他团队的查询完全不受影响
- View 不算"表"，所以不增加需要管理的表数量

**知识点：** `View别名兼容` `最小化中断` `schema变更策略`

---

### Q36 ❌ — Delta Log Data Skipping（非分区列）

**原题：**
A Delta Lake table representing metadata about content posts from users has the following schema:
user_id LONG, post_text STRING, post_id STRING, longitude FLOAT, latitude FLOAT, post_time TIMESTAMP, date DATE

This table is partitioned by the date column. A query is run with the following filter:
`longitude < 20 & longitude > -20`

Which statement describes how data will be filtered?

**选项：**
A. Statistics in the Delta Log will be used to identify partitions that might include files in the filtered range.
B. No file skipping will occur because the optimizer does not know the relationship between the partition column and the longitude.
C. The Delta Engine will use row-level statistics in the transaction log to identify the files that meet the filter criteria.
D. Statistics in the Delta Log will be used to identify data files that might include records in the filtered range. ✅
E. The Delta Engine will scan the parquet file footers to identify each row that meets the filter criteria.

**我的答案：** A | **正确答案：** D

**解析：**
- 查询条件是 longitude（非分区列），所以分区裁剪不适用
- Delta Log 中存储了每个数据文件的列级统计信息（min/max/null count）
- 引擎用这些统计信息跳过不可能包含匹配记录的文件（data skipping）
- 选项 A 说"identify partitions" → 错误，这里跳过的是数据文件（data files），不是分区
- 选项 C 说"row-level statistics" → 错误，Delta Log 存的是文件级别统计，不是行级别
- 选项 E 说扫描 Parquet footer → 那是纯 Parquet 的做法，Delta 用 transaction log
- 这题和 Q10 考点几乎一样！注意区分"文件级"和"分区级"

**知识点：** `Data Skipping` `文件级统计` `Delta Log` `分区裁剪 vs 文件跳过`

---

### Q37 ✅ — 跨区域部署 Workspace

**原题：**
A small company based in the United States has recently contracted a consulting firm in India to implement several new data engineering pipelines to power artificial intelligence applications. All the company's data is stored in regional cloud storage in the United States.

The workspace administrator at the company is uncertain about where the Databricks workspace used by the contractors should be deployed.

Assuming that all data governance considerations are accounted for, which statement accurately informs this decision?

**选项：**
A. Databricks runs HDFS on cloud volume storage; as such, cloud virtual machines must be deployed in the region where the data is stored.
B. Databricks workspaces do not rely on any regional infrastructure; as such, the decision should be made based upon what is most convenient for the workspace administrator.
C. Cross-region reads and writes can incur significant costs and latency; whenever possible, compute should be deployed in the same region the data is stored. ✅
D. Databricks leverages user workstations as the driver during interactive development; as such, users should always use a workspace deployed in a region they are physically near.
E. Databricks notebooks send all executable code from the user's browser to virtual machines over the open internet; whenever possible, choosing a workspace region near the end users is the most secure.

**我的答案：** C ✅ | **正确答案：** C

**解析：**
- 核心原则：计算跟着数据走，数据在美国 → workspace 部署在美国
- 跨区域读写会产生显著的网络传输成本和延迟

**知识点：** `Workspace部署` `跨区域成本` `计算与数据同区域`

---

### Q38 ✅ — CHECK Constraint 要求现有数据合规

**原题：**
The downstream consumers of a Delta Lake table have been complaining about data quality issues impacting performance in their applications. Specifically, they have complained that invalid latitude and longitude values in the activity_details table have been breaking their ability to use other geolocation processes.

A junior engineer has written the following code to add CHECK constraints to the Delta Lake table. A senior engineer has confirmed the above logic is correct and the valid ranges for latitude and longitude are provided, but the code fails when executed.

Which statement explains the cause of this failure?

**选项：**
A. Because another team uses this table to support a frequently running application, two-phase locking is preventing the operation from committing.
B. The activity_details table already exists; CHECK constraints can only be added during initial table creation.
C. The activity_details table already contains records that violate the constraints; all existing data must pass CHECK constraints in order to add them to an existing table. ✅
D. The activity_details table already contains records; CHECK constraints can only be added prior to inserting values into a table.
E. The current table schema does not contain the field valid_coordinates; schema evolution will need to be enabled before altering the table to add a constraint.

**我的答案：** C ✅ | **正确答案：** C

**解析：**
- 给已有表添加 CHECK 约束时，所有现有数据必须满足约束条件，否则添加失败
- 解决方案：先清理违规数据（UPDATE/DELETE），再添加约束

**知识点：** `CHECK Constraint` `现有数据必须合规` `ALTER TABLE ADD CONSTRAINT`

---

### Q39 ✅ — Delta Lake 前32列自动统计

**原题：**
Which of the following is true of Delta Lake and the Lakehouse?

**选项：**
A. Because Parquet compresses data row by row, strings will only be compressed when a character is repeated multiple times.
B. Delta Lake automatically collects statistics on the first 32 columns of each table which are leveraged in data skipping based on query filters. ✅
C. Views in the Lakehouse maintain a valid cache of the most recent versions of source tables at all times.
D. Primary and foreign key constraints can be leveraged to ensure duplicate values are never entered into a dimension table.
E. Z-order can only be applied to numeric values stored in Delta Lake tables.

**我的答案：** B ✅ | **正确答案：** B

**解析：**
- Delta Lake 默认对前32列收集 min/max/null count 统计，用于 data skipping
- 可通过 `delta.dataSkippingNumIndexedCols` 修改列数
- 选项 A 错：Parquet 是列式存储，按列压缩
- 选项 D 错：Delta Lake 的 PK/FK 是信息性约束，不强制执行

**知识点：** `前32列统计` `data skipping` `dataSkippingNumIndexedCols`

---

### Q40 ✅ — Type 2 SCD 实现

**原题：**
The view updates represents an incremental batch of all newly ingested data to be inserted or updated in the customers table.

The following logic is used to process these records.

Which statement describes this implementation?

**选项：**
A. The customers table is implemented as a Type 3 table; old values are maintained as a new column alongside the current value.
B. The customers table is implemented as a Type 2 table; old values are maintained but marked as no longer current and new values are inserted. ✅
C. The customers table is implemented as a Type 0 table; all writes are append only with no changes to existing values.
D. The customers table is implemented as a Type 1 table; old values are overwritten by new values and no history is maintained.
E. The customers table is implemented as a Type 2 table; old values are overwritten and new customers are appended.

**我的答案：** B ✅ | **正确答案：** B

**解析：**
- 旧值保留但标记为不再当前（current=false），新值插入 → 典型 Type 2 SCD
- Type 1 = 直接覆盖；Type 2 = 保留历史+标记失效；Type 3 = 旧值存为新列

**知识点：** `Type 2 SCD` `Slowly Changing Dimension` `历史版本保留`

---

## Page 4 高频易错知识点汇总

| 知识点 | 出现题号 | 核心要记住的 |
|--------|---------|-------------|
| 数据库分层隔离 | Q33 | bronze/silver/gold 分库，用数据库 ACL 管权限 |
| External Table | Q34 | 表级 LOCATION 关键字，不是 EXTERNAL 关键字 |
| Data Skipping | Q36 | Delta Log 跳过的是数据文件，不是分区；文件级统计不是行级 |
| Schema 推断 vs 手动 | Q31 | 手动设置类型 → 更严格的数据质量保证 |
| CHECK Constraint | Q38 | 添加约束时现有数据必须全部合规 |
| Delta 前32列统计 | Q39 | 自动收集，用于 data skipping |
| Type 2 SCD | Q40 | 旧值保留+标记失效，新值插入 |

---

## Page 5: Q41 - Q50

---

### Q41 ✅ — DLT expectations

**知识点：** `DLT` `expectations`

---

### Q42 ✅ — DLT expectations

**知识点：** `DLT` `expectations`

---

### Q43 ✅ — DLT expectations

**知识点：** `DLT` `expectations`

---

### Q44 ✅ — DLT expectations

**知识点：** `DLT` `expectations`

---

### Q45 ❌ — Managed table with custom location

**原题:**
An external object storage container has been mounted to the location /mnt/finance_eda_bucket.
The following logic was executed to create a database for the finance team:
After the database was successfully created and permissions configured, a member of the finance team runs the following code:
If all users on the finance team are members of the finance group, which statement describes how the tx_sales table will be created?

**选项:**
A. A logical table will persist the query plan to the Hive Metastore in the Databricks control plane.
B. An external table will be created in the storage container mounted to /mnt/finance_eda_bucket.
C. A logical table will persist the physical plan to the Hive Metastore in the Databricks control plane.
D. A managed table will be created in the storage container mounted to /mnt/finance_eda_bucket. ✅
E. A managed table will be created in the DBFS root storage container.

**我的答案:** B | **正确答案:** D

**解析:**
- 当数据库用 `LOCATION` 指定了存储位置时，该数据库下创建的 managed table 会存储在该位置
- 即使指定了自定义 location，只要没有用 `EXTERNAL` 关键字，仍然是 managed table
- Managed table 的元数据和数据都由 Databricks 管理，DROP TABLE 会删除数据
- External table 只管理元数据，DROP TABLE 不删除数据

**知识点:** `managed table` `external table` `database location`

---

### Q46 ❌ — Databricks Secrets 限制

**原题:**
Although the Databricks Utilities Secrets module provides tools to store sensitive credentials and avoid accidentally displaying them in plain text users should still be careful with which credentials are stored here and which users have access to using these secrets.
Which statement describes a limitation of Databricks Secrets?

**选项:**
A. Because the SHA256 hash is used to obfuscate stored secrets, reversing this hash will display the value in plain text.
B. Account administrators can see all secrets in plain text by logging on to the Databricks Accounts console.
C. Secrets are stored in an administrators-only table within the Hive Metastore; database administrators have permission to query this table by default.
D. Iterating through a stored secret and printing each character will display secret contents in plain text. ✅
E. The Databricks REST API can be used to list secrets in plain text if the personal access token has proper credentials.

**我的答案:** C | **正确答案:** D

**解析:**
- Databricks Secrets 在 notebook 中直接打印会显示 `[REDACTED]`
- 但如果逐字符遍历并打印，可以绕过保护机制显示明文
- 这是一个已知的安全限制，提醒用户要控制谁有权限访问 secrets
- Secrets 不存储在 Hive Metastore，也不能通过 REST API 获取明文

**知识点:** `dbutils.secrets` `security`

---

### Q47 ❌ — Job run history retention

**原题:**
What statement is true regarding the retention of job run history?

**选项:**
A. It is retained until you export or delete job run logs
B. It is retained for 30 days, during which time you can deliver job run logs to DBFS or S3
C. It is retained for 60 days, during which you can export notebook run results to HTML ✅
D. It is retained for 60 days, after which logs are archived
E. It is retained for 90 days or until the run-id is re-used through custom run configuration

**我的答案:** D | **正确答案:** C

**解析:**
- Databricks 保留 job run history **60 天**
- 在这 60 天内，可以导出 notebook 运行结果为 HTML
- 60 天后日志会被删除（不是归档）
- 如果需要长期保留，应该在 60 天内导出

**知识点:** `Jobs` `run history` `retention`

---

### Q48 ✅ — Structured Streaming

**知识点:** `Structured Streaming`

---

### Q49 ❌ — 性能测试最佳实践

**原题:**
A user new to Databricks is trying to troubleshoot long execution times for some pipeline logic they are working on. Presently, the user is executing code cell-by-cell, using display() calls to confirm code is producing the logically correct results as new transformations are added to an operation. To get a measure of average time to execute, the user is running each cell multiple times interactively.
Which of the following adjustments will get a more accurate measure of how code is likely to perform in production?

**选项:**
A. Scala is the only language that can be accurately tested using interactive notebooks; because the best performance is achieved by using Scala code compiled to JARs, all PySpark and Spark SQL logic should be refactored.
B. The only way to meaningfully troubleshoot code execution times in development notebooks is to use production-sized data and production-sized clusters with Run All execution. ✅
C. Production code development should only be done using an IDE; executing code against a local build of open source Spark and Delta Lake will provide the most accurate benchmarks for how code will perform in production.
D. Calling display() forces a job to trigger, while many transformations will only add to the logical query plan; because of caching, repeated execution of the same logic does not provide meaningful results.
E. The Jobs UI should be leveraged to occasionally run the notebook as a job and track execution time during incremental code development because Photon can only be enabled on clusters launched for scheduled jobs.

**我的答案:** C | **正确答案:** B

**解析:**
- 交互式 notebook 逐 cell 执行会触发缓存，无法准确反映生产性能
- `display()` 会触发 action，但重复执行会利用缓存，结果不准确
- 要准确测试性能，需要：
  - 使用生产规模的数据
  - 使用生产规模的集群
  - 用 Run All 一次性执行整个 notebook
- 选项 D 说法部分正确但不是最佳实践

**知识点:** `performance testing` `caching` `Run All`

---

### Q50 ✅ — Structured Streaming

**知识点:** `Structured Streaming`

---


## Page 6: Q51 - Q60

---

### Q51 ❌ — Spark UI 诊断 predicate push-down

**原题:**
Where in the Spark UI can one diagnose a performance problem induced by not leveraging predicate push-down?

**选项:**
A. In the Executor's log file, by grepping for "predicate push-down"
B. In the Stage's Detail screen, in the Completed Stages table, by noting the size of data read from the Input column
C. In the Storage Detail screen, by noting which RDDs are not stored on disk
D. In the Delta Lake transaction log, by noting the column statistics
E. In the Query Detail screen, by interpreting the Physical Plan ✅

**我的答案:** A | **正确答案:** E

**解析:**
- Predicate push-down 是否生效需要查看 **Physical Plan**
- 在 Spark UI 的 **SQL/Query Detail** 页面可以看到 Physical Plan
- 如果看到 `PushedFilters: []` 或者 filter 在 scan 之后，说明没有 push-down
- 正确的 push-down 会在 scan 阶段就应用过滤条件，减少读取的数据量
- Executor log 不会显示 push-down 信息

**知识点:** `Spark UI` `predicate push-down` `Physical Plan`

---

### Q52 ✅ — Python modules

**知识点:** `Python` `modules`

---

### Q53 ✅ — REST API

**知识点:** `REST API` `Jobs`

---

### Q54 ❌ — Python sys.path

**原题:**
Which Python variable contains a list of directories to be searched when trying to locate required modules?

**选项:**
A. importlib.resource_path
B. sys.path ✅
C. os.path
D. pypi.path
E. pylib.source

**我的答案:** D | **正确答案:** B

**解析:**
- `sys.path` 是 Python 搜索模块的目录列表
- 可以用 `sys.path.append()` 添加自定义路径
- `os.path` 是路径操作模块，不是搜索路径列表
- `importlib` 是导入机制，但 `resource_path` 不是标准属性

**知识点:** `Python` `sys.path` `module import`

---

### Q55 ✅ — REST API

**知识点:** `REST API` `Jobs`

---

### Q56 ✅ — REST API

**知识点:** `REST API` `Jobs`

---

### Q57 ❌ — Jobs REST API

**原题:**
Which REST API call can be used to review the notebooks configured to run as tasks in a multi-task job?

**选项:**
A. /jobs/runs/list
B. /jobs/runs/get-output
C. /jobs/runs/get
D. /jobs/get ✅
E. /jobs/list

**我的答案:** A | **正确答案:** D

**解析:**
- `/jobs/get` 返回 job 的**定义**，包括所有 task 配置（notebook、参数等）
- `/jobs/runs/*` 系列 API 返回的是**运行历史**，不是 job 定义
- `/jobs/runs/list` 列出运行记录
- `/jobs/runs/get` 获取单次运行的详情
- `/jobs/list` 列出所有 job，但不包含详细的 task 配置

**知识点:** `Jobs API` `/jobs/get` `multi-task job`

---

### Q58 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q59 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q60 ✅ — Delta Lake

**知识点:** `Delta Lake`

---


## Page 7: Q61 - Q70

---

### Q61 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q62 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q63 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q64 ❌ — DROP TABLE on managed table

**原题:**
A Delta Lake table was created with the below query:
Consider the following query:
DROP TABLE prod.sales_by_store
If this statement is executed by a workspace admin, which result will occur?

**选项:**
A. Nothing will occur until a COMMIT command is executed.
B. The table will be removed from the catalog but the data will remain in storage.
C. The table will be removed from the catalog and the data will be deleted. ✅
D. An error will occur because Delta Lake prevents the deletion of production data.
E. Data will be marked as deleted but still recoverable with Time Travel.

**我的答案:** E | **正确答案:** C

**解析:**
- 题目说表是用默认方式创建的（没有 EXTERNAL 关键字），所以是 **managed table**
- DROP managed table 会：
  - 从 catalog 删除元数据
  - **物理删除数据文件**
- DROP external table 只删除元数据，数据保留
- Delta Lake 不会阻止删除生产数据，权限控制由 ACL 负责
- Time Travel 只能恢复未被物理删除的数据

**知识点:** `DROP TABLE` `managed table` `external table`

---

### Q65 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q66 ❌ — %sh 性能问题

**原题:**
The following code has been migrated to a Databricks notebook from a legacy workload:
The code executes successfully and provides the logically correct results, however, it takes over 20 minutes to extract and load around 1 GB of data.
Which statement is a possible explanation for this behavior?

**选项:**
A. %sh triggers a cluster restart to collect and install Git. Most of the latency is related to cluster startup time.
B. Instead of cloning, the code should use %sh pip install so that the Python code can get executed in parallel across all nodes in a cluster.
C. %sh does not distribute file moving operations; the final line of code should be updated to use %fs instead.
D. Python will always execute slower than Scala on Databricks. The run.py script should be refactored to Scala.
E. %sh executes shell code on the driver node. The code does not take advantage of the worker nodes or Databricks optimized Spark. ✅

**我的答案:** C | **正确答案:** E

**解析:**
- `%sh` 只在 **driver node** 执行，不会分布式执行
- 1GB 数据用单节点处理会很慢，无法利用集群的并行能力
- 应该用 Spark API（如 `spark.read`）来利用分布式计算
- `%fs` 是 Databricks 文件系统命令，但也不是分布式的
- 正确做法：用 Spark DataFrame API 读取和处理数据

**知识点:** `%sh` `driver node` `distributed computing`

---

### Q67 ✅ — Databricks CLI

**知识点:** `Databricks CLI`

---

### Q68 ❌ — Databricks CLI upload file

**原题:**
Assuming that the Databricks CLI has been installed and configured correctly, which Databricks CLI command can be used to upload a custom Python Wheel to object storage mounted with the DBFS for use with a production job?

**选项:**
A. configure
B. fs ✅
C. jobs
D. libraries
E. workspace

**我的答案:** D | **正确答案:** B

**解析:**
- `databricks fs` 命令用于文件系统操作（上传、下载、列出文件等）
- 上传 wheel 文件：`databricks fs cp my_package.whl dbfs:/path/to/wheel/`
- `databricks libraries` 用于管理集群库，不是上传文件
- `databricks workspace` 用于管理 notebook 和文件夹，不是 DBFS 文件
- `databricks jobs` 用于管理 job 配置

**知识点:** `Databricks CLI` `fs` `upload wheel`

---

### Q69 ❌ — BI dashboard 数据刷新策略

**原题:**
The business intelligence team has a dashboard configured to track various summary metrics for retail stores. This includes total sales for the previous day alongside totals and averages for a variety of time periods. The fields required to populate this dashboard have the following schema:
For demand forecasting, the Lakehouse contains a validated table of all itemized sales updated incrementally in near real-time. This table, named products_per_order, includes the following fields:
Because reporting on long-term sales trends is less volatile, analysts using the new dashboard only require data to be refreshed once daily. Because the dashboard will be queried interactively by many users throughout a normal business day, it should return results quickly and reduce total compute associated with each materialization.
Which solution meets the expectations of the end users while controlling and limiting possible costs?

**选项:**
A. Populate the dashboard by configuring a nightly batch job to save the required values as a table overwritten with each update. ✅
B. Use Structured Streaming to configure a live dashboard against the products_per_order table within a Databricks notebook.
C. Configure a webhook to execute an incremental read against products_per_order each time the dashboard is refreshed.
D. Use the Delta Cache to persist the products_per_order table in memory to quickly update the dashboard with each query.
E. Define a view against the products_per_order table and define the dashboard against this view.

**我的答案:** E | **正确答案:** A

**解析:**
- 需求：每天刷新一次，快速查询，控制成本
- **A 最佳**：nightly batch job 预计算聚合结果，存为表，查询时直接读取（快速 + 低成本）
- B：实时流处理成本高，不符合"每天刷新一次"的需求
- C：每次查询都触发计算，成本高
- D：Delta Cache 只是缓存，不解决重复计算问题
- E：View 每次查询都要重新计算聚合，成本高且慢

**知识点:** `batch job` `materialized table` `cost optimization`

---

### Q70 ❌ — Parquet 文件大小控制（无 shuffle）

**原题:**
A data ingestion task requires a one-TB JSON dataset to be written out to Parquet with a target part-file size of 512 MB. Because Parquet is being used instead of Delta Lake, built-in file-sizing features such as Auto-Optimize & Auto-Compaction cannot be used.
Which strategy will yield the best performance without shuffling data?

**选项:**
A. Set spark.sql.files.maxPartitionBytes to 512 MB, ingest the data, execute the narrow transformations, and then write to parquet. ✅
B. Set spark.sql.shuffle.partitions to 2,048 partitions (1TB*1024*1024/512), ingest the data, execute the narrow transformations, optimize the data by sorting it (which automatically repartitions the data), and then write to parquet.
C. Set spark.sql.adaptive.advisoryPartitionSizeInBytes to 512 MB bytes, ingest the data, execute the narrow transformations, coalesce to 2,048 partitions (1TB*1024*1024/512), and then write to parquet.
D. Ingest the data, execute the narrow transformations, repartition to 2,048 partitions (1TB*1024*1024/512), and then write to parquet.
E. Set spark.sql.shuffle.partitions to 512, ingest the data, execute the narrow transformations, and then write to parquet.

**我的答案:** C | **正确答案:** A

**解析:**
- 要求：**不 shuffle**，控制文件大小为 512 MB
- `spark.sql.files.maxPartitionBytes` 控制读取时每个 partition 的大小，影响写出的文件大小
- 只要执行的是 narrow transformation（不触发 shuffle），partition 数量保持不变，写出的文件大小就由这个参数控制
- B/D：`repartition()` 和 `sort()` 都会触发 shuffle
- C：`coalesce()` 虽然不 shuffle，但 `advisoryPartitionSizeInBytes` 是 AQE 的参数，用于 shuffle 后的优化

**知识点:** `maxPartitionBytes` `narrow transformation` `no shuffle`

---


## Page 8: Q71 - Q80

---

### Q71 ✅ — Structured Streaming

**知识点:** `Structured Streaming`

---

### Q72 ❌ — Structured Streaming schema 变更

**原题:**
A data team's Structured Streaming job is configured to calculate running aggregates for item sales to update a downstream marketing dashboard. The marketing team has introduced a new promotion, and they would like to add a new field to track the number of times this promotion code is used for each item. A junior data engineer suggests updating the existing query as follows. Note that proposed changes are in bold.
Original query:
Proposed query:
Which step must also be completed to put the proposed query into production?

**选项:**
A. Specify a new checkpointLocation ✅
B. Increase the shuffle partitions to account for additional aggregates
C. Run REFRESH TABLE delta.'/item_agg'
D. Register the data in the "/item_agg" directory to the Hive metastore
E. Remove .option('mergeSchema', 'true') from the streaming write

**我的答案:** E | **正确答案:** A

**解析:**
- Structured Streaming 的 **checkpoint** 存储了查询的状态和元数据
- 修改聚合逻辑（添加新字段）后，旧的 checkpoint 不兼容
- 必须指定**新的 checkpointLocation**，否则会报错
- `mergeSchema` 只处理 schema 变化，不解决聚合逻辑变化的问题
- 不需要手动 REFRESH TABLE 或注册到 metastore

**知识点:** `Structured Streaming` `checkpoint` `schema evolution`

---

### Q73 ✅ — Structured Streaming

**知识点:** `Structured Streaming`

---

### Q74 ✅ — Structured Streaming

**知识点:** `Structured Streaming`

---

### Q75 ❌ — Delta Lake 去重（跨批次）

**原题:**
A data engineer is configuring a pipeline that will potentially see late-arriving, duplicate records.
In addition to de-duplicating records within the batch, which of the following approaches allows the data engineer to deduplicate data against previously processed records as it is inserted into a Delta table?

**选项:**
A. Set the configuration delta.deduplicate = true.
B. VACUUM the Delta table after each batch completes.
C. Perform an insert-only merge with a matching condition on a unique key. ✅
D. Perform a full outer join on a unique key and overwrite existing data.
E. Rely on Delta Lake schema enforcement to prevent duplicate records.

**我的答案:** A | **正确答案:** C

**解析:**
- 跨批次去重需要用 **MERGE INTO** 语句
- Insert-only merge：`WHEN NOT MATCHED THEN INSERT`，只插入不存在的记录
- Matching condition 基于 unique key（如 `id`），如果已存在则跳过
- `delta.deduplicate` 配置不存在
- VACUUM 是清理旧文件，不是去重
- Schema enforcement 只检查类型，不检查重复

**知识点:** `MERGE INTO` `deduplication` `insert-only merge`

---

### Q76 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q77 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q78 ✅ — Delta Lake

**知识点:** `Delta Lake`

---

### Q79 ❌ — External table 创建方式

**原题:**
The data architect has mandated that all tables in the Lakehouse should be configured as external (also known as "unmanaged") Delta Lake tables.
Which approach will ensure that this requirement is met?

**选项:**
A. When a database is being created, make sure that the LOCATION keyword is used.
B. When configuring an external data warehouse for all table storage, leverage Databricks for all ELT.
C. When data is saved to a table, make sure that a full file path is specified alongside the Delta format. ✅
D. When tables are created, make sure that the EXTERNAL keyword is used in the CREATE TABLE statement.
E. When the workspace is being configured, make sure that external cloud object storage has been mounted.

**我的答案:** A | **正确答案:** C

**解析:**
- 创建 external table 的方式：
  - `CREATE TABLE ... USING delta LOCATION '/path/to/data'`
  - 或者 `df.write.format("delta").save("/path/to/data")`，然后 `CREATE TABLE ... USING delta LOCATION '/path'`
- **关键**：必须指定 `LOCATION`，数据存储在指定路径
- `EXTERNAL` 关键字在 Hive 中使用，Databricks 不需要（有 LOCATION 就是 external）
- 数据库的 LOCATION 只影响该数据库下 managed table 的默认位置，不影响 external table

**知识点:** `external table` `LOCATION` `Delta Lake`

---

### Q80 ✅ — Delta Lake

**知识点:** `Delta Lake`

---


## Page 9: Q81 - Q90

错题：Q81, Q82, Q83, Q84, Q85

### Q81 ❌ — Delta CHECK 约束违反导致整批写入失败

**原题：**
A CHECK constraint is added to a Delta table. A batch job inserts records including one where latitude=45.50 and longitude=212.67 (violates the constraint). What is the outcome?

**选项：**
A. The write will fail when the violating record is reached; previously processed records will be recorded.
B. The write will fail completely because of the constraint violation and no records will be inserted. ✅
C. The write will insert all records except violating ones; violating records go to a quarantine table.
D. The write will include all records; violations indicated in a boolean column named valid_coordinates.
E. The write will insert all records except violating ones; violating records reported in a warning log.

**我的答案：** D | **正确答案：** B

**解析：**
- Delta Lake 的 CHECK 约束是强制性的写时约束（write-time constraint）。一旦任何记录违反约束，整个批次写入操作会原子性地失败回滚，不会有任何记录被写入。
- 选 D 错误：Delta 不会把违规信息写入 boolean 列，CHECK 约束不是软性标记，而是硬性阻断。
- Delta 的 ACID 事务保证要么全部成功，要么全部失败，不存在部分写入的情况。

**知识点：** `Delta Lake CHECK constraint` `ACID atomicity` `write-time validation`

---

### Q82 ❌ — Job Owner 不能转让给群组

**原题：**
A junior engineer created Databricks jobs and is listed as "Owner". They try to transfer "Owner" privileges to the "DevOps" group but cannot.

**选项：**
A. Databricks jobs must have exactly one owner; "Owner" privileges cannot be assigned to a group. ✅
B. The creator of a Databricks job will always have "Owner" privileges; this configuration cannot be changed.
C. Other than the default "admins" group, only individual users can be granted privileges on jobs.
D. A user can only transfer job ownership to a group if they are also a member of that group.
E. Only workspace administrators can grant "Owner" privileges to a group.

**我的答案：** B | **正确答案：** A

**解析：**
- Databricks Jobs 的 Owner 必须是单个用户（individual user），不能是群组（group）。这是平台设计限制。
- 选 B 错误：Job ownership 是可以转让的（可以转给另一个用户），但不能转给群组。
- 选 A 正确：核心原因是 Owner 权限不支持分配给群组，只能是单个用户持有。

**知识点：** `Databricks Jobs ownership` `ACL permissions` `group vs user privileges`

---

### Q83 ❌ — Kafka PII 隔离应按 topic 分区

**原题：**
Kafka records ingested into a single Delta table. 5 topics, only "registration" topic has PII. Need to restrict PII access and delete PII after 14 days, retain non-PII indefinitely.

**选项：**
A. Delete all data biweekly; use time travel to maintain non-PII history.
B. Partition by the registration field, allowing ACLs and delete statements for the PII directory.
C. Binary value field is not considered PII; no special precautions needed.
D. Separate object storage containers based on the partition field for storage-level isolation.
E. Partition by the topic field, allowing ACLs and delete statements to leverage partition boundaries. ✅

**我的答案：** B | **正确答案：** E

**解析：**
- 正确做法是按 topic 字段分区，这样 "registration" topic 的数据会存储在独立的分区目录中，可以对该分区设置 ACL 限制访问，也可以用 DELETE WHERE topic='registration' 高效删除 PII 数据（利用分区裁剪）。
- 选 B 错误："registration field" 不是 Kafka schema 中的字段，schema 中只有 key/value/topic/partition/offset/timestamp，不存在 registration 列。
- 按 topic 分区是处理多 topic Kafka 数据的标准模式，既支持权限隔离，又支持高效的分区级删除。

**知识点：** `Delta Lake partitioning` `Kafka multi-topic ingestion` `PII data management` `partition pruning`

---

### Q84 ❌ — GRANT USAGE + SELECT 仅赋予只读查询权限

**原题：**
GRANT USAGE ON DATABASE prod TO eng; GRANT SELECT ON DATABASE prod TO eng; What are the privileges of the eng group?

**选项：**
A. Full permissions on prod database and can assign permissions to others.
B. Can list all tables but cannot see query results.
C. Can query and modify all tables/views but cannot create new ones.
D. Can query all tables and views in prod, but cannot create or edit anything. ✅
E. Can create, query, and modify all tables/views but cannot define custom functions.

**我的答案：** C | **正确答案：** D

**解析：**
- USAGE 权限允许用户访问数据库（列出对象），SELECT 权限允许查询表和视图。两者组合只赋予只读查询能力。
- 选 C 错误：SELECT 不包含 MODIFY（INSERT/UPDATE/DELETE）权限，没有授予 MODIFY 就不能修改数据。
- 要修改数据需要额外的 MODIFY 权限，要创建对象需要 CREATE 权限，这些都没有被授予。

**知识点：** `Databricks table ACL` `GRANT USAGE` `GRANT SELECT` `privilege hierarchy`

---

### Q85 ❌ — 集群扩缩容事件记录在 Cluster Event Log

**原题：**
Admin wants to evaluate whether cluster upscaling is caused by many concurrent users or resource-intensive queries. Where to review the timeline for cluster resizing events?

**选项：**
A. Workspace audit logs
B. Driver's log file
C. Ganglia
D. Cluster Event Log ✅
E. Executor's log file

**我的答案：** B | **正确答案：** D

**解析：**
- Cluster Event Log 专门记录集群生命周期事件，包括扩容（upscaling）、缩容（downscaling）、启动、终止等事件，并带有时间戳，是查看集群 resize 历史的正确位置。
- 选 B 错误：Driver log 记录的是 Spark 应用程序日志（stdout/stderr），不记录集群基础设施层面的扩缩容事件。
- Ganglia（选 C）是实时性能监控工具，不保存历史 resize 事件时间线。
- Workspace audit logs 记录用户操作审计，不是集群 resize 事件的来源。

**知识点：** `Databricks Cluster Event Log` `autoscaling` `cluster monitoring`

---

### Q86 ✅ | Q87 ✅ | Q88 ✅ | Q89 ✅ | Q90 ✅

**知识点:** 待补充

---

## Page 10: Q91 - Q100

错题：Q96, Q97, Q98, Q100

### Q91 ✅ | Q92 ✅ | Q93 ✅ | Q94 ✅ | Q95 ✅

**知识点:** 待补充

---

### Q96 ❌ — Delta Lake ACID 仅针对单表，不强制外键约束

**原题：**
Junior engineer migrating star schema workload from RDBMS to Databricks. Source uses foreign key constraints and multi-table inserts. Which consideration impacts migration decisions?

**选项：**
A. Databricks only allows foreign key constraints on hashed identifiers.
B. Databricks supports Spark SQL and JDBC; all logic can be directly migrated without refactoring.
C. Committing to multiple tables simultaneously requires multiple table locks and can lead to deadlock.
D. All Delta Lake transactions are ACID compliant against a single table, and Databricks does not enforce foreign key constraints. ✅
E. Foreign keys must reference a primary key field; multi-table inserts must leverage Delta Lake's upsert functionality.

**我的答案：** X（未答） | **正确答案：** D

**解析：**
- Delta Lake 的 ACID 事务是单表级别的，不支持跨表的原子性事务。外键约束在 Delta Lake 中只是元数据声明，不会被强制执行（informational only）。
- 这意味着从 RDBMS 迁移时，依赖外键约束保证数据完整性的逻辑需要重新设计，不能直接迁移。
- 选 B 错误：不能直接迁移，需要重构依赖 FK 约束和跨表事务的逻辑。

**知识点：** `Delta Lake ACID` `foreign key constraints` `star schema migration` `single-table transactions`

---

### Q97 ❌ — Delta time travel 不适合长期版本化方案

**原题：**
Architect wants Type 1 table + Delta time travel for long-term auditing of customer addresses. Engineer prefers Type 2. Which info is critical to this decision?

**选项：**
A. Data corruption can occur if a query fails in a partially completed state with Type 2 tables.
B. Shallow clones combined with Type 1 tables can accelerate historic queries for long-term versioning.
C. Delta Lake time travel cannot be used to query previous versions because Type 1 changes modify data files in place.
D. Delta Lake time travel does not scale well in cost or latency to provide a long-term versioning solution. ✅
E. Delta Lake only supports Type 0 tables; records cannot be modified once inserted.

**我的答案：** C | **正确答案：** D

**解析：**
- Delta time travel 依赖保留历史数据文件（transaction log + old Parquet files），随着时间推移，存储成本线性增长，且查询历史版本的延迟也会增加，不适合作为长期审计方案。
- 选 C 错误：Type 1 表（overwrite/update）确实会修改数据，但 Delta 的 MVCC 机制会保留旧版本文件，time travel 仍然可以查询历史版本，所以 C 的说法不准确。
- Type 2 SCD（保留历史行）是更适合长期审计的方案，因为历史数据作为普通行存储，查询性能稳定，不依赖 time travel 的版本文件保留策略。

**知识点：** `Delta time travel` `SCD Type 1 vs Type 2` `long-term versioning` `VACUUM` `data retention`

---

### Q98 ❌ — View WHERE 过滤行而非置 null

**原题：**
View user_ltv_no_minors is created from user_ltv (email, age, ltv). An analyst NOT in the auditing group runs SELECT * FROM user_ltv_no_minors. What results are returned?

**选项：**
A. All columns displayed normally for records with age > 17; records not meeting this condition will be omitted. ✅
B. All age values < 18 returned as null; other columns returned with values in user_ltv.
C. All values for age column returned as null; other columns returned with values in user_ltv.
D. All records from all columns displayed with values in user_ltv.
E. All columns displayed normally for records with age > 18; records not meeting this condition will be omitted.

**我的答案：** E | **正确答案：** A

**解析：**
- View 中的 WHERE 条件（age > 17，即 age >= 18）会过滤掉不满足条件的行，而不是将字段置为 null。满足条件的行完整返回所有列。
- 选 E 错误：age > 18 意味着 18 岁的记录也会被排除，而正确的条件是 age > 17（即 17 岁以下排除，18 岁及以上保留）。注意 >17 和 >18 的边界差异。
- 这道题考察对 SQL WHERE 过滤语义的理解：过滤是行级别的删除，不是列值的 null 化。

**知识点：** `SQL view filtering` `WHERE clause semantics` `row filtering vs null masking` `age boundary condition`

---

### Q99 ✅

**知识点:** 待补充

---

### Q100 ❌ — Secrets 最小权限应设 Read 在 scope 级别

**原题：**
Each team group has its own login credential in external DB. Databricks Secrets will store these credentials. How to grant minimum necessary access?

**选项：**
A. "Manage" permissions on a secret key mapped to those credentials.
B. "Read" permissions on a secret key mapped to those credentials.
C. "Read" permissions on a secret scope containing only those credentials for a given team. ✅
D. "Manage" permissions on a secret scope containing only those credentials for a given team.
E. No additional configuration needed as long as all users are workspace administrators.

**我的答案：** D | **正确答案：** C

**解析：**
- Databricks Secrets 的权限是在 scope 级别设置的，不是在单个 key 级别。每个团队应有独立的 secret scope，只包含该团队的凭证。
- 最小权限原则：只需要 "Read" 权限（允许读取 secret 值），不需要 "Manage"（Manage 允许管理 scope 的 ACL，权限过大）。
- 选 D 错误：Manage 权限过大，违反最小权限原则。
- 选 B 错误：Databricks Secrets ACL 是 scope 级别的，不能对单个 key 设置权限。

**知识点：** `Databricks Secrets` `secret scope ACL` `least privilege` `Read vs Manage permission`

---

## Page 11: Q101 - Q110

错题：Q101, Q102, Q105, Q106, Q107, Q108, Q110

### Q101 ❌ — MEMORY_ONLY 缓存异常信号：Size on Disk > 0

**原题：**
Using Spark UI's Storage tab with MEMORY_ONLY storage level. Which indicator signals a cached table is NOT performing optimally?

**选项：**
A. Size on Disk is < Size in Memory
B. The RDD Block Name includes the "*" annotation signaling a failure to cache
C. Size on Disk is > 0 ✅
D. The number of Cached Partitions > the number of Spark Partitions
E. On Heap Memory Usage is within 75% of Off Heap Memory Usage

**我的答案：** X（未答） | **正确答案：** C

**解析：**
- 使用 MEMORY_ONLY 存储级别时，数据应该完全存储在内存中，不应该有任何数据写入磁盘。如果 "Size on Disk > 0"，说明内存不足，部分分区无法缓存到内存，被溢写到磁盘或直接丢弃（MEMORY_ONLY 下实际是丢弃，不溢写磁盘，但 UI 显示 disk > 0 表示有问题）。
- 这意味着缓存没有按预期工作，查询时需要重新计算这些分区，性能下降。
- 选 B 错误：Spark UI 中没有 "*" 注解这种标记方式。

**知识点：** `Spark caching` `MEMORY_ONLY storage level` `Spark UI Storage tab` `cache eviction`

---

### Q102 ❌ — Databricks notebook 文件首行格式

**原题：**
What is the first line of a Databricks Python notebook when viewed in a text editor?

**选项：**
A. %python
B. // Databricks notebook source
C. # Databricks notebook source ✅
D. -- Databricks notebook source
E. # MAGIC %python

**我的答案：** A | **正确答案：** C

**解析：**
- Databricks notebook 导出为源文件时，第一行固定是 `# Databricks notebook source`，这是 Python 注释格式（# 开头），用于标识该文件是 Databricks notebook。
- 选 A 错误：`%python` 是 notebook cell 中的 magic command，用于在非 Python 默认语言的 notebook 中切换到 Python，不是文件的第一行。
- `//` 是 Java/Scala 注释，`--` 是 SQL 注释，都不适用于 Python notebook。

**知识点：** `Databricks notebook format` `notebook source file` `magic commands`

---

### Q103 ✅ | Q104 ✅

**知识点:** 待补充

---

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

**知识点：** `MLflow` `pyfunc UDF` `Spark DataFrame select`

---

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

**知识点：** `insert-only merge` `Delta Lake去重` `批处理成本优化`

---

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

**知识点：** `Optimized Writes` `Auto Compaction` `Delta Lake小文件问题`

---

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

**知识点：** `Auto Loader` `directory listing` `file notification` `增量加载`

---

### Q109 ✅

**知识点:** 待补充

---

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

**知识点：** `High Concurrency集群` `计算存储分离` `云原生架构`

---

## Page 12: Q111 - Q120

错题：Q111, Q113, Q115, Q117

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

**知识点：** `%pip install` `notebook-scoped library` `cluster library`

---

### Q112 ✅

**知识点:** 待补充

---

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

**知识点：** `Delta Lake time travel` `Change Data Feed` `版本差异查询`

---

### Q114 ✅

**知识点:** 待补充

---

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

**知识点：** `Databricks Jobs API` `run_id` `multi-task job` `REST API响应结构`

---

### Q116 ✅

**知识点:** 待补充

---

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

**知识点：** `Databricks Jobs API` `creator_user_name` `personal access token` `job owner vs run creator`

---

### Q118 ✅ | Q119 ✅ | Q120 ✅

**知识点:** 待补充

---

## Page 13: Q121 - Q130

错题：Q126

### Q121 ✅ | Q122 ✅ | Q123 ✅ | Q124 ✅ | Q125 ✅

**知识点:** 待补充

---

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

**知识点：** `notebook语言互操作` `createOrReplaceTempView` `Python变量 vs Spark视图`

---

### Q127 ✅ | Q128 ✅ | Q129 ✅ | Q130 ✅

**知识点:** 待补充

---

## Page 14: Q131 - Q140

错题：Q139

### Q131 ✅ | Q132 ✅ | Q133 ✅ | Q134 ✅ | Q135 ✅ | Q136 ✅ | Q137 ✅ | Q138 ✅

**知识点:** 待补充

---

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

**知识点：** `Structured Streaming` `trigger once` `microbatch` `存储成本优化` `checkpoint`

---

### Q140 ✅

**知识点:** 待补充

---

## Page 15: Q141 - Q150

错题：Q147

### Q141 ✅ | Q142 ✅ | Q143 ✅ | Q144 ✅ | Q145 ✅ | Q146 ✅

**知识点:** 待补充

---

### Q147 ❌ — CDF 无增量状态导致全量重复追加

**原题：**
A junior data engineer uses Delta Lake Change Data Feed to build a Type 1 table from a bronze table (delta.enableChangeDataFeed=true), running a daily job.

**选项：**
A. Each execution merges newly updated records, overwriting previous values with same primary keys.
B. Each execution appends the ENTIRE available history of inserted/updated records, resulting in many duplicates. ✅
C. Each execution appends only records inserted/updated since last execution, giving desired result.
D. Each execution calculates differences between original and current versions; may result in duplicates.

**我的答案：** C | **正确答案：** B

**解析：**
- 该代码使用 `table_changes()` 但没有传入起始版本号参数，每次执行都会读取从版本0开始的全部变更历史。
- 因此每次运行都会把所有历史变更重新追加到目标表，造成大量重复记录。
- 选C错误：要实现“只追加上次之后的变更”，需要记录上次处理的版本号并作为起始参数传入，该代码没有这个逻辑。

**知识点：** `Delta CDF` `table_changes` `增量读取` `Type 1 SCD`



**我的答案:** C | **正确答案:** B

**知识点:** 待补充

---

### Q148 ✅ | Q149 ✅ | Q150 ✅

**知识点:** 待补充

---

## Page 16: Q151 - Q160

全对！

### Q151 ✅ | Q152 ✅ | Q153 ✅ | Q154 ✅ | Q155 ✅ | Q156 ✅ | Q157 ✅ | Q158 ✅ | Q159 ✅ | Q160 ✅

**知识点:** 待补充

---

## Page 17: Q161 - Q170

错题：Q164, Q167

### Q161 ✅ | Q162 ✅ | Q163 ✅

**知识点:** 待补充

---

### Q164 ❌ — PII 数据隔离与保留策略——按 topic 分区

**原题：**
Kafka data ingested into one Delta table (5 topics). Only "registration" topic has PII. Need: restrict PII access, delete PII after 14 days, retain non-PII indefinitely.

**选项：**
A. Delete all data biweekly; use time travel to maintain non-PII history.
B. Partition by registration field; set ACLs and delete statements for PII directory.
C. Partition by topic field; use ACLs and delete statements leveraging partition boundaries. ✅
D. Use separate object storage containers based on partition field for storage-level isolation.

**我的答案：** D | **正确答案：** C

**解析：**
- 按 topic 字段分区后，"registration" topic 的数据会存储在独立的分区目录中。
- 可以对该分区目录设置 ACL 限制访问，并用 `DELETE WHERE topic="registration" AND ingestion_date < 14天前` 精确删除 PII 数据。
- 选D错误：题目是单张表，不是多个存储容器；且 Databricks 中分区已经实现了目录级隔离，无需额外的存储容器。
- 选B错误："registration field" 不是 schema 中的字段，schema 中是 topic 字段。

**知识点：** `Delta Lake` `分区策略` `PII` `ACL` `数据保留`



**我的答案:** D | **正确答案:** C

**知识点:** 待补充

---

### Q165 ✅ | Q166 ✅

**知识点:** 待补充

---

### Q167 ❌ — Secrets 最小权限——scope 级别 Read 权限

**原题：**
External DB with group-based security. Each group has a login credential stored in Databricks Secrets. How to grant minimum necessary access?

**选项：**
A. No additional config needed if all users are workspace admins.
B. "Read" permissions on a secret KEY mapped to those credentials.
C. "Read" permissions on a secret SCOPE containing only those credentials for a given team. ✅
D. "Manage" permissions on a secret scope containing only those credentials.

**我的答案：** B | **正确答案：** C

**解析：**
- Databricks Secrets 权限粒度：Manage > Write > Read，最小权限原则应使用 Read。
- 权限应设置在 scope 级别而非 key 级别：每个团队有独立的 secret scope，只包含该团队的凭证，对该 scope 授予 Read 权限。
- 选B错误：Databricks Secrets ACL 的权限是设置在 scope 上的，不能单独对某个 key 设置权限。
- 选D错误：Manage 权限过高，违反最小权限原则，且允许用户修改/删除 secrets。

**知识点：** `Databricks Secrets` `ACL` `secret scope` `最小权限`



**我的答案:** B | **正确答案:** C

**知识点:** 待补充

---

### Q168 ✅ | Q169 ✅ | Q170 ✅

**知识点:** 待补充

---

## Page 18: Q171 - Q180

错题：Q174, Q175

### Q171 ✅ | Q172 ✅ | Q173 ✅

**知识点:** 待补充

---

### Q174 ❌ — %pip install 作用于 notebook 级别所有节点

**原题：**
What is a method of installing a Python package scoped at the notebook level to ALL nodes in the currently active cluster?

**选项：**
A. Run `source env/bin/activate` in a notebook setup script.
B. Install libraries from PyPI using the cluster UI.
C. Use `%pip install` in a notebook cell. ✅
D. Use `%sh pip install` in a notebook cell.

**我的答案：** D | **正确答案：** C

**解析：**
- `%pip install` 是 Databricks 魔法命令，会在 notebook 级别安装包并分发到集群所有节点（driver + workers），安装后自动重启 Python 解释器。
- `%sh pip install` 只在 driver 节点的 shell 中执行，不会分发到 worker 节点，因此 worker 上无法使用该包。
- 选B（Cluster UI）是集群级别安装，对所有使用该集群的 notebook 生效，不是 notebook 级别。

**知识点：** `%pip` `library installation` `notebook scope` `cluster scope`



**我的答案:** D | **正确答案:** C

**知识点:** 待补充

---

### Q175 ❌ — Databricks Python notebook 首行格式

**原题：**
What is the first line of a Databricks Python notebook when viewed in a text editor?

**选项：**
A. `%python`
B. `// Databricks notebook source`
C. `# Databricks notebook source` ✅
D. `-- Databricks notebook source`

**我的答案：** D | **正确答案：** C

**解析：**
- Databricks notebook 导出为 .py 文件时，第一行固定为 `# Databricks notebook source`（Python 注释格式）。
- `//` 是 Java/Scala 注释，`--` 是 SQL 注释，Python 使用 `#` 注释。
- 选D（`--`）是 SQL notebook 的注释风格，不适用于 Python notebook。

**知识点：** `Databricks notebook` `Python` `文件格式`



**我的答案:** D | **正确答案:** C

**知识点:** 待补充

---

### Q176 ✅ | Q177 ✅ | Q178 ✅ | Q179 ✅ | Q180 ✅

**知识点:** 待补充

---

## Page 19: Q181 - Q190

错题：Q184, Q187, Q188

### Q181 ✅ | Q182 ✅ | Q183 ✅

**知识点:** 待补充

---

### Q184 ❌ — DBFS 是对象存储的文件系统抽象层

**原题：**
Which statement about DBFS root storage vs external object storage mounted with dbutils.fs.mount() is correct?

**选项：**
A. DBFS is a file system protocol allowing users to interact with object storage using Unix-like syntax and guarantees. ✅
B. By default, both DBFS root and mounted sources are only accessible to workspace admins.
C. DBFS root is most secure; mounted volumes must have full public read/write permissions.
D. DBFS root stores files in ephemeral block volumes on driver; mounted directories always persist to external storage.

**我的答案：** B | **正确答案：** A

**解析：**
- DBFS（Databricks File System）是一个抽象层，底层数据实际存储在对象存储（如 S3/ADLS）中，提供类 Unix 文件系统的操作接口。
- 选B错误：DBFS 默认对所有工作区用户可访问，不仅限于管理员。
- 选C错误：挂载外部存储不需要公开读写权限，可以通过 IAM 角色或服务主体进行安全访问。
- 选D错误：DBFS root 的数据也持久化在对象存储中，不是 driver 的临时块存储。

**知识点：** `DBFS` `对象存储` `dbutils.fs.mount` `存储架构`



**我的答案:** B | **正确答案:** A

**知识点:** 待补充

---

### Q185 ✅ | Q186 ✅

**知识点:** 待补充

---

### Q187 ❌ — MLflow 预测结果追加写入 Delta 表

**原题：**
MLflow production model outputs preds DataFrame (customer_id LONG, predictions DOUBLE, date DATE). Save to Delta table to compare predictions across time, at most once per day. Minimize compute costs.

**选项：**
A. `preds.write.mode("append").saveAsTable("churn_preds")` ✅
B. `preds.write.format("delta").save("/preds/churn_preds")`
C. (merge/upsert logic)
D. (overwrite logic)

**我的答案：** B | **正确答案：** A

**解析：**
- 需求是"保留所有历史预测以便跨时间比较"，因此使用 append 模式追加每天的预测结果。
- `saveAsTable` 将数据注册到 Hive metastore，便于后续 SQL 查询；`save` 只写文件路径，不注册元数据。
- 选B错误：`save()` 写到路径但不创建托管表，且没有指定 mode，默认 ErrorIfExists 会在第二次运行时报错。
- 每天 append 一次，计算成本最低（无需 merge 的额外扫描开销）。

**知识点：** `MLflow` `Delta Lake` `append mode` `saveAsTable`



**我的答案:** B | **正确答案:** A

**知识点:** 待补充

---

### Q188 ❌ — %sh 只在 driver 执行，无法利用 worker 节点

**原题：**
Legacy code migrated to Databricks notebook using %sh to clone a Git repo and move ~1GB data. Executes correctly but takes 20+ minutes.

**选项：**
A. %sh triggers cluster restart; most latency is cluster startup time.
B. Should use %sh pip install so Python code executes in parallel across all nodes.
C. %sh does not distribute file moving; final line should use %fs instead.
D. %sh executes shell code on driver node only; does not leverage worker nodes or Databricks optimized Spark. ✅

**我的答案：** C | **正确答案：** D

**解析：**
- `%sh` 魔法命令在 driver 节点上执行 shell 命令，是单节点操作，无法利用集群的 worker 节点并行处理。
- 1GB 数据在单节点上串行处理自然很慢；应该用 Spark 的分布式读写来处理大数据。
- 选C错误：`%fs` 是 DBFS 文件操作命令，也主要在 driver 执行，并不能解决并行化问题；且问题根源是整个 %sh 代码块都是单节点执行。

**知识点：** `%sh` `driver node` `分布式计算` `Spark 优化`



**我的答案:** C | **正确答案:** D

**知识点:** 待补充

---

### Q189 ✅ | Q190 ✅

**知识点:** 待补充

---

## Page 20: Q191 - Q200

全对！

### Q191 ✅ | Q192 ✅ | Q193 ✅ | Q194 ✅ | Q195 ✅ | Q196 ✅ | Q197 ✅ | Q198 ✅ | Q199 ✅ | Q200 ✅

**知识点:** 待补充

---


## Page 21: Q201 - Q210

错题：Q207

### Q201 ✅ | Q202 ✅ | Q203 ✅ | Q204 ✅ | Q205 ✅ | Q206 ✅

**知识点:** 待补充

---

### Q207 ❌ — 新表 + 视图维持旧 schema，最小化影响其他团队

**原题：**
Aggregate table used by many teams needs fields renamed and new fields added (driven by one customer-facing app). Minimize disruption to other teams without increasing tables to manage.

**选项：**
A. Notify all users of schema change; provide logic to revert to historic queries.
B. Create new table with required fields/names for customer app; create a VIEW maintaining original schema/name by aliasing fields from new table. ✅
C. Create new table with required schema; use Delta deep clone to sync changes between tables.
D. Replace current table with a logical view; create new table for customer-facing app.

**我的答案：** C | **正确答案：** B

**解析：**
- 最优方案：新建满足新需求的表供客户应用使用，同时创建一个视图用旧名称和旧 schema（通过字段别名）供其他团队继续使用，零中断。
- 选C错误：deep clone 是复制数据，不是同步 schema 变更；且两张表需要分别维护，增加了管理负担。
- 选D错误：把现有表替换为视图会改变数据写入逻辑（视图不能直接写入），影响现有 ETL 流程。
- 选B"不增加需要管理的表数量"——新表替换旧表，视图不算额外的表，符合要求。

**知识点：** `视图` `schema 演进` `向后兼容` `Delta Lake`



**我的答案:** C | **正确答案:** B

**知识点:** 待补充

---

### Q208 ✅ | Q209 ✅ | Q210 ✅

**知识点:** 待补充

---

## Page 22: Q211 - Q220

错题：Q212, Q218, Q219, Q220

### Q211 ✅

**知识点:** 待补充

---

### Q212 ❌ — DLT 跨 notebook 复用数据质量规则——Delta 表存储

**原题：**
DLT pipeline has repetitive expectations across many tables. Team wants to reuse data quality rules across all tables in the pipeline.

**选项：**
A. Add constraints using an external job with access to pipeline config files.
B. Use global Python variables to make expectations visible across DLT notebooks in same pipeline.
C. Maintain rules in a separate Databricks notebook that each DLT notebook imports as a library.
D. Maintain data quality rules in a Delta table outside pipeline's target schema; provide schema name as pipeline parameter. ✅

**我的答案：** C | **正确答案：** D

**解析：**
- DLT pipeline 中的 notebook 不能像普通 Python 那样互相 import，因为 DLT 有自己的执行上下文。
- 正确做法：将规则存储在 Delta 表中，通过 pipeline 参数传入 schema 名，在 DLT notebook 中动态读取规则并应用为 expectations。
- 选C错误：DLT notebook 不支持 `%run` 或 `import` 其他 notebook 作为库，这是 DLT 执行模型的限制。
- 选B错误：DLT pipeline 中多个 notebook 不共享 Python 全局变量空间。

**知识点：** `DLT` `expectations` `数据质量` `pipeline 参数` `Delta 表`



**我的答案:** C | **正确答案:** D

**知识点:** 待补充

---

### Q213 ✅ | Q214 ✅ | Q215 ✅ | Q216 ✅ | Q217 ✅

**知识点:** 待补充

---

### Q218 ❌ — DLT 跨表验证——临时表 + left join 检测缺失记录

**原题：**
User wants DLT expectations to validate that derived table "report" contains all records from "validation_copy". Adding expectation directly to report table fails.

**选项：**
A. Define a TEMPORARY TABLE doing left outer join on validation_copy and report; expect no null report key values. ✅
B. Define a SQL UDF doing left outer join; check for null values in DLT expectation for report table.
C. Define a VIEW doing left outer join; reference this view in DLT expectations for report table.
D. Define a function doing left outer join; check result in DLT expectation for report table.

**我的答案：** B | **正确答案：** A

**解析：**
- DLT expectations 只能引用当前表定义中的列，不能直接跨表查询。要验证跨表完整性，需要创建一个中间表。
- 使用临时表（`@dlt.table` 加 `temporary=True`）执行 left join：validation_copy LEFT JOIN report，若 report 中缺少某条记录，则 report 的 key 列为 null。
- 在该临时表上定义 expectation：`report_key IS NOT NULL`，即可检测缺失记录。
- 选B错误：DLT expectations 不支持在 expectation 条件中调用 SQL UDF 执行跨表 join。
- 选C错误：DLT 中 view 不会触发 expectations 的物化执行，且 view 不能作为 expectation 的数据源。

**知识点：** `DLT` `expectations` `临时表` `left join` `数据完整性验证`



**我的答案:** B | **正确答案:** A

**知识点:** 待补充

---

### Q219 ❌ — display() 触发 job，缓存导致重复执行结果不准确

**原题：**
User runs code cell-by-cell with display() calls, running each cell multiple times to measure average execution time. Which adjustment gives more accurate production performance measure?

**选项：**
A. Use Jobs UI to run notebook as job; Photon only enabled on scheduled job clusters.
B. Only meaningful troubleshooting uses production-sized data and clusters with Run All.
C. Production development should use local IDE with open source Spark/Delta for accurate benchmarks.
D. display() forces a job to trigger; many transformations only add to logical plan; repeated execution of same logic is skewed by caching. ✅

**我的答案：** B | **正确答案：** D

**解析：**
- Spark 的懒执行：大多数 DataFrame 转换只是构建逻辑查询计划，不触发实际计算；只有 action（如 display()、count()、collect()）才触发 job。
- 重复执行同一 cell 时，Spark 会缓存中间结果，后续执行比第一次快很多，导致计时不准确。
- 选B错误：使用生产规模数据和集群是好实践，但题目问的是"重复执行同一逻辑"的问题，选B没有解释为什么重复执行不准确。
- 选A错误：Photon 可以在交互式集群上启用，不仅限于 scheduled job 集群。

**知识点：** `Spark 懒执行` `display()` `缓存` `性能测试` `action vs transformation`



**我的答案:** B | **正确答案:** D

**知识点:** 待补充

---

### Q220 ❌ — Spark UI 中 Physical Plan 诊断 predicate pushdown 缺失

**原题：**
Where in the Spark UI can one diagnose a performance problem induced by NOT leveraging predicate push-down?

**选项：**
A. In Executor's log file, grep for 'predicate push-down'.
B. In Stage's Detail screen, Completed Stages table, note data size in Input column.
C. In the Query Detail screen, by interpreting the Physical Plan. ✅
D. In the Delta Lake transaction log, note column statistics.

**我的答案：** A | **正确答案：** C

**解析：**
- Predicate pushdown 是否生效体现在查询的物理执行计划（Physical Plan）中：若下推成功，会看到 `PushedFilters` 或 filter 在 scan 节点之前；若未下推，filter 在 scan 之后。
- Spark UI → SQL/DataFrame 标签 → 点击具体 query → 查看 Physical Plan（或 DAG 图）。
- 选A错误：Executor 日志不包含 predicate pushdown 的诊断信息。
- 选B错误：Input 列大小可以间接反映数据读取量，但不能直接诊断 pushdown 是否生效。
- 选D错误：Delta transaction log 记录的是文件级别的统计信息，不是执行计划。

**知识点：** `Spark UI` `Physical Plan` `predicate pushdown` `性能优化` `query plan`



**我的答案:** A | **正确答案:** C

**知识点:** 待补充

---

## Page 23: Q221 - Q230

错题：Q221, Q226, Q228, Q230

### Q221 ❌ — Databricks CLI 复制 pipeline 配置——get + create

**原题：**
Data engineer needs to capture settings from an existing DLT pipeline and use them to create and version a JSON file for a new pipeline. Which Databricks CLI command sequence?

**选项：**
A. Use `list pipelines` to get specs for all pipelines; parse and use to create a pipeline.
B. Stop existing pipeline; use returned settings in a reset command.
C. Use `get` to capture settings for existing pipeline; remove pipeline_id and rename; use in a `create` command. ✅
D. Use `clone` command to copy existing pipeline; use `get JSON` to get definition; save to git.

**我的答案：** A | **正确答案：** C

**解析：**
- Databricks CLI pipeline 操作流程：`databricks pipelines get --pipeline-id <id>` 获取完整 JSON 配置，然后删除 `pipeline_id` 字段（避免冲突），修改 pipeline 名称，最后用 `databricks pipelines create --settings <file.json>` 创建新 pipeline。
- 选A错误：`list` 命令返回的是 pipeline 列表摘要，不包含完整的 pipeline 配置 spec。
- 选D错误：Databricks CLI 没有 `clone` 命令用于 pipeline；`clone` 是 Delta Lake 的表操作命令。
- 这也是将 pipeline 配置版本化到 git 的标准做法。

**知识点：** `Databricks CLI` `DLT pipeline` `pipeline 配置` `get/create` `IaC`



**我的答案:** A | **正确答案:** C

**知识点:** 待补充

---

### Q222 ✅ | Q223 ✅ | Q224 ✅ | Q225 ✅

**知识点:** 待补充

---

### Q226 ❌ — Files in Repos 单元测试

**原题：** How can a data engineer run unit tests using common Python testing frameworks on functions defined across several Databricks notebooks?

**选项：**
- A. Define and import unit test functions from a separate Databricks notebook
- B. Define and unit test functions using Files in Repos ✅
- C. Run unit tests against non-production data that closely mirrors production
- D. Define unit tests and functions within the same notebook

**我的答案:** A | **正确答案:** B

**解析：** Databricks Files in Repos 允许将函数定义在可复用的 .py 文件中，而非 notebook 里，从而可以用 pytest/unittest 等标准框架进行单元测试。选项 A 虽然也涉及 notebook 导入，但 notebook 之间的 %run 导入不支持标准测试框架的运行方式。Files in Repos 实现了代码模块化、版本控制和测试代码与生产代码的分离。

**知识点:** `Databricks Repos` `Unit Testing` `Files in Repos`

---

### Q227 ✅

**知识点:** 待补充

---

### Q228 ❌ — Delta 转 Iceberg 格式

**原题：** Analytics team needs to use a Delta transactions table with a tool that requires Apache Iceberg format. What should the data engineer do?

**选项：**
- A. Require the analytics team to use a tool which supports Delta table
- B. Create an Iceberg copy of the transactions Delta table which can be used by the analytics team ✅
- C. Convert the transactions Delta to Iceberg and enable uniform so that the table can be read as a Delta table
- D. Enable uniform on the transactions table to iceberg so that the table can be read as an Iceberg table

**我的答案:** D | **正确答案:** B

**解析：** 正确做法是创建一个 Iceberg 格式的副本供分析团队使用，这样不会影响原始 Delta 表的现有工作流。选项 D 的 UniForm 虽然可以让 Delta 表以 Iceberg 格式被读取，但本题的正确答案是创建独立副本。注意区分 UniForm（统一格式读取）和直接创建副本（独立格式表）的适用场景。

**知识点:** `Delta Lake` `Apache Iceberg` `UniForm` `Table Format`

---

### Q229 ✅

**知识点:** 待补充

---

### Q230 ❌ — Schema Owner 权限不自动继承表

**原题：** A platform engineer owns a catalog and schema but cannot access underlying tables in Schema_A. What explains this?

**选项：**
- A. The owner of the schema does not automatically have permission to tables within the schema, but can grant them to themselves at any point ✅
- B. Users granted with USE CATALOG can modify the owner permissions to downstream tables
- C. Permissions explicitly given by the table creator are the only way the Platform Engineer could access the underlying tables
- D. The platform engineer needs to execute a REFRESH statement as the table permissions did not automatically update for owners

**我的答案:** C | **正确答案:** A

**解析：** 在 Unity Catalog 中，拥有 catalog 或 schema 的所有权并不会自动授予对其中表的访问权限，表级权限是独立管理的。但作为 owner，工程师可以随时给自己授权。选项 C 说"只有表创建者显式授权才能访问"是错误的，因为 schema owner 可以自行授权，不需要依赖表创建者。

**知识点:** `Unity Catalog` `Permissions` `Schema Owner` `Table Access`

---

## Page 24: Q231 - Q240

错题：Q232, Q233, Q235, Q238

### Q231 ✅

**知识点:** 待补充

---

### Q232 ❌ — Databricks CLI 创建集群命令

**原题：** Which Databricks CLI command should be used to create a cluster with 5 workers, i3.xlarge node type, and 14.3.x-scala2.12 runtime?

**选项：**
- A. databricks compute add ... --num-workers 5 --node-type-id i3.xlarge
- B. databricks clusters create ... --num-workers 5 --node-type-id i3.xlarge ✅
- C. databricks compute create ... --num-workers 5 --node-type-id i3.xlarge
- D. databricks clusters add ... --num-workers 5 --node-type-id i3.xlarge

**我的答案:** A | **正确答案:** B

**解析：** Databricks CLI 中创建集群的正确命令是 `databricks clusters create`，而不是 `compute add` 或 `compute create`。CLI 的集群管理子命令组是 `clusters`，操作动词是 `create`。选项 A 使用了错误的子命令 `compute` 和动词 `add`。

**知识点:** `Databricks CLI` `Cluster Management` `clusters create`

---

### Q233 ❌ — Liquid Clustering 不支持 INSERT INTO

**原题：** A transactions table has been liquid clustered on product_id, user_id and event_date. Which operation lacks support for cluster on write?

**选项：**
- A. CTAS and RTAS statements
- B. spark.writeStream.format(delta).mode(append)
- C. spark.write.format(delta).mode(append)
- D. INSERT INTO operations ✅

**我的答案:** B | **正确答案:** D

**解析：** Liquid Clustering 的 cluster on write 功能支持 CTAS/RTAS 语句、Spark 的 .write 和 .writeStream append 模式，但不支持 INSERT INTO 操作。INSERT INTO 只是简单追加数据，不会按照聚簇列重新组织数据。选项 B 的 writeStream append 模式是支持 cluster on write 的。

**知识点:** `Liquid Clustering` `Cluster on Write` `INSERT INTO` `Delta Lake`

---

### Q234 ✅

**知识点:** 待补充

---

### Q235 ❌ — Jobs API 获取运行历史

**原题：** How should a data engineer format the request to collect information about the latest job run including repair history?

**选项：**
- A. Call /api/2.1/jobs/runs/list with the run_id and include_history parameters
- B. Call /api/2.1/jobs/runs/get with the run_id and include_history parameters
- C. Call /api/2.1/jobs/runs/get with the job_id and include_history parameters
- D. Call /api/2.1/jobs/runs/list with the job_id and include_history parameters ✅

**我的答案:** B | **正确答案:** D

**解析：** 要获取某个 job 的最新运行信息及修复历史，应使用 runs/list 端点配合 job_id 和 include_history 参数。runs/list 返回指定 job 的所有运行列表（包括重试和修复记录），而 runs/get 只能通过 run_id 获取单次运行的信息，无法列出完整的运行历史。

**知识点:** `Databricks REST API` `Jobs API` `runs/list` `Repair History`

---

### Q236 ✅ | Q237 ✅

**知识点:** 待补充

---

### Q238 ❌ — Z-Order 优化高基数列查询

**原题：** Which partitioning strategy should be used for a large, fast-growing orders table with high cardinality columns, data skew, and frequent concurrent writes?

**选项：**
- A. ALTER TABLE orders PARTITION BY user_id, product_id, event_timestamp
- B. OPTIMIZE orders ZORDER BY (user_id, product_id) WHERE event_timestamp = current date - 1 DAY
- C. ALTER TABLE orders CLUSTER BY user_id, product_id, event_timestamp
- D. OPTIMIZE orders ZORDER BY (user_id, product_id, event_timestamp) ✅

**我的答案:** C | **正确答案:** D

**解析：** Z-Order 通过 OPTIMIZE 命令优化数据跳过（data skipping），适合高基数列的查询优化，且不需要物理重分区表。选项 C 的 Liquid Clustering（CLUSTER BY）虽然也能优化，但对于已有大表且存在数据倾斜和频繁并发写入的场景，Z-Order 更灵活，避免了分区和聚簇可能带来的数据倾斜和维护开销。

**知识点:** `Z-Order` `OPTIMIZE` `Data Skipping` `High Cardinality`

---

### Q239 ✅ | Q240 ✅

**知识点:** 待补充

---

## Page 25: Q241 - Q250

错题：Q242, Q244, Q247, Q248, Q249

### Q241 ✅

**知识点:** 待补充

---

### Q242 ❌ — LDP 声明式管道

**原题：** A small team needs to build a production-grade pipeline to process change data, filter invalid records, and ensure timely delivery with minimal operational overhead. Which approach?

**选项：**
- A. Ingest via Spark jobs, apply UDFs for data quality, use LDP for Materialized Views
- B. Use Auto Loader into Bronze, then SQL queries in Workflows for Silver/Gold tables on schedule
- C. Use Auto Loader with Structured Streaming, manage invalid data with checkpointing and merge logic
- D. Use LDP with Streaming Tables and Materialized Views, leveraging built-in data expectations ✅

**我的答案:** B | **正确答案:** D

**解析：** LDP（Lakeflow Declarative Pipelines）提供声明式框架，Streaming Tables 和 Materialized Views 自动处理增量数据，内置的 data expectations 可以过滤或隔离无效记录。对于小团队和紧迫的截止日期，LDP 大幅简化了维护工作，比选项 B 的手动 Workflows 调度方案更高效、更可审计。

**知识点:** `LDP` `Streaming Tables` `Materialized Views` `Data Expectations`

---

### Q243 ✅

**知识点:** 待补充

---

### Q244 ❌ — Spark UI 查询计划可视化

**原题：** Where can the visualization of the query plan be found when monitoring a complex workload?

**选项：**
- A. In the Spark UI, under the Jobs tab
- B. In the Query Profiler, under Query Source
- C. In the Spark UI, under the SQL/DataFrame tab ✅
- D. In the Query Profiler, under the Stages tab

**我的答案:** A | **正确答案:** C

**解析：** Spark UI 的 SQL/DataFrame 标签页展示了逻辑和物理查询计划的可视化，可以看到各个 stage 和 operator 的执行细节。Jobs 标签页只显示 job 和 stage 的概览信息，不包含完整的查询计划可视化。

**知识点:** `Spark UI` `Query Plan` `SQL/DataFrame Tab`

---

### Q245 ✅ | Q246 ✅

**知识点:** 待补充

---

### Q247 ❌ — DataFrame.transform 模块化 ETL

**原题：** Which approach demonstrates a modular and testable way to use DataFrame transform for ETL code in PySpark?

**选项：**
- A. (代码选项 A)
- B. (代码选项 B)
- C. (代码选项 C — 使用 DataFrame.transform 配合纯转换函数) ✅
- D. (代码选项 D)

**我的答案:** A | **正确答案:** C

**解析：** 使用 DataFrame.transform() 配合纯转换函数（接收 DataFrame 返回 DataFrame）是 PySpark 中模块化、可测试 ETL 代码的最佳实践。每个转换封装为独立函数，可以单独进行单元测试，并在管道中干净地组合，不依赖编排逻辑或类状态。

**知识点:** `DataFrame.transform` `PySpark` `Modular ETL` `Unit Testing`

---

### Q248 ❌ — Deletion Vectors 减少文件重写

**原题：** A company needs to apply frequent account-level UPDATE statements on a Delta table but wants to avoid rewriting entire Parquet files. Which feature should they enable?

**选项：**
- A. Enable automatic file compaction on writes
- B. Enable change data feed on the Delta table
- C. Partition the Delta table by account_id
- D. Enable deletion vectors on the Delta table ✅

**我的答案:** B | **正确答案:** D

**解析：** Deletion Vectors 允许 Delta Lake 在不重写整个 Parquet 文件的情况下跟踪行级删除和更新，通过将变更记录在基础文件之外来显著减少文件重写（file churn）。选项 B 的 Change Data Feed 是用于捕获变更数据供下游消费的，不能减少文件重写开销。

**知识点:** `Deletion Vectors` `Delta Lake` `Row-level Updates` `File Churn`

---

### Q249 ❌ — DAB 部署 App 和 Volume 权限（未答）

**原题：** In a Databricks Asset Bundle project, how should the data engineer deploy a Databricks App and Volume, and grant the App Service Principal READ/WRITE permissions to the Volume?

**选项：**
- A. 正确配置：引用 deployed app 的 service principal，授予 Volume 级别的 READ/WRITE 权限 ✅
- B. (代码选项 B)
- C. (代码选项 C)
- D. (代码选项 D)

**我的答案:** X | **正确答案:** A

**解析：** 在 Databricks Asset Bundle 的 resources/app.yml 中，需要正确引用已部署 App 资源的 service principal 标识符，并在 Volume 级别授予 READ 和 WRITE 权限。选项 A 使用了正确的 Volume 特定权限格式，确保 App 部署后可以安全访问 Volume。

**知识点:** `Databricks Asset Bundles` `Service Principal` `Volume Permissions` `DAB`

---

### Q250 ✅

**知识点:** 待补充

---

## Page 26: Q251 - Q260

错题：Q257, Q259, Q260

### Q251 ✅ | Q252 ✅ | Q253 ✅ | Q254 ✅ | Q255 ✅ | Q256 ✅

**知识点:** 待补充

---

### Q257 ❌ — Auto Loader schema不匹配导致记录被隔离

**原题：** Auto Loader quarantines well-formed JSON records over time; what is the cause?

**选项：**
- A. The source data is valid JSON, but doesn't conform to their defined schema in some way ✅
- B. The badRecordsPath location is accumulating many small files
- C. The engineer forgot to set the option "cloudFiles.quarantineMode", "rescue".
- D. At some point, the upstream data provider switched everything to multi-line JSON.

**我的答案:** C | **正确答案:** A

**解析：** Auto Loader 的隔离机制不仅针对格式错误的 JSON，还会隔离与定义的 schema 不匹配的记录。即使 JSON 本身格式正确，如果包含额外字段、缺少字段或数据类型不兼容，都会被路由到 badRecordsPath。选项 C 的 quarantineMode 并不是导致合法 JSON 被隔离的原因，真正原因是 schema 不匹配。

**知识点:** `Auto Loader` `Schema Enforcement` `badRecordsPath`

---

### Q258 ✅

**知识点:** 待补充

---

### Q259 ❌ — Unity Catalog工作区目录默认权限范围

**原题：** New team members can create tables in default schema but cannot access tables in other schemas within the workspace catalog. Why?

**选项：**
- A. Workspace catalog permissions are not subject to inheritance rules.
- B. Workspace users receive USE CATALOG and specific privileges on default schema only. ✅
- C. Tables in other schemas require additional BROWSE privileges that new users don't receive automatically
- D. New users only receive CREATE TABLE privileges on the default schema.

**我的答案:** C | **正确答案:** B

**解析：** 当工作区自动启用 Unity Catalog 并创建 workspace catalog 时，新用户默认只获得 USE CATALOG 权限和 default schema 上的特定权限。访问其他 schema 中的表需要管理员显式授权。选项 C 提到的 BROWSE 权限不是核心问题，关键是用户只在 default schema 上有权限，其他 schema 需要额外授予 USE SCHEMA 等权限。

**知识点:** `Unity Catalog` `Workspace Catalog` `权限模型`

---

### Q260 ❌ — 使用foreach task实现可扩展的REST API下载

**原题：** How should a data engineer download multiple PDF files from a REST API with retry support and scalable throughput?

**选项：**
- A. Use a foreach task with a list of report types as its inputs. ✅
- B. Define ten Notebook tasks to clearly track which report download failed.
- C. Use a Delta Lake table to track each report download status as 10 rows, and use it as a source table to execute the download function as a Pandas UDF.
- D. Define a list variable within a Notebook to loop through the report types to download them, and print the download results. Execute it as a Notebook task.

**我的答案:** C | **正确答案:** A

**解析：** Databricks Jobs 的 foreach task 可以动态遍历可配置的报告类型列表，并行执行下载，并独立跟踪每个任务的成功或失败状态，支持部分重试。选项 C 使用 Delta 表和 Pandas UDF 过于复杂且不适合 REST API 调用场景。foreach task 是 Databricks 原生支持的迭代任务类型，天然支持可扩展吞吐量和列表动态变更。

**知识点:** `Databricks Jobs` `foreach task` `任务编排`

---

## Page 27: Q261 - Q270

错题：Q263, Q264, Q266, Q268, Q269, Q270

### Q261 ✅ | Q262 ✅

**知识点:** 待补充

---

### Q263 ❌ — Asset Bundle管理现有生产Job的正确流程

**原题：** How to bring an existing production Databricks job under asset bundle management and ensure future deploys update the job in-place?

**选项：**
- A. Run databricks bundle generate job --existing-job-id to generate YAML, then run databricks bundle deploy to deploy (will always update automatically).
- B. Export the job definition as JSON, convert it to YAML, and place it in your bundle. Then run databricks bundle deploy.
- C. Manually create the YAML configuration for the job in your bundle project, ensuring all settings match. Then run databricks bundle deploy.
- D. Run databricks bundle generate job --existing-job-id to generate YAML and download referenced files. Then run databricks bundle deployment bind to link the bundle's job resource to the existing job. ✅

**我的答案:** C | **正确答案:** D

**解析：** 将现有生产 Job 纳入 Asset Bundle 管理需要两步：首先用 `databricks bundle generate job --existing-job-id` 自动生成 YAML 配置并下载引用文件；然后用 `databricks bundle deployment bind` 将 bundle 中的 job 资源绑定到现有的 Databricks Job。bind 操作建立持久链接，确保后续部署更新同一个 Job 而非创建新的。选项 A 缺少 bind 步骤，仅 deploy 不能保证更新现有 Job。选项 C 手动创建 YAML 容易出错且同样缺少 bind。

**知识点:** `Databricks Asset Bundles` `bundle generate` `bundle bind`

---

### Q264 ❌ — Delta Sharing创建共享所需权限

**原题：** Which permissions and roles must be assigned to enable users to create, configure, and manage Delta Shares?

**选项：**
- A. Only workspace admins can create and manage shares
- B. Users need the MANAGE SHARES permission on the workspace
- C. Users need to be metastore admins or have CREATE SHARE privilege for the metastore ✅
- D. Any user with USE_CATALOG privilege can create shares

**我的答案:** B | **正确答案:** C

**解析：** 创建和管理 Delta Share 需要 metastore 级别的权限。用户必须是 metastore admin 或被授予 CREATE SHARE 特权才能创建共享。选项 B 的 MANAGE SHARES 不是 Unity Catalog 中的标准权限名称。Delta Sharing 的权限控制在 metastore 层面而非 workspace 层面，这确保了集中化的审计和安全治理。

**知识点:** `Delta Sharing` `Unity Catalog权限` `CREATE SHARE`

---

### Q265 ✅

**知识点:** 待补充

---

### Q266 ❌ — Predictive Optimization自动执行的操作（未答）

**原题：** Which two operations does Predictive Optimization run to maintain Delta tables? (Choose two.)

**选项：**
- A. PARTITION BY
- B. COMPACT
- C. ANALYZE ✅
- D. OPTIMIZE ✅
- E. BUCKETING

**我的答案:** X | **正确答案:** CD

**解析：** Predictive Optimization 自动运行两种操作来维护 Delta 表：OPTIMIZE 用于合并小文件并改善数据布局，ANALYZE 用于收集和刷新表统计信息。这两个操作配合使用，确保高效的查询计划、更好的数据跳过以及优化的性能和成本。COMPACT 不是 Delta 的标准命令，PARTITION BY 和 BUCKETING 是建表时的操作而非维护操作。

**知识点:** `Predictive Optimization` `OPTIMIZE` `ANALYZE` `Delta表维护`

---

### Q267 ✅

**知识点:** 待补充

---

### Q268 ❌ — Lakeflow中streaming table与materialized view的选择

**原题：** Which approach should be used for streaming tables and materialized views to support real-time truck monitoring and daily aggregated reports?

**选项：**
- A. Streaming table for raw data, streaming table for daily aggregation, materialized view for real-time monitoring.
- B. Streaming table for raw data, materialized view for real-time monitoring, materialized view for daily aggregation.
- C. Streaming table for raw data, streaming table for real-time monitoring (incremental), materialized view for daily aggregation. ✅
- D. Materialized view for raw data, streaming table for real-time monitoring.

**我的答案:** D | **正确答案:** C

**解析：** 原始遥测数据应使用 streaming table 进行增量摄取。近实时监控（每辆卡车最新位置/速度/油量）适合用 streaming table 增量计算，因为数据持续流入且需要低延迟更新。每日聚合报告（总距离和平均油耗）适合用 materialized view，因为它是批量计算且可以在每次刷新时完全重算。选项 D 错误地用 materialized view 摄取原始数据，这不适合流式场景。

**知识点:** `Lakeflow Declarative Pipelines` `Streaming Table` `Materialized View`

---

### Q269 ❌ — SQL Warehouse使用归因自动化报告（未答）

**原题：** How should a platform lead generate an automated daily report for SQL Warehouse usage attribution at the individual user level?

**选项：**
- A. Use system tables to capture audit and billing data and share the queries with the executive team for manual execution.
- B. Use system tables to capture audit and billing data and create a dashboard with daily refresh schedules shared with the executive team. ✅
- C. Restrict users from running SQL queries unless they provide all query details for attribution.
- D. Let users run SQL queries and directly report usage to executives.

**我的答案:** X | **正确答案:** B

**解析：** System tables 提供了权威的审计和计费数据，可用于按用户级别归因 SQL Warehouse 使用量。创建带有每日定时刷新的 Dashboard 可以自动化报告生成，确保高管团队无需手动执行查询即可获得一致、最新的使用情况洞察。选项 A 需要高管手动执行查询，不满足"自动化"要求。

**知识点:** `System Tables` `SQL Warehouse` `Dashboard调度`

---

### Q270 ❌ — Databricks Git Folder团队协作最佳实践

**原题：** How should data engineers collaborate on a Databricks project with independent development and testing while avoiding overwrites?

**选项：**
- A. Each team member creates their own Databricks Git folder, mapped to the same remote Git repository, and works in their own development branch. ✅
- B. All team members work in the same Databricks Git folder and perform Git operations directly in that shared folder.
- C. Team members edit notebooks directly in the workspace's shared folder and periodically copy changes into a Git folder.
- D. Team members use the Databricks CLI to clone the Git repository and perform Git operations from a cluster's web terminal.

**我的答案:** C | **正确答案:** A

**解析：** 每个团队成员创建自己的 Databricks Git Folder，映射到同一个远程 Git 仓库，并在各自的开发分支上工作。这样可以避免意外覆盖和分支切换冲突，同时确保所有更改都受版本控制并可轻松集成到 CI/CD 流程。选项 B 共享同一个 Git Folder 会导致分支切换互相影响；选项 C 在共享文件夹直接编辑不具备版本控制能力。

**知识点:** `Databricks Git Folders` `Repos` `团队协作`

---

## Page 28: Q271 - Q280

全对！⭐

### Q271 ✅ | Q272 ✅ | Q273 ✅ | Q274 ✅ | Q275 ✅ | Q276 ✅ | Q277 ✅ | Q278 ✅ | Q279 ✅ | Q280 ✅

**知识点:** 待补充

---

## Page 29: Q281 - Q290

错题：Q287, Q288, Q289

### Q281 ✅ | Q282 ✅ | Q283 ✅ | Q284 ✅ | Q285 ✅ | Q286 ✅

**知识点:** 待补充

---

### Q287 ❌ — 待补充

**我的答案:** C | **正确答案:** D

**知识点:** 待补充

---

### Q288 ❌ — 待补充（未答，多选题）

**我的答案:** X | **正确答案:** AB

**知识点:** 待补充

---

### Q289 ❌ — 待补充

**我的答案:** A | **正确答案:** C

**知识点:** 待补充

---

### Q290 ✅

**知识点:** 待补充

---

## Page 30: Q291 - Q300

错题：Q291, Q292, Q294, Q295, Q296, Q297, Q298, Q299, Q300（9道！）

### Q291 ❌ — 待补充

**我的答案:** C | **正确答案:** A

**知识点:** 待补充

---

### Q292 ❌ — 待补充（未答，多选题）

**我的答案:** X | **正确答案:** DE

**知识点:** 待补充

---

### Q293 ✅

**知识点:** 待补充

---

### Q294 ❌ — 待补充

**我的答案:** D | **正确答案:** A

**知识点:** 待补充

---

### Q295 ❌ — 待补充（未答）

**我的答案:** X | **正确答案:** C

**知识点:** 待补充

---

### Q296 ❌ — 待补充

**我的答案:** B | **正确答案:** A

**知识点:** 待补充

---

### Q297 ❌ — 待补充

**我的答案:** C | **正确答案:** B

**知识点:** 待补充

---

### Q298 ❌ — 待补充（多选题）

**我的答案:** A | **正确答案:** BD

**知识点:** 待补充

---

### Q299 ❌ — 待补充

**我的答案:** A | **正确答案:** C

**知识点:** 待补充

---

### Q300 ❌ — 待补充

**我的答案:** B | **正确答案:** D

**知识点:** 待补充

---

## Page 31: Q301 - Q310

错题：Q301, Q302, Q303, Q308, Q309

### Q301 ❌ — 待补充

**我的答案:** B | **正确答案:** C

**知识点:** 待补充

---

### Q302 ❌ — 待补充

**我的答案:** B | **正确答案:** C

**知识点:** 待补充

---

### Q303 ❌ — 待补充

**我的答案:** D | **正确答案:** C

**知识点:** 待补充

---

### Q304 ✅ | Q305 ✅ | Q306 ✅ | Q307 ✅

**知识点:** 待补充

---

### Q308 ❌ — PII数据脱敏管道设计

**原题：** How should a data engineer design a compliant pipeline on Databricks that supports both batch and streaming, applies PII masking, and maintains traceability?

**选项：**
- A. Use Lakeflow Spark Declarative Pipelines, apply masking via Unity Catalog column masks at read time
- B. Load batch with notebooks, streaming with SQL Warehouses; use UC column masks on Silver tables after storage
- C. Use Lakeflow Spark Declarative Pipelines for both, define a PII masking function, apply it during Bronze ingestion before writing ✅
- D. Store PII unmasked in Bronze for lineage, apply masking in Gold tables for reporting

**我的答案:** D | **正确答案:** C

**解析：** 题目要求PII在存储前就被脱敏，且批处理和流处理需要一致的处理逻辑，同时脱敏过程需可审计和可复现。选项C使用Lakeflow Spark Declarative Pipelines统一处理批和流数据，在Bronze层写入前就应用脱敏函数，满足所有要求。选项D将PII明文存储在Bronze层违反了"存储前脱敏"的隐私合规要求，即使后续在Gold层做了脱敏，Bronze层仍存在数据泄露风险。

**知识点:** `Lakeflow Declarative Pipelines` `PII Masking` `Data Privacy`

---

### Q309 ❌ — 查询执行总耗时的查看方法

**原题：** Which method can be used to determine the total wall-clock time it took to execute a query?

**选项：**
- A. In the Spark UI, take the job duration of the longest-running job associated with that query
- B. In the Spark UI, take the sum of all task durations across all stages/jobs
- C. Open the Query Profiler and use the Total wall-clock duration metric ✅
- D. Open the Query Profiler and use the Aggregated task time metric

**我的答案:** D | **正确答案:** C

**解析：** Query Profiler提供了专门的"Total wall-clock duration"指标，直接表示从查询开始到完成的实际经过时间（墙钟时间）。选项D的"Aggregated task time"是所有任务执行时间的累加，由于并行执行的存在，这个值通常远大于实际耗时，不能代表真实的端到端执行时间。Spark UI中的job duration也不能准确反映整个查询的wall-clock时间。

**知识点:** `Query Profiler` `Wall-Clock Time` `Performance Monitoring`

---

### Q310 ✅

**知识点:** 待补充

---

## Page 32: Q311 - Q320

错题：Q311, Q313, Q315, Q316, Q317, Q319, Q320（7道！）

### Q311 ❌ — 减少磁盘溢出的优化方法（多选题）

**原题：** A query has significant disk spill with a 1:2 core-to-memory ratio instance. What two steps should minimize spillage? (Choose two.)

**选项：**
- A. Increase spark.sql.files.maxPartitionBytes
- B. Choose a compute instance with more disk space
- C. Choose a compute instance with a higher core-to-memory ratio ✅
- D. Reduce spark.sql.files.maxPartitionBytes ✅
- E. Choose a compute instance with more network bandwidth

**我的答案:** C | **正确答案:** CD

**解析：** 磁盘溢出（disk spill）的根本原因是内存不足以容纳处理中的数据。选项C选择更高核心-内存比的实例（如1:4或1:8），每个核心分配更多内存，从而减少溢出。选项D减小maxPartitionBytes使每个分区更小，每个任务处理的数据量减少，降低峰值内存压力。我只选了C漏选了D，需要注意减小分区大小也是减少溢出的有效手段。增加磁盘空间（B）只是让溢出有更多空间，并不能减少溢出本身。

**知识点:** `Disk Spill` `spark.sql.files.maxPartitionBytes` `Memory Optimization`

---

### Q312 ✅

**知识点:** 待补充

---

### Q313 ❌ — 邮箱列的固定长度哈希脱敏（未答）

**原题：** A data engineer is masking an email column. The goal is same-length output for all rows with different outputs for different values. Which SQL function should be used?

**选项：**
- A. hash(email)
- B. mask(email, '?')
- C. sha1('email') ✅
- D. sha2(email, 0)

**我的答案:** X | **正确答案:** C

**解析：** 题目要求两个条件：所有行输出长度相同，且不同值产生不同输出。SHA-1是加密哈希函数，对任意输入都产生固定40字符的十六进制字符串，且不同输入产生不同输出（碰撞概率极低），完美满足两个要求。hash()函数返回整数而非固定长度字符串；mask()函数只是字符替换，输出长度随输入变化；sha2(email,0)等价于SHA-256产生64字符输出，虽然也满足条件但题目答案选C。

**知识点:** `SHA-1` `Data Masking` `SQL Functions`

---

### Q314 ✅

**知识点:** 待补充

---

### Q315 ❌ — 基于映射表的动态列掩码

**原题：** A data engineer needs column masking that dynamically checks user groups against a separate group_access mapping table. Which approach should be used?

**选项：**
- A. Create a view without selecting the sensitive column
- B. Apply a column mask that references the group_access mapping table in its UDF ✅
- C. Create a UDF that hardcodes allowed groups and apply it as a column mask
- D. Use a row filter to restrict access based on the user's group

**我的答案:** D | **正确答案:** B

**解析：** 题目要求根据外部映射表（group_access）动态判断用户组权限来决定是否脱敏。选项B通过在UDF中引用group_access映射表实现动态列掩码，当映射表更新时掩码逻辑自动生效，无需重新部署代码。选项D使用行过滤器（row filter）是控制行级访问的，不是列级脱敏。选项C硬编码用户组不满足"动态"要求。选项A直接排除列过于粗暴，不能根据用户组灵活控制。

**知识点:** `Unity Catalog Column Mask` `Dynamic Data Masking` `UDF`

---

### Q316 ❌ — Git Folders合并冲突解决

**原题：** Two engineers edited the same notebook section in separate branches. A merge conflict occurs in Databricks Git folders UI. How should they resolve it?

**选项：**
- A. Use Git CLI to force-push, overriding the remote branch
- B. Use Git folders UI to manually edit the notebook, select desired lines from both versions, remove conflict markers, then mark resolved ✅
- C. Abort the merge, discard all local changes, and retry without reviewing
- D. Delete the conflicted notebook, commit deletion, and recreate from scratch

**我的答案:** D | **正确答案:** B

**解析：** Databricks Git Folders UI提供了可视化的合并冲突解决功能。正确做法是在UI中打开冲突文件，手动选择保留两个版本中需要的代码，移除冲突标记（conflict markers），然后标记冲突已解决并完成合并提交。选项D删除文件重建会丢失所有历史记录且极其低效。选项A的force-push会覆盖他人的更改。选项C不审查就丢弃更改是不负责任的做法。

**知识点:** `Databricks Git Folders` `Merge Conflict Resolution` `Version Control`

---

### Q317 ❌ — Jobs API自动化监控与重跑

**原题：** A data engineer wants to automate job monitoring and recovery using the Jobs API. They need to list all jobs, identify a failed job, and rerun it. Which sequence of API actions?

**选项：**
- A. jobs list -> jobs runs list (check statuses) -> jobs run-now (rerun failed job) ✅
- B. jobs get -> jobs update to rerun failed jobs
- C. jobs list -> jobs create (new job) -> jobs run-now
- D. jobs cancel -> jobs create -> run new ones

**我的答案:** B | **正确答案:** A

**解析：** 正确的API调用流程是：先用jobs/list获取所有作业列表，再用jobs/runs/list查看各作业的运行状态找到失败的运行，最后用jobs/run-now重新触发该作业。选项B的jobs/update是用来更新作业配置的，不能用来重跑作业。选项C创建新作业是不必要的，应该重跑已有作业。选项D取消再重建更是多此一举。

**知识点:** `Databricks Jobs API` `Job Monitoring` `API Workflow`

---

### Q318 ✅

**知识点:** 待补充

---

### Q319 ❌ — 高基数列的数据布局优化

**原题：** Business users query a managed Delta table with high-cardinality column filters via SQL Serverless Warehouse. The engineer needs an incremental, easy-to-maintain, evolvable data layout optimization. Which command?

**选项：**
- A. Hive Style Partitions + Z-ORDER with periodic OPTIMIZE
- B. Hive Style Partitions with periodic OPTIMIZE
- C. Z-ORDER with periodic OPTIMIZE
- D. Liquid Clustering with periodic OPTIMIZE ✅

**我的答案:** A | **正确答案:** D

**解析：** Liquid Clustering是专为高基数列设计的数据布局优化技术，具有增量优化、自动适应数据和查询模式变化的特点，完美满足题目"增量、易维护、可演进"的要求。Hive Style Partitions对高基数列会产生过多小文件（small file problem），不适合高基数场景。Z-ORDER虽然支持高基数列，但每次OPTIMIZE需要重写整个表，不是增量的。Liquid Clustering消除了僵化的分区策略，运维成本最低。

**知识点:** `Liquid Clustering` `Data Layout Optimization` `High Cardinality`

---

### Q320 ❌ — 流处理进度追踪配置

**原题：** An append-only pipeline handles both batch and streaming in Delta Lake. The team wants the streaming component to efficiently track which data has been processed. Which configuration?

**选项：**
- A. checkpointLocation ✅
- B. overwriteSchema
- C. mergeSchema
- D. partitionBy

**我的答案:** B | **正确答案:** A

**解析：** checkpointLocation是Spark Structured Streaming的核心配置，用于持久化流处理的进度信息（offsets和state），使流能可靠追踪已处理的数据位置，支持exactly-once语义，并在重启后正确恢复。选项B的overwriteSchema用于覆盖表schema，选项C的mergeSchema用于schema演进，选项D的partitionBy用于数据分区，这三个都与流处理进度追踪无关。

**知识点:** `checkpointLocation` `Structured Streaming` `Exactly-Once Semantics`

---

## Page 33: Q321 - Q327

错题：Q322, Q325, Q326, Q327

### Q321 ✅

**知识点:** 待补充

---

### Q322 ❌ — Pandas UDF 分组状态处理选择

**原题：** A data engineer needs to design a Pandas UDF to process financial time series data with complex calculations requiring state across rows within each stock symbol group. Which approach has minimum overhead while preserving data integrity?

**选项：**
- A. Use a SCALAR_ITER Pandas UDF with iterator-based processing, implementing state management through persistent storage (Delta tables) updated after each batch
- B. Use a GROUPED_AGG UDF that processes each stock symbol group independently, maintaining state through intermediate aggregation results via broadcast variables
- C. Use a SCALAR Pandas UDF that processes the entire dataset at once, implementing custom partitioning logic within the UDF with global variables shared across executors
- D. Use ApplyInPandas method on spark dataframe that receives all rows for each stock symbol as a pandas DataFrame, maintaining state variables local to each group's processing function ✅

**我的答案:** A | **正确答案:** D

**解析：** `applyInPandas`（grouped map）会将同一 stock_symbol 分组的所有行作为一个完整的 pandas DataFrame 传入处理函数，因此可以在函数内部自然地维护每个分组的状态变量，无需跨批次的外部状态管理。选项 A 使用 Delta 表做持久化状态管理引入了不必要的 I/O 开销；选项 C 使用全局变量在多个 executor 之间共享状态是不安全的；选项 B 的 broadcast 变量方式也不适合逐组状态维护。`applyInPandas` 是处理分组内有状态计算的最佳实践。

**知识点:** `Pandas UDF` `applyInPandas` `Grouped Map` `状态管理`

---

### Q323 ✅ | Q324 ✅

**知识点:** 待补充

---

### Q325 ❌ — 多任务ETL流水线自动化触发（未答，多选题）

**原题：** A data team is automating a daily multitask ETL pipeline (notebook ingestion, Python wheel transformation, SQL aggregation). They need programmatic triggering, GUI run history, task-level retries, and email notifications on failure. Which two approaches meet these requirements?

**选项：**
- A. Trigger the job programmatically using the Databricks Jobs REST API (/jobs/run-now), the CLI (databricks jobs run-now), or one of the Databricks SDKs ✅
- B. Use Databricks Asset Bundles (DABs) to deploy the workflow, then trigger individual tasks directly by referencing each task's notebook or script path in the workspace
- C. Create a multi-task job using the UI, DABs, or the Jobs REST API (/jobs/create) with notebook, Python wheel, and SQL tasks. Configure task-level retries and email notifications in the job definition ✅
- D. Use the REST API endpoint /jobs/runs/submit to trigger each task individually as separate job runs and implement retries using custom logic in the orchestrator
- E. Create a single notebook that uses dbutils.notebook.run() to call each step in sequence. Define a job on this orchestrator notebook and configure retries and notifications at the notebook level

**我的答案:** X | **正确答案:** AC

**解析：** 这是一道多选题。选项 A 正确，因为通过 `/jobs/run-now`、CLI 或 SDK 触发已有 Job 可以实现编程式触发，同时运行记录会自动显示在 Jobs GUI 中。选项 C 正确，因为创建多任务 Job（支持 notebook、Python wheel、SQL 任务类型）可以在 Job 定义中直接配置任务级别的重试策略和失败邮件通知。选项 D 的 `/jobs/runs/submit` 是一次性提交，不会关联到已有 Job，且需要自定义重试逻辑。选项 E 使用 `dbutils.notebook.run()` 串行编排无法实现任务级别的重试和通知。

**知识点:** `Jobs REST API` `多任务Job` `run-now` `任务重试` `邮件通知`

---

### Q326 ❌ — Unity Catalog 托管表与预测优化

**原题：** A data engineer is designing a system to process batch patient encounter data from S3 into a Delta table queried frequently by patient_id and encounter_date. Fine-grained access controls and minimal maintenance with best performance are required. How should the table be created?

**选项：**
- A. Create an external table in Unity Catalog, specifying an S3 location. Enable predictive optimization through table properties, and configure Unity Catalog permissions for access controls
- B. Create a managed table in Unity Catalog. Configure Unity Catalog permissions, schedule jobs to run OPTIMIZE and VACUUM daily for best performance
- C. Create a managed table in Hive metastore. Configure Hive metastore permissions, and rely on predictive optimization to enhance query performance
- D. Create a managed table in Unity Catalog. Configure Unity Catalog permissions, and rely on predictive optimization to enhance query performance and simplify maintenance ✅

**我的答案:** C | **正确答案:** D

**解析：** Unity Catalog 的托管表（managed table）提供细粒度访问控制和集中治理能力，满足安全需求。Predictive Optimization（预测优化）可以自动执行文件压缩、数据布局优化和聚类，无需手动调度 OPTIMIZE/VACUUM，从而最小化维护开销。选项 C 错误，因为 Hive metastore 不支持细粒度访问控制，且预测优化是 Unity Catalog 的功能，不适用于 Hive metastore。选项 B 虽然用了 Unity Catalog 但手动调度 OPTIMIZE/VACUUM 增加了维护负担。选项 A 使用外部表无法享受托管表的全部优化特性。

**知识点:** `Unity Catalog` `Managed Table` `Predictive Optimization` `细粒度访问控制`

---

### Q327 ❌ — Deletion Vectors 删除行为（未答）

**原题：** A data engineer has a Delta table with deletion vectors enabled. When executing `DELETE FROM orders WHERE status = 'cancelled'`, what is the behavior of deletion vectors?

**选项：**
- A. Files are physically rewritten without the deleted row
- B. Rows are marked as deleted both in metadata and in files
- C. Rows are marked as deleted in metadata, not in files ✅
- D. Delta automatically removes all cancelled orders permanently

**我的答案:** X | **正确答案:** C

**解析：** 启用 Deletion Vectors 后，Delta Lake 的删除操作不会重写底层数据文件，而是在元数据中记录哪些行被删除（即 deletion vectors）。读取时会自动过滤掉这些被标记删除的行。这种方式避免了昂贵的文件重写操作，显著提升了 DELETE/UPDATE/MERGE 等 DML 操作的性能。只有在执行 OPTIMIZE 或 VACUUM 时，被标记删除的行才会被物理清除。

**知识点:** `Deletion Vectors` `Delta Lake DML` `元数据删除` `读时过滤`

---

