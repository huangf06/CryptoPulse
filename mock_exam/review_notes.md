# Databricks Certified Data Engineer Professional - 错题复习笔记

> 来源：examtopics.com | 总题数：327 | 开始日期：2026-02-21

## 统计

| 批次 | 题号范围 | 得分 | 日期 |
|------|---------|------|------|
| 第1轮 Page 1 | Q1-Q10 | 1/10 | 2026-02-21 |
| 第2轮 Page 2 | Q11-Q20 | 6/10 | 2026-02-22 |
| 第3轮 Page 3 | Q21-Q30 | 5/10 | 2026-02-22 |
| 第4轮 Page 4 | Q31-Q40 | 7/10 | 2026-02-23 |

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

**我的答案：** D ✅ | **正确答案：** D
Databricks 自动推断 schema 时使用能容纳所有数据的类型，手动设置类型能更严格地保证数据质量。

---

### Q32 ✅ — DLT Live Table（非增量）JOIN 行为

**我的答案：** B ✅ | **正确答案：** B
DLT 中非增量的 live table（无 STREAMING 关键字）每次执行时用当前有效版本的源表数据完整重写目标表。

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

**我的答案：** B ✅ | **正确答案：** B
创建新表满足新需求，同时创建 view 保持旧 schema 兼容，最小化对其他团队的影响且不增加需要维护的表数量。

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

**我的答案：** C ✅ | **正确答案：** C
跨区域读写会产生显著的成本和延迟，计算资源应尽量部署在数据存储的同一区域。

---

### Q38 ✅ — CHECK Constraint 要求现有数据合规

**我的答案：** C ✅ | **正确答案：** C
给已有表添加 CHECK 约束时，所有现有数据必须满足约束条件，否则添加失败。

---

### Q39 ✅ — Delta Lake 前32列自动统计

**我的答案：** B ✅ | **正确答案：** B
Delta Lake 自动收集前32列的统计信息（min/max/null count），用于 data skipping 优化查询。

---

### Q40 ✅ — Type 2 SCD 实现

**我的答案：** B ✅ | **正确答案：** B
旧值保留但标记为不再当前（current=false），新值插入 → 典型的 Type 2 SCD（Slowly Changing Dimension）。

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
