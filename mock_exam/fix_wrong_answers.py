import re

with open('mock_exam/review_notes.md', 'r', encoding='utf-8') as f:
    content = f.read()

replacements = {}

replacements['Q226'] = '''### Q226 ❌ — Databricks Notebooks 单元测试最佳实践

**原题：**
A Data Engineer wants to run unit tests using common Python testing frameworks on Python functions defined across several Databricks notebooks currently used in production. How can the data engineer run unit tests against functions that work with data in production?

**选项：**
A. Define and import unit test functions from a separate Databricks notebook
B. Define and unit test functions using Files in Repos ✅
C. Run unit tests against non-production data that closely mirrors production
D. Define unit tests and functions within the same notebook

**我的答案：** A | **正确答案：** B

**解析：**
- Files in Repos 允许将 Python 文件（.py）存储在 Databricks Repos 中，可以像普通 Python 模块一样导入和测试，支持标准 pytest 等测试框架
- 选项 A 从另一个 notebook 导入函数，notebook 不是标准 Python 模块，无法直接用 pytest 等框架测试
- Files in Repos 是 Databricks 推荐的模块化代码组织方式，支持版本控制和单元测试

**知识点：** `Files in Repos` `Unit Testing` `Databricks Notebooks`
'''

replacements['Q228'] = '''### Q228 ❌ — Delta UniForm / Iceberg 兼容性

**原题：**
A data engineer has created a 'transactions' Delta table on Databricks that should be used by the analytics team. The analytics team wants to use the table with another tool which requires Apache Iceberg format. What should the data engineer do?

**选项：**
A. Require the analytics team to use a tool which supports Delta table.
B. Create an Iceberg copy of the 'transactions' Delta table which can be used by the analytics team. ✅
C. Convert the 'transactions' Delta to Iceberg and enable uniform so that the table can be read as a Delta table.
D. Enable uniform on the transactions table to 'iceberg' so that the table can be read as an Iceberg table.

**我的答案：** D | **正确答案：** B

**解析：**
- Delta UniForm（选项 D）确实可以让 Delta 表被 Iceberg 客户端读取，但题目说的是"另一个工具需要 Iceberg 格式"，UniForm 是让 Delta 表暴露 Iceberg 元数据，而不是创建副本
- 正确答案 B 是创建一个 Iceberg 副本，这是最直接的解决方案，不影响原始 Delta 表
- 选项 C 逻辑错误：先转成 Iceberg 再用 UniForm 读成 Delta，方向反了
- 注意：UniForm 是在 Delta 表上启用，让其同时支持 Iceberg/Hudi 读取，而不是"转换"

**知识点：** `Delta UniForm` `Apache Iceberg` `Delta Lake`
'''

replacements['Q230'] = '''### Q230 ❌ — Unity Catalog 权限继承规则

**原题：**
A platform engineer created Catalog_A and Schema_A, granted USE CATALOG/USE SCHEMA/CREATE TABLE to the dev team. Despite being owner of the catalog and schema, the engineer noticed they do not have access to the underlying tables in Schema_A. What explains this?

**选项：**
A. The owner of the schema does not automatically have permission to tables within the schema, but can grant them to themselves at any point. ✅
B. Users granted with USE CATALOG can modify the owner's permissions to downstream tables.
C. Permissions explicitly given by the table creator are the only way the Platform Engineer could access the underlying tables.
D. The platform engineer needs to execute a REFRESH statement as the table permissions did not automatically update for owners.

**我的答案：** C | **正确答案：** A

**解析：**
- Unity Catalog 中，拥有 catalog/schema 的 owner 不会自动继承其中所有对象的权限
- 权限是对象级别的，table 的权限需要单独授予，即使你是 schema 的 owner
- 但 schema owner 可以随时给自己授予表的权限（GRANT ... TO self）
- 选项 C 错误：不是"只有表创建者才能授权"，owner 也可以自行授权给自己

**知识点：** `Unity Catalog` `权限继承` `Schema Owner`
'''

replacements['Q232'] = '''### Q232 ❌ — Databricks CLI 创建集群命令

**原题：**
A data engineer wants to create a cluster using the Databricks CLI with 5 workers, 1 driver of type i3.xlarge, runtime '14.3.x-scala2.12'. Which command should be used?

**选项：**
A. databricks compute add 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
B. databricks clusters create 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster ✅
C. databricks compute create 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
D. databricks clusters add 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster

**我的答案：** A | **正确答案：** B

**解析：**
- Databricks CLI 创建集群的正确命令是 `databricks clusters create`，不是 `compute add` 或 `compute create`
- `clusters` 是正确的子命令组，`create` 是操作动词
- 选项 A/D 使用了 `compute` 子命令，这不是标准 CLI 命令
- 选项 D 使用 `clusters add`，`add` 不是有效操作

**知识点：** `Databricks CLI` `clusters create` `集群管理`
'''

replacements['Q233'] = '''### Q233 ❌ — Liquid Clustering 不支持 cluster on write 的操作

**原题：**
A 'transactions' table has been liquid clustered on columns 'product_id', 'user_id' and 'event_date'. Which operation lacks support for cluster on write?

**选项：**
A. CTAS and RTAS statements
B. spark.writeStream.format('delta').mode('append')
C. spark.write.format('delta').mode('append')
D. INSERT INTO operations ✅

**我的答案：** B | **正确答案：** D

**解析：**
- Liquid Clustering 支持 cluster on write 的操作：CTAS/RTAS、spark.write append、spark.writeStream append
- INSERT INTO 操作不支持 cluster on write，数据写入后需要单独运行 OPTIMIZE 才能聚类
- 这是一个容易混淆的知识点：INSERT INTO 是 SQL DML，不走 DataFrame write 路径，因此不支持自动聚类写入

**知识点：** `Liquid Clustering` `cluster on write` `INSERT INTO`
'''

replacements['Q235'] = '''### Q235 ❌ — Jobs API 获取最新运行及修复历史

**原题：**
A data engineer needs to create an application that will collect information about the latest job run including the repair history. How should the data engineer format the request?

**选项：**
A. Call /api/2.1/jobs/runs/list with the run_id and include_history parameters
B. Call /api/2.1/jobs/runs/get with the run_id and include_history parameters
C. Call /api/2.1/jobs/runs/get with the job_id and include_history parameters
D. Call /api/2.1/jobs/runs/list with the job_id and include_history parameters ✅

**我的答案：** B | **正确答案：** D

**解析：**
- 要获取"最新运行"需要用 `runs/list`（列出所有运行，取最新），而不是 `runs/get`（需要已知 run_id）
- `runs/list` 接受 `job_id` 参数来过滤特定 job 的运行记录，并支持 `include_history` 参数获取修复历史
- 选项 B 用 `runs/get` + `run_id`：如果已知 run_id 可以获取单次运行详情，但无法获取"最新"运行
- 关键区别：`list` 用 job_id 查询，`get` 用 run_id 查询

**知识点：** `Jobs API` `runs/list` `include_history` `repair history`
'''

replacements['Q238'] = '''### Q238 ❌ — 大表高基数列的分区策略选择

**原题：**
A large, fast-growing 'orders' table with high cardinality columns, significant data skew, frequent concurrent writes. Columns 'user_id', 'event_timestamp', 'product_id' are heavily used in filters but may change. Which partitioning strategy for immediate data skipping, incremental management, and flexibility?

**选项：**
A. ALTER TABLE orders PARTITION BY user_id, product_id, event_timestamp
B. OPTIMIZE orders ZORDER BY (user_id, product_id) WHERE event_timestamp = current_date() - 1 DAY
C. ALTER TABLE orders CLUSTER BY user_id, product_id, event_timestamp
D. OPTIMIZE orders ZORDER BY (user_id, product_id, event_timestamp) ✅

**我的答案：** C | **正确答案：** D

**解析：**
- 题目强调"immediate data skipping"（立即数据跳过）和"flexibility"（灵活性，列可能变化）
- Z-order 通过 OPTIMIZE 命令执行，可以随时更改 Z-order 列，灵活性高
- Liquid Clustering（选项 C）虽然也灵活，但题目说"immediate"——Z-order 在 OPTIMIZE 后立即生效
- 选项 A 静态分区对高基数列效果差，会产生大量小分区
- 选项 B 只对部分数据做 Z-order，不够全面
- 注意：Liquid Clustering 是更现代的方案，但此题答案选 Z-order，可能因为强调"immediate"

**知识点：** `Z-order` `Liquid Clustering` `数据跳过` `高基数列`
'''

replacements['Q242'] = '''### Q242 ❌ — 小团队生产级数据管道最佳实践

**原题：**
A small team under tight deadlines needs to process change data from cloud object storage, filter invalid records, ensure timely delivery to downstream consumers, minimize operational overhead while keeping pipelines auditable and maintainable. Which approach?

**选项：**
A. Ingest via Spark jobs, apply quality filters using UDFs, use LDP for Materialized Views
B. Auto Loader into Bronze, SQL queries in Workflows for Silver/Gold on schedule
C. Auto Loader with Structured Streaming, invalid data handling via checkpointing and merge logic
D. Use LDP (Lakeflow Declarative Pipeline) with Streaming Tables and Materialized Views, built-in data expectations and incremental processing ✅

**我的答案：** B | **正确答案：** D

**解析：**
- LDP（Lakeflow Declarative Pipelines，即 Delta Live Tables）专为声明式管道设计，内置数据质量期望（expectations）、自动错误隔离、增量处理
- 小团队 + 紧迫截止 + 最小运维开销 = LDP 是最佳选择，无需手动管理 checkpointing 和 merge 逻辑
- 选项 B 用 Workflows + SQL 查询，需要手动管理调度和增量逻辑，运维开销更大
- 选项 C 手动管理 checkpointing 和 merge，复杂度高
- LDP 的 expectations 功能直接支持"filter out or isolate invalid records"需求

**知识点：** `Lakeflow Declarative Pipeline` `Delta Live Tables` `data expectations` `小团队最佳实践`
'''

replacements['Q244'] = '''### Q244 ❌ — Spark UI 查询计划可视化位置

**原题：**
When monitoring a complex workload, being able to see the query plan is critical. Where can the visualization of the query plan be found?

**选项：**
A. In the Spark UI, under the Jobs tab
B. In the Query Profiler, under Query Source
C. In the Spark UI, under the SQL/DataFrame tab ✅
D. In the Query Profiler, under the Stages tab

**我的答案：** A | **正确答案：** C

**解析：**
- Spark UI 中，查询计划（Query Plan）的可视化在 SQL/DataFrame 标签页下，不是 Jobs 标签页
- Jobs 标签页显示的是 Job 级别的执行信息（stages、tasks）
- SQL/DataFrame 标签页专门显示 SQL 查询和 DataFrame 操作的执行计划，包括 DAG 可视化
- Query Profiler 是 Databricks SQL 的功能，不是 Spark UI 的一部分

**知识点：** `Spark UI` `SQL/DataFrame tab` `查询计划可视化`
'''

replacements['Q247'] = '''### Q247 ❌ — PySpark DataFrame transform 模块化测试（代码图题）

**原题：**
Which approach demonstrates a modular and testable way to use DataFrame transform for ETL code in PySpark? (选项为代码图，无法提取文本)

**我的答案：** A | **正确答案：** C

**解析：**
- PySpark 的 `DataFrame.transform()` 方法接受一个函数作为参数，支持链式调用
- 模块化和可测试的方式：将转换逻辑定义为独立函数，通过 `df.transform(func1).transform(func2)` 链式调用
- 正确答案 C 通常展示的是：定义独立的转换函数，使用 `.transform()` 方法链式组合，便于单独测试每个函数
- 这种模式符合函数式编程原则，每个转换函数可以独立单元测试

**知识点：** `DataFrame.transform()` `PySpark 模块化` `ETL 最佳实践`
'''

replacements['Q248'] = '''### Q248 ❌ — Delta Lake Deletion Vectors 减少文件改写

**原题：**
A company stores account transactions in Delta Lake. They need frequent account-level UPDATE statements but want to avoid rewriting entire Parquet files for each change to reduce file churn and improve write performance. Which Delta Lake feature?

**选项：**
A. Enable automatic file compaction on writes
B. Enable change data feed on the Delta table
C. Partition the Delta table by account_id
D. Enable deletion vectors on the Delta table ✅

**我的答案：** B | **正确答案：** D

**解析：**
- Deletion Vectors（删除向量）是 Delta Lake 的特性，UPDATE/DELETE 操作时不立即重写 Parquet 文件，而是在单独的删除向量文件中标记被修改的行
- 这大幅减少了文件改写（file churn），提升写入性能
- Change Data Feed（选项 B）用于捕获变更数据，不能减少文件改写
- 自动文件压缩（选项 A）反而会增加写入时的文件操作
- Deletion Vectors 正是为了解决频繁小更新导致大量文件重写的问题

**知识点：** `Deletion Vectors` `Delta Lake` `文件改写优化` `UPDATE 性能`
'''

replacements['Q249'] = '''### Q249 ❌ — Databricks Asset Bundle 部署 App 和 Volume 权限（未答）

**原题：**
In a Databricks Asset Bundle project, in resources/app.yml, the data engineer would like to deploy a Databricks App 'databricks_app_deployed' and Volume 'volume_deployed' and grant the Service Principal behind Databricks Apps permissions to READ and WRITE to the Volume. How should the data engineer achieve the deployment? (选项为代码图)

**我的答案：** 未答 | **正确答案：** A

**解析：**
- Databricks Asset Bundle 中，可以在 YAML 配置文件中同时定义 App 资源和 Volume 资源
- 正确答案 A 展示了在 resources/app.yml 中：定义 databricks_app 资源、定义 volume 资源，并在 grants 部分给 App 的 Service Principal 授予 READ_VOLUME 和 WRITE_VOLUME 权限
- 关键点：App 的 Service Principal 身份需要通过 bundle 的 permissions/grants 机制显式授权访问 Volume

**知识点：** `Databricks Asset Bundle` `Databricks Apps` `Volume 权限` `Service Principal`
'''
