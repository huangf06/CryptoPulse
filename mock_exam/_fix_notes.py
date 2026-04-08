#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('mock_exam/review_notes.md', 'r', encoding='utf-8') as f:
    content = f.read()

replacements = {}

replacements[287] = (
"### Q287 ❌ — 待补充\n\n**我的答案:** C | **正确答案:** D\n\n**知识点:** 待补充\n\n---",
"""### Q287 ❌ — Liquid Clustering 新数据处理机制

**原题：** How does liquid clustering in Delta Lake handle new data that is inserted after the initial table creation?

**选项：**
- A. New data is rejected if it doesn't match the clustering pattern.
- B. New data is automatically clustered during write operations.
- C. New data is written to a staging area and clustered during scheduled maintenance.
- D. New data remains unclustered until the next OPTIMIZE operation. ✅

**我的答案:** C | **正确答案:** D

**解析：** Liquid clustering 不会在写入时自动对新数据进行聚簇，也不存在所谓的 staging area。新插入的数据会以未聚簇的状态写入表中，只有在下一次执行 OPTIMIZE 操作时，才会对新旧数据进行增量式重新聚簇。选项 C 的"staging area + scheduled maintenance"是虚构的概念，Delta Lake 没有这种机制。

**知识点:** `Liquid Clustering` `OPTIMIZE` `Delta Lake`

---""")

replacements[288] = (
"### Q288 ❌ — 待补充（未答，多选题）\n\n**我的答案:** X | **正确答案:** AB\n\n**知识点:** 待补充\n\n---",
"""### Q288 ❌ — 优化 MERGE 操作性能（未答，多选题）

**原题：** Which two actions should the engineer prioritize to improve MERGE performance on an 800GB UC-managed table? (Choose two.)

**选项：**
- A. Apply liquid clustering using the merge join keys. ✅
- B. Enable deletion vectors on the table if not already enabled. ✅
- C. Partition the table by date.
- D. Use ZORDER on high-cardinality columns.
- E. Overwrite the table instead of Merge.

**我的答案:** X | **正确答案:** AB

**解析：** 对于大表的 MERGE 优化，Liquid Clustering 按 merge join keys 聚簇可以大幅减少扫描的数据量，提升数据局部性。Deletion Vectors 避免了更新/删除时重写整个 Parquet 文件，显著减少 I/O 开销。分区按日期（C）对 MERGE 帮助有限，ZORDER（D）已被 Liquid Clustering 取代，直接 Overwrite（E）会丢失未匹配的数据。

**知识点:** `MERGE优化` `Liquid Clustering` `Deletion Vectors` `Unity Catalog`

---""")

replacements[289] = (
"### Q289 ❌ — 待补充\n\n**我的答案:** A | **正确答案:** C\n\n**知识点:** 待补充\n\n---",
"""### Q289 ❌ — 流式增量摄取 JSON 文件的最佳方案

**原题：** How should the data engineer build a streaming pipeline to ingest JSON files incrementally with schema evolution and exactly-once processing?

**选项：**
- A. Use Lakeflow Spark Declarative Pipelines with a static DataFrame read, merge schema with spark.conf.set
- B. Use Auto Loader in batch mode with a daily job to overwrite the Delta table.
- C. Use Lakeflow Spark Declarative Pipelines with Auto Loader and "cloudFiles.schemaEvolutionMode"= "addNewColumns" ✅
- D. Use traditional Spark Structured Streaming with Auto Loader, manually configuring checkpoints

**我的答案:** A | **正确答案:** C

**解析：** Lakeflow Spark Declarative Pipelines + Auto Loader 是最佳组合：Auto Loader 提供增量文件发现和 exactly-once 保证，Declarative Pipelines 提供全托管的编排和基础设施管理，cloudFiles.schemaEvolutionMode = "addNewColumns" 自动处理 schema 演进。选项 A 使用 static DataFrame read 不是流式处理；选项 D 需要手动管理 checkpoint，不满足"minimize manual infrastructure management"的要求。

**知识点:** `Auto Loader` `Lakeflow Declarative Pipelines` `Schema Evolution` `Streaming`

---""")

replacements[291] = (
"### Q291 ❌ — 待补充\n\n**我的答案:** C | **正确答案:** A\n\n**知识点:** 待补充\n\n---",
"""### Q291 ❌ — Unity Catalog 项目团队权限分配

**原题：** Which rights should the project group be granted to create/manage schemas, tables, volumes but cannot rename/delete/change catalog permissions?

**选项：**
- A. The group needs to have USE CATALOG and USE SCHEMA on the catalog. ✅
- B. The group needs to have ALL PRIVILEGES and the MANAGE on the catalog.
- C. The group needs to have ALL PRIVILEGES on the catalog.
- D. The group should be made OWNER of the catalog.

**我的答案:** C | **正确答案:** A

**解析：** USE CATALOG + USE SCHEMA 允许团队在 catalog 内创建和管理 schemas、tables、volumes，但不赋予修改 catalog 本身（重命名、删除、权限管理）的能力。ALL PRIVILEGES（选项 C）会赋予过多权限，包括可能修改 catalog 级别设置的能力，违反了最小权限原则和 IT 集中管控的要求。MANAGE（选项 B）和 OWNER（选项 D）更是给予了完全的管理权限。

**知识点:** `Unity Catalog` `权限管理` `USE CATALOG` `最小权限原则`

---""")

replacements[292] = (
"### Q292 ❌ — 待补充（未答，多选题）\n\n**我的答案:** X | **正确答案:** DE\n\n**知识点:** 待补充\n\n---",
"""### Q292 ❌ — 生产化 Spark 应用的依赖和配置管理（未答，多选题）

**原题：** Which two methods will help productionize a Spark application with external dependencies, custom environment variables and Spark configuration? (Choose two.)

**选项：**
- A. Install libraries on DBFS
- B. Add libraries to compute policies
- C. Use secrets in init scripts to store configuration data
- D. Use compute policies to set system properties, environment variables, and Spark configuration parameters. ✅
- E. Create init scripts on DBFS. ✅

**我的答案:** X | **正确答案:** DE

**解析：** Compute Policies 可以集中定义和强制执行 Spark 配置参数、系统属性和环境变量，确保生产环境配置一致性。Init Scripts 在集群启动时运行，可以安装外部依赖库和执行自定义环境设置，是处理复杂依赖需求的关键工具。选项 A 直接在 DBFS 安装库不是推荐做法，选项 B 的 compute policies 不能直接添加库，选项 C 混淆了 secrets 和 init scripts 的用途。

**知识点:** `Compute Policies` `Init Scripts` `Spark配置` `生产化部署`

---""")

replacements[294] = (
"### Q294 ❌ — 待补充\n\n**我的答案:** D | **正确答案:** A\n\n**知识点:** 待补充\n\n---",
"""### Q294 ❌ — Delta 表删除文件保留期设置

**原题：** Which code snippet correctly sets the deleted file retention period to 15 days for a Delta table?

**选项：**
- A. ALTER TABLE SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 15 days') ✅
- B. (invalid option)
- C. VACUUM my_table RETAIN HOURS
- D. spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", "15 days")

**我的答案:** D | **正确答案:** A

**解析：** Delta Lake 的删除文件保留期通过表属性 delta.deletedFileRetentionDuration 控制，必须使用 ALTER TABLE SET TBLPROPERTIES 在表级别持久化设置。选项 D 使用 spark.conf.set 只是会话级别的配置，不会持久化到表元数据中，重启后失效，无法满足"permanently comply"的要求。VACUUM（选项 C）是执行清理操作，不是设置保留策略。

**知识点:** `Delta Lake` `deletedFileRetentionDuration` `TBLPROPERTIES` `数据保留策略`

---""")

replacements[295] = (
"### Q295 ❌ — 待补充（未答）\n\n**我的答案:** X | **正确答案:** C\n\n**知识点:** 待补充\n\n---",
"""### Q295 ❌ — Databricks Secrets CLI 命令（未答）

**原题：** Which Databricks CLI command should the Data Engineer use to create a secret for an OpenAI API access token?

**选项：**
- A. databricks secrets put-secret KEY SCOPE; dbutils.secrets.get(KEY, SCOPE)
- B. databricks tokens put-token SCOPE KEY; dbutils.tokens.get(SCOPE, KEY)
- C. databricks secrets put-secret SCOPE KEY; dbutils.secrets.get(SCOPE, KEY) ✅
- D. databricks tokens put-token KEY SCOPE; dbutils.secrets.get(KEY, SCOPE)

**我的答案:** X | **正确答案:** C

**解析：** Databricks CLI 创建 secret 的正确命令格式是 databricks secrets put-secret <scope> <key>，参数顺序是先 scope 后 key。读取时使用 dbutils.secrets.get(scope, key)，同样是先 scope 后 key。选项 A 的参数顺序反了（先 key 后 scope），选项 B 和 D 使用了不存在的 tokens 命令。

**知识点:** `Databricks Secrets` `CLI命令` `dbutils.secrets` `API密钥管理`

---""")

replacements[296] = (
"### Q296 ❌ — 待补充\n\n**我的答案:** B | **正确答案:** A\n\n**知识点:** 待补充\n\n---",
"""### Q296 ❌ — 使用 CDF 实现流式表增量 enrichment

**原题：** How should a data engineer enrich a streaming table (tasks_status with CDF enabled) by adding department info from a static dimension table in near real-time?

**选项：**
- A. Use readStream() with readChangeFeed to read CDF; enrich with employee table; use apply_changes(). ✅
- B. Use readStream() to read tasks_status directly; enrich with employee table; store in new streaming table.
- C. Use readStream() with skipChangeCommits to read tasks_status; enrich; store in new streaming table.
- D. Use read() to read tasks_status; enrich with employee table; store in materialized view.

**我的答案:** B | **正确答案:** A

**解析：** tasks_status 是一个维护最新状态的表（有 UPSERT 操作），直接用 readStream 读取会遇到问题，因为流式读取不支持包含删除/更新的变更。正确做法是通过 CDF（Change Data Feed）读取变更流，获取 INSERT/UPDATE/DELETE 事件，然后用 apply_changes() 将 enriched 后的变更应用到目标表。选项 B 直接 readStream 会因为表有 deletion vectors 和 row tracking 导致流式读取失败或数据不一致。

**知识点:** `Change Data Feed` `apply_changes()` `Streaming Table` `Lakeflow Declarative Pipelines`

---""")

replacements[297] = (
"### Q297 ❌ — 待补充\n\n**我的答案:** C | **正确答案:** B\n\n**知识点:** 待补充\n\n---",
"""### Q297 ❌ — PII 数据匿名化处理

**原题：** Which anonymization code should be used to hash user emails (PII) when copying production data to sandbox?

**选项：**
- A. df.withColumn("user_email", F.expr("uuid()"))
- B. df.withColumn("user_email", F.sha2("user_email")) ✅
- C. df.withColumn("hashed_email", sha2("user_email"))
- D. df.withColumn("user_email", F.regexp_replace("user_email", "@*", "@anonymized.com"))

**我的答案:** C | **正确答案:** B

**解析：** sha2 哈希可以将邮箱替换为确定性的、不可逆的哈希值，同时保留其作为唯一标识符的功能（相同邮箱产生相同哈希）。选项 B 正确地覆盖了原列名 user_email，而选项 C 创建了新列 hashed_email 但没有删除原始的 user_email 列，PII 数据仍然存在。此外选项 C 缺少 F. 前缀（未使用 pyspark.sql.functions 模块）。

**知识点:** `PII匿名化` `sha2哈希` `数据脱敏` `PySpark`

---""")

replacements[298] = (
"### Q298 ❌ — 待补充（多选题）\n\n**我的答案:** A | **正确答案:** BD\n\n**知识点:** 待补充\n\n---",
"""### Q298 ❌ — Auto Loader 摄取图片文件（多选题）

**原题：** Which two configurations are recommended to incrementally ingest image files (JPEG/PNG) from cloud storage into a UC-managed table? (Choose two.)

**选项：**
- A. Use Auto Loader and set cloudFiles.format to "TEXT"
- B. Use Auto Loader and set cloudFiles.format to "BINARYFILE" ✅
- C. Use Auto Loader and set cloudFiles.format to "IMAGE"
- D. Use the pathGlobFilter option to select only image files (e.g., "*.jpg, *.png") ✅
- E. Move files to a volume and read with SQL editor

**我的答案:** A | **正确答案:** BD

**解析：** 图片是二进制文件，应使用 BINARYFILE 格式读取，它会保留文件内容、路径和元数据。pathGlobFilter 用于过滤只摄取特定扩展名的文件，避免非图片文件进入管道。TEXT 格式（选项 A）用于文本文件，不适合二进制图片；IMAGE 格式（选项 C）在 Auto Loader 中不存在；选项 E 不支持增量处理。

**知识点:** `Auto Loader` `BINARYFILE` `pathGlobFilter` `二进制文件摄取`

---""")

replacements[299] = (
"### Q299 ❌ — 待补充\n\n**我的答案:** A | **正确答案:** C\n\n**知识点:** 待补充\n\n---",
"""### Q299 ❌ — Delta Sharing 配置优化（时间旅行+流式读取）

**原题：** Which Delta Sharing configuration provides optimal performance while enabling time travel queries and streaming reads?

**选项：**
- A. Use the open sharing protocol instead of Databricks-to-Databricks sharing for better performance.
- B. Share tables WITHOUT HISTORY and enable partitioning for better query performance.
- C. Share tables WITH HISTORY, ensure tables don't have partitioning enabled, and enable CDF before sharing. ✅
- D. Share the entire schema WITHOUT HISTORY and rely on recipient-side caching for performance.

**我的答案:** A | **正确答案:** C

**解析：** 时间旅行查询需要 WITH HISTORY 共享（保留表历史版本），流式读取需要启用 CDF（Change Data Feed）。在 Databricks-to-Databricks 场景下，非分区表的性能更优，因为共享协议对非分区表的谓词下推和元数据处理做了优化。Open sharing（选项 A）性能不如 D2D sharing；WITHOUT HISTORY（选项 B/D）不支持时间旅行。

**知识点:** `Delta Sharing` `WITH HISTORY` `CDF` `Databricks-to-Databricks`

---""")

replacements[300] = (
"### Q300 ❌ — 待补充\n\n**我的答案:** B | **正确答案:** D\n\n**知识点:** 待补充\n\n---",
"""### Q300 ❌ — Delta Sharing 协议限制

**原题：** What is a limitation of the Delta Sharing protocol when used with Databricks-to-Databricks or Open Sharing?

**选项：**
- A. Delta Sharing does not support Unity Catalog enabled tables; only legacy Hive Metastore tables are shareable.
- B. With D2D sharing, UC recipients must re-ingest data manually using COPY INTO or REST APIs.
- C. Delta Sharing allows recipients to modify the source data if they have SELECT privileges.
- D. With Open sharing, recipients cannot access Volumes, Models, or notebooks — only static Delta tables are supported. ✅

**我的答案:** B | **正确答案:** D

**解析：** Open Sharing 协议只支持共享 Delta 表，不支持 Volumes、ML Models、Notebooks 等其他 Unity Catalog 对象类型，这些只在 Databricks-to-Databricks 共享中可用。选项 B 错误，D2D 共享不需要手动重新摄取数据；选项 C 错误，Delta Sharing 是只读的，接收方无法修改源数据；选项 A 错误，Delta Sharing 完全支持 Unity Catalog 表。

**知识点:** `Delta Sharing` `Open Sharing` `D2D Sharing` `共享限制`

---""")

replacements[301] = (
"### Q301 ❌ — 待补充\n\n**我的答案:** B | **正确答案:** C\n\n**知识点:** 待补充\n\n---",
"""### Q301 ❌ — Job 条件任务中的时区问题

**原题：** A job uses {{job.start_time.is_weekday}} for weekday/weekend routing across multiple time zones. Should the PR be merged or rejected?

**选项：**
- A. Reject. As they should use {{job.trigger_time.is_weekday}} instead.
- B. Reject. As the {{job.start_time.is_weekday}} is not a valid value reference.
- C. Reject. As the {{job.start_time.is_weekday}} is for the UTC timezone. ✅
- D. Merge. As the job configuration looks good.

**我的答案:** B | **正确答案:** C

**解析：** job.start_time.is_weekday 是有效的引用（选项 B 错误），但它基于 UTC 时区计算，而非本地时区。当同一个 Job 在多个时区使用时，UTC 的工作日/周末判断可能与本地时间不一致（例如 UTC 周六凌晨在东八区已经是周六白天，但在美西还是周五）。这会导致路由逻辑在某些时区出错，因此应该 reject。

**知识点:** `Databricks Jobs` `条件任务` `UTC时区` `job.start_time`

---""")

replacements[302] = (
"### Q302 ❌ — 待补充\n\n**我的答案:** B | **正确答案:** C\n\n**知识点:** 待补充\n\n---",
"""### Q302 ❌ — Declarative Pipelines vs Structured Streaming 运维差异

**原题：** How are the operational aspects of Lakeflow Spark Declarative Pipelines different from Spark Structured Streaming?

**选项：**
- A. Declarative Pipelines automatically handle schema evolution, while Structured Streaming always requires manual schema management.
- B. Structured Streaming can process continuous data streams, while Declarative Pipelines cannot.
- C. Declarative Pipelines manage orchestration of multi-stage pipelines automatically, while Structured Streaming requires external orchestration. ✅
- D. Declarative Pipelines can write to Delta Lake format while Structured Streaming cannot.

**我的答案:** B | **正确答案:** C

**解析：** Lakeflow Declarative Pipelines 的核心运维优势在于自动管理多阶段管道的编排、任务依赖、重试和执行，提供内置的运维管理能力。Structured Streaming 专注于流处理执行本身，需要外部编排工具（如 Jobs、Airflow）来管理复杂的多阶段管道依赖。选项 B 错误，Declarative Pipelines 完全支持连续流处理；选项 D 错误，Structured Streaming 也能写入 Delta Lake。

**知识点:** `Lakeflow Declarative Pipelines` `Structured Streaming` `管道编排` `运维管理`

---""")

replacements[303] = (
"### Q303 ❌ — 待补充\n\n**我的答案:** D | **正确答案:** C\n\n**知识点:** 待补充\n\n---",
"""### Q303 ❌ — 委托 Catalog 权限管理

**原题：** Which privilege should be granted to a finance team lead to delegate permission management on a catalog without full admin rights?

**选项：**
- A. GRANT_OPTION privilege on the finance_data catalog.
- B. Make the finance team lead a metastore admin.
- C. MANAGE privilege on the finance_data catalog. ✅
- D. ALL PRIVILEGES on the finance_data catalog.

**我的答案:** D | **正确答案:** C

**解析：** MANAGE 权限允许用户管理指定 catalog 内的权限和对象（schemas、tables、grants），而不需要 metastore admin 或完全管理员权限，实现了最小权限原则下的委托治理。ALL PRIVILEGES（选项 D）赋予了所有数据操作权限但不包含权限管理能力；GRANT_OPTION（选项 A）不是 Unity Catalog 中的独立权限；metastore admin（选项 B）权限范围过大。

**知识点:** `Unity Catalog` `MANAGE权限` `权限委托` `最小权限原则`

---""")

count = 0
for qnum in sorted(replacements.keys()):
    old, new = replacements[qnum]
    if old in content:
        content = content.replace(old, new, 1)
        count += 1
        print(f"Q{qnum}: replaced successfully")
    else:
        print(f"Q{qnum}: OLD STRING NOT FOUND")

with open('mock_exam/review_notes.md', 'w', encoding='utf-8') as f:
    f.write(content)

print(f"\nTotal replacements: {count}/15")
