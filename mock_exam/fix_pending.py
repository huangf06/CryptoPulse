# -*- coding: utf-8 -*-
import re

with open('mock_exam/review_notes.md', 'r', encoding='utf-8') as f:
    content = f.read()

replacements = {}

replacements[147] = {
    'title': 'CDF 无增量状态导致全量重复追加',
    'question': 'A junior data engineer uses Delta Lake Change Data Feed to build a Type 1 table from a bronze table (delta.enableChangeDataFeed=true), running a daily job.',
    'options': [
        ('A', 'Each execution merges newly updated records, overwriting previous values with same primary keys.'),
        ('B', 'Each execution appends the ENTIRE available history of inserted/updated records, resulting in many duplicates. \u2705'),
        ('C', 'Each execution appends only records inserted/updated since last execution, giving desired result.'),
        ('D', 'Each execution calculates differences between original and current versions; may result in duplicates.'),
    ],
    'mine': 'C', 'correct': 'B',
    'analysis': [
        '\u8be5\u4ee3\u7801\u4f7f\u7528 `table_changes()` \u4f46\u6ca1\u6709\u4f20\u5165\u8d77\u59cb\u7248\u672c\u53f7\u53c2\u6570\uff0c\u6bcf\u6b21\u6267\u884c\u90fd\u4f1a\u8bfb\u53d6\u4ece\u7248\u672c0\u5f00\u59cb\u7684\u5168\u90e8\u53d8\u66f4\u5386\u53f2\u3002',
        '\u56e0\u6b64\u6bcf\u6b21\u8fd0\u884c\u90fd\u4f1a\u628a\u6240\u6709\u5386\u53f2\u53d8\u66f4\u91cd\u65b0\u8ffd\u52a0\u5230\u76ee\u6807\u8868\uff0c\u9020\u6210\u5927\u91cf\u91cd\u590d\u8bb0\u5f55\u3002',
        '\u9009C\u9519\u8bef\uff1a\u8981\u5b9e\u73b0\u201c\u53ea\u8ffd\u52a0\u4e0a\u6b21\u4e4b\u540e\u7684\u53d8\u66f4\u201d\uff0c\u9700\u8981\u8bb0\u5f55\u4e0a\u6b21\u5904\u7406\u7684\u7248\u672c\u53f7\u5e76\u4f5c\u4e3a\u8d77\u59cb\u53c2\u6570\u4f20\u5165\uff0c\u8be5\u4ee3\u7801\u6ca1\u6709\u8fd9\u4e2a\u903b\u8f91\u3002',
    ],
    'tags': ['Delta CDF', 'table_changes', '\u589e\u91cf\u8bfb\u53d6', 'Type 1 SCD'],
}

replacements[164] = {
    'title': 'PII \u6570\u636e\u9694\u79bb\u4e0e\u4fdd\u7559\u7b56\u7565\u2014\u2014\u6309 topic \u5206\u533a',
    'question': 'Kafka data ingested into one Delta table (5 topics). Only "registration" topic has PII. Need: restrict PII access, delete PII after 14 days, retain non-PII indefinitely.',
    'options': [
        ('A', 'Delete all data biweekly; use time travel to maintain non-PII history.'),
        ('B', 'Partition by registration field; set ACLs and delete statements for PII directory.'),
        ('C', 'Partition by topic field; use ACLs and delete statements leveraging partition boundaries. \u2705'),
        ('D', 'Use separate object storage containers based on partition field for storage-level isolation.'),
    ],
    'mine': 'D', 'correct': 'C',
    'analysis': [
        '\u6309 topic \u5b57\u6bb5\u5206\u533a\u540e\uff0c"registration" topic \u7684\u6570\u636e\u4f1a\u5b58\u50a8\u5728\u72ec\u7acb\u7684\u5206\u533a\u76ee\u5f55\u4e2d\u3002',
        '\u53ef\u4ee5\u5bf9\u8be5\u5206\u533a\u76ee\u5f55\u8bbe\u7f6e ACL \u9650\u5236\u8bbf\u95ee\uff0c\u5e76\u7528 `DELETE WHERE topic="registration" AND ingestion_date < 14\u5929\u524d` \u7cbe\u786e\u5220\u9664 PII \u6570\u636e\u3002',
        '\u9009D\u9519\u8bef\uff1a\u9898\u76ee\u662f\u5355\u5f20\u8868\uff0c\u4e0d\u662f\u591a\u4e2a\u5b58\u50a8\u5bb9\u5668\uff1b\u4e14 Databricks \u4e2d\u5206\u533a\u5df2\u7ecf\u5b9e\u73b0\u4e86\u76ee\u5f55\u7ea7\u9694\u79bb\uff0c\u65e0\u9700\u989d\u5916\u7684\u5b58\u50a8\u5bb9\u5668\u3002',
        '\u9009B\u9519\u8bef\uff1a"registration field" \u4e0d\u662f schema \u4e2d\u7684\u5b57\u6bb5\uff0cschema \u4e2d\u662f topic \u5b57\u6bb5\u3002',
    ],
    'tags': ['Delta Lake', '\u5206\u533a\u7b56\u7565', 'PII', 'ACL', '\u6570\u636e\u4fdd\u7559'],
}

replacements[167] = {
    'title': 'Secrets 最小权限——scope 级别 Read 权限',
    'question': 'External DB with group-based security. Each group has a login credential stored in Databricks Secrets. How to grant minimum necessary access?',
    'options': [
        ('A', 'No additional config needed if all users are workspace admins.'),
        ('B', '"Read" permissions on a secret KEY mapped to those credentials.'),
        ('C', '"Read" permissions on a secret SCOPE containing only those credentials for a given team. ✅'),
        ('D', '"Manage" permissions on a secret scope containing only those credentials.'),
    ],
    'mine': 'B', 'correct': 'C',
    'analysis': [
        'Databricks Secrets 权限粒度：Manage > Write > Read，最小权限原则应使用 Read。',
        '权限应设置在 scope 级别而非 key 级别：每个团队有独立的 secret scope，只包含该团队的凭证，对该 scope 授予 Read 权限。',
        '选B错误：Databricks Secrets ACL 的权限是设置在 scope 上的，不能单独对某个 key 设置权限。',
        '选D错误：Manage 权限过高，违反最小权限原则，且允许用户修改/删除 secrets。',
    ],
    'tags': ['Databricks Secrets', 'ACL', 'secret scope', '最小权限'],
}

replacements[174] = {
    'title': '%pip install 作用于 notebook 级别所有节点',
    'question': 'What is a method of installing a Python package scoped at the notebook level to ALL nodes in the currently active cluster?',
    'options': [
        ('A', 'Run `source env/bin/activate` in a notebook setup script.'),
        ('B', 'Install libraries from PyPI using the cluster UI.'),
        ('C', 'Use `%pip install` in a notebook cell. ✅'),
        ('D', 'Use `%sh pip install` in a notebook cell.'),
    ],
    'mine': 'D', 'correct': 'C',
    'analysis': [
        '`%pip install` 是 Databricks 魔法命令，会在 notebook 级别安装包并分发到集群所有节点（driver + workers），安装后自动重启 Python 解释器。',
        '`%sh pip install` 只在 driver 节点的 shell 中执行，不会分发到 worker 节点，因此 worker 上无法使用该包。',
        '选B（Cluster UI）是集群级别安装，对所有使用该集群的 notebook 生效，不是 notebook 级别。',
    ],
    'tags': ['%pip', 'library installation', 'notebook scope', 'cluster scope'],
}

replacements[175] = {
    'title': 'Databricks Python notebook 首行格式',
    'question': 'What is the first line of a Databricks Python notebook when viewed in a text editor?',
    'options': [
        ('A', '`%python`'),
        ('B', '`// Databricks notebook source`'),
        ('C', '`# Databricks notebook source` ✅'),
        ('D', '`-- Databricks notebook source`'),
    ],
    'mine': 'D', 'correct': 'C',
    'analysis': [
        'Databricks notebook 导出为 .py 文件时，第一行固定为 `# Databricks notebook source`（Python 注释格式）。',
        '`//` 是 Java/Scala 注释，`--` 是 SQL 注释，Python 使用 `#` 注释。',
        '选D（`--`）是 SQL notebook 的注释风格，不适用于 Python notebook。',
    ],
    'tags': ['Databricks notebook', 'Python', '文件格式'],
}

replacements[184] = {
    'title': 'DBFS 是对象存储的文件系统抽象层',
    'question': 'Which statement about DBFS root storage vs external object storage mounted with dbutils.fs.mount() is correct?',
    'options': [
        ('A', 'DBFS is a file system protocol allowing users to interact with object storage using Unix-like syntax and guarantees. ✅'),
        ('B', 'By default, both DBFS root and mounted sources are only accessible to workspace admins.'),
        ('C', 'DBFS root is most secure; mounted volumes must have full public read/write permissions.'),
        ('D', 'DBFS root stores files in ephemeral block volumes on driver; mounted directories always persist to external storage.'),
    ],
    'mine': 'B', 'correct': 'A',
    'analysis': [
        'DBFS（Databricks File System）是一个抽象层，底层数据实际存储在对象存储（如 S3/ADLS）中，提供类 Unix 文件系统的操作接口。',
        '选B错误：DBFS 默认对所有工作区用户可访问，不仅限于管理员。',
        '选C错误：挂载外部存储不需要公开读写权限，可以通过 IAM 角色或服务主体进行安全访问。',
        '选D错误：DBFS root 的数据也持久化在对象存储中，不是 driver 的临时块存储。',
    ],
    'tags': ['DBFS', '对象存储', 'dbutils.fs.mount', '存储架构'],
}

replacements[187] = {
    'title': 'MLflow 预测结果追加写入 Delta 表',
    'question': 'MLflow production model outputs preds DataFrame (customer_id LONG, predictions DOUBLE, date DATE). Save to Delta table to compare predictions across time, at most once per day. Minimize compute costs.',
    'options': [
        ('A', '`preds.write.mode("append").saveAsTable("churn_preds")` ✅'),
        ('B', '`preds.write.format("delta").save("/preds/churn_preds")`'),
        ('C', '(merge/upsert logic)'),
        ('D', '(overwrite logic)'),
    ],
    'mine': 'B', 'correct': 'A',
    'analysis': [
        '需求是"保留所有历史预测以便跨时间比较"，因此使用 append 模式追加每天的预测结果。',
        '`saveAsTable` 将数据注册到 Hive metastore，便于后续 SQL 查询；`save` 只写文件路径，不注册元数据。',
        '选B错误：`save()` 写到路径但不创建托管表，且没有指定 mode，默认 ErrorIfExists 会在第二次运行时报错。',
        '每天 append 一次，计算成本最低（无需 merge 的额外扫描开销）。',
    ],
    'tags': ['MLflow', 'Delta Lake', 'append mode', 'saveAsTable'],
}

replacements[188] = {
    'title': '%sh 只在 driver 执行，无法利用 worker 节点',
    'question': 'Legacy code migrated to Databricks notebook using %sh to clone a Git repo and move ~1GB data. Executes correctly but takes 20+ minutes.',
    'options': [
        ('A', '%sh triggers cluster restart; most latency is cluster startup time.'),
        ('B', 'Should use %sh pip install so Python code executes in parallel across all nodes.'),
        ('C', '%sh does not distribute file moving; final line should use %fs instead.'),
        ('D', '%sh executes shell code on driver node only; does not leverage worker nodes or Databricks optimized Spark. ✅'),
    ],
    'mine': 'C', 'correct': 'D',
    'analysis': [
        '`%sh` 魔法命令在 driver 节点上执行 shell 命令，是单节点操作，无法利用集群的 worker 节点并行处理。',
        '1GB 数据在单节点上串行处理自然很慢；应该用 Spark 的分布式读写来处理大数据。',
        '选C错误：`%fs` 是 DBFS 文件操作命令，也主要在 driver 执行，并不能解决并行化问题；且问题根源是整个 %sh 代码块都是单节点执行。',
    ],
    'tags': ['%sh', 'driver node', '分布式计算', 'Spark 优化'],
}

replacements[207] = {
    'title': '新表 + 视图维持旧 schema，最小化影响其他团队',
    'question': 'Aggregate table used by many teams needs fields renamed and new fields added (driven by one customer-facing app). Minimize disruption to other teams without increasing tables to manage.',
    'options': [
        ('A', 'Notify all users of schema change; provide logic to revert to historic queries.'),
        ('B', 'Create new table with required fields/names for customer app; create a VIEW maintaining original schema/name by aliasing fields from new table. ✅'),
        ('C', 'Create new table with required schema; use Delta deep clone to sync changes between tables.'),
        ('D', 'Replace current table with a logical view; create new table for customer-facing app.'),
    ],
    'mine': 'C', 'correct': 'B',
    'analysis': [
        '最优方案：新建满足新需求的表供客户应用使用，同时创建一个视图用旧名称和旧 schema（通过字段别名）供其他团队继续使用，零中断。',
        '选C错误：deep clone 是复制数据，不是同步 schema 变更；且两张表需要分别维护，增加了管理负担。',
        '选D错误：把现有表替换为视图会改变数据写入逻辑（视图不能直接写入），影响现有 ETL 流程。',
        '选B"不增加需要管理的表数量"——新表替换旧表，视图不算额外的表，符合要求。',
    ],
    'tags': ['视图', 'schema 演进', '向后兼容', 'Delta Lake'],
}

replacements[212] = {
    'title': 'DLT 跨 notebook 复用数据质量规则——Delta 表存储',
    'question': 'DLT pipeline has repetitive expectations across many tables. Team wants to reuse data quality rules across all tables in the pipeline.',
    'options': [
        ('A', 'Add constraints using an external job with access to pipeline config files.'),
        ('B', 'Use global Python variables to make expectations visible across DLT notebooks in same pipeline.'),
        ('C', 'Maintain rules in a separate Databricks notebook that each DLT notebook imports as a library.'),
        ('D', "Maintain data quality rules in a Delta table outside pipeline's target schema; provide schema name as pipeline parameter. ✅"),
    ],
    'mine': 'C', 'correct': 'D',
    'analysis': [
        'DLT pipeline 中的 notebook 不能像普通 Python 那样互相 import，因为 DLT 有自己的执行上下文。',
        '正确做法：将规则存储在 Delta 表中，通过 pipeline 参数传入 schema 名，在 DLT notebook 中动态读取规则并应用为 expectations。',
        '选C错误：DLT notebook 不支持 `%run` 或 `import` 其他 notebook 作为库，这是 DLT 执行模型的限制。',
        '选B错误：DLT pipeline 中多个 notebook 不共享 Python 全局变量空间。',
    ],
    'tags': ['DLT', 'expectations', '数据质量', 'pipeline 参数', 'Delta 表'],
}

replacements[218] = {
    'title': 'DLT 跨表验证——临时表 + left join 检测缺失记录',
    'question': 'User wants DLT expectations to validate that derived table "report" contains all records from "validation_copy". Adding expectation directly to report table fails.',
    'options': [
        ('A', 'Define a TEMPORARY TABLE doing left outer join on validation_copy and report; expect no null report key values. ✅'),
        ('B', 'Define a SQL UDF doing left outer join; check for null values in DLT expectation for report table.'),
        ('C', 'Define a VIEW doing left outer join; reference this view in DLT expectations for report table.'),
        ('D', 'Define a function doing left outer join; check result in DLT expectation for report table.'),
    ],
    'mine': 'B', 'correct': 'A',
    'analysis': [
        'DLT expectations 只能引用当前表定义中的列，不能直接跨表查询。要验证跨表完整性，需要创建一个中间表。',
        '使用临时表（`@dlt.table` 加 `temporary=True`）执行 left join：validation_copy LEFT JOIN report，若 report 中缺少某条记录，则 report 的 key 列为 null。',
        '在该临时表上定义 expectation：`report_key IS NOT NULL`，即可检测缺失记录。',
        '选B错误：DLT expectations 不支持在 expectation 条件中调用 SQL UDF 执行跨表 join。',
        '选C错误：DLT 中 view 不会触发 expectations 的物化执行，且 view 不能作为 expectation 的数据源。',
    ],
    'tags': ['DLT', 'expectations', '临时表', 'left join', '数据完整性验证'],
}

replacements[219] = {
    'title': 'display() 触发 job，缓存导致重复执行结果不准确',
    'question': 'User runs code cell-by-cell with display() calls, running each cell multiple times to measure average execution time. Which adjustment gives more accurate production performance measure?',
    'options': [
        ('A', 'Use Jobs UI to run notebook as job; Photon only enabled on scheduled job clusters.'),
        ('B', 'Only meaningful troubleshooting uses production-sized data and clusters with Run All.'),
        ('C', 'Production development should use local IDE with open source Spark/Delta for accurate benchmarks.'),
        ('D', 'display() forces a job to trigger; many transformations only add to logical plan; repeated execution of same logic is skewed by caching. ✅'),
    ],
    'mine': 'B', 'correct': 'D',
    'analysis': [
        'Spark 的懒执行：大多数 DataFrame 转换只是构建逻辑查询计划，不触发实际计算；只有 action（如 display()、count()、collect()）才触发 job。',
        '重复执行同一 cell 时，Spark 会缓存中间结果，后续执行比第一次快很多，导致计时不准确。',
        '选B错误：使用生产规模数据和集群是好实践，但题目问的是"重复执行同一逻辑"的问题，选B没有解释为什么重复执行不准确。',
        '选A错误：Photon 可以在交互式集群上启用，不仅限于 scheduled job 集群。',
    ],
    'tags': ['Spark 懒执行', 'display()', '缓存', '性能测试', 'action vs transformation'],
}

replacements[220] = {
    'title': 'Spark UI 中 Physical Plan 诊断 predicate pushdown 缺失',
    'question': 'Where in the Spark UI can one diagnose a performance problem induced by NOT leveraging predicate push-down?',
    'options': [
        ('A', "In Executor's log file, grep for 'predicate push-down'."),
        ('B', "In Stage's Detail screen, Completed Stages table, note data size in Input column."),
        ('C', 'In the Query Detail screen, by interpreting the Physical Plan. ✅'),
        ('D', 'In the Delta Lake transaction log, note column statistics.'),
    ],
    'mine': 'A', 'correct': 'C',
    'analysis': [
        'Predicate pushdown 是否生效体现在查询的物理执行计划（Physical Plan）中：若下推成功，会看到 `PushedFilters` 或 filter 在 scan 节点之前；若未下推，filter 在 scan 之后。',
        'Spark UI → SQL/DataFrame 标签 → 点击具体 query → 查看 Physical Plan（或 DAG 图）。',
        '选A错误：Executor 日志不包含 predicate pushdown 的诊断信息。',
        '选B错误：Input 列大小可以间接反映数据读取量，但不能直接诊断 pushdown 是否生效。',
        '选D错误：Delta transaction log 记录的是文件级别的统计信息，不是执行计划。',
    ],
    'tags': ['Spark UI', 'Physical Plan', 'predicate pushdown', '性能优化', 'query plan'],
}

replacements[221] = {
    'title': 'Databricks CLI 复制 pipeline 配置——get + create',
    'question': 'Data engineer needs to capture settings from an existing DLT pipeline and use them to create and version a JSON file for a new pipeline. Which Databricks CLI command sequence?',
    'options': [
        ('A', 'Use `list pipelines` to get specs for all pipelines; parse and use to create a pipeline.'),
        ('B', 'Stop existing pipeline; use returned settings in a reset command.'),
        ('C', 'Use `get` to capture settings for existing pipeline; remove pipeline_id and rename; use in a `create` command. ✅'),
        ('D', 'Use `clone` command to copy existing pipeline; use `get JSON` to get definition; save to git.'),
    ],
    'mine': 'A', 'correct': 'C',
    'analysis': [
        'Databricks CLI pipeline 操作流程：`databricks pipelines get --pipeline-id <id>` 获取完整 JSON 配置，然后删除 `pipeline_id` 字段（避免冲突），修改 pipeline 名称，最后用 `databricks pipelines create --settings <file.json>` 创建新 pipeline。',
        '选A错误：`list` 命令返回的是 pipeline 列表摘要，不包含完整的 pipeline 配置 spec。',
        '选D错误：Databricks CLI 没有 `clone` 命令用于 pipeline；`clone` 是 Delta Lake 的表操作命令。',
        '这也是将 pipeline 配置版本化到 git 的标准做法。',
    ],
    'tags': ['Databricks CLI', 'DLT pipeline', 'pipeline 配置', 'get/create', 'IaC'],
}

# Perform replacements
for qn, data in replacements.items():
    opts_lines = '\n'.join(f'{o}. {t}' for o, t in data['options'])
    analysis_lines = '\n'.join(f'- {line}' for line in data['analysis'])
    tags_str = ' '.join(f'`{t}`' for t in data['tags'])

    new_block = f"""### Q{qn} ❌ — {data['title']}

**原题：**
{data['question']}

**选项：**
{opts_lines}

**我的答案：** {data['mine']} | **正确答案：** {data['correct']}

**解析：**
{analysis_lines}

**知识点：** {tags_str}

"""

    pattern = rf'(### Q{qn} ❌ — 待补充.*?)((?=### Q\d+)|(?=^---$)|$)'
    content = re.sub(pattern, new_block, content, flags=re.DOTALL | re.MULTILINE)

with open('mock_exam/review_notes.md', 'w', encoding='utf-8') as f:
    f.write(content)

print("Done. Verifying...")
with open('mock_exam/review_notes.md', 'r', encoding='utf-8') as f:
    verify = f.read()

for qn in [147,164,167,174,175,184,187,188,207,212,218,219,220,221]:
    if f'### Q{qn} ❌ — 待补充' in verify:
        print(f"Q{qn}: STILL 待补充 - FAILED")
    else:
        print(f"Q{qn}: replaced OK")
