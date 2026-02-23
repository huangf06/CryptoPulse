#!/usr/bin/env python3
"""
批量上传 Databricks DE Professional 域 1/3/4/5/6 复习卡片到 Notion Anki 数据库
（域 2 已在之前上传）
"""

import os
import sys
import time
import requests
from dotenv import load_dotenv
from pathlib import Path

# Fix Windows encoding
sys.stdout.reconfigure(encoding='utf-8')

# 加载环境变量
env_path = Path(__file__).parent / ".env"
if not env_path.exists():
    env_path = Path(r"C:\Users\huang\github\Speakup\notion-kit\.env")
load_dotenv(env_path)

NOTION_TOKEN = os.getenv("NOTION_TOKEN")
ANKI_DATABASE_ID = os.getenv("ANKI_DATABASE_ID")
NOTION_VERSION = "2025-09-03"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": NOTION_VERSION,
}


def get_data_source_id(database_id: str) -> str:
    url = f"https://api.notion.com/v1/databases/{database_id}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        data_sources = data.get("data_sources", [])
        if data_sources:
            return data_sources[0]["id"]
    return database_id


def create_anki_card(data_source_id: str, front: str, back: str, deck: str, tags: list):
    url = "https://api.notion.com/v1/pages"
    payload = {
        "parent": {"data_source_id": data_source_id},
        "properties": {
            "Front": {"title": [{"text": {"content": front}}]},
            "Back": {"rich_text": [{"text": {"content": back[:2000]}}]},
            "Deck": {"select": {"name": deck}},
            "Tags": {"multi_select": [{"name": tag} for tag in tags]},
            "Synced": {"checkbox": False},
        },
    }
    resp = requests.post(url, headers=HEADERS, json=payload, timeout=30)
    if resp.status_code == 200:
        print(f"  + {front[:60]}...")
        return True
    else:
        print(f"  x FAILED: {front[:50]}... ({resp.status_code}: {resp.text[:100]})")
        return False


# ============================================================
# Domain 1: Databricks Tooling
# ============================================================

CARDS_DOMAIN_1 = [
    {
        "front": "DABs CLI: validate vs deploy vs run 的区别？",
        "back": "validate：只检查 YAML 语法和配置是否合法，不推送任何东西。\ndeploy：把资源定义推到 Workspace，但不运行。\nrun：部署 + 立即触发运行。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DABs", "Domain1"],
    },
    {
        "front": "DABs 如何实现 Dev/Staging/Prod 环境隔离？",
        "back": "通过 databricks.yml 中的 targets 配置。每个 target 可以指定不同的 workspace host、variables（如 catalog 名）、run_as（Service Principal）。同一套代码通过变量切换环境。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DABs", "Domain1"],
    },
    {
        "front": "DLT Pipeline 中如何安装第三方 Python 库？",
        "back": "DLT 不支持 %pip install。必须在 Pipeline Settings 的 Configuration 中通过 libraries 字段声明 PyPI 依赖。\n例如：{\"libraries\": [{\"pypi\": {\"package\": \"requests==2.31.0\"}}]}",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain1"],
    },
    {
        "front": "%pip install 后为什么要 dbutils.library.restartPython()？",
        "back": "因为 %pip install 修改了 Python 环境，但已加载的模块还是旧版本。restartPython() 重启 Python 解释器，确保所有 import 使用新安装的库版本。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Domain1"],
    },
    {
        "front": "Python UDF vs Pandas UDF vs SQL UDF 性能排序及原因？",
        "back": "SQL UDF > Pandas UDF >> Python UDF\nSQL UDF：在 JVM 中由 Catalyst 优化，零序列化开销。\nPandas UDF：Apache Arrow 批量序列化，开销低。\nPython UDF：逐行 JVM↔Python 序列化，开销极高。\n结论：能用 SQL 内置函数就不写 UDF，必须写就用 Pandas UDF。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "UDF", "Domain1"],
    },
    {
        "front": "为什么 Pandas UDF 比 Python UDF 快？",
        "back": "Pandas UDF 使用 Apache Arrow 做批量序列化：一次性把整列数据从 JVM 传到 Python（pd.Series），处理完再批量传回。\nPython UDF 每行数据都要单独做一次 JVM→Python→JVM 的序列化/反序列化，开销是 N 倍。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "UDF", "Domain1"],
    },
    {
        "front": "Job Cluster vs All-Purpose Cluster？",
        "back": "Job Cluster：Job 开始时自动创建，结束时自动销毁。便宜约 50%，适合生产 Job。\nAll-Purpose Cluster：手动管理，多人共享，适合交互式开发。\n生产 Job 必须用 Job Cluster。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Compute", "Domain1"],
    },
    {
        "front": "Cluster Policy 的作用？",
        "back": "管理员通过 Cluster Policy 限制用户能创建什么样的集群：限制 worker 数量范围、限制机型选择、限定 Spark 版本等。目的是控制成本、防止资源浪费。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Compute", "Domain1"],
    },
    {
        "front": "Databricks Job 中 Task 间如何传递参数？",
        "back": "写入方：dbutils.jobs.taskValues.set(key=\"row_count\", value=12345)\n读取方：dbutils.jobs.taskValues.get(taskKey=\"task_a\", key=\"row_count\")\n注意 get 时必须指定 taskKey（哪个 Task 的值）。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Jobs", "Domain1"],
    },
    {
        "front": "Job 失败后如何高效恢复？什么是 Repair Run？",
        "back": "Repair Run = 只重跑失败的 Task 及其下游 Task，不重跑已成功的 Task。\n比重跑整个 Job 高效得多，是 Job 故障恢复的首选方式。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Jobs", "Domain1"],
    },
]

# ============================================================
# Domain 3: Data Modeling
# ============================================================

CARDS_DOMAIN_3 = [
    {
        "front": "Medallion Architecture 三层各自的职责？",
        "back": "Bronze：原封不动摄取原始数据，只加元数据（摄取时间、源文件名）。\nSilver：清洗、去重、标准化、关联维度表。\nGold：按业务需求聚合，维度建模，直接供报表使用。\n核心原则：越往上越干净。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Medallion", "Domain3"],
    },
    {
        "front": "为什么 Bronze 层必须保留原始数据？",
        "back": "1. 可重放：Silver/Gold 出问题可从 Bronze 重算。\n2. 合规审计：GDPR 等法规要求可追溯原始数据。\n3. 瞬态源保护：Kafka 等有保留期限，过期数据就没了，Bronze 是永久备份。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Medallion", "Domain3"],
    },
    {
        "front": "Simplex vs Multiplex 摄取模式？",
        "back": "Simplex：一个数据源 → 一个 Bronze 表。表多，管理复杂。\nMultiplex：多个数据源 → 一个 Bronze 表（用 event_type 列区分）。表少，管理简单。\n适合数据源结构相似的场景。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Medallion", "Domain3"],
    },
    {
        "front": "为什么不能把 Kafka 当 Bronze 表（反模式）？",
        "back": "Kafka 数据有保留期限（通常 7 天），过期就没了。如果跳过 Bronze 直接从 Kafka 读到 Silver，Silver 出错后无法重算（原始数据已过期）。正确做法：永远先写入 Bronze（Delta Lake），再从 Bronze 转换到 Silver。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Medallion", "Domain3"],
    },
    {
        "front": "Star Schema vs Snowflake Schema？Lakehouse 推荐哪个？",
        "back": "Star Schema：事实表 + 非规范化维度表，JOIN 少，查询快，存储略大。\nSnowflake Schema：事实表 + 规范化维度表（进一步拆分），JOIN 多，查询慢，存储更小。\nLakehouse 推荐 Star Schema：存储便宜不需要过度规范化，查询性能更重要。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DataModeling", "Domain3"],
    },
    {
        "front": "事实表 (Fact) vs 维度表 (Dimension) 的区别？",
        "back": "事实表：存可度量的业务事件（订单、点击），数据量极大，只增不改（append-only），包含度量值和外键。\n维度表：存描述性属性（客户、产品），数据量小，缓慢变化（SCD），包含主键和描述属性。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DataModeling", "Domain3"],
    },
]

# ============================================================
# Domain 4: Security and Governance
# ============================================================

CARDS_DOMAIN_4 = [
    {
        "front": "Unity Catalog 三层命名空间？访问表需要几层权限？",
        "back": "命名空间：Catalog → Schema → Table/View/Volume/Function\n完整路径：catalog.schema.table\n访问表需要同时拥有三层权限：USE CATALOG + USE SCHEMA + SELECT。缺任何一层都查不了。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "UnityCatalog", "Domain4"],
    },
    {
        "front": "Managed Table vs External Table？DROP 后的区别？",
        "back": "Managed Table：存储由 UC 自动管理。DROP 后数据+元数据都删除。推荐使用。\nExternal Table：存储在用户指定的外部路径。DROP 后只删元数据，数据保留在 S3/ADLS 上。\n可以重新 CREATE TABLE ... LOCATION 恢复。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "UnityCatalog", "Domain4"],
    },
    {
        "front": "Dynamic Views 如何实现行级安全和列级掩码？",
        "back": "使用 CASE WHEN + is_account_group_member() 函数：\n列掩码：CASE WHEN is_account_group_member('finance') THEN amount ELSE 0 END\n行过滤：WHERE CASE WHEN is_account_group_member('global') THEN TRUE ELSE region = 'local' END\n不同用户看到不同内容。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Security", "Domain4"],
    },
    {
        "front": "Delta Sharing 是什么？两种模式？",
        "back": "开放协议，跨组织安全共享数据，不复制数据。\nDatabricks-to-Databricks：公司内部跨团队共享。\nDatabricks-to-Open：向非 Databricks 平台共享（接收方用 delta_sharing Python 库读取）。\n提供方通过 CREATE SHARE + GRANT 授权。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaSharing", "Domain4"],
    },
    {
        "front": "dbutils.secrets.get() 返回的值在 Notebook 中显示什么？",
        "back": "显示 [REDACTED]。即使 print() 也只显示 [REDACTED]，防止密码/API Key 意外泄露到 Notebook 输出中。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Security", "Domain4"],
    },
    {
        "front": "GDPR 删除流程？为什么 DELETE 后还要 VACUUM？",
        "back": "流程：1. DELETE FROM 所有层 2. VACUUM 清理物理文件 3. 审计日志\nDELETE 只在事务日志中标记删除，物理文件仍存在（为了 Time Travel）。VACUUM 才真正从磁盘删除物理文件。不 VACUUM = 数据还在。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "GDPR", "Domain4"],
    },
    {
        "front": "Service Principal 是什么？为什么生产 Job 要用它？",
        "back": "非人类身份，用于自动化和 CI/CD。\n生产 Job 用 SP 运行的原因：1. 个人离职不影响 Job 2. 权限管理更清晰 3. 符合最小权限原则。\n在 DABs 中配置：run_as: service_principal_name: \"etl-prod-sp\"",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Security", "Domain4"],
    },
    {
        "front": "Lakehouse Federation vs ETL 复制数据？",
        "back": "Federation：直接查询外部数据库（PostgreSQL/MySQL/Snowflake），不复制数据。实时但慢，适合低频查询。\nETL：复制数据到 Lakehouse（Delta Lake）。有延迟但快，适合高频查询。\nFederation 通过 CREATE CONNECTION + CREATE FOREIGN CATALOG 实现。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Federation", "Domain4"],
    },
]

# ============================================================
# Domain 5: Monitoring and Logging
# ============================================================

CARDS_DOMAIN_5 = [
    {
        "front": "Spark UI 数据倾斜的典型表现？解决方案？",
        "back": "表现：某个 Stage 中 99% 的 Task 秒完，但 1 个 Task 跑了很久。\n方案1（推荐）：AQE Skew Join — spark.sql.adaptive.skewJoin.enabled = true\n方案2：Salting — 给倾斜 key 添加随机后缀打散分区。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "SparkUI", "Domain5"],
    },
    {
        "front": "Spark UI 性能诊断速查：5 种现象 → 原因 → 解决？",
        "back": "1 个 Task 特慢 → 数据倾斜 → AQE Skew Join\nShuffle 数据巨大 → Join 策略不当 → Broadcast Join\nOOM → 数据超内存 → 增加内存/避免 collect()\n几千 Task 每个只处理几 KB → 小文件 → Optimize Write\nScan 100 万行 Filter 后剩 100 → 缺 Data Skipping → Z-Order/Liquid Clustering",
        "deck": "DE-Professional",
        "tags": ["Databricks", "SparkUI", "Domain5"],
    },
    {
        "front": "Databricks System Tables 有哪些？各查什么？",
        "back": "system.billing.usage：DBU 消费、成本监控\nsystem.access.audit：谁做了什么操作、安全审计\nsystem.compute.clusters：集群信息、利用率\nsystem.lakeflow.events：DLT Pipeline 事件\nsystem.information_schema.*：表/列元数据",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Monitoring", "Domain5"],
    },
    {
        "front": "DLT Expectations 结果在哪里查询？",
        "back": "通过 event_log() 函数查询，过滤 event_type = 'flow_progress'：\nSELECT details:flow_progress.data_quality.expectations[0].name, .passed_records, .failed_records\nFROM event_log(TABLE(my_pipeline))\nWHERE event_type = 'flow_progress' AND details:flow_progress.data_quality IS NOT NULL",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain5"],
    },
    {
        "front": "Query Profile 中 Spill to Disk 说明什么？",
        "back": "说明内存不够，数据溢出到磁盘，会严重影响性能。解决方案：增加 Executor 内存、减少分区大小、优化查询减少中间数据量。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Monitoring", "Domain5"],
    },
]

# ============================================================
# Domain 6: Testing and Deployment
# ============================================================

CARDS_DOMAIN_6 = [
    {
        "front": "测试金字塔三层：Unit / Integration / E2E？",
        "back": "Unit：测单个转换函数，local[*] SparkSession，造假数据，秒级。数量最多。\nIntegration：测跨模块链路（Bronze→Silver），真实 Spark 环境，分钟级。数量中等。\nE2E：测完整 Pipeline 输入到输出，生产级环境，小时级。数量最少。\n单元测试遵循 Arrange → Act → Assert 模式。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Testing", "Domain6"],
    },
    {
        "front": "生产环境部署最佳实践清单？",
        "back": "1. DABs targets 管理多环境（Dev/Staging/Prod 不同 Catalog）\n2. Service Principal 运行生产 Job（不用个人账号）\n3. Job Cluster（不用 All-Purpose，省 50% 成本）\n4. CI/CD 自动部署（PR → Test → Deploy）\n5. Git-based 版本管理",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Deployment", "Domain6"],
    },
    {
        "front": "Repair Run 是什么？",
        "back": "Job 中某个 Task 失败后，Repair Run 只重跑失败的 Task 及其下游 Task，不重跑已成功的 Task。比重跑整个 Job 高效得多，是故障恢复的首选方式。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Jobs", "Domain6"],
    },
    {
        "front": "For Each Task 的作用？",
        "back": "动态并行执行：第一个 Task 输出一个列表（如 [\"us\", \"eu\", \"apac\"]），For Each Task 自动为每个元素并行创建一个子 Task 执行。\n不需要硬编码每个区域，动态生成。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Jobs", "Domain6"],
    },
    {
        "front": "Dev/Staging/Prod 环境隔离策略？",
        "back": "Dev：dev_catalog + 小集群/Serverless，开发调试。\nStaging：staging_catalog + 中等集群，集成测试/UAT。\nProd：prod_catalog + Job Cluster，生产运行。\n通过 Unity Catalog 的不同 Catalog 实现数据隔离，通过 DABs targets 切换。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Deployment", "Domain6"],
    },
]

# ============================================================
# 薄弱点强化卡片
# ============================================================

CARDS_WEAK_POINTS = [
    {
        "front": "【易错】有聚合 + 无 Watermark 时，三种 Output Mode 哪个报错？",
        "back": "Append 报错！\nAppend 要求输出的行是「最终值不会再变」，但无 Watermark 时聚合结果随时可能因迟到数据而改变，Spark 无法保证最终性。\nUpdate ✅ Complete ✅ Append ❌",
        "deck": "DE-Professional",
        "tags": ["Databricks", "WeakPoint", "Streaming"],
    },
    {
        "front": "【易错】Watermark + Append 模式：窗口何时输出？",
        "back": "条件：watermark > 窗口结束时间\n其中 watermark = max_event_time - threshold\n注意：不要把 threshold 加两次！watermark 本身已减去了 threshold。\n判断的是 watermark 是否「越过」窗口结束时间，不是 watermark 是否「在窗口里」。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "WeakPoint", "Streaming"],
    },
    {
        "front": "【易错】foreachBatch 中为什么 append 有问题？",
        "back": "微批失败后 Spark 会重新执行整个 foreachBatch 函数。append 模式下重试会写入重复数据（非幂等）。\n应优先用 MERGE 替代 append，因为 MERGE 天然幂等（同一条数据 MERGE 多次结果一样）。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "WeakPoint", "Streaming"],
    },
]


ALL_CARDS = CARDS_DOMAIN_1 + CARDS_DOMAIN_3 + CARDS_DOMAIN_4 + CARDS_DOMAIN_5 + CARDS_DOMAIN_6 + CARDS_WEAK_POINTS


def main():
    print(f"Notion Token: {NOTION_TOKEN[:12]}...")
    print(f"Anki Database ID: {ANKI_DATABASE_ID}")
    print(f"Total cards to create: {len(ALL_CARDS)}")
    print()

    data_source_id = get_data_source_id(ANKI_DATABASE_ID)
    print(f"Data Source ID: {data_source_id}")
    print()

    success = 0
    failed = 0

    for i, card in enumerate(ALL_CARDS, 1):
        print(f"[{i}/{len(ALL_CARDS)}]", end="")
        ok = create_anki_card(
            data_source_id,
            card["front"],
            card["back"],
            card["deck"],
            card["tags"],
        )
        if ok:
            success += 1
        else:
            failed += 1

        if i % 3 == 0:
            time.sleep(0.5)

    print()
    print(f"Done! Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()
