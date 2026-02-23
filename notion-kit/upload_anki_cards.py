#!/usr/bin/env python3
"""
批量上传 Databricks DE Professional 复习卡片到 Notion Anki 数据库
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
    # fallback to Speakup's .env
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
    """获取 data_source_id"""
    url = f"https://api.notion.com/v1/databases/{database_id}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 200:
        data = resp.json()
        data_sources = data.get("data_sources", [])
        if data_sources:
            return data_sources[0]["id"]
    return database_id


def create_anki_card(data_source_id: str, front: str, back: str, deck: str, tags: list):
    """创建一张 Anki 卡片"""
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
        print(f"  + {front[:50]}...")
        return True
    else:
        print(f"  x FAILED: {front[:50]}... ({resp.status_code}: {resp.text[:100]})")
        return False


# ============================================================
# Domain 2: Data Processing - Anki 卡片
# ============================================================

CARDS = [
    # --- 2.1 Transaction Log ---
    {
        "front": "Delta Lake 的事务日志(_delta_log)存储什么？",
        "back": "存储 JSON 格式的 commit 文件，记录每次写操作的元数据（添加/删除的文件、Schema、操作类型等）。每 10 次 commit 生成一个 Parquet 格式的 Checkpoint 文件加速读取。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    {
        "front": "Delta Lake OCC 冲突矩阵：INSERT(APPEND) + DELETE 会冲突吗？",
        "back": "不会冲突。APPEND 只添加新文件，不读旧文件，与任何操作都不冲突。只有「读+改 vs 读+改」同一文件时才冲突。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    # --- 2.2 VACUUM ---
    {
        "front": "VACUUM 的默认保留期是多少？低于该值会怎样？",
        "back": "默认 168 小时（7 天）。低于该值直接报错。必须先设置 spark.databricks.delta.retentionDurationCheck.enabled = false 才能执行更短保留期的 VACUUM。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    {
        "front": "VACUUM 和 OPTIMIZE 的区别？",
        "back": "VACUUM：删除不再被引用的旧数据文件，回收存储空间。\nOPTIMIZE：合并小文件为大文件，提高读取性能。\n两者解决不同问题，互不替代。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    # --- 2.3 Time Travel ---
    {
        "front": "RESTORE TO VERSION 3 后，表的版本号变成几？",
        "back": "不是 3！RESTORE 创建一个新的 commit，版本号 = 原最新版本 + 1。事务日志只追加，永不回退。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    {
        "front": "VACUUM 后还能 Time Travel 到旧版本吗？",
        "back": "不能。VACUUM 删除了旧版本引用的数据文件，虽然事务日志还在，但数据文件已经没了，查询会报 FileNotFoundException。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    # --- 2.4 Clone ---
    {
        "front": "Deep Clone vs Shallow Clone vs CTAS 的区别？",
        "back": "Deep Clone：完整复制数据+元数据，独立副本。\nShallow Clone：只复制元数据，数据指向源表文件（省空间但依赖源表）。\nCTAS：只复制数据，不复制元数据（历史、约束、分区等丢失）。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    # --- 2.5 CDF ---
    {
        "front": "CDF (Change Data Feed) 的 _change_type 有哪 4 种值？",
        "back": "insert：新插入的行\nupdate_preimage：更新前的旧值\nupdate_postimage：更新后的新值\ndelete：被删除的行",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    {
        "front": "CDF 和 CDC 有什么区别？",
        "back": "CDC (Change Data Capture)：通用概念，捕获数据变更的技术统称。\nCDF (Change Data Feed)：Delta Lake 的具体实现，记录表的变更历史。\nCDF 是 CDC 的一种实现方式。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DeltaLake", "Domain2"],
    },
    # --- 2.6 MERGE INTO ---
    {
        "front": "Structured Streaming 中如何执行 MERGE？",
        "back": "使用 foreachBatch。流式 DataFrame 不能直接 MERGE，foreachBatch 将每个微批转为普通 DataFrame，然后用 DeltaTable.merge() API 执行。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.7 Structured Streaming ---
    {
        "front": "Structured Streaming 的 Checkpoint 存储哪三类内容？",
        "back": "1. Offset：已读取的数据位置（Kafka offset 等）\n2. State：有状态操作的中间状态（聚合、join 等）\n3. Commit Log：已完成的微批 ID",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Delta 表作为流数据源时有什么重要限制？",
        "back": "假设源是 append-only。对源表的任何非追加操作（UPDATE、DELETE）会导致流查询抛出异常。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.8 Auto Loader ---
    {
        "front": "Auto Loader 的两种文件发现模式？",
        "back": "1. Directory Listing（默认）：定期扫描目录，适合小规模。\n2. File Notification：利用云事件通知（S3 SNS/SQS、ADLS Event Grid），适合大规模（百万+文件）。\n通过 cloudFiles.useNotifications = true 开启。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Auto Loader Schema Evolution 的 4 种模式？",
        "back": "addNewColumns：自动添加新列（最常用）\nfailOnNewColumns：发现新列则报错停止\nrescue：新列放入 _rescued_data 列\nnone：忽略新列",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Auto Loader vs COPY INTO？",
        "back": "Auto Loader：Structured Streaming 流式处理，内部 RocksDB 跟踪文件，支持 Schema 演进，适合数十亿文件。\nCOPY INTO：SQL 批量命令，基于 commit log，不支持自动演进，适合千级别文件。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.9 Output Modes ---
    {
        "front": "三种 Output Mode 的核心区别？",
        "back": "Append：只输出最终确定的新行（给了不会改）\nUpdate：只输出本批变化的行（下次可能再变）\nComplete：每次输出完整结果表（全量重写）",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "有聚合 + 无 Watermark 时，哪个 Output Mode 会报错？",
        "back": "Append 报错。因为 Append 要求输出的行是「最终值」，但无 Watermark 时聚合结果随时可能变，Spark 无法保证最终性。\nUpdate 和 Complete 可以正常运行。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Stream-Stream Join 支持哪种 Output Mode？",
        "back": "只支持 Append。匹配上的结果就是最终的，符合 Append 语义。Complete 不行（流是无限的，无法维护完整结果表）。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.10 Trigger ---
    {
        "front": "Trigger.Once vs Trigger.AvailableNow 的区别？",
        "back": "Once：所有积压数据作为单个微批处理，可能 OOM（已废弃）。\nAvailableNow：自动拆成多个微批，更安全，中途失败可从 checkpoint 恢复（推荐替代）。\n两者都是处理完后自动停止。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "生产环境最常用的 Trigger 类型？流批一体调度场景呢？",
        "back": "生产持续运行：Fixed Interval（processingTime=\"10 seconds\"）\n流批一体调度：AvailableNow（处理完积压数据后停止，配合 Job 定时调度，省钱）",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.11 Watermark ---
    {
        "front": "Watermark 的计算公式和两个作用？",
        "back": "公式：watermark = 已见最大 event_time - threshold\n作用1：丢弃迟到数据（event_time < watermark → 丢弃）\n作用2：释放内存（watermark 越过窗口结束时间 → 清除窗口状态）",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Append 模式下，窗口聚合结果何时输出？",
        "back": "当 watermark 严格大于窗口结束时间时输出。\n注意：watermark 本身已减去 threshold，不要重复减。判断条件是 watermark > 窗口结束时间，不是「watermark 在窗口里」。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Window（窗口）和 Watermark（水印）的根本区别？",
        "back": "Window：按时间把数据分组的方式（决定数据放哪个桶）\nWatermark：不断推进的时间分界线（决定数据要不要、桶什么时候盖盖子）\nWindow 是「怎么分组」，Watermark 是「等多久」。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.12 Stream Joins ---
    {
        "front": "Stream-Static Join vs Stream-Stream Join 的要求区别？",
        "back": "Stream-Static：不需要 Watermark，不需要时间约束，无状态，静态表每个微批重新读取最新版本。\nStream-Stream：两边都必须有 Watermark + 时间范围约束，有状态（缓存未匹配记录），只支持 Append。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "Stream-Stream Join 为什么必须有 Watermark + 时间约束？",
        "back": "这三个条件共同告诉 Spark 一条记录在内存里最多等多久就可以丢弃。缺少任何一个，Spark 必须永远保留所有历史记录，内存必然爆炸。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.13 DLT ---
    {
        "front": "DLT 三种表类型的区别？",
        "back": "Streaming Table：增量处理，append-only，不支持聚合 → Bronze 层\nMaterialized View：全量重算，支持聚合/JOIN → Silver/Gold 层\nTemporary View：不持久化，Pipeline 结束后消失 → 中间步骤",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain2"],
    },
    {
        "front": "DLT Expectations 三种违规处理行为？",
        "back": "Warn（默认）：EXPECT(condition) → 记录违规，保留行\nDrop：EXPECT ... ON VIOLATION DROP ROW → 丢弃违规行\nFail：EXPECT ... ON VIOLATION FAIL UPDATE → 整个 Pipeline 停止\n严格程度递增：Warn < Drop < Fail",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain2"],
    },
    {
        "front": "DLT APPLY CHANGES 的关键配置项？",
        "back": "KEYS (id)：主键匹配\nAPPLY AS DELETE WHEN op='DELETE'：删除条件\nSEQUENCE BY seq_num：排序保证顺序\nSTORED AS SCD TYPE 1/2：Type1 直接覆盖，Type2 保留历史",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain2"],
    },
    {
        "front": "pipelines.reset.allowed = false 有什么作用？",
        "back": "防止 DLT Pipeline 完全刷新时删除 Bronze 表数据。Bronze 数据来自 Kafka 等瞬态源，如果刷新时删掉了，Kafka 数据可能已过期，数据永久丢失。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain2"],
    },
    # --- 2.14 foreachBatch ---
    {
        "front": "foreachBatch 的三大使用场景？",
        "back": "1. 流中执行 MERGE（upsert）\n2. 同时写入多个目标表\n3. 调用不支持流式写入的外部 API\n本质：把流的每个微批「降级」为普通 DataFrame，解锁批处理的全部能力。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "foreachBatch 为什么要求幂等性？",
        "back": "微批失败后 Spark 会重新执行整个函数。如果用 append，重试会导致重复数据。应优先用 MERGE（天然幂等：同一条数据 MERGE 多次结果一样）。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    {
        "front": "foreachBatch + MERGE vs DLT APPLY CHANGES？",
        "back": "foreachBatch + MERGE：手动挡，自己写所有逻辑，灵活但代码量大，任何 Spark 环境可用。\nAPPLY CHANGES：自动挡，声明式，SCD Type 2 一行搞定，但只能在 DLT Pipeline 内使用。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Streaming", "Domain2"],
    },
    # --- 2.15 性能优化 ---
    {
        "front": "Partitioning vs Z-Ordering vs Liquid Clustering？",
        "back": "Partitioning：按列值物理分文件夹，适合低基数列（<1000值），不能改分区列。\nZ-Ordering：重排文件内数据，支持多列，需手动 OPTIMIZE。\nLiquid Clustering：新表首选，写入时自动聚类，可随时 ALTER 聚类列，支持高基数。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Optimization", "Domain2"],
    },
    {
        "front": "流处理产生小文件的解决方案？",
        "back": "Optimize Write：写入时自动合并小文件。\nAuto Compaction：写入后后台自动合并。\nOPTIMIZE 命令：手动合并 + 可加 ZORDER。\n流处理场景优先开启前两者。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Optimization", "Domain2"],
    },
    {
        "front": "三种 Join 策略的性能排序和适用条件？",
        "back": "Broadcast Hash Join（最快）：一侧 < 10MB，无 Shuffle。\nShuffle Hash Join（中等）：一侧能放进内存。\nSort Merge Join（默认）：两侧都大，最稳定。\nAQE 可在运行时自动切换为 Broadcast。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Optimization", "Domain2"],
    },
    {
        "front": "Photon 引擎加速什么？不加速什么？",
        "back": "加速：Scan、Filter、Join、Aggregation（C++ 向量化执行）。\n不加速：Python UDF（在 Python 进程执行，Photon 管不到）。\nSQL Warehouse 默认启用。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "Optimization", "Domain2"],
    },
    {
        "front": "SCD Type 1 vs SCD Type 2？",
        "back": "SCD = Slowly Changing Dimension（缓慢变化维度）\nType 1：直接覆盖旧值，只保留最新状态。\nType 2：保留历史版本，新增一行，自动添加 __START_AT / __END_AT 列记录有效期。",
        "deck": "DE-Professional",
        "tags": ["Databricks", "DLT", "Domain2"],
    },
]


def main():
    print(f"Notion Token: {NOTION_TOKEN[:12]}...")
    print(f"Anki Database ID: {ANKI_DATABASE_ID}")
    print(f"Total cards to create: {len(CARDS)}")
    print()

    # 获取 data_source_id
    data_source_id = get_data_source_id(ANKI_DATABASE_ID)
    print(f"Data Source ID: {data_source_id}")
    print()

    success = 0
    failed = 0

    for i, card in enumerate(CARDS, 1):
        print(f"[{i}/{len(CARDS)}]", end="")
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

        # 避免速率限制
        if i % 3 == 0:
            time.sleep(0.5)

    print()
    print(f"Done! Success: {success}, Failed: {failed}")


if __name__ == "__main__":
    main()
