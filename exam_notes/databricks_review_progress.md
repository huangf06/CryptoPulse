# Databricks Certified Data Engineer Professional 复习进度

## 当前进度

- 正在复习：**全部 6 个域已完成！进入总复习阶段**
- 已完成：**域 1~6 全部 ✅** + Parquet 补充

## 已完成知识点

| # | 知识点 | 状态 |
|---|--------|------|
| 2.1 | Delta Lake Transaction Log（_delta_log、Checkpoint、OCC、冲突矩阵） | ✅ |
| 2.2 | VACUUM（保留期、安全检查、DRY RUN、GDPR 场景、vs OPTIMIZE） | ✅ |
| 2.3 | Time Travel（TIMESTAMP/VERSION AS OF、RESTORE、与 VACUUM 关系） | ✅ |
| 2.4 | Clone（Deep vs Shallow vs CTAS） | ✅ |
| 补充 | Parquet 深度讲解（列式存储、文件结构、编码压缩） | ✅ |
| 2.5 | CDF（_change_type 四种值、table_changes()、CDF vs CDC、OPTIMIZE 假更新） | ✅ |
| 2.6 | MERGE INTO（SQL/Python 语法、WHEN 子句顺序、foreachBatch 流式 MERGE、vs INSERT OVERWRITE） | ✅ |
| 2.7 | Structured Streaming 核心（Exactly-once、Checkpoint 三类内容、Delta 源 append-only 限制、流批一体） | ✅ |
| 2.8 | Auto Loader（cloudFiles format、Directory Listing vs File Notification、Schema Evolution 四模式、rescue data、vs COPY INTO） | ✅ |
| 2.9 | Output Modes（Append/Update/Complete 区别、兼容性规则、聚合+WM 时 Append 延迟输出） | ✅ |
| 2.10 | Trigger（Default/Fixed Interval/Once/AvailableNow/Continuous、Once vs AvailableNow 区别） | ✅ |
| 2.11 | Watermark（定义与计算、迟到数据丢弃、内存释放、与 Window 区别、Append 输出时机） | ✅ |
| 2.12 | Stream Joins（Stream-Static vs Stream-Stream、WM+时间约束要求、只支持 Append） | ✅ |
| 2.13 | DLT（Streaming Table vs Materialized View vs Temp View、Expectations 三种行为、APPLY CHANGES/SCD、Pipeline 模式、reset.allowed） | ✅ |
| 2.14 | foreachBatch（三大场景：流中MERGE/多目标/外部API、幂等性要求、vs APPLY CHANGES） | ✅ |
| 2.15 | 性能优化（AQE三功能、Join策略、Partitioning vs Z-Ordering vs Liquid Clustering、小文件治理、Photon） | ✅ |
| 3.1 | Medallion Architecture（Bronze/Silver/Gold 职责、Simplex vs Multiplex、反模式） | ✅ |
| 3.2 | 维度建模（Star vs Snowflake Schema、事实表 vs 维度表、Lakehouse 推荐 Star） | ✅ |
| 4.1 | Unity Catalog（三层命名空间、权限模型三层递进、Managed vs External Table） | ✅ |
| 4.2 | Dynamic Views + Delta Sharing + Secrets + Service Principal + GDPR 删除流程 + Federation | ✅ |
| 5.1 | Spark UI 诊断（数据倾斜、Shuffle、OOM、小文件、分区剪裁）| ✅ |
| 5.2 | System Tables + DLT Event Log + SQL Alerts + Query Profile | ✅ |
| 6.1 | 测试策略（测试金字塔、Unit/Integration/E2E、Arrange-Act-Assert） | ✅ |
| 6.2 | CI/CD 与部署（DABs targets 环境隔离、Job vs All-Purpose Cluster、Repair Run、For Each Task、Git Folders） | ✅ |
| 1.1 | DABs 配置（databricks.yml、CLI 命令 validate/deploy/run、变量引用） | ✅ |
| 1.2 | 第三方库管理（%pip install、Cluster Library、DLT 中安装库） | ✅ |
| 1.3 | UDF 类型（Python UDF vs Pandas UDF vs SQL UDF 性能对比、Arrow 序列化） | ✅ |
| 1.4 | Compute 类型（All-Purpose/Job/SQL Warehouse/Serverless、Cluster Policy） | ✅ |
| 1.5 | Lakeflow Jobs（Task 类型、taskValues 参数传递、重试配置） | ✅ |

## 待复习知识点（域 2 剩余）

2.5 CDF → 2.6 MERGE INTO → 2.7 Structured Streaming → 2.8 Auto Loader
→ 2.9 Output Modes → 2.10 Trigger → 2.11 Watermark → 2.12 Stream Joins
→ 2.13 foreachBatch → 2.14 性能优化 → 2.15 文件布局优化

## 薄弱点（答错的题目，需强化）

### 1. OCC 冲突矩阵
- 错误：认为 INSERT(APPEND) + DELETE 会冲突
- 正确：APPEND 只添加新文件，不读旧文件，与任何操作都不冲突
- 记忆口诀：**APPEND 永不冲突，只有 读+改 vs 读+改 才冲突**

### 2. VACUUM 保留期安全检查
- 错误：认为 RETAIN 0 HOURS 会直接执行
- 正确：Delta Lake 有安全检查，保留期 < 168h 会报错
- 需先设置 `spark.databricks.delta.retentionDurationCheck.enabled = false`

### 4. Output Mode 兼容性规则
- 错误：认为有聚合无 Watermark 时 Update 会报错
- 正确：报错的是 **Append**，因为无 WM 时 Spark 无法确定结果是最终值
- 记忆口诀：**Append 要求"最终答案"，没 Watermark 就永远给不出最终答案**

### 5. Watermark 与 Append 输出时机
- 错误：认为要 watermark 越过 (窗口结束时间 + threshold) 才输出
- 再次错误：看到 watermark 在窗口范围内就认为已输出
- 正确：watermark **严格大于窗口结束时间**才输出
- 关键理解：**watermark = max_event_time - threshold，不要重复减；判断条件是 watermark > 窗口结束时间，不是"在窗口里"**

### 6. foreachBatch 幂等性
- 错误：认为 append 在 foreachBatch 中不允许
- 正确：append 可以用，但微批重试时会导致**重复数据**（非幂等）
- 关键理解：**foreachBatch 中应优先用 MERGE 而非 append，因为 MERGE 天然幂等**

## 答题记录

| 知识点 | 题号 | 你的答案 | 正确答案 | 结果 |
|--------|------|----------|----------|------|
| 2.1 Transaction Log | 1 (Checkpoint 数量) | C | C | ✅ |
| 2.1 Transaction Log | 2 (INSERT+DELETE 冲突) | A | B | ❌ |
| 2.2 VACUUM | 1 (DELETE 后物理删除) | B | B | ✅ |
| 2.2 VACUUM | 2 (RETAIN 0 HOURS) | A | B | ❌ |
| 2.3 Time Travel | 1 (RESTORE 版本号) | A | C | ❌ |
| 2.3 Time Travel | 2 (VACUUM 后查询) | C | C | ✅ |
| 2.4 Clone | 1 (Shallow Clone 报错) | B | B | ✅ |
| 2.4 Clone | 2 (备份方式选择) | C | C | ✅ |
| 2.5 CDF | 1 (UPDATE 产生几行) | B | B | ✅ |
| 2.5 CDF | 2 (Silver 重复更新原因) | B | B | ✅ |
| 2.6 MERGE INTO | 1 (多 WHEN MATCHED 执行顺序) | B | B | ✅ |
| 2.6 MERGE INTO | 2 (流中 MERGE 方式) | B | B | ✅ |
| 2.7 Structured Streaming | 1 (源表 DELETE 后果) | C | C | ✅ |
| 2.7 Structured Streaming | 2 (Checkpoint 丢失) | B | B | ✅ |
| 2.8 Auto Loader | 1 (大规模文件发现模式) | D | D | ✅ |
| 2.8 Auto Loader | 2 (rescue 模式新列行为) | C | C | ✅ |
| 2.9 Output Modes | 1 (有聚合无WM哪个报错) | B | C | ❌ |
| 2.9 Output Modes | 2 (Append+WM输出时机) | D | C | ❌ |
| 2.10 Trigger | 1 (定时批处理Trigger选择) | C | C | ✅ |
| 2.10 Trigger | 2 (AvailableNow错误说法) | D | D | ✅ |
| 2.11 Watermark | 1 (迟到数据是否丢弃) | B | B | ✅ |
| 2.11 Watermark | 2 (哪个窗口已输出) | B | C/A | ❌ |
| 2.12 Stream Joins | 1 (不需要WM的场景) | B | B | ✅ |
| 2.12 Stream Joins | 2 (SS Join用Complete) | C | C | ✅ |
| 2.13 DLT | 1 (Bronze/Gold表类型选择) | C | C | ✅ |
| 2.13 DLT | 2 (Warn默认行为) | B | B | ✅ |
| 2.14 foreachBatch | 1 (MERGE+多目标写入) | C | C | ✅ |
| 2.14 foreachBatch | 2 (幂等性问题) | A | C | ❌ |
| 2.15 性能优化 | 1 (查询模式不确定的优化) | C | C | ✅ |
| 2.15 性能优化 | 2 (流式小文件治理) | B | B | ✅ |
| 3.1 Medallion | 1 (Multiplex摄取选择) | B | B | ✅ |
| 3.2 维度建模 | 1 (为何推荐Star Schema) | C | C | ✅ |
| 4.1 Unity Catalog | 1 (权限三层递进) | A | A | ✅ |
| 4.2 GDPR删除 | 1 (DELETE后是否物理删除) | B | B | ✅ |
| 4.3 External Table | 1 (DROP后数据是否保留) | B | B | ✅ |
| 5.1 Spark UI | 1 (数据倾斜诊断) | B | B | ✅ |
| 5.2 DLT Event Log | 1 (Expectations在哪查) | C | C | ✅ |
| 6.1 Job编排 | 1 (Repair Run) | B | B | ✅ |
| 6.2 部署最佳实践 | 1 (错误说法) | C | C | ✅ |
| 1.2 第三方库管理 | 1 (团队级库安装) | D | D | ✅ |
| 1.3 UDF | 1 (10亿行脱敏性能) | C | C | ✅ |
| 1.5 Jobs | 1 (Task间传值) | B | B | ✅ |

**正确率：35/42（83.3%）**

## 复习规则

1. 三层追问法：是什么 → 为什么 → 对比辨析
2. 每个知识点后 1-2 道验证题（含陷阱选项）
3. 中文讲解，代码术语保留英文
4. 每次 1-2 个知识点，确认后继续
5. 答错的标记薄弱点，最后汇总强化

## 如何继续

换电脑后新开会话，发送以下内容即可：
```
请继续复习，exam_notes\databricks_review_progress.md
```
