# 考前 30 分钟速查卡 — 外部模拟考全部错题

> 来源：SkillCertPro #1(20错) + #2(6错) + Udemy Quiz 001(15错) + CertSafari #2(10错) + Udemy #3(7错)
> 共 58 道错题，去重压缩为 ~50 条规则
> 生成日期：2026-04-26

---

## 1. Unity Catalog 权限与治理 (12 条)

| # | 规则 | 来源 |
|---|------|------|
| 1 | **最小权限**：CREATE TABLE + SELECT ON SCHEMA，不用 ALL PRIVILEGES | Quiz001-Q1,Q24 |
| 2 | **MANAGE on CATALOG 过大**：可 DROP catalog。权限委托用 USE CATALOG + CREATE SCHEMA | Udemy3-Q29 |
| 3 | **USE CATALOG** 允许遍历但不能 DROP/改权限 | Quiz001-Q24 |
| 4 | **SET/UNSET TAGS**：`ALTER TABLE SET TAGS ('k'='v')` / `UNSET TAGS ('k')`。DROP 删对象，不删 tag | Quiz001-Q20, Udemy3-Q20 |
| 5 | **TBLPROPERTIES 持久化**，spark.conf.set 仅当前会话。持久配置永远用 ALTER TABLE SET TBLPROPERTIES | Quiz001-Q39 |
| 6 | **PK/FK = Informational Only**，运行时不强制执行。NOT NULL/CHECK 才强制 | Udemy3-Q34 |
| 7 | **Surrogate Key** = `GENERATED ALWAYS AS IDENTITY`（ANSI）。uuid() 不递增 | CertSafari-Q41 |
| 8 | **UC 独有功能**：跨 workspace 集中权限 + 自动 lineage + Delta Sharing + Federation。Cluster Policies 不是 UC 独有 | CertSafari-Q60 |
| 9 | **UC Lineage 缺失** → 首先检查 Access Mode。Single User = lineage gap。改用 Shared Access Mode | CertSafari-Q43 |
| 10 | **Lakehouse Federation** 三步：CREATE CONNECTION → CREATE FOREIGN CATALOG → GRANT 权限 | Quiz001-Q43 |
| 11 | **deletedFileRetentionDuration** 默认 7 天。VACUUM RETAIN 7 DAYS 与之冲突会报错 | Quiz001-Q15 |
| 12 | **Column-level Security > File-level**。合规 = 数据隔离 + 列级安全 + Masking | SCP1-Q35 |

## 2. Structured Streaming (6 条)

| # | 规则 | 来源 |
|---|------|------|
| 13 | **Streaming MERGE/Upsert** → 只能用 `foreachBatch`。`writer.mode("merge")` 和 `updateMode` 不存在 | Quiz001-Q10 |
| 14 | **Stream-Static join 的 static 表不自动刷新** → 开 CDF + readStream 改为 Stream-Stream join | CertSafari-Q42 |
| 15 | **Sessionization**（自定义有状态+超时）→ `flatMapGroupsWithState` + `GroupStateTimeout`。Window function 不能跨 batch 维护状态 | CertSafari-Q45 |
| 16 | **Stateful streaming 并行度锁定在 checkpoint**。扩容需：停流 → 调高 shuffle.partitions → **新 checkpoint** 重启 | Udemy3-Q12 |
| 17 | **DLT 声明式去重**：`dropDuplicates` + watermark。不用 MERGE INTO foreachBatch（那是 imperative） | Quiz001-Q3 |
| 18 | **APPLY CHANGES INTO**：`SEQUENCE BY` 解决时序冲突/重复。`ignoreDuplicates` 不处理乱序 | Quiz001-Q11 |

## 3. Spark 性能与 API (8 条)

| # | 规则 | 来源 |
|---|------|------|
| 19 | **Broadcast Join 消除 shuffle**（小表广播）。Custom partitioner 只改善均匀性，shuffle 仍在 | SCP1-Q5 |
| 20 | **broadcastTimeout ≠ autoBroadcastJoinThreshold**。用了 `broadcast()` hint 后 threshold 无关。超时报错 → 调 broadcastTimeout | Udemy3-Q19 |
| 21 | **CBO 依赖统计信息**：`ANALYZE TABLE COMPUTE STATISTICS`。Optimizer hints 绕过 CBO，不是利用 CBO | SCP1-Q19 |
| 22 | **性能层级**：Native Column Expr > Pandas UDF > Python UDF。简单运算永远用原生 `F.col()` | Quiz001-Q59 |
| 23 | **Scalar Iterator UDF**：`Iterator[pd.Series] → Iterator[pd.Series]` 模型初始化一次；普通 `pd.Series → pd.Series` 每 batch 初始化 | CertSafari-Q56 |
| 24 | **Multi-table join varying sizes** → 选择性 broadcast 小表，不要 uniform repartition 所有表 | SCP2-Q12 |
| 25 | **Struct 导航**：`col("a.b.c")` dot notation。explode 只用于 Array/Map，不用于 Struct | Udemy3-Q44 |
| 26 | **Higher-Order Functions**：transform=逐元素变换保持结构，filter=筛选，explode=拆行，reduce=单值 | CertSafari-Q50 |

## 4. Delta Lake / DLT (5 条)

| # | 规则 | 来源 |
|---|------|------|
| 27 | **Fact Table 优化**：Partition by date + Z-Order by query key > Denormalize | SCP1-Q3 |
| 28 | **MERGE 慢 → 两板斧**：(1) Z-Order on join key = Data Skipping；(2) Partition predicate = Partition Pruning。Broadcast 大表会 OOM | CertSafari-Q19 |
| 29 | **`OPTIMIZE ... PARTITION BY` 不存在**。VACUUM RETAIN 0 HOURS 清理旧文件 | Quiz001-Q5 |
| 30 | **DLT 对象选型**：ST = append-only 原始数据 (Bronze)；MV = 当前状态/聚合 (Silver/Gold)。"Latest per ID" = MV，不是 ST | Quiz001-Q36 |
| 31 | **OLAP / multi-dimensional / time-series / BI → Star Schema**。几乎永远是正确答案 | SCP2-Q9,Q54 |

## 5. DABs / 平台配置 (5 条)

| # | 规则 | 来源 |
|---|------|------|
| 32 | **DABs YAML 引用 Secret**：`{{secrets/scope/key}}`。`dbutils.secrets.get()` 是 runtime 调用，不能放 YAML | CertSafari-Q44 |
| 33 | **jobs update = 增量修改**（安全），**jobs reset = 全量覆盖**（危险） | SCP1-Q34 |
| 34 | **Compute Policy** 替代 DBFS init scripts（已弃用）。管理 Spark conf + env vars + libs | Quiz001-Q19 |
| 35 | **Spot Instances**：Driver 永远 On-Demand，Worker 用 Spot。Driver 被回收 = 全 job 重来 | Udemy3-Q47 |
| 36 | **Clusters API 必需参数**：spark_version + node_type_id + autoscale/num_workers。三者都需要 | SCP1-Q51 |

## 6. System Tables / Monitoring (4 条)

| # | 规则 | 来源 |
|---|------|------|
| 37 | **system.query.history** = SQL 文本 + 执行者 + 耗时。**system.access.audit** = 登录/权限变更事件。**system.billing.usage** = DBU/费用 | CertSafari-Q25, Quiz001-Q13 |
| 38 | **DATE() 是 ANSI 标准**，考试优先于 TO_DATE() | Quiz001-Q13 |
| 39 | **Query Plan DAG** → Spark UI SQL/DataFrame tab。不在 Query Profiler Stages tab | Quiz001-Q42 |
| 40 | **Streaming 性能实时监控** → Spark UI Structured Streaming tab（内置、零配置），不需要 Event Hubs 绕 | SCP2-Q23 |

## 7. Azure 安全与集成 (8 条)

| # | 规则 | 来源 |
|---|------|------|
| 41 | **Data exfiltration** → 网络控制 (NSG + Private Link)，不是加密或访问控制 | SCP2-Q19 |
| 42 | **spark.io.encryption** = 临时数据 (shuffle/spill)，**不是** at-rest。At-rest → HDFS Encryption Zones / 存储层 | SCP1-Q17 |
| 43 | **PII 密钥管理** → Key Vault + Secret Scopes（端到端方案）。TDE 缺乏密钥管理灵活性 | SCP1-Q28 |
| 44 | **合规审计自动化** → Azure Policy（自动合规评估）+ Sentinel（SIEM/SOAR）。Manual review 不满足自动化要求 | SCP1-Q7 |
| 45 | **非合规资源自动修复** → Azure Policy + Functions。Policy 原生评估 > Logic Apps 自建监控 | SCP1-Q43 |
| 46 | **跨服务日志分析** → Log Analytics（统一汇聚）+ KQL（跨服务查询）。Azure Monitor 太笼统 | SCP1-Q31 |
| 47 | **实时预测 pipeline 故障** → Event Hubs + Stream Analytics + Azure ML（流式架构）。Log Analytics 是批量的 | SCP1-Q26 |
| 48 | **事件驱动调度** → Event Grid + Logic Apps → REST API 触发 Job。不用 VM 中间层 | SCP1-Q41 |

## 8. ML / MLflow (3 条)

| # | 规则 | 来源 |
|---|------|------|
| 49 | **MLflow 四组件**：Tracking=logging，Models=打包部署，Projects=代码打包，Registry=版本管理 | SCP1-Q49 |
| 50 | **ML Serving 解耦**：外部 ML 服务独立更新模型 > 集群内重训练（紧耦合） | SCP1-Q30 |
| 51 | **Azure ML** = managed training + web service endpoint；**MLflow** = tracking + registry | SCP2-Q24, SCP1-Q46 |

## 9. 存储 (1 条)

| # | 规则 | 来源 |
|---|------|------|
| 52 | **Parquet 对 null 极高效**：definition levels + RLE。Delta Lake 底层就是 Parquet，没有单独的 binary format | SCP1-Q6 |

## 10. 审题提醒 (2 条)

| # | 规则 | 来源 |
|---|------|------|
| 53 | **NOT 题型**：审题看清 "does NOT enhance / is NOT correct"。标记关键词后再选 | SCP1-Q48 |
| 54 | **"All of the above"**：逐个验证每个选项是否都成立，不要只看到一个对的就选它 | SCP1-Q51 |

---

## 高频失分模式（按严重度排序）

| 模式 | 累计错题 | 修正 |
|------|---------|------|
| UC 权限给多了 (ALL PRIVILEGES / MANAGE / OWNER) | 5 | 永远选"刚好够用"的最小权限 |
| Streaming 高级 API 不熟 (foreachBatch / flatMapGroupsWithState / CDF) | 5 | 有状态/Upsert/刷新 = 专用 API，不是简单模式 |
| Azure 服务选择混淆 (Policy vs Monitor vs Sentinel vs Log Analytics) | 8 | Policy=合规，Sentinel=SIEM，Log Analytics+KQL=日志分析 |
| 语法记错 (SET/UNSET TAGS, OPTIMIZE PARTITION BY, updateMode) | 4 | 不确定的语法选最接近 ANSI 标准的 |
| 配置层级混淆 (spark.conf vs TBLPROPERTIES vs YAML) | 3 | spark.conf=临时，TBLPROPERTIES=持久，YAML={{secrets}} |
| Star Schema 条件反射缺失 | 2 | OLAP/BI/multi-dimensional → Star Schema |
