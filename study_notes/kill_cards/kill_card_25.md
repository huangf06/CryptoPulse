# 25 题速杀卡 — 考前默写用

> 目标：每条规则能脱口而出。考前最后一天全部默写一遍。

---

## A. Unity Catalog 权限（8 题）

| # | 规则 |
|---|------|
| Q187 | MLflow 推理输出 = 静态 DF → `.write.mode("append").saveAsTable()` → 不能用 writeStream |
| Q230 | Schema Owner 不自动有表权限，但可以自行 GRANT SELECT |
| Q231 | 集群权限三级：CAN ATTACH TO → CAN RESTART → CAN MANAGE（**CAN VIEW 不存在**） |
| Q241 | Row Filter = **哪些行**（按 region）；Column Mask = **显示什么值**（按 role）；实现方式：SQL UDF + ALTER TABLE SET ROW FILTER / ALTER COLUMN SET MASK |
| Q281 | 同 Q241 —— **维度不能搞反**：region 控制行，role 控制列值 |
| Q291 | 团队需要在 catalog 内自治 → **USE CATALOG + USE SCHEMA** 就够；ALL PRIVILEGES / MANAGE / OWNER 都给了删除/改权限的能力 → 过度 |
| Q314 | Federation 为什么解决一致性？**不复制 → 不存在** 副本过期 / 同步延迟 / 版本冲突 |
| Q326 | 细粒度控制 + 最少维护 + 最佳性能 = **UC Managed Table + Predictive Optimization**（Predictive Opt 只支持 Managed Table） |

**记忆口诀：** Owner 不继承、View 不存在、行按区域列按角色、USE 够用别给多、不复制就不矛盾、托管才能预测优化

---

## B. Spark 性能机制（7 题）

| # | 规则 |
|---|------|
| Q220 | 确认 predicate pushdown → **SQL tab → Physical Plan → FileScan 节点的 PushedFilters**（不是 Stages tab） |
| Q236 | `collect()` 把所有数据拉到 **driver** → OOM → 用 `display(df)` 替代 |
| Q245 | DBSQL 慢查询诊断 → **Query Profile → Top Operators**（EXPLAIN 只给计划，无运行时数据） |
| Q279 | `repartition("key")` 先 shuffle 一次 → 后续 `groupBy("key")` 变成 **partition-local**，不再 shuffle → 净效果更少 |
| Q280 | Spill = task 数据 > 可用内存 → 溢出到磁盘 → 解决：**加内存 / 增加并行度**（减少每个 task 数据量） |
| Q310 | **MV** = 预计算聚合 / BI dashboard；**Streaming Table** = 连续数据流 / 实时监控 |
| Q324 | **Dynamic File Pruning** = Delta 在 join 时根据条件跳过无关文件 → 数据量减少 → Shuffle Hash Join 替代 Sort Merge Join |

**记忆口诀：** 下推看SQL-Plan、collect炸Driver、Profile有运行时、提前分区省shuffle、溢出加内存、聚合MV实时ST、DFP跳文件

---

## C. Delta 高级特性（4 题）

| # | 规则 |
|---|------|
| Q233 | ~~PDF答案错误，已修正~~ LC cluster-on-write 支持 INSERT INTO / CTAS / RTAS / spark.write；**Streaming 默认不支持**，需启用 `spark.databricks.delta.liquid.eagerClustering.streaming.enabled = true` |
| Q240 | 查询模式**常变** → Liquid Clustering（变 key 无需重写）；**稳定** → Z-ORDER（变列需重新 OPTIMIZE 全表） |
| Q288 | MERGE 优化两板斧：**(1)** LC 按 merge join key 聚簇（数据局部性，减少扫描）**(2)** Deletion Vectors（软删除，减少写放大） |
| Q324 | （已计入 Spark 性能） |

**记忆口诀：** INSERT不聚、常变选LC、MERGE双刀LC+DV

---

## D. DLT / Lakeflow（3 题）

| # | 规则 |
|---|------|
| Q227 | Python 闭包捕获**引用**不是值 → 循环结束后所有函数指向最后一项 → **工厂函数**创建独立作用域 |
| Q268 | 原始流数据 + 实时监控 → **Streaming Table**；每日聚合报告 → **Materialized View** |
| Q310 | （已计入 Spark 性能） |

**记忆口诀：** 闭包用工厂、实时ST聚合MV

---

## E. 平台操作（5 题）

| # | 规则 |
|---|------|
| Q60 | 聚合表（7 天总和 / YTD / QTD）历史被审计修改 → **全量重算 + overwrite**（upsert 不会级联重算聚合窗口） |
| Q73 | 延迟可容忍 → **trigger(once=True) + 定时作业** 省 10x 成本 |
| Q226 | 单元测试 → 函数写成**标准 .py 文件**放在 Files in Repos → pytest 直接 import |
| Q235 | 获取单次运行历史 → **runs/get + include_history** 参数（不是 runs/list） |
| Q260 | 并行下载多文件 → **foreach task**（原生列表迭代 + 并行执行 + 独立重试） |
| Q292 | 生产化 = **Compute Policies**（env vars / Spark config）+ **Init Scripts**（安装外部库） |

**记忆口诀：** 聚合改了全重算、容忍延迟用once、测试放py文件、get历史不是list、并行用foreach、生产策略加脚本

---

## 默写检查模板

把这页打印出来，遮住右列，看着左列题号默写规则。全对 = 过关。

```
Q187: _______________
Q220: _______________
Q226: _______________
Q227: _______________
Q230: _______________
Q231: _______________
Q233: _______________
Q235: _______________
Q236: _______________
Q240: _______________
Q241: _______________
Q245: _______________
Q260: _______________
Q268: _______________
Q279: _______________
Q280: _______________
Q281: _______________
Q288: _______________
Q291: _______________
Q292: _______________
Q310: _______________
Q314: _______________
Q324: _______________
Q326: _______________
Q60:  _______________
Q73:  _______________
```
