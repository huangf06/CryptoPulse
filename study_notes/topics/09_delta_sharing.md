# 09. Delta Sharing

**题量：3 题 | 全错（0/3）| 状态：严重薄弱**

---

## 概念框架

### Delta Sharing 是什么

Delta Sharing 是一个开放协议，允许安全地共享 Delta 表数据，**无需复制数据**。它是 Unity Catalog 的一部分，提供集中化的访问控制和审计。

### 核心模型：Provider / Recipient / Share

```
Provider（数据提供方，拥有数据的 Databricks workspace）
  |
  +-- 创建 Share（共享单元，包含要共享的表/schema）
  |     +-- ALTER SHARE ... ADD TABLE ...
  |
  +-- 创建 Recipient（接收方身份）
  |
  +-- GRANT SELECT ON SHARE ... TO RECIPIENT ...
  
Recipient（数据接收方）
  +-- CREATE CATALOG ... USING SHARE ...
  +-- 通过 catalog.schema.table 访问数据
```

### 两种共享模式对比

| 特性 | Databricks-to-Databricks | Open Sharing |
|------|--------------------------|--------------|
| 接收方平台 | Databricks workspace | 任意平台（Spark, Pandas, etc.） |
| Time Travel | 支持（需 WITH HISTORY） | 不支持 |
| Streaming Reads | 支持（需启用 CDF） | 不支持 |
| 可共享对象 | Tables, Volumes, Models, Notebooks | 仅 Delta Tables |
| 性能 | 最优（原生协议） | 通过 REST API，性能较低 |
| 分区优化 | 非分区表性能更优 | N/A |

### 权限模型

- 创建 Share：需要 **metastore admin** 或 **CREATE SHARE** privilege（metastore 级别）
- 不存在 "MANAGE SHARES" 这个权限名称
- 权限控制在 **metastore 层面**，不是 workspace 层面

---

## 错题精析

### Q264 -- Delta Sharing 创建共享所需权限

**原题：**

> A data architect is implementing Delta Sharing as part of their data governance strategy to enable secure data collaboration with external partners and internal business units. The architect must establish a permission framework that allows designated data stewards to create shares for their respective domains while maintaining security boundaries and audit compliance.
>
> Which specific permissions and roles must be assigned to enable users to create, configure, and manage Delta Shares while maintaining proper security governance and access controls?

**选项：**
- A. Only workspace admins can create and manage shares
- B. Users need the MANAGE SHARES permission on the workspace
- C. Users need to be metastore admins or have CREATE SHARE privilege for the metastore
- D. Any user with USE_CATALOG privilege can create shares

**我的答案：** B | **正确答案：** C

**解析：**
- Delta Sharing 的权限在 **metastore 级别**管理，不是 workspace 级别
- 创建 Share 需要 metastore admin 角色或 CREATE SHARE 特权
- B 选项的 "MANAGE SHARES" 是编造的权限名称，Unity Catalog 中不存在
- D 选项的 USE_CATALOG 只是允许访问 catalog，权限远远不够创建 Share

**错因：** 混淆了权限层级（workspace vs metastore），且没有记住 Unity Catalog 的标准权限名称。

---

### Q299 -- Delta Sharing 支持 Time Travel 和 Streaming 的配置（pending）

**原题：**

> A data engineer is configuring Delta Sharing for a Databricks-to-Databricks scenario to optimize read performance. The recipient needs to perform time travel queries and streaming reads on shared sales data.
>
> Which configuration will provide the optimal performance while enabling these capabilities?

**选项：**
- A. Use the open sharing protocol instead of Databricks-to-Databricks sharing for better performance.
- B. Share tables WITHOUT HISTORY and enable partitioning for better query performance.
- C. Share tables WITH HISTORY, ensure tables don't have partitioning enabled, and enable CDF before sharing.
- D. Share the entire schema WITHOUT HISTORY and rely on recipient-side caching for performance.

**我的答案：** A | **正确答案：** C

**解析：**
- **Time Travel** 需要共享时指定 `WITH HISTORY`，否则接收方无法访问历史版本
- **Streaming Reads** 需要在共享前启用 **Change Data Feed (CDF)**，因为流式读取依赖 CDF 追踪增量变更
- **非分区表性能更优**：在 Databricks-to-Databricks 共享中，协议对非分区表有优化，predicate pushdown 通过 shared metadata 实现，不需要物理分区
- A 选项完全错误：Open Sharing 性能不如 Databricks-to-Databricks，且不支持 time travel 和 streaming
- B 选项自相矛盾：WITHOUT HISTORY 无法支持 time travel
- D 选项：WITHOUT HISTORY + 接收方缓存不能替代 time travel 功能

**关键记忆点：** WITH HISTORY + CDF + 非分区 = Databricks-to-Databricks 最优配置。

---

### Q300 -- Delta Sharing 的限制（pending）

**原题：**

> A data organization has adopted Delta Sharing to securely distribute curated datasets from a Unity Catalog-enabled workspace. The data engineering team is sharing large Delta tables with an internal partner via Databricks-to-Databricks and aggregated reports with an external client via Open Sharing. While testing new sharing workflows, the data engineering team encounters challenges related to access control, data update visibility, and shareable object types.
>
> What is a limitation of the Delta Sharing protocol or implementation when used with Databricks-to-Databricks or Open Sharing?

**选项：**
- A. Delta Sharing does not support Unity Catalog enabled tables; only legacy Hive Metastore tables are shareable.
- B. With Databricks-to-Databricks sharing, Unity Catalog recipients must re-ingest data manually using COPY INTO or REST APIs.
- C. Delta Sharing (both Databricks-to-Databricks and Open sharing) allows recipients to modify the source data if they have SELECT privileges.
- D. With Open sharing, recipients cannot access Volumes, Models, or notebooks -- only static Delta tables are supported.

**我的答案：** B | **正确答案：** D

**解析：**
- Open Sharing 的核心限制：**只支持共享 Delta Tables**，不支持 Volumes、ML Models、Notebooks 等其他 Unity Catalog 对象类型
- 这些高级对象类型只在 Databricks-to-Databricks 模式下才能共享
- A 错误：Delta Sharing 完全支持 Unity Catalog 表，这是其核心用例
- B 错误：Databricks-to-Databricks 接收方可以直接读取，无需手动 re-ingest
- C 错误：接收方永远无法修改源数据，Delta Sharing 是只读共享协议

**错因：** 不了解 Open Sharing 与 Databricks-to-Databricks 在可共享对象类型上的差异。

---

## 核心对比表

| 维度 | 考点 | 易混淆项 |
|------|------|----------|
| 权限层级 | metastore 级别的 CREATE SHARE | workspace 级别的权限（不存在） |
| Time Travel 支持 | 需要 WITH HISTORY 共享 | WITHOUT HISTORY 不支持 |
| Streaming 支持 | 需要启用 CDF | 不启用 CDF 无法流式读取 |
| Open Sharing 限制 | 只能共享 Delta Tables | D2D 可共享 Volumes/Models/Notebooks |
| 数据修改 | 接收方永远只读 | SELECT 权限不等于可修改 |
| 分区 vs 非分区 | D2D 中非分区表性能更优 | 分区不一定提升 sharing 性能 |

---

## 自测清单

- [ ] Delta Sharing 创建 Share 需要什么级别的权限？（metastore admin 或 CREATE SHARE privilege）
- [ ] 接收方要进行 Time Travel 查询，共享时需要什么配置？（WITH HISTORY）
- [ ] 接收方要进行 Streaming Reads，需要提前做什么？（在源表上启用 CDF）
- [ ] Open Sharing 与 Databricks-to-Databricks 在可共享对象类型上有什么区别？（Open 只支持 Delta Tables）
- [ ] 接收方能否修改源数据？（不能，Delta Sharing 是只读协议）
- [ ] 在 D2D 共享中，分区表和非分区表哪个性能更优？（非分区表）
