# Unity Catalog 与权限管理

> 17 题 | 错 14 题 | 未答 3 题 (Q249, Q266, Q269) | 待补充分析 3 题 (Q287, Q291, Q303)
> 正确率：0/17 = 0%（全部做错或未答）
> **这是整个考试中最薄弱的领域，必须作为 P0 优先级突破。**

---

## 一、概念框架

### 1.1 权限检查链 — 最核心的心智模型

Unity Catalog 的权限是**层级式**的，每一层都是独立的检查点。要访问任何对象，必须从上到下逐层通过：

```
用户请求 SELECT catalog_a.schema_b.table_c
  -> 检查 1：用户是否有 catalog_a 的 USE CATALOG？ -> 否则拒绝
  -> 检查 2：用户是否有 schema_b 的 USE SCHEMA？ -> 否则拒绝
  -> 检查 3：用户是否有 table_c 的 SELECT？  -> 否则拒绝
  -> 全部通过 -> 执行查询
```

**关键推论**：
- 没有 USE CATALOG，即使有表的 SELECT 也无法访问
- 没有 USE SCHEMA，即使有表的 SELECT 也无法访问
- OWNERSHIP 不自动继承下层对象的权限（schema owner 不自动有 table 的 SELECT）

### 1.2 关键权限对比表

| 权限 | 作用 | 适用对象 | 备注 |
|------|------|---------|------|
| USE CATALOG | 允许访问 catalog 下的 schema | Catalog | 门卫权限，不含读写能力 |
| USE SCHEMA | 允许访问 schema 下的对象 | Schema | 门卫权限，不含读写能力 |
| SELECT | 读取表/视图数据 | Table/View | 只读 |
| MODIFY | 写入表数据（INSERT/UPDATE/DELETE） | Table | 不含 SELECT |
| CREATE TABLE/SCHEMA | 在父对象下创建子对象 | Schema/Catalog | |
| ALL PRIVILEGES | 所有权限（不含 OWNERSHIP） | 任意对象 | 不能 GRANT 给他人 |
| MANAGE | 管理权限（可以 GRANT 给他人） | 任意对象 | 委托治理用 |
| OWNERSHIP | 完全控制（含删除、转移所有权） | 任意对象 | 只能是单个用户或组 |

### 1.3 Managed vs External Table

| 特性 | Managed Table | External Table |
|------|--------------|----------------|
| 数据位置 | UC 管理的存储 | 用户指定的 External Location |
| DROP TABLE | 删除元数据 + 数据文件 | 只删除元数据，数据文件保留 |
| 创建方式 | 不指定 LOCATION | 指定 LOCATION |
| Predictive Optimization | 支持 | 不支持 |
| Liquid Clustering 新数据 | OPTIMIZE 时聚簇 | OPTIMIZE 时聚簇 |

### 1.4 Workspace Catalog 默认权限

当 workspace 自动启用 Unity Catalog 时：
- 新用户自动获得 **USE CATALOG** 权限
- 新用户自动获得 **default schema** 上的特定权限（USE SCHEMA + CREATE TABLE 等）
- 新用户**不**自动获得其他 schema 的任何权限 -> 需要管理员显式 GRANT

### 1.5 Secrets 权限模型

- 权限粒度：scope 级别（不是 key 级别）
- 权限层级：MANAGE > WRITE > READ
- 最小权限原则 -> 使用 READ 权限 + 独立 scope

---

## 二、错题精析

### A. 权限层级与最小权限原则（6 题）

---

#### Q2 -- 集群最小权限

**QUESTION 2**
The Databricks workspace administrator has configured interactive clusters for each of the data engineering groups. To control costs, clusters are set to terminate after 30 minutes of inactivity. Each user should be able to execute workloads against their assigned clusters at any time of the day.

Assuming users have been added to a workspace but not granted any permissions, which of the following describes the minimal permissions a user would need to start and attach to an already configured cluster.

A. "Can Manage" privileges on the required cluster
B. Workspace Admin privileges, cluster creation allowed, "Can Attach To" privileges on the required cluster
C. Cluster creation allowed, "Can Attach To" privileges on the required cluster
D. "Can Restart" privileges on the required cluster
E. Cluster creation allowed, "Can Restart" privileges on the required cluster

**我的答案：** B | **正确答案：** D

**解析：**
- Can Attach To：只能 attach 到**已运行**的集群 -> 集群会自动关闭后无法使用，权限不够
- Can Restart：可以启动/重启集群 + 隐含 attach 权限 -> 刚好够用
- Can Manage：完全控制 -> 权限过多
- 选 B 过度：不需要 Workspace Admin，也不需要 cluster creation（集群已经配置好了）
- **"minimal permissions" 是考试高频信号词**，永远选权限最小但刚好满足需求的选项

**错因：** 选了过度权限方案，没有理解集群权限层级 Can Attach To < Can Restart < Can Manage

---

#### Q84 -- GRANT USAGE + SELECT 仅赋予只读权限

**QUESTION 84**
The data architect has decided that once data has been ingested from external sources into the Databricks Lakehouse, table access controls will be leveraged to manage permissions for all production tables and views.

The following logic was executed to grant privileges for interactive queries on a production database to the core engineering group.

```sql
GRANT USAGE ON DATABASE prod TO eng;
GRANT SELECT ON DATABASE prod TO eng;
```

Assuming these are the only privileges that have been granted to the eng group along with permission on the catalog, and that these users are not workspace administrators, which statement describes their privileges?

A. Group members have full permissions on the prod database and can also assign permissions to other users or groups.
B. Group members are able to list all tables in the prod database but are not able to see the results of any queries on those tables.
C. Group members are able to query and modify all tables and views in the prod database, but cannot create new tables or views.
D. Group members are able to query all tables and views in the prod database, but cannot create or edit anything in the database.
E. Group members are able to create, query, and modify all tables and views in the prod database, but cannot define custom functions.

**我的答案：** C | **正确答案：** D

**解析：**
- USAGE（即 USE SCHEMA）= 允许访问数据库内的对象（门卫权限）
- SELECT = 允许读取表和视图（只读）
- 两者组合 = 只读查询能力，**不包含任何写入或创建能力**
- 选 C 错误：SELECT 不包含 MODIFY（INSERT/UPDATE/DELETE），没有 MODIFY 就不能修改数据
- 要修改数据需要 MODIFY 权限，要创建对象需要 CREATE 权限

**错因：** 混淆了 SELECT 和 MODIFY 的边界，以为 SELECT 隐含部分写入能力

---

#### Q100 / Q167 -- Secrets 最小权限（重复题，两次都错）

**QUESTION 100 / QUESTION 167**
The data engineering team has been tasked with configuring connections to an external database that does not have a supported native connector with Databricks. The external database already has data security configured by group membership. These groups map directly to user groups already created in Databricks that represent various teams within the company.

A new login credential has been created for each group in the external database. The Databricks Utilities Secrets module will be used to make these credentials available to Databricks users.

Assuming that all the credentials are configured correctly on the external database and group membership is properly configured on Databricks, which statement describes how teams can be granted the minimum necessary access to using these credentials?

A. "Manage" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
B. "Read" permissions should be set on a secret key mapped to those credentials that will be used by a given team.
C. "Read" permissions should be set on a secret scope containing only those credentials that will be used by a given team.
D. "Manage" permissions should be set on a secret scope containing only those credentials that will be used by a given team.
E. No additional configuration is necessary as long as all users are configured as administrators in the workspace where secrets have been added.

**Q100 我的答案：** D | **Q167 我的答案：** B | **正确答案：** C

**解析：**
- Databricks Secrets ACL 的权限粒度是 **scope 级别**，不是 key 级别 -> 排除 A、B
- 最小权限原则：只需要 Read（读取 secret 值），不需要 Manage（Manage 允许管理 scope 的 ACL，权限过大）-> 排除 D
- 正确做法：每个团队有独立的 secret scope，只包含该团队的凭证，对该 scope 授予 Read 权限

**错因：**
- Q100：选了 Manage（过度权限）
- Q167：选了 key 级别（粒度错误）
- **同一道题做了两次，两次错法不同，说明没有建立稳固的知识结构**

**必须记住的两个要点：**
1. Secret 权限只能设在 scope 级别
2. 最小权限 = Read，不是 Manage

---

#### Q291 [PENDING] -- USE CATALOG + USE SCHEMA 是最小权限方案

**QUESTION 291**
When a new Databricks project starts, the central IT team provisions the required infrastructure using Terraform and a Service Principal. This includes creating a Databricks workspace, a Unity Catalog linked to an External Location, and a Databricks group containing all project team members. Project teams must store all assets -- e.g., tables and volumes, as Managed assets in Unity Catalog. This model hides infrastructure complexity while giving teams autonomy within their catalog. They can create and manage schemas, tables, volumes, and related objects but cannot rename, delete, or change catalog permissions, those remain under IT's control.

Which rights should the project group be granted to enable this model?

A. The group needs to have USE CATALOG and USE SCHEMA on the catalog.
B. The group needs to have ALL PRIVILEGES and the MANAGE on the catalog.
C. The group needs to have ALL PRIVILEGES on the catalog.
D. The group should be made OWNER of the catalog.

**我的答案：** C | **正确答案：** A

**解析：**
- 题目要求：团队可以创建和管理 schemas、tables、volumes，但**不能**重命名/删除 catalog 或修改 catalog 级别的权限
- USE CATALOG + USE SCHEMA 让团队可以在 catalog 内工作
- ALL PRIVILEGES（选项 C）包含了 catalog 级别的管理权限，允许删除 schema 等操作，违反了"IT 保留控制"的要求
- MANAGE（选项 B）允许 GRANT 权限给他人，更是过度
- OWNER（选项 D）是完全控制，权限最大

**但这里有一个疑点**：仅有 USE CATALOG + USE SCHEMA 是否足够让团队"create and manage schemas, tables, volumes"？按照权限模型，创建对象需要 CREATE TABLE/CREATE SCHEMA 等权限。官方答案 A 的解释是 USE CATALOG + USE SCHEMA 足以让团队在 catalog 内自主工作，可能隐含了其他默认权限或题目简化了场景。

**考试策略**：遇到"最小权限"题，永远选权限最小且刚好满足需求的选项。如果不确定，宁可选太小也不选太大。

---

#### Q303 [PENDING] -- MANAGE 权限用于委托治理

**QUESTION 303**
A workspace admin has created a new catalog called 'finance_data' and wants to delegate permission management to a finance team lead without giving them full admin rights.

Which privilege should be granted to the finance team lead?

A. GRANT_OPTION privilege on the finance_data catalog.
B. Make the finance team lead a metastore admin.
C. MANAGE privilege on the finance_data catalog.
D. ALL PRIVILEGES on the finance_data catalog.

**我的答案：** D | **正确答案：** C

**解析：**
- 需求：委托权限管理，但不给完全管理员权限
- MANAGE 权限 = 可以管理该 catalog 上的权限（GRANT/REVOKE 给他人），是专为委托治理设计的权限
- ALL PRIVILEGES = 所有数据操作权限（SELECT/MODIFY/CREATE 等），但**不包含 GRANT 权限给他人的能力**，所以反而不满足需求
- Metastore admin = 全局管理员，权限过大
- GRANT_OPTION 不是 Unity Catalog 的独立权限名

**关键区分：**
| 权限 | 能做什么 | 不能做什么 |
|------|---------|-----------|
| ALL PRIVILEGES | 对对象执行所有操作 | 不能 GRANT 给他人 |
| MANAGE | 管理权限（GRANT/REVOKE） | 专注于治理，不是数据操作 |
| OWNERSHIP | 完全控制 | 过于强大 |

**错因：** 混淆了 ALL PRIVILEGES 和 MANAGE 的区别，以为 ALL PRIVILEGES 包含权限管理能力

---

### B. Schema/Database 隔离与组织（2 题）

---

#### Q33 -- 按数据质量层级分库实现权限隔离

**QUESTION 33**
The data engineering team is migrating an enterprise system with thousands of tables and views into the Lakehouse. They plan to implement the target architecture using a series of bronze, silver, and gold tables. Bronze tables will almost exclusively be used by production data engineering workloads, while silver tables will be used to support both data engineering and machine learning workloads. Gold tables will largely serve business intelligence and reporting purposes. While personal identifying information (PII) exists in all tiers of data, pseudonymization and anonymization rules are in place for all data at the silver and gold levels.

The organization is interested in reducing security concerns while maximizing the ability to collaborate across diverse teams.

Which statement exemplifies best practices for implementing this system?

A. Isolating tables in separate databases based on data quality tiers allows for easy permissions management through database ACLs and allows physical separation of default storage locations for managed tables.
B. Because databases on Databricks are merely a logical construct, choices around database organization do not impact security or discoverability in the Lakehouse.
C. Storing all production tables in a single database provides a unified view of all data assets available throughout the Lakehouse, simplifying discoverability by granting all users view privileges on this database.
D. Working in the default Databricks database provides the greatest security when working with managed tables, as these will be created in the DBFS root.
E. Because all tables must live in the same storage containers used for the database they're created in, organizations should be prepared to create between dozens and thousands of databases depending on their data isolation requirements.

**我的答案：** C | **正确答案：** A

**解析：**
- 按数据质量层级（bronze/silver/gold）分库 -> 通过数据库级别 ACL 统一管理权限
- 不同数据库可以指定不同的默认存储位置（LOCATION）-> 实现物理隔离
- Bronze 层有 PII 明文，限制只有工程团队访问；Silver/Gold 层已脱敏，可以开放给更多团队
- 选 C（所有表放一个库）-> 权限管理困难，PII 暴露风险高
- 选 B（数据库只是逻辑构造）-> 错误，数据库 ACL 是重要的安全机制

**错因：** 错误地认为统一视图优于安全隔离，忽视了 PII 数据保护的优先级

---

#### Q207 -- 视图维持向后兼容

**QUESTION 207**
To reduce storage and compute costs, the data engineering team has been tasked with curating a series of aggregate tables leveraged by business intelligence dashboards, customer-facing applications, production machine learning models, and ad hoc analytical queries.

The data engineering team has been made aware of new requirements from a customer-facing application, which is the only downstream workload they manage entirely. As a result, an aggregate table used by numerous teams across the organization will need to have a number of fields renamed, and additional fields will also be added.

Which of the solutions addresses the situation while minimally interrupting other teams in the organization without increasing the number of tables that need to be managed?

A. Send all users notice that the schema for the table will be changing; include in the communication the logic necessary to revert the new table schema to match historic queries.
B. Configure a new table with all the requisite fields and new names and use this as the source for the customer-facing application; create a view that maintains the original data schema and table name by aliasing select fields from the new table.
C. Create a new table with the required schema and new fields and use Delta Lake's deep clone functionality to sync up changes committed to one table to the corresponding table.
D. Replace the current table definition with a logical view defined with the query logic currently writing the aggregate table; create a new table to power the customer-facing application.

**我的答案：** C | **正确答案：** B

**解析：**
- 新建表满足客户应用的新需求（字段重命名 + 新增字段）
- 创建视图用旧名称和旧 schema（通过字段别名 alias）-> 其他团队继续使用原有查询，零中断
- 视图不算额外的"表"，不增加管理负担
- Deep clone（选项 C）是数据复制，不是 schema 变更同步，且需要分别维护两张表
- 选项 D 把现有表替换为视图会影响写入逻辑

**错因：** 不熟悉视图作为向后兼容层的标准模式

---

### C. Unity Catalog 特有权限行为（3 题）

---

#### Q230 -- Schema Owner 权限不自动继承表

**QUESTION 230**
A platform engineer is creating catalogs and schemas for the development team to use. The engineer has created an initial catalog, Catalog_A, and initial schema, Schema_A. The engineer has also granted USE CATALOG, USE SCHEMA, and CREATE TABLE to the development team so that the engineer can begin populating the schema with new tables.

Despite being owner of the catalog and schema, the engineer noticed that they do not have access to the underlying tables in Schema_A.

What explains the engineer's lack of access to the underlying tables?

A. The owner of the schema does not automatically have permission to tables within the schema, but can grant them to themselves at any point.
B. Users granted with USE CATALOG can modify the owner's permissions to downstream tables.
C. Permissions explicitly given by the table creator are the only way the Platform Engineer could access the underlying tables in their schema.
D. The platform engineer needs to execute a REFRESH statement as the table permissions did not automatically update for owners.

**我的答案：** C | **正确答案：** A

**解析：**
- Unity Catalog 中，**ownership 不自动继承下层对象的权限**
- Schema owner 不自动拥有 schema 内表的 SELECT/MODIFY 权限
- 但 schema owner 可以随时给自己授权（因为拥有 schema 的 OWNERSHIP）
- 选 C 说"只有表创建者显式授权才能访问"-> 错误，schema owner 可以自行授权，不依赖表创建者

**核心原则：** UC 中权限是显式的、非继承的。OWNERSHIP 赋予的是"可以自行授权"的能力，而不是"自动拥有所有权限"。

---

#### Q259 -- Workspace Catalog 默认权限范围

**QUESTION 259**
A data engineering workspace was automatically enabled for Unity Catalog, creating a workspace catalog. New team members report they can create tables in the default schema but cannot access table in other schemas within the same workspace catalog.

Why are the new team members unable to access tables in other schemas?

A. Workspace catalog permissions are not subject to inheritance rules.
B. Workspace users receive USE CATALOG and specific privileges on default schema only.
C. Tables in other schemas require additional BROWSE privileges that new users don't receive automatically
D. New users only receive CREATE TABLE privileges on the default schema.

**我的答案：** C | **正确答案：** B

**解析：**
- Workspace 自动启用 UC 时，新用户默认获得：
  - USE CATALOG 权限
  - default schema 上的特定权限（USE SCHEMA + CREATE TABLE 等）
- **其他 schema 没有自动授权** -> 需要管理员显式 GRANT USE SCHEMA
- 选 C 提到的 BROWSE 权限不是核心问题，关键是缺少 USE SCHEMA

**错因：** 不了解 workspace catalog 的默认权限范围

---

#### Q82 -- Job Owner 只能是单个用户

**QUESTION 82**
A junior data engineer has manually configured a series of jobs using the Databricks Jobs UI. Upon reviewing their work, the engineer realizes that they are listed as the "Owner" for each job. They attempt to transfer "Owner" privileges to the "DevOps" group, but cannot successfully accomplish this task.

Which statement explains what is preventing this privilege transfer?

A. Databricks jobs must have exactly one owner; "Owner" privileges cannot be assigned to a group.
B. The creator of a Databricks job will always have "Owner" privileges; this configuration cannot be changed.
C. Other than the default "admins" group, only individual users can be granted privileges on jobs.
D. A user can only transfer job ownership to a group if they are also a member of that group.
E. Only workspace administrators can grant "Owner" privileges to a group.

**我的答案：** B | **正确答案：** A

**解析：**
- Databricks Jobs 的 Owner 必须是**单个用户**，不能是群组（group）
- Job ownership **可以转让**（转给另一个用户），但不能转给群组
- 选 B 错误：ownership 是可以改变的，只是不能给 group

**错因：** 以为 ownership 不可变，实际上只是不能给 group

---

### D. Predictive Optimization 与 Managed Table（3 题）

---

#### Q266 -- Predictive Optimization 自动执行 OPTIMIZE + ANALYZE

**QUESTION 266**
Predictive Optimization is an automated Databricks service enabled by default for Unity Catalog Managed tables. It helps maintain Delta tables by continuously optimizing them to ensure optimal performance and costs.

Which two operations does Predictive Optimization run to maintain the Delta tables? (Choose two.)

A. PARTITION BY
B. COMPACT
C. ANALYZE
D. OPTIMIZE
E. BUCKETING

**我的答案：** X（未答） | **正确答案：** CD

**解析：**
- **OPTIMIZE**：合并小文件，改善数据布局（file compaction）
- **ANALYZE**：收集和刷新表统计信息（statistics collection）
- COMPACT 不是 Delta 的标准命令
- PARTITION BY 和 BUCKETING 是建表时的定义操作，不是维护操作
- Predictive Optimization 仅适用于 **Unity Catalog Managed Tables**，不适用于 External Tables

---

#### Q326 -- UC Managed Table + Predictive Optimization 是最优组合

**QUESTION 326**
A data engineer is designing a system to process batch patient encounter data stored in an S3 bucket, creating a Delta table (patient_encounters) with columns encounter_id, patient_id, encounter_date, diagnostic_code, and treatment_cost. The table is queried frequently by patient_id and encounter_date, requiring fast performance. Fine grained access controls must be enforced. The engineer wants to minimize maintenance and boost performance.

How should the data engineer create the patient_encounters table?

A. Create an external table in Unity Catalog, specifying an S3 location for the data files. Enable predictive optimization through table properties, and configure Unity Catalog permissions for access controls.
B. Create a managed table in Unity Catalog. Configure Unity Catalog permissions for access controls, schedule jobs to run OPTIMIZE and vacuum command daily to achieve best performance.
C. Create a managed table in Hive metastore. Configure Hive metastore permissions for access controls, and rely on predictive optimization to enhance query performance and simplify maintenance.
D. Create a managed table in Unity Catalog. Configure Unity Catalog permissions for access controls, and rely on predictive optimization to enhance query performance and simplify maintenance.

**我的答案：** C | **正确答案：** D

**解析：**
- 需求：细粒度访问控制 + 最小维护 + 高性能
- UC Managed Table：提供细粒度 ACL、集中治理
- Predictive Optimization：自动执行 OPTIMIZE/ANALYZE，无需手动调度
- 选 A 错误：External table 不支持 Predictive Optimization
- 选 B 错误：手动调度 OPTIMIZE/VACUUM 增加维护负担
- 选 C 错误：Hive metastore 不支持细粒度 ACL，且 Predictive Optimization 是 UC 功能

**决策链：** 细粒度 ACL -> 必须 UC（排除 C）-> 最小维护 -> Predictive Optimization -> 必须 Managed Table（排除 A）-> 不手动调度（排除 B）-> D

---

#### Q287 [PENDING] -- Liquid Clustering 新数据在 OPTIMIZE 时才聚簇

**QUESTION 287**
A data engineer is implementing liquid clustering on a Delta Lake table and needs to understand how it affects data management operations. The table will be updated frequently with new data. The table is an external table and not managed by Unity Catalog.

How does liquid clustering in Delta Lake handle new data that is inserted after the initial table creation?

A. New data is rejected if it doesn't match the clustering pattern.
B. New data is automatically clustered during write operations.
C. New data is written to a staging area and clustered during scheduled maintenance.
D. New data remains unclustered until the next OPTIMIZE operation.

**我的答案：** C | **正确答案：** D

**解析：**
- Liquid Clustering 的新数据写入行为：**写入时不聚簇**，数据直接追加
- 聚簇操作在后续 **OPTIMIZE** 时增量执行，重新组织新旧数据的布局
- 不存在"staging area"或"scheduled maintenance"的概念（排除 C）
- 新数据不会被拒绝（排除 A）
- 写入操作时不会自动聚簇（排除 B，注意：部分写入模式如 CTAS 支持 cluster-on-write，但 INSERT INTO 不支持）

**与 Predictive Optimization 的关系：**
- 如果是 UC Managed Table，Predictive Optimization 会自动运行 OPTIMIZE -> 自动聚簇
- 如果是 External Table（如本题），需要手动调度 OPTIMIZE

---

### E. 列掩码与数据安全（1 题）

---

#### Q315 -- 基于映射表的动态列掩码

**QUESTION 315**
A data engineer needs to implement column masking for a sensitive column in a Unity Catalog-managed table. The masking logic must dynamically check if users belong to specific groups defined in a separate table (group_access) that maps groups to allowed departments.

Which approach should the engineer use to efficiently enforce this requirement?

A. Create a view without selecting the sensitive column
B. Apply a column mask that references the group_access mapping table in its UDF
C. Create a UDF that hardcodes allowed groups and apply it as a column mask.
D. Use a row filter to restrict access based on the user's group.

**我的答案：** D | **正确答案：** B

**解析：**
- 需求：**动态**列掩码，基于外部映射表判断用户组权限
- 列掩码（Column Mask）通过 SQL UDF 实现，UDF 可以引用外部表（group_access）
- 当映射表更新时，掩码逻辑自动生效，无需重新部署代码
- 选 D 错误：**Row Filter 控制行级访问**，不是列级脱敏。题目要求的是列掩码
- 选 C 错误：硬编码用户组不满足"动态"要求
- 选 A 错误：直接排除列过于粗暴，无法根据用户组灵活控制

**Column Mask vs Row Filter：**
| 功能 | Column Mask | Row Filter |
|------|------------|------------|
| 控制维度 | 列级 | 行级 |
| 实现方式 | UDF 返回脱敏后的值 | UDF 返回 boolean 过滤行 |
| 典型场景 | 敏感列按用户组脱敏 | 按部门/地区限制可见行 |

---

### F. DAB 与 Service Principal（1 题）

---

#### Q249 -- DAB 部署 App 和 Volume 权限

**QUESTION 249**
In a Databricks Asset Bundle project, in the file resources/app.yml, the data engineer would like to deploy a Databricks Apps databricks_app_deployed and Volume volume_deployed and grant the Service Principal behind Databricks Apps permissions to READ and WRITE to the Volume.

How should the data engineer achieve the deployment?

A. (正确配置：引用 deployed app 的 service principal，授予 Volume 级别的 READ/WRITE 权限)
B. (配置选项 B)
C. (配置选项 C)
D. (配置选项 D)

**我的答案：** X（未答） | **正确答案：** A

**解析：**
- 在 DAB 的 resources/app.yml 中，需要正确引用已部署 App 资源的 service principal 标识符
- 权限授予在 Volume 级别，使用 Volume 特定的 READ/WRITE 权限
- 确保 App 部署后可以安全访问 Volume

**注意：** 此题为代码配置题，PDF 中选项为代码块，需要理解 DAB YAML 配置语法。

---

### G. 系统表与监控（1 题）

---

#### Q269 -- SQL Warehouse 使用归因自动化报告

**QUESTION 269**
A platform team lead is responsible for automating the individual teams attribution towards SQL Warehouse usage. The requirement is to identify the SQL warehouse usage at the individual user's level and generate a daily report to be shared with an executive team that includes leaders from all business units.

How should the platform lead generate an automated report that can be shared daily?

A. Use the system tables to capture the audit and billing usage data and share the queries with the executive team. This enables the executives to execute the query and see the latest results any time.
B. Use the system tables to capture the audit and billing usage data and create a dashboard with daily refresh schedules and shared with the executive team.
C. Restrict users from running any SQL query unless they provide all the query details so that the attribution can be calculated and shared with the executive team.
D. Let the users run the SQL query and then directly report the usage to the executives. The ownership of the SQL warehouse usage will be with the individual teams.

**我的答案：** X（未答） | **正确答案：** B

**解析：**
- System tables 提供权威的审计和计费数据，可按用户级别归因 SQL Warehouse 使用量
- 创建 Dashboard + 每日定时刷新 = 自动化报告，高管无需手动执行查询
- 选 A 要求高管手动执行查询 -> 不满足"自动化"要求
- 关键词"automated" + "daily" -> Dashboard with scheduled refresh

---

## 三、核心对比表

### 3.1 权限层级速查

```
OWNERSHIP > ALL PRIVILEGES > MANAGE > 具体权限（SELECT/MODIFY/CREATE）

OWNERSHIP: 完全控制，含删除和转移所有权
ALL PRIVILEGES: 所有操作权限，但不能 GRANT 给他人
MANAGE: 专注于权限管理（GRANT/REVOKE），用于委托治理
```

### 3.2 集群权限层级

```
Can Manage > Can Restart > Can Attach To

Can Attach To: 只能 attach 到已运行的集群
Can Restart: 可以启动/重启 + 隐含 attach（最常用的"最小权限"答案）
Can Manage: 完全控制集群配置
```

### 3.3 Secrets 权限层级

```
MANAGE > WRITE > READ

粒度：scope 级别（不是 key 级别）
最小权限：Read on scope
```

### 3.4 Managed vs External Table 决策树

```
需要 Predictive Optimization？
  -> 是 -> Managed Table (UC)
  -> 否 -> 看数据管理需求

DROP TABLE 时要保留数据？
  -> 是 -> External Table
  -> 否 -> Managed Table

需要细粒度 ACL？
  -> 是 -> Unity Catalog（不是 Hive metastore）
```

### 3.5 Column Mask vs Row Filter vs View

| 方案 | 控制维度 | 动态性 | 适用场景 |
|------|---------|--------|---------|
| Column Mask (UDF) | 列级 | 可引用外部表，完全动态 | 按用户组脱敏敏感列 |
| Row Filter (UDF) | 行级 | 可引用外部表，完全动态 | 按部门/地区限制可见行 |
| View (不含敏感列) | 列级 | 静态 | 简单场景，完全隐藏列 |

---

## 四、自测清单

### 权限检查链
- [ ] 能否默写权限检查链的三个步骤？（USE CATALOG -> USE SCHEMA -> SELECT/MODIFY）
- [ ] Schema owner 自动拥有 schema 内表的 SELECT 权限吗？（否）
- [ ] Workspace catalog 新用户默认在哪些 schema 有权限？（仅 default schema）

### 关键权限区分
- [ ] ALL PRIVILEGES vs MANAGE：哪个可以 GRANT 权限给他人？（MANAGE）
- [ ] USAGE + SELECT 组合能做什么？（只读查询，不能修改或创建）
- [ ] 委托权限管理应该用什么权限？（MANAGE on catalog）

### Secrets
- [ ] Secret 权限的粒度是什么级别？（scope 级别）
- [ ] 最小权限应该选什么？（Read on scope）

### 集群权限
- [ ] 需要启动已终止集群的最小权限是什么？（Can Restart）
- [ ] Can Attach To 能启动集群吗？（不能）

### Managed vs External
- [ ] Predictive Optimization 支持 External Table 吗？（不支持）
- [ ] Predictive Optimization 自动执行哪两个操作？（OPTIMIZE + ANALYZE）
- [ ] Hive metastore 支持细粒度 ACL 吗？（不支持）
- [ ] Liquid Clustering 新插入的数据何时被聚簇？（下次 OPTIMIZE 时）

### Column Mask / Row Filter
- [ ] 动态列掩码应该用什么实现？（Column Mask UDF，可引用外部映射表）
- [ ] Row Filter 和 Column Mask 的区别？（行级 vs 列级）

### Job Ownership
- [ ] Job Owner 可以是 group 吗？（不可以，只能是单个用户）
- [ ] Job ownership 可以转让吗？（可以，但只能转给另一个用户）

### 架构最佳实践
- [ ] Bronze/Silver/Gold 应该放在同一个 database 还是分开？（分开，利用 database ACL 隔离）
- [ ] Schema 变更要向后兼容时应该怎么做？（新表 + 视图 alias 维持旧 schema）
- [ ] SQL Warehouse 使用量报告应该如何自动化？（System tables + Dashboard with scheduled refresh）
