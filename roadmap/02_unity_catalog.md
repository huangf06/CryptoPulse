# Unity Catalog 与权限管理 -- 深度复习辅导材料

> 来源：17 题全错（含 3 题未答），考试中最薄弱领域，P0 优先级。

---

## 一、错误模式诊断

### 1. "最小权限"判断力缺失（系统性问题，贯穿 6+ 题）

- **Q2**：把 Can Restart 和 Can Manage / Workspace Admin 混淆，选了过度权限方案。根本问题不是不知道这些权限存在，而是**缺少"权限层级排序"的心智模型**，无法快速定位"刚好够用"的那一档。
- **Q84**：知道 USAGE 和 SELECT 是什么，但**错误地认为 SELECT 隐含部分写入能力**，把 SELECT 和 MODIFY 的边界搞模糊了。
- **Q100/Q167**：同一道 Secrets 题做了两次，第一次选了过度权限（MANAGE），第二次选了错误粒度（key 级别）。两次错法不同，说明**没有把 "scope 级别 + READ" 这个组合固化为条件反射**。
- **Q291**：在"IT 保留 catalog 控制权"的约束下仍然选了 ALL PRIVILEGES，说明**读题时没有把约束条件转化为排除规则**。
- **Q303**：混淆 ALL PRIVILEGES 和 MANAGE。核心缺陷：**不理解 ALL PRIVILEGES 不含 GRANT 能力**，以为"all"就是"everything"。

**诊断**：不是知识量的问题，而是**决策框架**的问题。你知道各个权限的名字，但没有建立"权限从大到小排列 -> 匹配约束条件 -> 选最小的"这个思维程序。

### 2. Ownership 行为的错误心智模型（2 题）

- **Q230**：以为 Schema Owner 自动拥有 schema 内表的 SELECT 权限。实际上 UC 的 ownership **不继承下层权限**，owner 拥有的是"可以随时给自己授权"的能力，不是"自动拥有所有权限"。
- **Q82**：以为 Job Owner 不可变。实际上 ownership 可以转让给另一个用户，但不能转给 group。

**诊断**：对 ownership 的理解停留在"owner = 上帝"的粗糙模型，没有区分"拥有授权能力"与"自动拥有权限"的差异。

### 3. 列级 vs 行级安全机制混淆（1 题）

- **Q315**：题目明确说"column masking"，却选了 Row Filter。说明**Column Mask 和 Row Filter 在脑中没有清晰分区**，看到"access control"就往 Row Filter 上靠。

**诊断**：知道名词但不理解行为差异。缺少"Column Mask = 改值，Row Filter = 删行"的对偶记忆。

### 4. Managed Table vs External Table 的功能边界模糊（2 题）

- **Q326**：在需要 Predictive Optimization 的场景下选了 Hive metastore。说明**没有建立 "Predictive Optimization -> 必须 UC Managed Table" 的硬性关联**。
- **Q287**：把 Liquid Clustering 的写入行为理解为"staging area + scheduled maintenance"，实际是"直接追加，等下次 OPTIMIZE"。

**诊断**：对 Delta Lake 自动维护机制的工作流程缺少准确理解，用想象填补了知识空白。

### 5. 视图作为兼容层的设计模式盲区（1 题）

- **Q207**：不熟悉"新表 + 旧名视图 alias"这个经典的向后兼容模式，选了 Deep Clone（数据复制，不解决 schema 变更问题）。

**诊断**：缺少数据库 schema 演进的实战经验或案例积累。

### 6. Workspace Catalog 默认权限不了解（1 题）

- **Q259**：不知道 workspace 自动启用 UC 时，新用户只在 default schema 有权限。

**诊断**：纯知识盲点，需要记忆。

---

## 二、子主题分解与学习方法

### 子主题 A：UC 权限检查链与最小权限原则

**知识类型**：程序性 + 辨析性
**依赖**：无，这是 UC 的根基
**推荐学习方法**：快速问答 + 对比表 + 场景判断题

**核心对比表**：

| 权限层级 | 含义 | 包含下层？ | 能 GRANT？ |
|---------|------|-----------|-----------|
| OWNERSHIP | 完全控制（删除、转移所有权） | 不自动继承下层对象权限 | 可以 |
| ALL PRIVILEGES | 所有操作权限 | 不含 GRANT 能力 | 不可以 |
| MANAGE | 权限管理（GRANT/REVOKE） | 不含数据操作 | 可以（这就是它的用途） |
| SELECT | 只读 | 不含 MODIFY | -- |
| MODIFY | 写入（INSERT/UPDATE/DELETE） | 不含 SELECT | -- |
| USE CATALOG / USE SCHEMA | 门卫，允许访问下层 | 不含任何读写能力 | -- |

**访问检查链决策程序**：
```
1. 有 USE CATALOG？ -> 否 -> 拒绝（即使有表的 SELECT）
2. 有 USE SCHEMA？ -> 否 -> 拒绝（即使有表的 SELECT）
3. 有目标权限（SELECT/MODIFY/CREATE）？ -> 否 -> 拒绝
4. 全部通过 -> 执行
```

**"最小权限"题的解题程序**：
```
1. 列出所有选项的权限大小
2. 从约束条件中提取"不能做什么"（如"不能修改 catalog 权限"）
3. 排除所有过度权限的选项
4. 在剩余选项中选权限最小的
5. 如果不确定，宁选太小不选太大
```

---

### 子主题 B：Ownership 行为

**知识类型**：概念性（需要纠正错误心智模型）
**依赖**：子主题 A（权限检查链）
**推荐学习方法**：苏格拉底对话 + What-if 推理

**必须内化的两条规则**：
1. Schema/Catalog Owner **不自动拥有**下层对象的 SELECT/MODIFY 权限，但**可以随时给自己授权**。
2. Job Owner 必须是**单个用户**，不能是 group。可以转让给另一个用户。

---

### 子主题 C：Secrets 权限模型

**知识类型**：记忆性
**依赖**：子主题 A（最小权限原则）
**推荐学习方法**：Anki 卡片 + 快速问答

**两个硬知识点**：
1. 权限粒度 = **scope 级别**（不是 key 级别）
2. 最小权限 = **READ on scope**（不是 MANAGE，不是 WRITE）

层级：MANAGE > WRITE > READ

---

### 子主题 D：集群权限层级

**知识类型**：辨析性
**依赖**：无
**推荐学习方法**：对比表 + 场景判断题

| 权限 | 能做什么 | 不能做什么 |
|------|---------|-----------|
| Can Attach To | attach 到**已运行**的集群 | 不能启动已终止的集群 |
| Can Restart | 启动/重启集群 + 隐含 attach | 不能修改集群配置 |
| Can Manage | 完全控制（配置、启停、删除） | -- |

**高频考点**：集群会自动终止 -> Can Attach To 不够 -> 最小权限 = Can Restart。

---

### 子主题 E：Managed Table vs External Table

**知识类型**：辨析性 + 架构性
**依赖**：无
**推荐学习方法**：对比表 + 决策树

| 特性 | Managed Table (UC) | External Table |
|------|-------------------|----------------|
| 数据位置 | UC 管理 | 用户指定 External Location |
| DROP TABLE | 删元数据 + 删数据 | 只删元数据 |
| Predictive Optimization | 支持 | 不支持 |
| Liquid Clustering | 支持 | 支持（但需手动 OPTIMIZE） |
| 细粒度 ACL | UC 提供 | UC 提供（Hive metastore 不提供） |

**决策树**：
```
需要 Predictive Optimization？ -> 是 -> UC Managed Table
需要 DROP 后保留数据？ -> 是 -> External Table
需要细粒度 ACL？ -> 是 -> 必须 UC（排除 Hive metastore）
要最小维护？ -> UC Managed + Predictive Optimization
```

---

### 子主题 F：Predictive Optimization 与 Liquid Clustering

**知识类型**：概念性 + 程序性
**依赖**：子主题 E（Managed vs External）
**推荐学习方法**：What-if 推理

**Predictive Optimization**：
- 自动执行 **OPTIMIZE**（文件合并）和 **ANALYZE**（统计信息收集）
- 仅适用于 UC Managed Tables
- 如果启用，不需要手动调度 OPTIMIZE/VACUUM

**Liquid Clustering 写入行为**：
- 新数据写入时**不聚簇**，直接追加
- 聚簇在下次 **OPTIMIZE** 时增量执行
- 不存在 staging area 或 scheduled maintenance
- 对于 UC Managed Table + Predictive Optimization，OPTIMIZE 自动执行 -> 聚簇自动发生
- 对于 External Table，需要手动调度 OPTIMIZE

---

### 子主题 G：Column Mask vs Row Filter

**知识类型**：辨析性
**依赖**：无
**推荐学习方法**：对比表 + 场景判断题

| 机制 | 控制维度 | 实现方式 | 效果 |
|------|---------|---------|------|
| Column Mask | 列级 | SQL UDF，返回脱敏后的值 | 用户看到的敏感列被替换（如 "***"） |
| Row Filter | 行级 | SQL UDF，返回 boolean | 不满足条件的行被过滤掉 |
| View（不含敏感列） | 列级 | 静态排除 | 列完全不可见 |

**关键**：Column Mask UDF 可以引用外部映射表 -> 动态脱敏。

---

### 子主题 H：Workspace Catalog 默认权限

**知识类型**：记忆性
**依赖**：子主题 A
**推荐学习方法**：Anki 卡片

Workspace 自动启用 UC 时，新用户默认获得：
- USE CATALOG 权限
- **仅 default schema** 上的 USE SCHEMA + CREATE TABLE 等权限
- 其他 schema：无权限，需管理员显式 GRANT

---

### 子主题 I：视图作为向后兼容层

**知识类型**：架构性
**依赖**：无
**推荐学习方法**：Case Study

**标准模式**：
```
需求：表 schema 要变（字段重命名 + 新增字段），但不能中断其他团队
方案：
1. 新建表（新 schema），作为数据源
2. 创建视图，用旧表名 + 字段 alias 映射回旧 schema
3. 其他团队的查询继续工作，零中断
4. 视图不算额外的"表"，不增加管理负担
```

---

### 子主题 J：System Tables 与自动化报告

**知识类型**：程序性
**依赖**：无
**推荐学习方法**：快速问答

- System tables 提供审计和计费数据
- 自动化报告 = Dashboard + scheduled refresh（不是让用户手动执行查询）
- 关键词"automated" + "daily" -> Dashboard with daily refresh schedule

---

### 子主题 K：DAB 配置与 Service Principal

**知识类型**：程序性
**依赖**：无
**推荐学习方法**：实操（阅读 DAB YAML 配置文档）

- 在 resources/app.yml 中引用 deployed app 的 service principal
- 权限授予在 Volume 级别（READ/WRITE）
- 需要理解 DAB YAML 的语法结构

---

## 三、苏格拉底式教学问题集

### 子主题 A：权限检查链与最小权限

1. 如果一个用户拥有 table_x 的 SELECT 权限，但没有 USE CATALOG 权限，他执行 `SELECT * FROM catalog_a.schema_b.table_x` 会发生什么？为什么？
2. ALL PRIVILEGES 看起来是"所有权限"，为什么它不能替代 MANAGE 用于委托治理场景？具体来说，ALL PRIVILEGES 缺少什么能力？
3. 一个管理员想让团队"在 catalog 内自主工作，但不能修改 catalog 级别的权限"。为什么 USE CATALOG + USE SCHEMA 比 ALL PRIVILEGES 更合适？ALL PRIVILEGES 多了哪些不该给的东西？
4. 考试中遇到"minimal permissions"这个信号词时，你的决策程序应该是什么？能否用三步描述？
5. USAGE + SELECT 组合具体能做什么、不能做什么？为什么不能修改数据？需要什么额外权限才能修改？

### 子主题 B：Ownership 行为

1. 一个工程师创建了 Schema_A 并成为 owner。另一个工程师在 Schema_A 内创建了 Table_X。Schema_A 的 owner 能直接 SELECT Table_X 吗？为什么？
2. 如果 Schema Owner 不自动拥有表的 SELECT 权限，那 ownership 的实际价值是什么？它赋予了什么"能力"而不是什么"权限"？
3. 为什么 Job Owner 不能是 group？这和 Unity Catalog 中 OWNERSHIP 只能是单个用户或组有什么区别？

### 子主题 C：Secrets 权限

1. 为什么 Databricks Secrets 的权限粒度是 scope 级别而不是 key 级别？这对权限设计有什么影响？
2. 一个团队需要使用 Secret 中存储的数据库密码来连接外部系统。应该授予 READ、WRITE 还是 MANAGE 权限？为什么其他两个都过度？
3. 如果两个团队需要访问不同的凭证集，正确的做法是什么？为什么不能把所有凭证放在一个 scope 里？

### 子主题 D：集群权限

1. 集群配置了 30 分钟自动终止。用户需要随时能使用集群。Can Attach To 为什么不够？
2. Can Restart 和 Can Manage 之间的差异是什么？在什么场景下必须选 Can Manage？
3. 题目说"集群已经配置好了"，为什么不需要 cluster creation 权限？

### 子主题 E/F：Managed Table、Predictive Optimization、Liquid Clustering

1. Predictive Optimization 自动执行哪两个操作？为什么 COMPACT 不是正确答案？
2. 如果表是 External Table，Predictive Optimization 能否工作？为什么？
3. Liquid Clustering 在 INSERT INTO 时会不会自动聚簇新数据？数据什么时候才会被聚簇？
4. 一个场景需要"细粒度 ACL + 最小维护 + 高性能"，为什么 Hive metastore 被排除？为什么手动调度 OPTIMIZE 被排除？最终答案的逻辑链是什么？

### 子主题 G：Column Mask vs Row Filter

1. Column Mask 和 Row Filter 的核心区别是什么？各自的 UDF 返回什么类型的值？
2. 题目说"dynamically check if users belong to specific groups defined in a separate table"。为什么 Row Filter 不是正确答案？
3. Column Mask UDF 如何实现"动态"？为什么硬编码用户组的 UDF 不满足需求？

---

## 四、场景判断题

### 子主题 A：权限检查链与最小权限

**A1.** 一个数据分析师需要查询 `analytics_catalog.sales_schema.revenue_table`。管理员执行了以下 GRANT：
```sql
GRANT SELECT ON TABLE analytics_catalog.sales_schema.revenue_table TO analyst_group;
GRANT USE SCHEMA ON SCHEMA analytics_catalog.sales_schema TO analyst_group;
```
分析师执行查询时仍然失败。最可能的原因是什么？

A. SELECT 权限不足以执行查询，还需要 MODIFY
B. 缺少 USE CATALOG 权限
C. 需要先执行 REFRESH 命令更新权限缓存
D. 分析师需要 OWNERSHIP 权限才能查询

**正确答案：B**
解析：权限检查链的第一步是 USE CATALOG。即使有 USE SCHEMA 和 SELECT，缺少 USE CATALOG 就无法访问 catalog 下的任何对象。

---

**A2.** CTO 要求数据治理团队的 lead 能够管理 `finance_catalog` 的权限（GRANT/REVOKE），但不能自己执行数据查询或修改。应该授予什么权限？

A. ALL PRIVILEGES on finance_catalog
B. OWNERSHIP of finance_catalog
C. MANAGE on finance_catalog
D. SELECT + MODIFY on finance_catalog

**正确答案：C**
解析：MANAGE 专为委托治理设计，允许 GRANT/REVOKE 权限给他人，但不含 SELECT/MODIFY 等数据操作权限。ALL PRIVILEGES 反而包含数据操作但不含 GRANT 能力，完全不满足需求。

---

**A3.** 管理员执行了以下操作：
```sql
GRANT USE CATALOG ON CATALOG prod TO data_team;
GRANT USE SCHEMA ON SCHEMA prod.reporting TO data_team;
GRANT SELECT ON SCHEMA prod.reporting TO data_team;
GRANT MODIFY ON TABLE prod.reporting.metrics TO data_team;
```
data_team 对 `prod.reporting.metrics` 表能做什么？

A. 只能查询，不能修改
B. 可以查询和修改（INSERT/UPDATE/DELETE）
C. 只能修改，不能查询
D. 可以查询、修改和创建新表

**正确答案：B**
解析：USE CATALOG + USE SCHEMA 打通了访问路径。SELECT on SCHEMA 赋予了 schema 内所有表的读取权限。MODIFY on TABLE 赋予了该特定表的写入权限。SELECT + MODIFY = 可读可写。注意 MODIFY 不含 SELECT，两者是独立权限，但这里两者都被授予了。

---

### 子主题 B：Ownership

**B1.** 工程师 Alice 是 Schema_X 的 owner。工程师 Bob 在 Schema_X 中创建了 Table_Y。Alice 尝试 `SELECT * FROM Table_Y` 失败了。Alice 应该怎么做？

A. 联系 metastore admin 授予权限
B. 自己执行 `GRANT SELECT ON TABLE Table_Y TO alice`（因为她是 schema owner，有权这样做）
C. 执行 REFRESH 命令刷新权限
D. 重新创建 Schema_X 以继承表权限

**正确答案：B**
解析：Schema owner 不自动拥有 schema 内表的 SELECT 权限，但可以随时给自己授权。这是 UC 中 ownership 的核心含义："可以自行授权"的能力，不是"自动拥有所有权限"。

---

**B2.** 初级工程师创建了 5 个 Jobs，想把 ownership 全部转给 DevOps 团队（一个 Databricks group）。操作失败了。为什么？

A. 初级工程师没有转让 ownership 的权限
B. 需要 workspace admin 批准转让
C. Job Owner 必须是单个用户，不能是 group
D. 一次只能转让一个 Job 的 ownership

**正确答案：C**
解析：Databricks Jobs 的 Owner 只能是单个用户。ownership 可以转让，但只能转给另一个用户，不能转给 group。

---

### 子主题 C：Secrets

**C1.** 公司有三个团队（Engineering、Analytics、ML），每个团队需要访问不同的外部数据库凭证。为了满足最小权限原则，应该如何配置 Databricks Secrets？

A. 创建一个 scope，存放所有凭证，对所有团队授予 READ 权限
B. 创建三个 scope（每个团队一个），对每个团队授予对应 scope 的 MANAGE 权限
C. 创建三个 scope（每个团队一个），对每个团队授予对应 scope 的 READ 权限
D. 创建一个 scope，对每个 key 分别授予 READ 权限

**正确答案：C**
解析：Secret ACL 的粒度是 scope 级别（不是 key 级别，排除 D）。每个团队独立 scope 实现隔离（一个 scope 里混在一起则所有团队都能读所有凭证，排除 A）。最小权限 = READ，不需要 MANAGE（排除 B）。

---

### 子主题 D：集群权限

**D1.** 集群配置了自动终止策略（60 分钟不活动后终止）。数据科学家需要在周末运行长时间实验，集群可能在上次使用后已经终止。最小权限是什么？

A. Can Attach To
B. Can Restart
C. Can Manage
D. Cluster creation allowed + Can Attach To

**正确答案：B**
解析：集群可能已终止，Can Attach To 只能 attach 到已运行的集群（不够）。Can Restart 可以启动已终止的集群且隐含 attach 权限（刚好够）。Can Manage 过度。不需要 cluster creation（集群已存在）。

---

### 子主题 E/F：Managed Table 与 Predictive Optimization

**E1.** 数据工程团队正在设计新的分析平台。需求：(1) 细粒度访问控制 (2) 表删除后数据不保留 (3) 自动文件合并和统计信息收集 (4) 最小运维负担。应该选择什么方案？

A. Hive metastore Managed Table + 手动 OPTIMIZE
B. UC External Table + Predictive Optimization
C. UC Managed Table + Predictive Optimization
D. UC Managed Table + 手动调度 OPTIMIZE/VACUUM

**正确答案：C**
解析：细粒度 ACL -> 必须 UC（排除 A）。Predictive Optimization 不支持 External Table（排除 B）。自动维护 -> Predictive Optimization（排除 D 的手动调度）。UC Managed Table + Predictive Optimization 满足所有需求。

---

**E2.** 一张启用了 Liquid Clustering 的 External Table 刚执行了大量 INSERT INTO 操作。查询性能下降了。最可能的原因和解决方案是什么？

A. Liquid Clustering 已损坏，需要重新创建表
B. 新插入的数据未被聚簇，需要运行 OPTIMIZE
C. 需要运行 VACUUM 清理旧文件
D. 需要增加集群计算资源

**正确答案：B**
解析：Liquid Clustering 在 INSERT INTO 时不自动聚簇新数据，新数据直接追加。需要运行 OPTIMIZE 触发增量聚簇。对于 External Table，Predictive Optimization 不可用，必须手动（或调度）OPTIMIZE。

---

### 子主题 G：Column Mask vs Row Filter

**G1.** 医疗数据团队需要对 `patient_records` 表实现以下安全策略：研究团队看到的 SSN 列应显示为 "XXX-XX-XXXX"，而合规团队看到真实值。允许访问的团队列表存储在 `access_config` 表中，可能随时更新。应该用什么方案？

A. 创建两个视图，一个包含 SSN 列，一个不包含
B. 使用 Row Filter 基于用户组过滤行
C. 创建 Column Mask UDF，引用 access_config 表判断用户组，返回真实值或掩码值
D. 创建 Column Mask UDF，硬编码允许的用户组列表

**正确答案：C**
解析：需求是列级脱敏（不是行级过滤，排除 B）。需要动态判断用户组（不能硬编码，排除 D）。Column Mask UDF 可以引用外部表（access_config），当映射关系变化时自动生效。两个视图（A）虽然可行但不灵活，且增加管理负担。

---

### 子主题 I：视图作为兼容层

**I1.** 数据工程团队维护一张聚合表 `daily_metrics`，被 BI 团队、ML 团队和客户应用共同使用。客户应用需要把字段 `rev` 改名为 `revenue` 并新增 `margin` 字段。如何在不中断其他团队的前提下，不增加需要管理的表数量？

A. 通知所有团队 schema 变更，提供迁移脚本
B. 用 Deep Clone 创建副本表，分别维护
C. 新建表（含新 schema），创建视图用旧名 + 字段 alias 映射旧 schema
D. 直接修改现有表的 schema，依赖 Delta 的 schema evolution

**正确答案：C**
解析：新建表满足客户应用的新需求。视图用旧名 + alias 保持其他团队的查询不变。视图不算额外的"表"。Deep Clone 是数据复制，不解决 schema 映射问题且增加维护负担。直接修改 schema 会中断依赖旧字段名的查询。

---

## 五、关键知识网络

### 子主题间关联

```
权限检查链 (A)
  |-- Ownership 行为 (B)：ownership 不继承下层权限，但可自行授权
  |-- Secrets 权限 (C)：scope 级别粒度，READ 最小权限
  |-- 集群权限 (D)：独立权限体系，Can Restart 是高频答案
  |
  +-- 所有权限题共享同一个决策框架："列出层级 -> 匹配约束 -> 选最小"

Managed vs External Table (E)
  |-- Predictive Optimization (F)：仅 UC Managed Table
  |-- Liquid Clustering (F)：新数据等下次 OPTIMIZE
  |
  +-- Column Mask / Row Filter (G)：都需要 UC 提供细粒度 ACL

视图兼容层 (I)：独立设计模式，与权限无直接关系
System Tables (J)：独立知识点，与 Dashboard 自动化相关
DAB 配置 (K)：独立知识点，与 Service Principal 权限相关
```

### 与其他 Topic 的关联

- **Delta Lake / DLT (Topic 01)**：Liquid Clustering、OPTIMIZE、VACUUM 的行为在 Delta Lake 主题中也会考到。Predictive Optimization 是 UC + Delta Lake 的交叉点。
- **Data Governance (Topic 03, if exists)**：Column Mask、Row Filter、Secrets 管理属于数据治理范畴。
- **Workspace Administration (Topic 04, if exists)**：集群权限、workspace catalog 默认权限、Job ownership 属于 workspace 管理。
- **ETL / Data Pipeline (Topic 05, if exists)**：视图作为兼容层、schema evolution 在数据管道设计中常见。
- **Monitoring / Observability**：System tables 和 Dashboard 自动化报告是平台监控主题的内容。

---

## 六、Anki 卡片（Q/A 格式）

**Card 1**
Q: Unity Catalog 中，用户有表的 SELECT 权限但没有 USE CATALOG 权限，能否查询该表？
A: 不能。权限检查链要求先通过 USE CATALOG，再通过 USE SCHEMA，最后检查 SELECT。任一环节缺失都会被拒绝。

**Card 2**
Q: ALL PRIVILEGES 和 MANAGE 的核心区别是什么？
A: ALL PRIVILEGES 包含所有数据操作权限（SELECT/MODIFY/CREATE 等）但不能 GRANT 给他人。MANAGE 专注于权限管理（GRANT/REVOKE），用于委托治理，但不含数据操作权限。

**Card 3**
Q: Schema Owner 能否直接 SELECT schema 内其他人创建的表？
A: 不能。UC 中 ownership 不自动继承下层对象权限。但 Schema Owner 可以随时给自己 GRANT SELECT 权限。

**Card 4**
Q: Databricks Secrets ACL 的权限粒度是什么级别？最小权限选什么？
A: Scope 级别（不是 key 级别）。最小权限选 READ on scope。

**Card 5**
Q: 集群配置了自动终止策略，用户需要随时使用已配置好的集群，最小权限是什么？
A: Can Restart。Can Attach To 只能 attach 到已运行的集群，无法启动已终止的集群。Can Restart 包含启动 + attach 能力。

**Card 6**
Q: Predictive Optimization 自动执行哪两个操作？适用于什么类型的表？
A: OPTIMIZE（文件合并）和 ANALYZE（统计信息收集）。仅适用于 Unity Catalog Managed Tables，不适用于 External Tables。

**Card 7**
Q: Liquid Clustering 在 INSERT INTO 后，新数据何时被聚簇？
A: 新数据在写入时不被聚簇，直接追加。在下次 OPTIMIZE 操作时增量聚簇。

**Card 8**
Q: Column Mask 和 Row Filter 的区别是什么？
A: Column Mask 控制列级脱敏，UDF 返回脱敏后的值（如 "XXX"）。Row Filter 控制行级过滤，UDF 返回 boolean 决定行是否可见。

**Card 9**
Q: Workspace 自动启用 Unity Catalog 后，新用户在哪些 schema 有权限？
A: 仅 default schema。新用户获得 USE CATALOG + default schema 上的 USE SCHEMA/CREATE TABLE 等权限。其他 schema 需管理员显式 GRANT。

**Card 10**
Q: Job Owner 可以是 group 吗？Job ownership 可以转让吗？
A: 不可以是 group，只能是单个用户。可以转让，但只能转给另一个用户。

**Card 11**
Q: 表 schema 需要变更（字段重命名 + 新增字段），如何在不中断其他团队查询的前提下实现？
A: 新建表（新 schema），创建视图用旧表名 + 字段 alias 映射旧 schema。视图不算额外的"表"，其他团队零中断。

**Card 12**
Q: SQL Warehouse 使用量归因报告如何实现自动化？
A: 使用 System tables 获取审计和计费数据，创建 Dashboard 并配置 daily refresh schedule。不要求用户手动执行查询。

---

## 七、掌握度自测

**T1.** 管理员给 analytics_group 授予了 `USE CATALOG on catalog_x` 和 `SELECT on catalog_x.schema_y.table_z`，但没有授予 USE SCHEMA。analytics_group 能查询 table_z 吗？

A. 能，USE CATALOG 已包含 USE SCHEMA
B. 不能，缺少 USE SCHEMA 权限
C. 能，SELECT 权限自动绕过 USE SCHEMA 检查
D. 取决于是否是 Managed Table

**答案：B。** 权限检查链的三个步骤必须全部通过。USE CATALOG 不包含 USE SCHEMA。

---

**T2.** Catalog Owner 对 catalog 内所有表自动拥有哪些权限？

A. SELECT + MODIFY
B. ALL PRIVILEGES
C. 没有自动权限，但可以自行授权
D. 仅 SELECT

**答案：C。** UC 中 ownership 不自动继承下层对象权限。Catalog Owner 可以随时给自己 GRANT 任何权限。

---

**T3.** 以下哪个场景必须使用 UC Managed Table 而非 External Table？

A. 需要在 DROP TABLE 后保留数据文件
B. 需要 Predictive Optimization 自动维护
C. 需要在 S3 指定位置存储数据
D. 需要 Liquid Clustering

**答案：B。** Predictive Optimization 仅支持 UC Managed Tables。Liquid Clustering 两者都支持。

---

**T4.** 团队 lead 需要能够管理 `hr_catalog` 的权限（GRANT/REVOKE 给团队成员），但不应该能读取 HR 数据。应该授予什么？

A. ALL PRIVILEGES on hr_catalog
B. OWNERSHIP of hr_catalog
C. MANAGE on hr_catalog
D. SELECT + MANAGE on hr_catalog

**答案：C。** MANAGE 允许 GRANT/REVOKE 权限但不含数据操作（SELECT/MODIFY）。ALL PRIVILEGES 反而包含 SELECT 但不含 GRANT 能力。OWNERSHIP 权限过大。

---

**T5.** 一张 External Table 启用了 Liquid Clustering。数据工程师每小时执行 INSERT INTO 追加新数据，但从未手动运行 OPTIMIZE。以下哪个描述正确？

A. 新数据在每次 INSERT 时自动聚簇
B. 新数据未被聚簇，查询性能可能逐渐下降
C. Predictive Optimization 会自动执行 OPTIMIZE
D. 新数据被写入 staging area 等待聚簇

**答案：B。** INSERT INTO 时新数据不聚簇（直接追加）。External Table 不支持 Predictive Optimization。没有手动 OPTIMIZE -> 新数据永远不会被聚簇 -> 性能逐渐下降。

---

**T6.** 数据安全团队需要对 `employee` 表的 `salary` 列实现动态脱敏：HR 组看到真实值，其他人看到 0。允许访问的组定义在 `access_rules` 表中。最佳方案是什么？

A. 创建两个视图，一个包含 salary 列，一个不包含
B. 对 salary 列应用 Column Mask UDF，UDF 内引用 access_rules 表
C. 使用 Row Filter 基于用户组过滤行
D. 对 salary 列应用 Column Mask UDF，UDF 内硬编码 HR 组名

**答案：B。** 需求是列级脱敏（Column Mask，非 Row Filter）。需要动态（引用外部表，非硬编码）。

---

**T7.** 工程师在 Databricks 中创建了 Secret Scope `team_alpha_creds`，里面有 3 个 key：`db_host`, `db_user`, `db_password`。要让 Team Alpha 成员只能读取这些凭证值（不能修改或管理 ACL），应该如何授权？

A. 对每个 key 分别授予 READ 权限
B. 对 scope `team_alpha_creds` 授予 READ 权限
C. 对 scope `team_alpha_creds` 授予 WRITE 权限
D. 对 scope `team_alpha_creds` 授予 MANAGE 权限

**答案：B。** Secret 权限粒度是 scope 级别（不是 key 级别，排除 A）。最小权限 = READ（排除 C、D）。

---

**T8.** 平台团队需要每天自动生成 SQL Warehouse 使用量报告并分享给管理层。以下哪个方案最合适？

A. 创建查询并分享给管理层，让他们每天手动执行
B. 使用 System tables 创建 Dashboard，配置每日定时刷新，分享给管理层
C. 让各团队自行报告使用量
D. 使用 Databricks CLI 导出日志并邮件发送

**答案：B。** "automated" + "daily" -> Dashboard + scheduled refresh。System tables 是权威数据源。手动执行不满足自动化要求。
