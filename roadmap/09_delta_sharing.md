# 09. Delta Sharing -- 深度复习辅导材料

**状态：3 题全错（0/3），严重薄弱**

---

## 一、错误模式诊断

### 1. 权限层级混淆（Q264）

你把 Delta Sharing 的权限管理错误地定位在 workspace 层面，选择了根本不存在的 "MANAGE SHARES" 权限。根本原因：没有建立 Unity Catalog 权限体系的层级模型 -- metastore > catalog > schema > table。Delta Sharing 的 Share 是 metastore 级别的对象，因此 CREATE SHARE 权限也在 metastore 级别授予。

**修正锚点：** Share 是 metastore-level securable，权限自然在 metastore 级别。

### 2. 共享模式能力边界模糊（Q299）

你选了 Open Sharing 性能更优，说明你没有建立两种模式的能力矩阵。Open Sharing 通过 REST API 传输数据，性能天然低于 Databricks-to-Databricks 的原生协议。更关键的是，Time Travel 和 Streaming Reads 只有 D2D 模式才支持。

**修正锚点：** Open Sharing = 最大兼容性 + 最小功能集；D2D = 最优性能 + 完整功能集。

### 3. 可共享对象类型差异不清（Q300）

你没有区分 Open Sharing 和 D2D 在可共享对象类型上的差异。Open Sharing 只能共享 Delta Tables，而 D2D 还能共享 Volumes、ML Models、Notebooks。

**修正锚点：** Open Sharing 的 "open" 是对接收方平台的开放，代价是功能受限。

### 总结模式

三道题错误的共同根源：**对 Delta Sharing 的架构层级缺乏系统理解**。你把它当作一个扁平的功能来记忆，而没有理解 Provider-Share-Recipient 模型中每个层级的约束条件。

---

## 二、子主题分解与学习方法

### 子主题 A：Provider / Recipient / Share 三元模型

**核心内容：**
- Provider 是数据所有方（拥有 Unity Catalog 的 workspace）
- Share 是权限容器，包含要共享的表/schema
- Recipient 是接收方身份的抽象
- 操作流程：创建 Share -> 添加表到 Share -> 创建 Recipient -> 授权 Recipient 访问 Share
- 接收方通过 `CREATE CATALOG ... USING SHARE ...` 挂载数据

**学习方法：** 手写一遍完整的 SQL 操作流程，从 CREATE SHARE 到接收方查询数据，不看笔记默写。

### 子主题 B：权限模型

**核心内容：**
- 创建 Share 需要 metastore admin 角色或 CREATE SHARE privilege
- CREATE SHARE 是 metastore 级别的权限
- 不存在 MANAGE SHARES、SHARE_ADMIN 等虚构权限
- GRANT SELECT ON SHARE 给 Recipient 授予读取权限
- 接收方只有只读权限，永远无法修改源数据

**学习方法：** 列出 Unity Catalog 中所有 metastore 级别的权限（CREATE CATALOG, CREATE SHARE, CREATE RECIPIENT 等），形成完整权限清单。

### 子主题 C：Open Sharing vs Databricks-to-Databricks

**核心内容：**

| 维度 | Databricks-to-Databricks | Open Sharing |
|------|--------------------------|--------------|
| 接收方 | 必须是 Databricks workspace | 任意平台 |
| 传输协议 | 原生协议 | REST API |
| Time Travel | 支持（WITH HISTORY） | 不支持 |
| Streaming Reads | 支持（需 CDF） | 不支持 |
| 可共享对象 | Tables, Volumes, Models, Notebooks | 仅 Delta Tables |
| 分区影响 | 非分区表性能更优 | N/A |

**学习方法：** 用一句话概括差异 -- "D2D 是全功能原生通道，Open Sharing 是最小公约数的跨平台通道"。然后逐项记忆 D2D 独有的功能。

### 子主题 D：WITH HISTORY / CDF / 分区优化

**核心内容：**
- WITH HISTORY：共享时指定，使接收方可以进行 Time Travel 查询
- CDF (Change Data Feed)：必须在源表上**提前启用**，才能让接收方进行 Streaming Reads
- 非分区表在 D2D 共享中性能更优：因为 D2D 协议通过 shared metadata 实现 predicate pushdown，不需要物理分区
- 最优配置公式：WITH HISTORY + CDF + 非分区 = D2D 最佳实践

**学习方法：** 把 Time Travel、Streaming、Performance 三个需求分别对应到 WITH HISTORY、CDF、无分区三个配置项，形成一一映射。

---

## 三、苏格拉底式教学问题集

### 子主题 A：三元模型

1. Share 作为 metastore-level securable 意味着什么？它的生命周期跨越多少个 workspace？
2. 如果一个 Provider 有三个 workspace，它们能共用同一个 Share 吗？为什么？
3. Recipient 和 User/Group 有什么本质区别？为什么需要单独的 Recipient 抽象？
4. 接收方执行 `CREATE CATALOG ... USING SHARE` 后，这个 catalog 里的数据存在哪里？是复制了一份吗？

### 子主题 B：权限模型

1. 为什么 CREATE SHARE 权限不能放在 catalog 级别？Share 和 Catalog 的关系是什么？
2. 如果一个用户有 CREATE SHARE 但没有某张表的 SELECT 权限，他能把这张表加到 Share 里吗？
3. Delta Sharing 是只读协议 -- 如果接收方需要写回数据给 Provider，应该用什么方案？
4. metastore admin 和 account admin 在 Delta Sharing 管理上有什么区别？

### 子主题 C：Open Sharing vs D2D

1. Open Sharing 为什么不支持 Time Travel？从协议层面解释原因。
2. 如果接收方是一个使用 Apache Spark（非 Databricks）的团队，你能用 D2D 模式吗？为什么？
3. Open Sharing 只能共享 Delta Tables，那 CSV 文件能通过 Delta Sharing 共享吗？如何处理？
4. 一个外部合作伙伴既需要 Delta Tables 又需要 ML Models，应该推荐什么方案？
5. Open Sharing 的 REST API 传输性能较低，有没有办法在不切换到 D2D 的情况下优化？

### 子主题 D：WITH HISTORY / CDF / 分区

1. 如果共享时不指定 WITH HISTORY，接收方尝试 `SELECT * FROM table VERSION AS OF 5` 会发生什么？
2. CDF 需要在共享前启用，如果共享后才启用 CDF，已经创建的 Share 会自动支持 Streaming 吗？
3. 为什么在 D2D 共享中非分区表反而性能更好？这和常规的 Delta 表分区优化逻辑矛盾吗？
4. WITH HISTORY 会增加多少存储开销？Provider 需要承担这些成本吗？

---

## 四、场景判断题

### 子主题 A：三元模型

**Q1.** 一个 Provider 想共享 `sales` schema 下的三张表给两个不同的外部合作伙伴，但两个合作伙伴应该看到不同的表子集。最佳实践是什么？

A. 创建一个 Share，把三张表都加进去，用 row-level security 控制访问  
B. 创建两个 Share，分别包含不同的表子集，创建两个 Recipient 分别授权  
C. 创建一个 Recipient，通过 GRANT 语句细粒度控制每张表的访问  
D. 使用 Dynamic Views 在一个 Share 中实现差异化访问

**答案：B**

**解析：** Share 是权限的最小授予单元 -- GRANT SELECT ON SHARE 是整体授予的，不能在 Share 内部对单张表差异化控制。因此需要创建多个 Share 来实现不同的访问策略。A 的 row-level security 不适用于 Share 级别的授权。C 中一个 Recipient 只能代表一个接收方。D 中 Dynamic Views 可以在 Share 中使用，但不是控制"不同合作伙伴看不同表"的正确方式。

---

**Q2.** 接收方执行 `CREATE CATALOG partner_data USING SHARE provider.sales_share` 后发现无法查询数据。最可能的原因是什么？

A. 接收方没有 USE CATALOG 权限  
B. Provider 没有执行 GRANT SELECT ON SHARE sales_share TO RECIPIENT partner  
C. 接收方的 workspace 没有绑定到同一个 metastore  
D. Share 中没有添加任何表

**答案：B**

**解析：** 创建 Share 和创建 Recipient 之后，还必须显式执行 GRANT SELECT ON SHARE ... TO RECIPIENT ... 来授权。这是最常被遗漏的步骤。A 中 USE CATALOG 是接收方自己 workspace 内的权限管理，与 Delta Sharing 授权无关。C 在 Open Sharing 场景下不需要同一 metastore。D 如果 Share 为空，CREATE CATALOG 仍然可以成功，只是查不到表。

---

### 子主题 B：权限模型

**Q3.** 公司的数据治理策略要求：只有数据管理员（Data Stewards）可以创建 Share，但他们不应该拥有 metastore admin 的全部权限。正确的做法是什么？

A. 给 Data Stewards 授予 workspace admin 角色  
B. 给 Data Stewards 授予 metastore 级别的 CREATE SHARE privilege  
C. 给 Data Stewards 授予 MANAGE SHARES permission  
D. 给 Data Stewards 授予 ALL PRIVILEGES on metastore

**答案：B**

**解析：** CREATE SHARE 是一个独立的 metastore 级别权限，可以单独授予，不需要给予完整的 metastore admin 角色。这符合最小权限原则。A 的 workspace admin 无法授予 metastore 级别的操作权限。C 的 MANAGE SHARES 是虚构的权限名称。D 的 ALL PRIVILEGES 权限过大，违反最小权限原则。

---

**Q4.** 一个 Recipient 被授予了 SELECT ON SHARE 权限。该 Recipient 能执行以下哪个操作？

A. 读取 Share 中的表数据  
B. 向 Share 中的表写入新数据  
C. 删除 Share 中的表  
D. 将 Share 转授给另一个 Recipient

**答案：A**

**解析：** Delta Sharing 是只读共享协议。SELECT ON SHARE 只允许读取数据，接收方无法写入、删除或转授。这是协议级别的限制，不是权限配置的问题。

---

### 子主题 C：Open Sharing vs D2D

**Q5.** 一个外部数据分析团队使用 Pandas 处理数据，需要接收你的 Delta 表。他们需要定期获取增量更新。最佳方案是什么？

A. 使用 Open Sharing + Streaming Reads  
B. 使用 D2D Sharing + CDF  
C. 使用 Open Sharing + 定期全量刷新  
D. 要求对方迁移到 Databricks 使用 D2D Sharing

**答案：C**

**解析：** 对方使用 Pandas 不是 Databricks 平台，只能用 Open Sharing。Open Sharing 不支持 Streaming Reads，因此无法使用增量流式读取。最实际的方案是定期全量刷新。A 自相矛盾 -- Open Sharing 不支持 Streaming。B 不可行 -- 对方不是 Databricks。D 虽然技术上最优，但不现实。

---

**Q6.** 你需要与外部合作伙伴共享一个 ML Model 和相关的训练数据表。对方也使用 Databricks。应该如何配置？

A. 创建一个 Share，使用 Open Sharing 协议共享 Model 和 Table  
B. 创建一个 Share，使用 D2D Sharing 共享 Model 和 Table  
C. 创建两个 Share，一个用 Open Sharing 共享 Table，一个用 D2D 共享 Model  
D. ML Models 无法通过 Delta Sharing 共享，需要单独导出

**答案：B**

**解析：** 对方使用 Databricks，可以使用 D2D 模式。D2D 模式支持共享 Tables、Volumes、Models 和 Notebooks，可以在一个 Share 中同时包含 Model 和 Table。A 错误 -- Open Sharing 不支持 Model 共享。C 不必要且 Open Sharing 仍然无法共享 Model。D 错误 -- D2D 模式支持 Model 共享。

---

### 子主题 D：WITH HISTORY / CDF / 分区

**Q7.** 配置 D2D 共享以支持接收方的 Time Travel 和 Streaming Reads 需求。以下哪个 SQL 序列是正确的？

A. `ALTER SHARE s ADD TABLE t`; 接收方直接使用 Time Travel  
B. `ALTER SHARE s ADD TABLE t WITH HISTORY`; 接收方启用 CDF 后进行 Streaming  
C. 先在源表启用 CDF；然后 `ALTER SHARE s ADD TABLE t WITH HISTORY`  
D. `ALTER SHARE s ADD TABLE t WITHOUT HISTORY`; 在 Share 级别启用 CDF

**答案：C**

**解析：** 正确顺序是 (1) 在源表上启用 CDF（`ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`），(2) 然后添加表到 Share 并指定 WITH HISTORY。CDF 是源表属性，必须在共享前配置。B 的顺序反了 -- CDF 不是接收方启用的。D 的 WITHOUT HISTORY 不支持 Time Travel，且 CDF 不是 Share 级别的设置。

---

**Q8.** 在 D2D 共享中，一个分区表和一个等大小的非分区表都被共享给接收方。接收方执行带过滤条件的查询时，哪个表性能更好？

A. 分区表，因为分区裁剪减少扫描量  
B. 非分区表，因为 D2D 协议对非分区表有 metadata 级别的优化  
C. 性能相同，D2D 协议会自动优化两种表  
D. 取决于接收方的 cluster 配置

**答案：B**

**解析：** 在 D2D 共享场景中，协议通过 shared metadata 实现 predicate pushdown，不需要依赖物理分区。非分区表避免了分区的额外开销（小文件问题、元数据管理等），反而性能更优。这与常规 Delta 表查询中"分区提升性能"的直觉不同，是考试的重要考点。

---

## 五、关键知识网络

```
Delta Sharing 架构
|
+-- 核心模型
|   +-- Provider（数据所有方）
|   +-- Share（metastore-level securable，权限容器）
|   +-- Recipient（接收方身份抽象）
|
+-- 权限体系
|   +-- metastore 级别
|   |   +-- CREATE SHARE（创建共享）
|   |   +-- CREATE RECIPIENT（创建接收方）
|   +-- Share 级别
|   |   +-- SELECT ON SHARE -> RECIPIENT（授权读取）
|   +-- 只读协议（接收方永远无法写入）
|
+-- 两种模式
|   +-- Databricks-to-Databricks（D2D）
|   |   +-- 原生协议，最优性能
|   |   +-- 支持：Tables + Volumes + Models + Notebooks
|   |   +-- 支持：Time Travel（需 WITH HISTORY）
|   |   +-- 支持：Streaming Reads（需 CDF）
|   |   +-- 非分区表性能更优
|   |
|   +-- Open Sharing
|       +-- REST API，跨平台兼容
|       +-- 仅支持：Delta Tables
|       +-- 不支持：Time Travel / Streaming
|
+-- 关键配置项
    +-- WITH HISTORY -> Time Travel
    +-- CDF (Change Data Feed) -> Streaming Reads
    +-- 非分区 -> D2D 最佳性能
    +-- 最优公式：WITH HISTORY + CDF + 非分区
```

**与其他主题的关联：**
- **Unity Catalog（权限体系）：** Delta Sharing 的权限管理完全依赖 Unity Catalog 的 metastore 层级
- **Change Data Feed (CDF)：** CDF 不仅用于 Delta Sharing 的 Streaming，也用于 Medallion 架构中的增量处理
- **Structured Streaming：** Streaming Reads 通过 Delta Sharing 实现跨组织的流式数据管道

---

## 六、Anki 卡片

**Q1:** 创建 Delta Share 需要什么权限？  
**A1:** 需要 metastore admin 角色或 metastore 级别的 CREATE SHARE privilege。

**Q2:** Delta Sharing 中 "MANAGE SHARES" 权限是否存在？  
**A2:** 不存在。这是虚构的权限名称，Unity Catalog 中没有该权限。

**Q3:** 接收方要进行 Time Travel 查询，Provider 共享时需要怎么做？  
**A3:** 使用 `ALTER SHARE ... ADD TABLE ... WITH HISTORY` 指定 WITH HISTORY。

**Q4:** 接收方要进行 Streaming Reads，Provider 需要提前做什么？  
**A4:** 在源表上启用 Change Data Feed (CDF)：`ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`。

**Q5:** Open Sharing 能共享哪些对象类型？  
**A5:** 只能共享 Delta Tables。Volumes、ML Models、Notebooks 只有 D2D 模式才能共享。

**Q6:** 在 D2D 共享中，分区表和非分区表哪个查询性能更优？  
**A6:** 非分区表。D2D 协议通过 shared metadata 实现 predicate pushdown，不依赖物理分区。

**Q7:** Delta Sharing 接收方能否修改源数据？  
**A7:** 不能。Delta Sharing 是只读协议，接收方只有读取权限，SELECT 权限不等于可修改。

**Q8:** 接收方如何挂载共享的数据？  
**A8:** 执行 `CREATE CATALOG <name> USING SHARE <provider>.<share_name>`，然后通过 catalog.schema.table 访问。

**Q9:** D2D 共享中支持 Streaming Reads 的完整配置步骤是什么？  
**A9:** (1) 在源表启用 CDF，(2) `ALTER SHARE ... ADD TABLE ... WITH HISTORY`。CDF 必须在共享前启用。

**Q10:** Open Sharing 和 D2D 的传输协议有什么区别？  
**A10:** Open Sharing 通过 REST API 传输，性能较低；D2D 使用 Databricks 原生协议，性能最优。

**Q11:** 一个使用 Apache Spark（非 Databricks）的团队能用 D2D 模式接收数据吗？  
**A11:** 不能。D2D 要求接收方也是 Databricks workspace。非 Databricks 平台只能用 Open Sharing。

**Q12:** WITH HISTORY + CDF + 非分区 这个组合的意义是什么？  
**A12:** 这是 D2D 共享的最优配置。WITH HISTORY 支持 Time Travel，CDF 支持 Streaming Reads，非分区表在 D2D 中性能最优。

---

## 七、掌握度自测

**1.** 一个数据工程师需要让外部合作伙伴（非 Databricks 用户）通过 Pandas 读取共享的 Delta 表。应该使用哪种共享模式？

A. Databricks-to-Databricks  
B. Open Sharing  
C. 两种都可以  
D. Delta Sharing 不支持 Pandas

<details><summary>答案</summary>B. Open Sharing 支持任意平台的接收方，包括 Pandas。</details>

---

**2.** 以下哪个权限名称在 Unity Catalog 中是真实存在的？

A. MANAGE SHARES  
B. SHARE_ADMIN  
C. CREATE SHARE  
D. SHARING_PRIVILEGE

<details><summary>答案</summary>C. CREATE SHARE 是 Unity Catalog metastore 级别的真实权限。其余三个都是虚构的。</details>

---

**3.** Provider 共享表时没有指定 WITH HISTORY。接收方在 D2D 模式下能进行 Time Travel 查询吗？

A. 能，D2D 模式默认支持 Time Travel  
B. 不能，必须在共享时指定 WITH HISTORY  
C. 能，但需要接收方额外配置  
D. 取决于源表是否有足够的历史版本

<details><summary>答案</summary>B. WITH HISTORY 必须在共享时显式指定，否则接收方无法 Time Travel。</details>

---

**4.** 以下关于 Delta Sharing 的说法，哪个是正确的？

A. 接收方可以通过 INSERT 语句向源表写入数据  
B. Delta Sharing 会将数据复制一份到接收方的存储中  
C. Delta Sharing 是只读协议，数据始终留在 Provider 的存储中  
D. 接收方拥有 SELECT 权限后可以删除源表中的行

<details><summary>答案</summary>C. Delta Sharing 是零复制（zero-copy）只读共享协议。</details>

---

**5.** 一个 Data Steward 拥有 CREATE SHARE 权限但不是 metastore admin。他能完成以下哪些操作？

A. 创建 Share 并添加他有 SELECT 权限的表  
B. 创建 Recipient  
C. 授予其他用户 CREATE SHARE 权限  
D. 删除其他人创建的 Share

<details><summary>答案</summary>A. CREATE SHARE 只允许创建 Share 并管理自己创建的 Share。创建 Recipient 需要 CREATE RECIPIENT 权限。授予权限和删除他人 Share 需要 metastore admin。</details>

---

**6.** 要使 D2D 共享的接收方能同时进行 Time Travel 和 Streaming Reads，且查询性能最优，正确的配置是什么？

A. WITH HISTORY + 分区表  
B. WITHOUT HISTORY + CDF + 非分区表  
C. WITH HISTORY + CDF + 非分区表  
D. WITH HISTORY + CDF + 分区表

<details><summary>答案</summary>C. WITH HISTORY 支持 Time Travel，CDF 支持 Streaming Reads，非分区表在 D2D 中性能最优。</details>

---

**7.** Open Sharing 不支持以下哪些功能？（多选）

A. 共享 Delta Tables  
B. 共享 ML Models  
C. Time Travel  
D. Streaming Reads  
E. 跨平台接收

<details><summary>答案</summary>B, C, D. Open Sharing 只支持共享 Delta Tables（A）和跨平台接收（E）。ML Models、Time Travel、Streaming Reads 都只有 D2D 模式支持。</details>

---

**8.** 一个组织将 CDF 在源表上启用后，将表通过 `ALTER SHARE s ADD TABLE t` 添加到 Share（未指定 WITH HISTORY）。接收方能进行 Streaming Reads 吗？

A. 能，因为 CDF 已启用  
B. 不能，因为 Streaming Reads 还需要 WITH HISTORY  
C. 能，但性能受限  
D. 取决于接收方的 Spark 版本

<details><summary>答案</summary>B. Streaming Reads 需要同时满足两个条件：源表启用 CDF 且共享时指定 WITH HISTORY。只有 CDF 不够。</details>
