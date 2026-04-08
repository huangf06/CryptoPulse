# Auto Loader -- 深度复习辅导材料

**题目数量:** 7 | **全错:** 7/7 (0%) | **题号:** Q14, Q108, Q242, Q257, Q268, Q289, Q298

---

## 一、错误模式诊断

7 道全错，错误模式可归纳为三个根因：

### 1. 适用边界混淆（Q14, Q298）
- **Q14**: 把 Auto Loader 当作万能增量工具，试图用它"订阅" Delta 表变更。Auto Loader 只能监听云存储中的原始文件（JSON/CSV/Parquet/etc），不能读 Delta 表。对 Delta 表的增量读取应该用 `readStream.table()` 或 Change Data Feed。
- **Q298**: 不熟悉 Auto Loader 支持的文件格式清单，选了 TEXT 格式读图片。二进制文件（图片、PDF）用 `BINARYFILE`。

**根因**: 对 Auto Loader 的"输入源"和"支持格式"没有形成精确的边界认知，导致在不适用的场景中错误选择它，在适用的场景中选错格式。

### 2. 默认行为记忆模糊（Q108, Q257）
- **Q108**: 知道 Auto Loader 有 file notification 模式，但题目问"默认"模式时选了 notification 而非 directory listing。
- **Q257**: 编造了不存在的选项 `cloudFiles.quarantineMode`，未能识别 schema 不匹配是合法 JSON 被隔离的真正原因。

**根因**: 对 Auto Loader 的默认配置（directory listing、schema enforcement 行为）只有模糊印象，未形成条件反射级的记忆。

### 3. LDP 协作模式判断失误（Q242, Q268, Q289）
- **Q242**: 选了 Auto Loader + Workflows 的手动编排方案，忽略题目"最小运维"的关键词，正确答案是 LDP 声明式管道。
- **Q268**: 混淆了 streaming table 和 materialized view 的适用场景，错误地用 materialized view 做原始流数据摄取。
- **Q289**: 选了 static DataFrame read，违反增量处理的核心需求。

**根因**: 未建立"需求关键词 -> 技术方案"的映射规则。当题目出现"最小运维 + 增量 + schema 演化 + exactly-once"时，答案几乎一定指向 LDP + Auto Loader。

---

## 二、子主题分解与学习方法

### 子主题 A: Auto Loader 核心机制

**知识点:**
- cloudFiles 数据源的基本用法
- 增量处理 + exactly-once 语义（通过 checkpoint）
- 输入源限制：只能是云存储文件，不能是 Delta 表

**学习方法:**
写出一个完整的 Auto Loader pipeline 代码模板，标注每个 option 的作用。然后写一个"反例"列表：哪些场景不能用 Auto Loader（Delta 表变更监听、已加载的表增量读取）。

### 子主题 B: 文件发现模式

**知识点:**
- Directory Listing（默认）：定期列举目录，零配置
- File Notification：需要 `cloudFiles.useNotifications = true`，需要云端队列/通知服务权限
- 两者的规模适用范围、延迟、成本差异

**学习方法:**
制作一张 2x5 对比表，默写三遍。关键记忆锚点："默认 = directory listing = 零配置"。

### 子主题 C: Schema 推断与演化

**知识点:**
- Schema inference：首次运行推断，存储到 `schemaLocation`
- Schema evolution modes：`addNewColumns`、`rescue`、`failOnNewColumns`、`none`
- `_rescued_data` 列：捕获不匹配 schema 的字段数据
- 合法 JSON 被隔离的原因：schema 不匹配，不是格式错误

**学习方法:**
用场景模拟法：假设上游 JSON 新增了一个字段 `new_field`，在四种 schemaEvolutionMode 下分别会发生什么？逐一推演并写下结果。

### 子主题 D: Auto Loader 支持的文件格式

**知识点:**
- 七种格式：json, csv, parquet, avro, text, binaryFile, orc
- 二进制文件（图片、PDF）用 `BINARYFILE`
- `pathGlobFilter` 过滤特定文件扩展名
- 不存在 `IMAGE` 格式

**学习方法:**
列出七种格式 + 典型场景，重点记住 `BINARYFILE` 的存在和用途。

### 子主题 E: Auto Loader vs COPY INTO

**知识点:**
- Auto Loader: streaming, 自动追踪, 性能随文件数增长稳定, 支持 schema 演化
- COPY INTO: batch SQL, 文件元数据去重, 文件多时性能下降, 不支持自动 schema 演化
- Databricks 首选 Auto Loader，COPY INTO 仅用于简单一次性加载

**学习方法:**
记住一个判断规则：如果题目涉及"持续增量摄取"，选 Auto Loader；如果是"一次性/低频批量加载"，选 COPY INTO。

### 子主题 F: LDP + Auto Loader 协作

**知识点:**
- LDP 自动管理 checkpoint、重试、编排
- Auto Loader 负责增量文件发现
- Streaming table 用于流式数据摄取和低延迟场景
- Materialized view 用于定期聚合、可完全重算的场景
- 关键词映射："最小运维 + 数据质量 + 可审计" -> LDP

**学习方法:**
建立关键词-方案映射表。每次做题时先提取题目中的需求关键词，再匹配方案。

---

## 三、苏格拉底式教学问题集

### 子主题 A: Auto Loader 核心机制

1. Auto Loader 的输入源为什么必须是云存储文件而非 Delta 表？从技术实现层面，cloudFiles 数据源是如何发现新数据的？
2. 如果你需要对一张 Delta 表做增量读取，应该用什么替代方案？这些替代方案与 Auto Loader 在语义上有何不同？
3. Auto Loader 的 exactly-once 语义是通过什么机制保证的？如果 checkpoint 被删除会发生什么？
4. 为什么说 Auto Loader 是幂等的？在什么条件下幂等性可能被破坏？

### 子主题 B: 文件发现模式

1. 为什么 Databricks 选择 directory listing 而非 file notification 作为默认模式？这体现了什么设计哲学？
2. 当文件数量从十万级增长到十亿级时，directory listing 的性能瓶颈出现在哪里？file notification 是如何绕过这个瓶颈的？
3. 如果一个团队已经在用 directory listing，迁移到 file notification 需要哪些额外的基础设施配置？
4. 在成本维度上，两种模式各自的成本来源是什么？在什么文件规模下，file notification 更经济？

### 子主题 C: Schema 推断与演化

1. "well-formed JSON" 和 "schema-conformant JSON" 的区别是什么？Auto Loader 在哪个层面做检查？
2. `schemaEvolutionMode` 的四种模式分别适合什么业务场景？如果你是数据平台负责人，你会选哪种作为默认？为什么？
3. `_rescued_data` 列中的数据是什么格式？下游处理这些数据的典型方式是什么？
4. 如果上游将一个 STRING 字段改为 INT 字段，在 `addNewColumns` 模式下会发生什么？在 `rescue` 模式下呢？
5. `schemaLocation` 存储的是什么？如果手动修改这个位置的文件会怎样？

### 子主题 D: Auto Loader 支持的文件格式

1. 为什么 Auto Loader 没有专门的 IMAGE 格式，而是用通用的 BINARYFILE？这种设计的优势是什么？
2. `pathGlobFilter` 和在 `load()` 路径中指定子目录相比，各自适用什么场景？
3. 如果要用 Auto Loader 摄取混合格式的文件（同一目录下有 JSON 和 CSV），应该怎么处理？

### 子主题 E: Auto Loader vs COPY INTO

1. Auto Loader 使用 RocksDB checkpoint 追踪文件，COPY INTO 使用文件元数据去重。为什么前者在大规模场景下性能更好？
2. 在什么具体场景下，COPY INTO 比 Auto Loader 更合适？列举至少两个。
3. 如果一个团队从 COPY INTO 迁移到 Auto Loader，需要注意哪些兼容性问题？

### 子主题 F: LDP + Auto Loader 协作

1. LDP "自动管理 checkpoint" 意味着什么？与手动配置 checkpoint 相比，消除了哪些运维风险？
2. 在 LDP 中，为什么原始流数据摄取必须用 streaming table 而不能用 materialized view？从底层执行机制解释。
3. 一个场景同时需要"近实时监控"和"每日聚合报告"，为什么前者用 streaming table、后者用 materialized view？如果反过来会怎样？
4. 题目中出现"最小运维"这个关键词时，为什么几乎一定排除手动 Structured Streaming + checkpoint 的方案？

---

## 四、场景判断题

### 子主题 A: Auto Loader 核心机制

**A1.** 一个数据工程师需要从云存储中的 JSON 文件持续摄取数据到 Delta Lake 表。文件每小时到达一批。以下哪种方案最合适？

- A. 使用 `spark.read.format("json")` 读取所有文件，每次覆盖写入 Delta 表
- B. 使用 `spark.readStream.format("cloudFiles")` 配置 Auto Loader 增量摄取
- C. 使用 `spark.readStream.table("source_table")` 读取源表的增量数据
- D. 编写定时任务记录已处理文件列表，手动过滤新文件后批量加载

**答案: B**
A 每次全量读取 + 覆盖，无法增量处理。C 的 `readStream.table()` 用于读取 Delta 表而非文件。D 可行但等于手动实现了 Auto Loader 的功能，运维开销大。B 是标准方案：cloudFiles 自动追踪新文件，增量幂等加载。

**A2.** 一个 ETL pipeline 需要监听 Delta 表 `account_history` 的新增记录，并将变更同步到下游表。以下哪种方案正确？

- A. 使用 Auto Loader 订阅 `account_history` 的文件目录
- B. 使用 `spark.readStream.table("account_history")` 读取增量数据
- C. 使用 `COPY INTO` 从 `account_history` 加载新数据
- D. 配置 Auto Loader 的 `cloudFiles.format` 为 `delta`

**答案: B**
Auto Loader 只能监听云存储中的原始文件（JSON/CSV/Parquet 等），不能读 Delta 表。`cloudFiles.format` 不支持 `delta` 格式。`COPY INTO` 也是从文件加载，不是从表。正确方式是用 Structured Streaming 的 `readStream.table()` 读取 Delta 表的增量。

**A3.** 以下关于 Auto Loader 的陈述，哪一项是错误的？

- A. Auto Loader 通过 checkpoint 保证 exactly-once 语义
- B. Auto Loader 可以监听云存储中新到达的文件
- C. Auto Loader 可以用于读取 Delta 表的变更数据
- D. Auto Loader 支持 schema 推断和 schema 演化

**答案: C**
Auto Loader 的输入源是云存储文件，不是 Delta 表。读取 Delta 表变更应使用 Change Data Feed 或 `readStream.table()`。

### 子主题 B: 文件发现模式

**B1.** 一个公司的数据湖中有超过 50 亿个文件，需要用 Auto Loader 持续摄取新文件。以下哪种配置最合适？

- A. 使用默认的 directory listing 模式
- B. 设置 `cloudFiles.useNotifications = true` 启用 file notification 模式
- C. 设置 `cloudFiles.maxFilesPerTrigger` 限制每批处理文件数
- D. 使用 COPY INTO 替代 Auto Loader

**答案: B**
50 亿文件远超 directory listing 的适用范围（百万级）。File notification 通过云端事件驱动，不需要列举全部文件，适合十亿级规模。C 只是限流措施，不解决文件发现的性能问题。D 的 COPY INTO 在大规模场景下性能更差。

**B2.** 关于 Auto Loader 的默认执行模式，以下哪项描述正确？

- A. 通过云厂商队列和通知服务追踪新文件，增量加载到 Delta Lake
- B. 通过列举输入目录发现新文件，增量幂等加载到 Delta Lake
- C. 通过 webhook 触发 Databricks job，自动合并新数据到目标表
- D. 通过列举输入目录发现新文件，直接查询所有有效文件物化目标表

**答案: B**
默认模式是 directory listing（列举目录），不是 file notification（云厂商队列）。加载方式是增量幂等加载到 Delta Lake，不是全量查询物化。

### 子主题 C: Schema 推断与演化

**C1.** 一个数据工程师使用 Auto Loader 摄取 JSON 数据，发现一些格式正确的 JSON 记录被隔离。最可能的原因是什么？

- A. JSON 文件编码不是 UTF-8
- B. JSON 记录不符合定义的 schema（字段类型不匹配或有额外字段）
- C. Auto Loader 的 `cloudFiles.quarantineMode` 未设置为 `rescue`
- D. 源文件使用了 multi-line JSON 格式

**答案: B**
"well-formed JSON" 不等于 "schema-conformant"。Auto Loader 按定义的 schema 严格检查，类型不匹配、额外字段、缺失字段都会导致记录被隔离。C 中的 `cloudFiles.quarantineMode` 是不存在的选项。

**C2.** 一个 LDP pipeline 使用 Auto Loader 摄取 JSON 数据，上游可能随时新增字段。数据工程师希望自动适应新列，同时确保数据不丢失。应该使用哪种 schema evolution 配置？

- A. `cloudFiles.schemaEvolutionMode = "none"`
- B. `cloudFiles.schemaEvolutionMode = "failOnNewColumns"`
- C. `cloudFiles.schemaEvolutionMode = "addNewColumns"`
- D. `cloudFiles.schemaEvolutionMode = "rescue"`

**答案: C**
`addNewColumns` 自动将新列添加到 schema，满足"自动适应新列"的需求。`none` 会忽略新列。`failOnNewColumns` 会报错停止。`rescue` 将不匹配数据放入 `_rescued_data` 列但不添加新列到 schema。

**C3.** `_rescued_data` 列中存储的是什么？

- A. 格式错误的 JSON 字符串原文
- B. 不匹配 schema 的字段值，序列化为 JSON 字符串
- C. 被 data quality expectations 过滤的记录
- D. 被 checkpoint 标记为重复的记录

**答案: B**
`_rescued_data` 捕获类型不匹配的字段值和 schema 中不存在的额外字段，将它们序列化为 JSON 字符串。它不处理格式错误的 JSON（那些直接是 bad records），也与 expectations 和 checkpoint 无关。

### 子主题 D: Auto Loader 支持的文件格式

**D1.** 一个数据工程师需要用 Auto Loader 从 S3 增量摄取 JPEG 和 PNG 图片文件。以下哪两项配置是推荐的？（选两项）

- A. `cloudFiles.format = "TEXT"`
- B. `cloudFiles.format = "BINARYFILE"`
- C. `cloudFiles.format = "IMAGE"`
- D. 使用 `pathGlobFilter` 选项过滤图片文件扩展名

**答案: B, D**
图片是二进制数据，用 `BINARYFILE` 格式读取。`IMAGE` 格式在 Auto Loader 中不存在。`TEXT` 用于纯文本文件，读图片无意义。`pathGlobFilter` 过滤 `*.jpg` 和 `*.png`，避免摄取非目标文件。

**D2.** Auto Loader 的 `cloudFiles.format` 支持以下哪些格式？（选所有正确项）

- A. json
- B. delta
- C. binaryFile
- D. image
- E. orc

**答案: A, C, E**
Auto Loader 支持 json, csv, parquet, avro, text, binaryFile, orc 共七种格式。不支持 delta（Delta 表用 `readStream.table()` 读取）和 image（不存在此格式）。

### 子主题 E: Auto Loader vs COPY INTO

**E1.** 一个数据湖每天接收数千个新 JSON 文件，总文件数已超过百万。以下哪种方案在文件数增长时性能最稳定？

- A. 使用 COPY INTO 每小时运行一次
- B. 使用 Auto Loader 持续增量摄取
- C. 使用 `spark.read.json()` 批量读取后去重写入
- D. 使用 Databricks Workflows 调度 SQL 查询加载

**答案: B**
Auto Loader 使用 RocksDB checkpoint 追踪已处理文件，文件数增长时性能保持稳定。COPY INTO 通过文件元数据去重，文件数量多时性能明显下降。批量读取无法保证 exactly-once。

**E2.** 以下哪种场景更适合使用 COPY INTO 而非 Auto Loader？

- A. 每天持续从 Kafka 消费数据写入 Delta Lake
- B. 一次性将历史数据从 Parquet 文件批量加载到 Delta 表
- C. 构建需要 schema 演化支持的实时数据管道
- D. 从 S3 增量摄取数百万个 JSON 文件

**答案: B**
COPY INTO 适合一次性或低频的批量加载场景。持续摄取（A, D）和 schema 演化（C）都是 Auto Loader 的强项。

### 子主题 F: LDP + Auto Loader 协作

**F1.** 一个小团队需要构建一个数据管道：从 S3 摄取 JSON 文件，过滤无效记录，确保数据质量，最小化运维。以下哪种方案最合适？

- A. Auto Loader + Structured Streaming，手动管理 checkpoint 和 merge logic
- B. Spark batch job + UDF 过滤 + LDP materialized view
- C. Auto Loader + Databricks Workflows SQL 查询编排
- D. LDP 声明式管道 + streaming table + materialized view + data expectations

**答案: D**
题目关键词"小团队、最小运维、数据质量"直接指向 LDP。LDP 提供声明式定义 + 自动编排 + 内置 expectations，运维开销最小。A 的手动 checkpoint 管理增加运维。B 的 static read 无法增量处理。C 的 SQL in Workflows 是手动编排。

**F2.** 在 LDP 中设计一个系统处理实时卡车遥测数据，需要支持：(1) 近实时监控每辆车的最新位置和速度，(2) 每日聚合总行驶距离报告。以下哪种组合正确？

- A. Streaming table 用于原始数据，streaming table 用于近实时监控，materialized view 用于每日聚合
- B. Streaming table 用于原始数据，materialized view 用于近实时监控，materialized view 用于每日聚合
- C. Materialized view 用于原始数据，streaming table 用于近实时监控，materialized view 用于每日聚合
- D. Streaming table 用于原始数据，materialized view 用于近实时监控，streaming table 用于每日聚合

**答案: A**
原始流数据（来自 Auto Loader）-> streaming table（流式源必须用 streaming table）。近实时监控 -> streaming table（需要低延迟增量更新）。每日聚合报告 -> materialized view（定期批量重算即可）。C 错误因为 materialized view 不能直接摄取流式源。D 反转了监控和聚合的类型。

**F3.** 构建一个增量 JSON 摄取管道，要求同时满足：增量处理、自动 schema 演化、exactly-once、最小运维。以下哪种方案满足所有要求？

- A. LDP + static DataFrame read + `spark.databricks.delta.schema.autoMerge.enabled = true`
- B. Auto Loader batch mode + 每日覆盖写入
- C. LDP + Auto Loader + `cloudFiles.schemaEvolutionMode = "addNewColumns"`
- D. Structured Streaming + Auto Loader + 手动 checkpoint 配置 + `mergeSchema = true`

**答案: C**
逐一检查：A 的 static read 不是增量处理。B 的覆盖写入不是 exactly-once。D 可行但"手动 checkpoint"违反"最小运维"。C 完美匹配：LDP（自动编排 + checkpoint + 最小运维）+ Auto Loader（增量文件发现）+ `addNewColumns`（自动 schema 演化）。

---

## 五、关键知识网络

```
Auto Loader (cloudFiles)
|
|-- 输入源: 云存储文件 (JSON/CSV/Parquet/Avro/Text/BinaryFile/ORC)
|   |-- 不支持: Delta 表 (用 readStream.table() 或 CDF)
|   |-- 不支持: delta/image 格式
|   |-- 二进制文件: BINARYFILE + pathGlobFilter
|
|-- 文件发现模式
|   |-- Directory Listing (默认): 零配置, 百万级文件
|   |-- File Notification: useNotifications=true, 十亿级文件, 需云端权限
|
|-- Schema 管理
|   |-- Schema inference: 首次运行推断, 存储到 schemaLocation
|   |-- Schema evolution modes:
|   |   |-- addNewColumns: 自动添加新列 (LDP 默认)
|   |   |-- rescue: 不匹配数据 -> _rescued_data
|   |   |-- failOnNewColumns: 遇新列报错
|   |   |-- none: 忽略新列
|   |-- _rescued_data: 类型不匹配 + 额外字段 -> JSON 字符串
|   |-- 关键点: well-formed JSON != schema-conformant
|
|-- 对比
|   |-- vs COPY INTO: streaming vs batch, 稳定性能 vs 文件多时下降
|   |-- vs readStream.table(): 文件 vs Delta 表
|
|-- 与 LDP 协作
    |-- LDP 管理 checkpoint, 重试, 编排
    |-- Streaming table: 流式摄取 + 低延迟场景
    |-- Materialized view: 定期聚合 + 可重算场景
    |-- 关键词触发: "最小运维 + 增量 + schema 演化 + exactly-once" -> LDP + Auto Loader
```

---

## 六、Anki 卡片

**Q1:** Auto Loader 的默认文件发现模式是什么？
**A1:** Directory listing。通过定期列举源目录发现新文件，零额外配置。File notification 模式需要显式设置 `cloudFiles.useNotifications = true`。

**Q2:** Auto Loader 能读取 Delta 表的变更数据吗？
**A2:** 不能。Auto Loader 只能监听云存储中的原始文件（JSON/CSV/Parquet 等）。读取 Delta 表增量用 `spark.readStream.table()` 或 Change Data Feed。

**Q3:** 格式正确的 JSON 记录为什么会被 Auto Loader 隔离？
**A3:** Schema 不匹配。Auto Loader 不仅检查 JSON 格式是否合法，还检查数据是否符合定义的 schema。字段类型不匹配、额外字段、缺失字段都会导致隔离。

**Q4:** `_rescued_data` 列存储什么内容？
**A4:** 不匹配 schema 的字段值（类型不匹配或 schema 中不存在的额外字段），序列化为 JSON 字符串。确保数据不会静默丢失。

**Q5:** `cloudFiles.schemaEvolutionMode` 的四种模式分别是什么？
**A5:** `addNewColumns`（自动添加新列）、`rescue`（不匹配数据进入 `_rescued_data`）、`failOnNewColumns`（遇新列报错停止）、`none`（忽略新列）。

**Q6:** 用 Auto Loader 摄取图片文件应该设置什么格式？
**A6:** `cloudFiles.format = "BINARYFILE"`。配合 `pathGlobFilter` 过滤文件扩展名（如 `*.jpg, *.png`）。不存在 `IMAGE` 格式。

**Q7:** Auto Loader 支持哪七种文件格式？
**A7:** json, csv, parquet, avro, text, binaryFile, orc。不支持 delta 和 image。

**Q8:** Auto Loader 和 COPY INTO 在大规模文件场景下的性能差异？
**A8:** Auto Loader 使用 RocksDB checkpoint 追踪文件，文件数增长时性能稳定。COPY INTO 通过文件元数据去重，文件数量多时性能明显下降。

**Q9:** 在 LDP 中，streaming table 和 materialized view 各自适用什么场景？
**A9:** Streaming table: 流式源摄取（Auto Loader/Kafka）、需要低延迟的增量更新。Materialized view: 定期聚合报告、可以完全重算的场景。

**Q10:** 当题目需求包含"增量 + schema 演化 + exactly-once + 最小运维"时，标准答案是什么？
**A10:** LDP + Auto Loader + `cloudFiles.schemaEvolutionMode = "addNewColumns"`。LDP 自动管理 checkpoint 和编排，Auto Loader 负责增量文件发现，`addNewColumns` 处理 schema 变更。

**Q11:** Directory listing 和 file notification 各自适用什么文件规模？
**A11:** Directory listing: 中小规模（百万级文件内）。File notification: 超大规模（十亿级文件），需要云端队列/通知服务权限。

**Q12:** Type 1 SCD 表的增量更新最佳方式是什么？
**A12:** 按时间过滤增量数据 + 按业务键取最新记录 + MERGE INTO。不要用 Auto Loader（它的输入源是文件不是表），也不要每次 overwrite 全表。

---

## 七、掌握度自测

**1.** Auto Loader 通过什么数据源标识符在 Structured Streaming 中使用？
<details><summary>答案</summary>
<code>cloudFiles</code>。使用方式：<code>spark.readStream.format("cloudFiles")</code>
</details>

**2.** 如果一个数据源是 Delta 表而非云存储文件，应该用什么方式做增量读取？
<details><summary>答案</summary>
<code>spark.readStream.table("table_name")</code> 或启用 Change Data Feed (CDF)。Auto Loader 不能读 Delta 表。
</details>

**3.** Auto Loader 的 file notification 模式如何启用？适用于什么规模？
<details><summary>答案</summary>
设置 <code>cloudFiles.useNotifications = true</code>。适用于十亿级文件规模。需要配置云端事件通知服务（S3 Events / Event Grid / Pub/Sub）权限。
</details>

**4.** 上游 JSON 数据新增了一个字段 `new_field`，在 `schemaEvolutionMode = "addNewColumns"` 模式下会怎样？在 `rescue` 模式下呢？
<details><summary>答案</summary>
<code>addNewColumns</code>: <code>new_field</code> 自动添加到表 schema 中，数据正常写入。<code>rescue</code>: <code>new_field</code> 不添加到 schema，该字段的值被序列化为 JSON 字符串存入 <code>_rescued_data</code> 列。
</details>

**5.** 以下哪种方案更适合"一次性将 10GB 历史 Parquet 文件加载到 Delta 表"：Auto Loader 还是 COPY INTO？
<details><summary>答案</summary>
COPY INTO。一次性/低频的批量加载场景是 COPY INTO 的适用范围。Auto Loader 的优势在于持续增量摄取。
</details>

**6.** 在 LDP 中，为什么从 Auto Loader 摄取的原始数据必须用 streaming table 而不能用 materialized view？
<details><summary>答案</summary>
Auto Loader 是流式数据源（readStream），只有 streaming table 能接收流式输入。Materialized view 基于批量查询结果完全重算，不能直接消费流式源。
</details>

**7.** 一个场景需要"近实时监控最新状态"和"每日聚合报告"，在 LDP 中分别应该用什么？
<details><summary>答案</summary>
近实时监控: streaming table（需要低延迟增量更新，数据持续到达）。每日聚合报告: materialized view（定期批量重算即可，不需要持续流式处理）。
</details>

**8.** `cloudFiles.format` 支持 `delta` 格式吗？支持 `image` 格式吗？
<details><summary>答案</summary>
都不支持。支持的七种格式是 json, csv, parquet, avro, text, binaryFile, orc。Delta 表用 <code>readStream.table()</code> 读取，图片等二进制文件用 <code>binaryFile</code> 格式。
</details>
