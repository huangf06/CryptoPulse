# 12. 平台操作与工具 -- 深度复习辅导

**来源：13 题全错（0/13），严重薄弱区域**

---

## 一、错误模式诊断

### 模式 1：跨语言边界的类型混淆（Q9, Q126）

两次在同一考点上犯错，且两次选的答案不同（B 和 C），说明不是"记混了"而是"没有建立模型"。核心问题：不理解 `.collect()` 是 Spark 到 Python 的边界操作。一旦 `.collect()` 执行，数据就脱离了 Spark 分布式世界，变成纯 Python 对象。list comprehension 进一步将 Row 对象提取为字符串列表。这个 Python 变量在 `%sql` cell 中完全不可见。

**根因：** 没有建立"Spark 分布式对象 vs Python 本地对象"的清晰心智模型。

### 模式 2：重复考点重复错、且每次错法不同（Q102/Q175, Q111/Q174）

- Notebook 首行格式：第一次选 A（%python），第二次选 D（-- Databricks notebook source）
- 包安装：第一次选 E（Cluster UI），第二次选 D（%sh pip install）

这不是混淆，是"知识空洞"。没有任何锚点记忆，每次都在猜。

**根因：** 纯记忆性知识点没有通过主动回忆（active recall）固化，只是被动看过。

### 模式 3：边界条件误读（Q98）

`age > 17` vs `age > 18` 的区别。这是审题精度问题，但也暴露了对 WHERE 子句行为的不够自信 -- 花时间在"是不是 null 化"上纠结，反而忽略了数值边界。

**根因：** 对 SQL 语义的理解不够自动化，还需要"想"而不是"知道"。

### 模式 4：API 记忆缺失（Q54, Q105, Q297）

`sys.path`、MLflow UDF 调用语法、`F.sha2()` -- 这些都是 API 精确记忆。错的原因是没有动手用过或用得太少。

**根因：** 缺乏动手实践形成的肌肉记忆。

---

## 二、子主题分解与学习方法

### 子主题 A：Notebook 跨语言互操作

**知识要点：**
- `%python` cell 中的变量存在于 Python 进程内存
- `%sql` cell 由 SQL 引擎执行，完全独立的命名空间
- `.collect()` 是分布式到本地的边界：DataFrame -> list of Row -> list comprehension -> Python list
- 跨语言传值三种方式：`createOrReplaceTempView()`、`spark.sql(f"...")`、widget

**学习方法：** 画一张"数据在哪里"的流图：DataFrame（Spark 集群）-> `.collect()`（Driver Python 内存）-> `createOrReplaceTempView()`（Spark Catalog，SQL 可见）。每次遇到跨语言题，先定位数据当前在哪个空间。

### 子主题 B：Notebook 文件格式与元数据

**知识要点：**
- Python notebook 源文件首行：`# Databricks notebook source`
- Scala notebook：`// Databricks notebook source`
- SQL notebook：`-- Databricks notebook source`
- 规律：注释符号 + 固定字符串 "Databricks notebook source"

**学习方法：** 记忆锚点 -- 注释符号和语言一一对应：`#` = Python, `//` = Scala, `--` = SQL。首行永远是该语言的注释符 + "Databricks notebook source"。

### 子主题 C：包安装作用域

**知识要点：**

| 方式 | 作用域 | 分发到 Workers | 重启行为 |
|------|--------|---------------|---------|
| `%pip install` | Notebook | 自动分发 | 自动重启 Python 解释器 |
| `%sh pip install` | Driver shell | 不分发 | 无 |
| Cluster UI | Cluster（所有 notebook） | 自动 | 需手动重启集群 |

**学习方法：** 核心区分维度是"谁执行的"。`%pip` 是 Databricks 内置 magic command，它知道集群拓扑，所以能分发。`%sh` 只是在 driver 上开了个 bash，它对 Spark 集群一无所知。

### 子主题 D：PII 脱敏函数

**知识要点：**
- 脱敏三要素：不可逆、确定性（同输入同输出）、替换原列
- `sha1` -> 固定 40 字符，确定性
- `sha2(col, 256)` -> 固定 64 字符，确定性
- `hash()` -> 返回整数，长度不固定
- `uuid()` -> 随机，非确定性，破坏关联分析能力
- `mask()` -> 输出长度随输入变化

**学习方法：** 两步判断法：(1) 题目要求固定长度？-> 排除 hash、mask (2) 题目要求确定性？-> 排除 uuid。剩下只有 sha1/sha2。再看是 PySpark（`F.sha2()`）还是 SQL（`sha1()`）上下文。

### 子主题 E：MLflow 模型调用

**知识要点：**
- `mlflow.pyfunc.spark_udf(spark, model_uri)` 将模型加载为 Spark UDF
- 调用方式：`model(*columns)` -- `*` 解包列名列表
- 在 `df.select()` 中使用，`.alias()` 命名输出列
- PySpark DataFrame 没有 `.apply()` 方法（那是 Pandas 的）

**学习方法：** 关键记忆点 -- MLflow 模型在 Spark 中就是 UDF，UDF 用在 `select()` 里，`*columns` 解包传参。

### 子主题 F：集群环境管理（Compute Policies + Init Scripts）

**知识要点：**
- Compute Policies：设置和强制执行 Spark 配置、系统属性、环境变量
- Init Scripts：集群启动时执行的脚本，安装外部依赖、配置环境
- Compute Policies 不能安装库（只管配置参数）
- Init Scripts 不是用来存储 secrets 的（secrets 有专门的 Databricks Secrets）

**学习方法：** Compute Policies = 参数管理，Init Scripts = 启动时执行命令。两者互补，题目问"需要设置配置参数 + 安装外部依赖"时，答案就是这两个。

### 子主题 G：DBFS 架构

**知识要点：**
- DBFS = 对象存储（S3/ADLS/GCS）上的文件系统抽象层
- 提供类 Unix 文件操作接口
- 数据持久化在对象存储中，不是 driver 临时磁盘
- 默认所有工作区用户可访问，不限管理员

**学习方法：** DBFS 本质是"翻译层"。它把对象存储的 API 翻译成 `ls`、`cp`、`rm` 等文件操作。底层永远是对象存储。

### 子主题 H：Python 基础（sys.path）

- `sys.path`：Python 搜索模块的目录列表
- `os.path`：路径操作工具模块（join, exists, dirname 等方法）
- 两者名字相似但功能完全不同

---

## 三、苏格拉底式教学问题集

### 子主题 A：跨语言互操作

1. 在 `%python` cell 中执行 `df = spark.table("sales")`，此时 `df` 存在于哪里？如果接下来执行 `rows = df.collect()`，`rows` 又存在于哪里？两者的本质区别是什么？
2. 为什么 `%sql` cell 看不到 Python 变量，但能看到 temp view？Spark Catalog 在这个过程中扮演什么角色？
3. 如果要把一个 Python 列表传给 SQL 查询，`spark.sql(f"SELECT * FROM t WHERE col IN {tuple(my_list)}")` 和 `createOrReplaceTempView` 哪种更安全？为什么？（提示：SQL 注入）
4. `.collect()` 之后再做 list comprehension，结果为什么不是 DataFrame？从 Spark 的 lazy evaluation 角度，`.collect()` 触发了什么？

### 子主题 B：Notebook 文件格式

1. 为什么 Databricks 选择用语言对应的注释符号作为首行标记，而不是统一用某种元数据格式？
2. 如果你在 VS Code 中打开一个 `.py` 文件，看到第一行是 `# Databricks notebook source`，这行代码会被 Python 解释器执行吗？为什么这种设计是安全的？
3. `%python` magic command 和 `# Databricks notebook source` 首行标记有什么本质区别？一个是运行时行为，一个是什么？

### 子主题 C：包安装

1. `%pip install` 能分发到所有 worker 节点，`%sh pip install` 不能。从技术实现角度，为什么 `%sh` 做不到？（提示：`%sh` 的执行上下文是什么？）
2. 如果一个数据工程师在 notebook A 中 `%pip install pandas==1.5.0`，另一个工程师在 notebook B 中 `%pip install pandas==2.0.0`，同一集群上会发生什么？为什么 `%pip` 被称为 notebook-scoped？
3. Cluster UI 安装需要重启集群，`%pip` 自动重启 Python 解释器。这两个"重启"有什么区别？哪个影响范围更大？

### 子主题 D：PII 脱敏

1. 为什么 `uuid()` 不适合脱敏？如果用 `uuid()` 替换 email，两个表中同一个用户会怎样？
2. `sha1` 和 `sha2` 都是确定性哈希，为什么选 C（`sha1('email')`）而不选 D（`sha2(email, 0)`）？（提示：看 Q313 的选项细节）
3. Q297 中 C 选项创建新列 `hashed_email` 但没删原列。如果题目改成"先创建 hashed_email，再 drop user_email"，这个方案是否可行？为什么原题中 B 更直接？
4. `hash()` 在 Spark SQL 中返回整数。为什么整数不满足"固定长度"要求？（提示：-2147483648 和 42 的字符串长度分别是多少？）

### 子主题 E：MLflow 模型调用

1. `model(*columns)` 中的 `*` 做了什么？如果 `columns = ["age", "income"]`，`model(*columns)` 等价于什么？
2. 为什么 MLflow 模型在 Spark 中被当作 UDF 使用，而不是 `model.predict(df)`？这和 Spark 的分布式执行模型有什么关系？
3. `pandas_udf` 和 `mlflow.pyfunc.spark_udf` 有什么区别？为什么 Q105 中 D 选项 `pandas_udf(model, columns)` 是错的？

### 子主题 F：集群环境管理

1. Compute Policies 能设置环境变量和 Spark 配置，Init Scripts 也能设置环境变量。两者的区别在哪里？（提示：一个是声明式，一个是命令式）
2. 为什么"在 DBFS 上放库文件"不是标准的库安装方式？Databricks 推荐的库管理方式是什么？
3. Init Scripts 和 Cluster UI 安装库都在集群启动时生效，但用途不同。什么场景用 Init Scripts 更合适？

### 子主题 G：DBFS

1. DBFS 说是"文件系统抽象层"，但底层是对象存储。对象存储本身不支持目录概念，DBFS 如何模拟目录结构的？
2. "DBFS 默认对所有工作区用户可访问" -- 这意味着在 DBFS root 中存储敏感数据安全吗？Databricks 推荐敏感数据存储在哪里？
3. 如果集群关闭，DBFS 中的数据还在吗？为什么？

---

## 四、场景判断题

### 子主题 A：跨语言互操作

**题 A1：**
一位数据工程师在 Databricks notebook 中运行以下代码：

Cell 1 (`%python`):
```python
product_ids = spark.table("products").select("id").filter("category = 'electronics'")
```

Cell 2 (`%sql`):
```sql
SELECT * FROM orders WHERE product_id IN product_ids
```

请问执行结果是什么？

- A. 两个 cell 都成功，返回电子产品的订单
- B. Cell 1 成功，Cell 2 报错，因为 SQL 引擎找不到名为 product_ids 的表或视图
- C. Cell 1 成功，Cell 2 成功，因为 Spark 自动将 DataFrame 注册为临时视图
- D. 两个 cell 都失败

**答案：B**

**解析：** `product_ids` 是一个 PySpark DataFrame 对象，存在于 Python 进程内存中。SQL 引擎的命名空间中没有这个变量，它会尝试在 Spark Catalog 中查找名为 `product_ids` 的表或视图，找不到就报错。正确做法是先 `product_ids.createOrReplaceTempView("product_ids_view")`，再在 SQL 中引用 `product_ids_view`。

---

**题 A2：**
以下代码的输出类型是什么？

```python
result = [row.name for row in spark.table("users").select("name").limit(10).collect()]
```

- A. PySpark DataFrame
- B. Python list of Row objects
- C. Python list of strings
- D. Pandas DataFrame

**答案：C**

**解析：** `.collect()` 将 DataFrame 转换为 Python list of Row 对象。`row.name` 提取每个 Row 的 `name` 字段值（字符串）。list comprehension 的结果是 Python list of strings。整个链条：DataFrame -> `.collect()` -> list[Row] -> list comprehension -> list[str]。

---

### 子主题 B：Notebook 文件格式

**题 B1：**
一位数据工程师从 Databricks 导出了一个 SQL notebook，在文本编辑器中打开。第一行最可能是什么？

- A. `%sql`
- B. `# Databricks notebook source`
- C. `-- Databricks notebook source`
- D. `/* Databricks notebook source */`

**答案：C**

**解析：** SQL 的注释符号是 `--`。Databricks notebook 导出为源文件时，第一行使用对应语言的注释符号 + "Databricks notebook source"。SQL notebook 就是 `-- Databricks notebook source`。

---

**题 B2：**
以下哪项是 Databricks notebook cell 中切换到 Python 语言的正确方式？

- A. 在 cell 第一行写 `# Databricks notebook source`
- B. 在 cell 第一行写 `%python`
- C. 在 notebook 设置中更改默认语言
- D. 在 cell 第一行写 `import python`

**答案：B**

**解析：** `%python` 是 magic command，用于在非 Python 默认语言的 notebook 中临时切换到 Python。`# Databricks notebook source` 是文件级元数据标识，不是 cell 级命令。两者用途完全不同。

---

### 子主题 C：包安装

**题 C1：**
一位数据工程师需要在 notebook 中使用 `cryptography` 库进行数据加密。集群上未预装此库。以下哪种方式能确保 driver 和所有 worker 节点都能使用此库？

- A. `%sh pip install cryptography`
- B. `%pip install cryptography`
- C. `import subprocess; subprocess.run(["pip", "install", "cryptography"])`
- D. `dbutils.library.install("cryptography")`

**答案：B**

**解析：** `%pip install` 是唯一能在 notebook 级别将库分发到 driver + 所有 worker 节点的方式。`%sh pip install` 和 `subprocess.run` 都只在 driver 的 shell 中执行。`dbutils.library.install` 已被弃用。

---

**题 C2：**
一位数据工程师在 notebook A 中执行了 `%pip install pandas==1.5.0`，同一集群上另一位工程师在 notebook B 中执行了 `%pip install pandas==2.0.0`。以下描述哪个正确？

- A. 后执行的版本覆盖前一个，两个 notebook 都使用相同版本
- B. 两个 notebook 各自使用自己安装的版本，互不影响
- C. 系统报错，因为不能在同一集群上安装两个版本
- D. 集群需要重启才能解决版本冲突

**答案：B**

**解析：** `%pip install` 是 notebook-scoped 的，每个 notebook 有独立的 Python 环境。两个 notebook 可以安装不同版本的同一个库，互不影响。这正是 `%pip` 相对于 Cluster UI 安装的优势。

---

### 子主题 D：PII 脱敏

**题 D1：**
数据工程师需要将 `users` 表复制到测试环境，但必须脱敏 `email` 列。要求：(1) 脱敏后不可逆 (2) 同一 email 始终产生相同输出 (3) 输出不包含原始 PII。以下哪个 PySpark 代码满足所有要求？

- A. `df.withColumn("email", F.expr("uuid()"))`
- B. `df.withColumn("email", F.sha2("email", 256))`
- C. `df.withColumn("masked_email", F.sha2("email", 256))`
- D. `df.withColumn("email", F.regexp_replace("email", "(.*)@", "***@"))`

**答案：B**

**解析：** B 使用 sha2 哈希替换原列，满足不可逆和确定性。A 使用 uuid 随机值，不满足确定性（同一 email 每次输出不同）。C 创建新列但保留了原始 email 列，PII 仍然存在。D 的正则替换仍保留部分信息（域名），且不同 email 可能映射到相同结果。

---

**题 D2：**
SQL 查询需要对 `phone_number` 列进行脱敏，要求所有输出长度相同且不同输入产生不同输出。以下哪个函数最合适？

- A. `hash(phone_number)`
- B. `mask(phone_number)`
- C. `sha1(phone_number)`
- D. `concat('XXX-', substring(phone_number, -4, 4))`

**答案：C**

**解析：** `sha1` 输出固定 40 字符十六进制字符串，不同输入产生不同输出（碰撞概率极低）。`hash` 返回整数，不同整数字符串长度不同（如 42 vs -2147483648）。`mask` 输出长度随输入变化。D 选项保留了后四位真实数据，且不同号码可能后四位相同。

---

### 子主题 E：MLflow 模型调用

**题 E1：**
数据科学团队用 MLflow 注册了一个模型，接受 `["age", "income", "credit_score"]` 三列输入，输出 DOUBLE 类型预测值。以下哪段代码能正确生成预测结果？

- A. `df.select("user_id", model.predict(["age", "income", "credit_score"]).alias("pred"))`
- B. `df.select("user_id", model(*["age", "income", "credit_score"]).alias("pred"))`
- C. `df.apply(model, ["age", "income", "credit_score"]).select("user_id", "pred")`
- D. `model.transform(df).select("user_id", "prediction")`

**答案：B**

**解析：** MLflow pyfunc 模型加载为 Spark UDF 后，在 `df.select()` 中以 `model(*columns)` 方式调用。`*["age", "income", "credit_score"]` 解包为 `model("age", "income", "credit_score")`。A 的 `model.predict()` 是 sklearn 风格，不是 Spark UDF 调用方式。C 的 `df.apply()` 是 Pandas API。D 的 `model.transform()` 是 Spark MLlib Pipeline 风格，不是 MLflow pyfunc。

---

### 子主题 F：集群环境管理

**题 F1：**
一位数据工程师需要：(1) 为生产集群设置 `spark.sql.shuffle.partitions=400` (2) 安装一个需要编译的 C 扩展库。最佳方案是什么？

- A. 两个需求都通过 Compute Policies 解决
- B. 两个需求都通过 Init Scripts 解决
- C. Spark 配置通过 Compute Policies，库安装通过 Init Scripts
- D. Spark 配置通过 Init Scripts，库安装通过 Cluster UI

**答案：C**

**解析：** Compute Policies 适合声明式地设置和强制执行 Spark 配置参数。Init Scripts 适合在集群启动时执行命令行操作（如编译安装 C 扩展库）。Compute Policies 不能安装库，Cluster UI 安装不适合需要编译的复杂依赖。

---

**题 F2：**
以下关于 Init Scripts 的描述，哪项是正确的？

- A. Init Scripts 只能由工作区管理员创建和执行
- B. Init Scripts 在集群启动时执行，可用于安装操作系统级别的依赖
- C. Init Scripts 存储的配置数据可以替代 Databricks Secrets
- D. Init Scripts 的修改不需要重启集群就能生效

**答案：B**

**解析：** Init Scripts 在集群启动时执行 shell 命令，可以安装 apt 包、编译 C 库等操作系统级别的操作。A 错误：非管理员也可以使用 cluster-scoped init scripts。C 错误：Init Scripts 不应存储敏感配置数据，Secrets 有专门的加密存储。D 错误：Init Scripts 在启动时执行，修改后需要重启集群。

---

### 子主题 G：DBFS

**题 G1：**
以下关于 DBFS 的描述，哪项是正确的？

- A. DBFS 将数据存储在集群 driver 节点的本地磁盘上
- B. DBFS 是对象存储上的文件系统抽象层，数据持久化在对象存储中
- C. DBFS 中的数据只有工作区管理员可以访问
- D. 集群关闭后，DBFS 中的数据会被自动清除

**答案：B**

**解析：** DBFS 是对象存储（S3/ADLS/GCS）上的抽象层，数据物理存储在对象存储中，与集群生命周期无关。A 错误：driver 本地磁盘是临时的。C 错误：默认所有工作区用户可访问。D 错误：数据在对象存储中持久化，不随集群关闭消失。

---

## 五、关键知识网络

```
Platform Ops 知识网络
======================

Notebook 环境
  |
  +-- 跨语言互操作
  |     |-- Python 变量空间（%python cell）
  |     |-- SQL 引擎命名空间（%sql cell）
  |     |-- 桥接方式：createOrReplaceTempView(), spark.sql(f"...")
  |     \-- .collect() = 分布式 -> 本地的边界
  |
  +-- 文件格式
  |     |-- 首行 = 语言注释符 + "Databricks notebook source"
  |     \-- # (Python) / // (Scala) / -- (SQL)
  |
  +-- 包管理
  |     |-- %pip install = notebook-scoped, 全节点分发
  |     |-- %sh pip install = driver only
  |     \-- Cluster UI = cluster-scoped, 需重启集群
  |
  \-- Python 基础
        \-- sys.path = 模块搜索路径列表

数据安全
  |
  +-- PII 脱敏
  |     |-- 哈希函数：sha1(40字符), sha2(64字符)
  |     |-- 属性矩阵：不可逆 x 确定性 x 固定长度 x 替换原列
  |     \-- 反面教材：uuid(非确定), hash(变长), mask(变长)
  |
  \-- DBFS 访问控制
        \-- 默认全员可访问 -> 敏感数据不应存 DBFS root

集群管理
  |
  +-- Compute Policies = 声明式参数管理（配置 + 环境变量）
  +-- Init Scripts = 命令式启动脚本（安装依赖 + 环境设置）
  \-- 两者互补：配置用 Policy，安装用 Script

模型部署
  |
  \-- MLflow pyfunc
        |-- mlflow.pyfunc.spark_udf() 加载为 Spark UDF
        |-- df.select(model(*columns).alias("pred")) 调用
        \-- *columns 解包列名列表
```

---

## 六、Anki 卡片

**Q1:** 在 Databricks notebook 中，`%python` cell 里的 Python 变量能直接在 `%sql` cell 中使用吗？
**A1:** 不能。Python 变量存在于 Python 进程内存，SQL 引擎有独立的命名空间。需要通过 `createOrReplaceTempView()` 或 `spark.sql(f"...")` 传值。

---

**Q2:** `spark.table("t").select("col").collect()` 的返回类型是什么？加上 `[x[0] for x in ...]` 之后呢？
**A2:** `.collect()` 返回 Python list of Row 对象。加上 list comprehension 后变成 Python list of strings（或对应类型的值）。数据已脱离 Spark 分布式世界。

---

**Q3:** Databricks Python notebook 导出为源文件时，第一行是什么？
**A3:** `# Databricks notebook source`。规律：语言对应的注释符号（# / // / --）+ "Databricks notebook source"。

---

**Q4:** `%pip install` 和 `%sh pip install` 的关键区别是什么？
**A4:** `%pip install` 是 notebook-scoped，自动分发到 driver + 所有 worker 节点，自动重启 Python 解释器。`%sh pip install` 只在 driver 的 shell 中执行，不分发到 worker。

---

**Q5:** Python 中哪个变量包含模块搜索路径列表？`os.path` 是做什么的？
**A5:** `sys.path` 是模块搜索路径列表。`os.path` 是路径操作工具模块（提供 join、exists、dirname 等方法），不是搜索路径。

---

**Q6:** MLflow pyfunc 模型在 Spark 中如何调用？
**A6:** 先用 `mlflow.pyfunc.spark_udf(spark, model_uri)` 加载为 UDF，然后在 `df.select("id", model(*columns).alias("predictions"))` 中调用。`*columns` 将列名列表解包为位置参数。

---

**Q7:** PII 脱敏时，`sha2` 和 `uuid` 的核心区别是什么？
**A7:** `sha2` 是确定性哈希 -- 同一输入始终产生相同输出，可用于跨表 join。`uuid()` 生成随机值，每次调用结果不同，破坏了用户标识的关联能力。

---

**Q8:** `hash()` 和 `sha1()` 都是确定性的，为什么"固定长度输出"要选 `sha1`？
**A8:** `sha1` 输出固定 40 字符十六进制字符串。`hash()` 返回整数，不同整数的字符串表示长度不同（如 42 是 2 字符，-2147483648 是 11 字符），不满足"固定长度"要求。

---

**Q9:** Compute Policies 和 Init Scripts 分别解决什么问题？
**A9:** Compute Policies 声明式地设置和强制执行 Spark 配置、系统属性、环境变量。Init Scripts 在集群启动时执行命令，安装外部依赖和配置环境。Policies 管参数，Scripts 管安装。

---

**Q10:** DBFS 中的数据存储在哪里？集群关闭后数据还在吗？
**A10:** 存储在底层对象存储（S3/ADLS/GCS）中。集群关闭后数据仍然持久存在，因为对象存储独立于计算资源。DBFS 只是抽象层。

---

**Q11:** DBFS 默认对谁可访问？
**A11:** 默认对所有工作区用户可访问，不限于管理员。因此不应在 DBFS root 中存储敏感数据。

---

**Q12:** SQL VIEW 中 `WHERE age > 17` 的效果是什么？是过滤行还是将字段置为 null？
**A12:** WHERE 子句是行级过滤。`age > 17` 保留 age >= 18 的行，完全删除不满足条件的行。不会将任何字段值置为 null。

---

## 七、掌握度自测

**1.** 以下代码执行后，`result` 的类型是什么？

```python
result = spark.table("events").filter("year = 2024").select("event_id").collect()
```

- A. PySpark DataFrame
- B. Python list of Row objects
- C. Python list of integers
- D. Pandas Series

<details><summary>答案</summary>

**B.** `.collect()` 返回 list of Row 对象。注意不是 list of integers -- 每个元素是 Row 对象（如 `Row(event_id=42)`），需要 `row[0]` 或 `row.event_id` 提取值。

</details>

---

**2.** Databricks Scala notebook 导出为源文件时，第一行是什么？

- A. `# Databricks notebook source`
- B. `// Databricks notebook source`
- C. `-- Databricks notebook source`
- D. `%scala`

<details><summary>答案</summary>

**B.** Scala 注释符号是 `//`，所以首行是 `// Databricks notebook source`。

</details>

---

**3.** 一位工程师需要在 notebook 中安装 `pyarrow` 并确保所有 worker 节点都能使用。以下哪种方式正确？

- A. `%sh pip install pyarrow`
- B. `%pip install pyarrow`
- C. `import subprocess; subprocess.run(["pip", "install", "pyarrow"])`
- D. 在 Cluster UI 中安装（不重启集群）

<details><summary>答案</summary>

**B.** `%pip install` 是唯一能 notebook-scoped 且自动分发到所有节点的方式。A 和 C 只在 driver 执行。D 通过 Cluster UI 安装后必须重启集群才能生效。

</details>

---

**4.** 需要对 `ssn`（社会安全号码）列脱敏，要求不可逆、确定性、固定长度输出。以下 SQL 中哪个函数最合适？

- A. `mask(ssn)`
- B. `hash(ssn)`
- C. `sha2(ssn, 256)`
- D. `uuid()`

<details><summary>答案</summary>

**C.** `sha2(ssn, 256)` 输出固定 64 字符，确定性，不可逆。`mask` 输出长度随输入变化。`hash` 返回整数，长度不固定。`uuid` 非确定性。

</details>

---

**5.** MLflow 模型加载为 Spark UDF 后，以下哪种调用方式正确？

- A. `model.predict(df[columns])`
- B. `df.select(model(*columns).alias("output"))`
- C. `df.apply(model, columns)`
- D. `pandas_udf(model, columns)(df)`

<details><summary>答案</summary>

**B.** MLflow pyfunc 作为 Spark UDF，在 `df.select()` 中使用 `model(*columns)` 调用，`*` 解包列名列表。

</details>

---

**6.** 以下关于 Compute Policies 的描述，哪项是正确的？

- A. Compute Policies 可以安装 Python 库到集群
- B. Compute Policies 可以设置 Spark 配置参数和环境变量
- C. Compute Policies 在集群启动时执行 shell 命令
- D. Compute Policies 存储敏感的密钥和凭证

<details><summary>答案</summary>

**B.** Compute Policies 是声明式地设置和强制执行配置参数的机制。不能安装库（A 错），不执行 shell 命令（C 是 Init Scripts），不存储密钥（D 是 Databricks Secrets）。

</details>

---

**7.** 一位数据工程师声称"DBFS 中的数据存储在集群 driver 的本地磁盘上，集群关闭后数据会丢失"。这个说法正确吗？

- A. 完全正确
- B. 部分正确：数据在本地磁盘但不会丢失
- C. 完全错误：DBFS 数据持久化在对象存储中，独立于集群生命周期
- D. 部分正确：DBFS root 在本地磁盘，但 mounted storage 在对象存储

<details><summary>答案</summary>

**C.** DBFS 是对象存储（S3/ADLS/GCS）上的抽象层。无论是 DBFS root 还是 mounted storage，底层数据都在对象存储中，与集群生命周期完全无关。

</details>

---

**8.** 在 PySpark 中，`df.withColumn("email", F.sha2("email", 256))` 和 `df.withColumn("hashed_email", F.sha2("email", 256))` 在 PII 脱敏场景下的关键区别是什么？

- A. 前者性能更好
- B. 前者替换原列，后者保留原始 PII 列
- C. 后者的哈希算法更安全
- D. 没有区别

<details><summary>答案</summary>

**B.** 前者用哈希值替换原始 `email` 列，PII 被完全移除。后者创建新列 `hashed_email`，但原始 `email` 列仍然存在，PII 仍可被访问。脱敏必须确保原始数据不残留。

</details>
