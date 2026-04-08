# 12. 平台操作与工具

**题量：13 题（含 2 道 "other" 类别）| 全错（0/13）| 状态：严重薄弱**

---

## 概念框架

### Notebook 操作核心知识

**1. Python 变量与 SQL 的跨语言互操作**
```
%python cell → Python 变量存在于 Python 内存空间
%sql cell    → SQL 引擎不认识 Python 变量

跨语言传值方法：
  - createOrReplaceTempView() → SQL 可查询
  - spark.sql(f"SELECT ... WHERE col IN {tuple(my_list)}")
  - widget 传参
```

**2. Notebook 文件格式**
```
Python notebook 导出为源文件时：
  第一行固定是：# Databricks notebook source
  
  # 是 Python 注释符号
  // 是 Java/Scala 注释（用于 Scala notebook）
  -- 是 SQL 注释（用于 SQL notebook）
```

**3. 包安装方式对比**

| 方式 | 作用域 | 分发范围 | 需重启？ |
|------|--------|----------|----------|
| `%pip install` | Notebook 级别 | Driver + 所有 Workers | 自动重启 Python 解释器 |
| `%sh pip install` | Shell 级别 | 仅 Driver | 不自动分发 |
| Cluster UI 安装 | Cluster 级别 | 所有 Notebook | 需重启集群 |

### MLflow 模型调用

```python
# MLflow pyfunc 模型加载后可直接作为 Spark UDF 使用
import mlflow
model = mlflow.pyfunc.spark_udf(spark, model_uri="...")

# 正确调用方式：在 select 中使用，*columns 解包列名列表
df.select("customer_id", model(*columns).alias("predictions"))
```

### PII 数据脱敏

| 函数 | 输出 | 固定长度 | 确定性 | 适用场景 |
|------|------|----------|--------|----------|
| `sha1(col)` | 40 字符十六进制 | 固定 | 是 | 固定长度 + 不同值不同输出 |
| `sha2(col, 256)` | 64 字符十六进制 | 固定 | 是 | 同上，更安全 |
| `F.sha2(col)` | PySpark 版本 | 固定 | 是 | PySpark 中使用 |
| `hash(col)` | 整数 | 非固定长度字符串 | 是 | 不满足"同长度"要求 |
| `mask(col)` | 字符替换 | 随输入变化 | 否 | 不满足"同长度"要求 |
| `F.expr("uuid()")` | 随机 UUID | 固定 | 否（不确定性） | 不保留唯一标识 |

### 集群与环境管理

**Compute Policies：** 集中定义和强制执行 Spark 配置、系统属性、环境变量
**Init Scripts：** 集群启动时执行，安装外部依赖和自定义环境设置

### DBFS 架构

```
DBFS（Databricks File System）
  = 对象存储（S3/ADLS/GCS）上的文件系统抽象层
  = 提供类 Unix 的文件操作接口（ls, cp, rm, mkdirs）
  = 数据持久化在对象存储中（不是 driver 的临时磁盘）
  = 默认对所有工作区用户可访问（不仅限管理员）
```

---

## 错题精析

### Q9 -- Python 变量 vs SQL 引用

**原题：**

> A junior member of the data engineering team is exploring the language interoperability of Databricks notebooks. The intended outcome of the below code is to register a view of all sales that occurred in countries on the continent of Africa that appear in the geo_lookup table.
>
> Before executing the code, running SHOW TABLES on the current database indicates the database contains only two tables: geo_lookup and sales.

**Cmd 1:**
```python
%python
countries_af = [x[0] for x in
    spark.table("geo_lookup").filter("continent='AF'").select("country").collect()]
```

**Cmd 2:**
```sql
%sql
CREATE VIEW sales_af AS
  SELECT *
  FROM sales
  WHERE city IN countries_af
  AND CONTINENT = "AF"
```

> Which statement correctly describes the outcome of executing these command cells in order in an interactive notebook?

**选项：**
- A. Both commands will succeed. Executing show tables will show that countries_af and sales_af have been registered as views.
- B. Cmd 1 will succeed. Cmd 2 will search all accessible databases for a table or view named countries_af: if this entity exists, Cmd 2 will succeed.
- C. Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable representing a PySpark DataFrame.
- D. Both commands will fail. No new variables, tables, or views will be created.
- E. Cmd 1 will succeed and Cmd 2 will fail. countries_af will be a Python variable containing a list of strings.

**我的答案：** B | **正确答案：** E

**解析：**
- Cmd 1：`.collect()` 将数据拉到 driver，list comprehension 提取第一个字段 -> 得到 **Python 字符串列表**
- Cmd 2：`%sql` cell 中 SQL 引擎不认识 Python 变量，`countries_af` 会被当作列名或表名解析 -> 报错
- C 选项错在说 countries_af 是 PySpark DataFrame（实际是 list，因为 `.collect()` + list comprehension 已经脱离了 Spark）
- 跨语言传值需要 `createOrReplaceTempView()` 或 `spark.sql(f"...")`

---

### Q54 -- Python sys.path

**原题：**

> Which Python variable contains a list of directories to be searched when trying to locate required modules?

**选项：**
- A. importlib.resource_path
- B. sys.path
- C. os.path
- D. pypi.path
- E. pylib.source

**我的答案：** D | **正确答案：** B

**解析：**
- `sys.path` 是 Python 搜索模块的目录列表，可以用 `sys.path.append()` 添加自定义路径
- `os.path` 是路径操作模块（join, exists, dirname），不是搜索路径列表
- `pypi.path` 和 `pylib.source` 不存在

**纯 Python 基础题，不应该错。**

---

### Q98 -- View WHERE 过滤行而非置 null（other 类别）

**原题：**

> A table named user_ltv is being used to create a view that will be used by data analysts on various teams. The user_ltv table has the following schema: email STRING, age INT, ltv INT
>
> An analyst who is not a member of the auditing group executes: SELECT * FROM user_ltv_no_minors
>
> Which statement describes the results returned by this query?

**选项：**
- A. All columns will be displayed normally for those records that have an age greater than 17; records not meeting this condition will be omitted.
- B. All age values less than 18 will be returned as null values, all other columns will be returned with the values in user_ltv.
- C. All values for the age column will be returned as null values, all other columns will be returned with the values in user_ltv.
- D. All records from all columns will be displayed with the values in user_ltv.
- E. All columns will be displayed normally for those records that have an age greater than 18; records not meeting this condition will be omitted.

**我的答案：** E | **正确答案：** A

**解析：**
- View 中的 WHERE 条件是 `age > 17`（即 age >= 18），过滤掉不满足条件的**行**，而不是将字段置为 null
- A 和 E 的区别在于边界：`age > 17` 意味着 18 岁**包含**，`age > 18` 意味着 18 岁**排除**
- 我选了 E（> 18），但正确是 A（> 17 即 >= 18），这是**边界条件的误读**
- 更根本的：WHERE 是行级过滤，不是列值 null 化（排除 B/C 选项）

---

### Q102 / Q175 -- Databricks Notebook 文件首行格式（重复考点）

**原题：**

> What is the first line of a Databricks Python notebook when viewed in a text editor?

**选项：**
- A. %python
- B. // Databricks notebook source
- C. # Databricks notebook source
- D. -- Databricks notebook source
- E. # MAGIC %python

**Q102 我的答案：** A | **Q175 我的答案：** D | **正确答案：** C

**解析：**
- Python notebook 导出为源文件时，第一行固定是 `# Databricks notebook source`
- `#` 是 Python 注释符号，这行是注释，标识文件身份
- `%python` 是 notebook cell 中的 magic command，用于在非 Python 默认语言的 notebook 中切换语言
- `//` 是 Scala 注释，`--` 是 SQL 注释

**两次答了不同的错误答案（A 和 D），说明对这个知识点完全没有记忆，而不是混淆。**

---

### Q105 -- MLflow 模型在 Spark 中的调用方式

**原题：**

> The data science team has created and logged a production model using MLflow. The model accepts a list of column names and returns a new column of type DOUBLE.
>
> Which code block will output a DataFrame with the schema "customer_id LONG, predictions DOUBLE"?

**选项：**
- A. df.map(lambda x:model(x[columns])).select("customer_id, predictions")
- B. df.select("customer_id", model(*columns).alias("predictions"))
- C. model.predict(df, columns)
- D. df.select("customer_id", pandas_udf(model, columns).alias("predictions"))
- E. df.apply(model, columns).select("customer_id, predictions")

**我的答案：** E | **正确答案：** B

**解析：**
- MLflow 加载的 pyfunc 模型可以直接作为 Spark UDF 调用
- `model(*columns)` 用 `*` 解包列名列表传入 UDF，返回新列
- `.alias("predictions")` 给输出列命名
- E 选项的 `df.apply()` 是 Pandas DataFrame 的方法，PySpark DataFrame 没有
- D 选项 `pandas_udf(model, columns)` 语法错误，pandas_udf 需要函数和返回类型

---

### Q111 / Q174 -- %pip install 作用域（重复考点）

**原题：**

> Which describes a method of installing a Python package scoped at the notebook level to all nodes in the currently active cluster?

**Q111 选项：**
- A. Run source env/bin/activate in a notebook setup script
- B. Use b in a notebook cell
- C. Use %pip install in a notebook cell
- D. Use %sh pip install in a notebook cell
- E. Install libraries from PyPI using the cluster UI

**Q111 我的答案：** E | **Q174 我的答案：** D | **正确答案：** C

**解析：**
- `%pip install` = notebook 级别，自动分发到 driver + 所有 worker 节点，自动重启 Python 解释器
- `%sh pip install` = 只在 driver 的 shell 执行，worker 节点上不会安装
- Cluster UI 安装 = cluster 级别（对所有 notebook 生效），需重启集群，不是 notebook 级别
- 题目关键词："notebook level" + "all nodes" = `%pip install`

**两次错了不同方向（E: cluster UI, D: %sh pip），说明没有建立 %pip 的核心认知。**

---

### Q126 -- Python 变量与 Spark 视图的跨语言互操作（Q9 变体）

**原题：** 与 Q9 几乎完全相同。

**我的答案：** C（PySpark DataFrame） | **正确答案：** D（Python list of strings）

**解析：**
- 又选错了类型判断：`.collect()` + list comprehension 的结果是 **Python list**，不是 PySpark DataFrame
- `.collect()` 是 action，将分布式数据拉到 driver 变成 Python Row 列表
- list comprehension `[x[0] for x in ...]` 进一步提取为纯 Python 字符串列表
- DataFrame 是 Spark 分布式数据结构，经过 `.collect()` 后就不是了

---

### Q184 -- DBFS 是对象存储的文件系统抽象层（other 类别）

**原题：**

> Two of the most common data locations on Databricks are the DBFS root storage and external object storage mounted with dbutils.fs.mount().
>
> Which of the following statements is correct?

**选项：**
- A. DBFS is a file system protocol that allows users to interact with files stored in object storage using syntax and guarantees similar to Unix file systems.
- B. By default, both the DBFS root and mounted data sources are only accessible to workspace administrators.
- C. The DBFS root is the most secure location to store data, because mounted storage volumes must have full public read and write permissions.
- D. The DBFS root stores files in ephemeral block volumes attached to the driver, while mounted directories will always persist saved data to external storage between sessions.

**我的答案：** B | **正确答案：** A

**解析：**
- DBFS 是**对象存储上的文件系统抽象层**，底层数据在 S3/ADLS/GCS 中，提供类 Unix 操作接口
- B 错误：DBFS 默认对所有工作区用户可访问，不仅限管理员
- C 错误：挂载外部存储不需要公开权限，可用 IAM 角色/服务主体
- D 错误：DBFS root 的数据也持久化在对象存储中，不是 driver 的临时存储

---

### Q292 -- Compute Policies 与 Init Scripts（pending，多选）

**原题：**

> A data engineer needs to productionize a new Spark application written by a teammate. This application has numerous external dependencies, including libraries, and requires custom environment variables and Spark configuration parameters to be set.
>
> Which two methods will help the data engineer accomplish the task? (Choose two.)

**选项：**
- A. Install libraries on DBFS
- B. Add libraries to compute policies
- C. Use secrets in init scripts to store configuration data
- D. Use compute policies to set system properties, environment variables, and Spark configuration parameters.
- E. Create init scripts on DBFS.

**我的答案：** X（未答） | **正确答案：** DE

**解析：**
- **D: Compute Policies** -- 允许集中定义和强制执行 Spark 配置参数、系统属性和环境变量，确保生产环境设置一致
- **E: Init Scripts** -- 在集群启动时执行，用于安装外部依赖（库）和自定义环境设置，适合复杂依赖管理
- A 错误：在 DBFS 上放库文件不是标准的库安装方式
- B 错误：Compute Policies 不直接管理库安装
- C 错误：init scripts 的用途不是存储配置数据（那是 secrets 的功能），题目说的是设置环境

---

### Q297 -- PII 脱敏（PySpark sha2）（pending）

**原题：**

> A data engineer is reviewing the PySpark code to copy a part of the production dataset to the sandbox environment, and needs to be sure that no PII (Personally Identifiable Information) data is being copied.
>
> After checking the sales table, the data engineer notices that it has user emails as the only PII data included as well as being the only column to identify the user.
>
> Which anonymized code should be used to achieve the required outcome?

**选项：**
- A. df.withColumn("user_email", F.expr("uuid()"))
- B. df.withColumn("user_email", F.sha2("user_email"))
- C. df.withColumn("hashed_email", sha2("user_email"))
- D. df.withColumn("user_email", F.regexp_replace("user_email", "@*", "@anonymized.com"))

**我的答案：** C | **正确答案：** B

**解析：**
- B 正确：用 `F.sha2()` 哈希 email 列，替换原列名，保留为唯一标识（确定性哈希），PII 不可逆
- C 错误：创建了新列 `hashed_email` 但没有删除原始 `user_email` 列，PII 仍然存在！而且 `sha2` 未加 `F.` 前缀可能报错
- A 错误：`uuid()` 是随机值，失去了用户标识的唯一性，无法用于 join 或分析
- D 错误：正则替换只是简单替换域名，仍可能泄露用户名部分

**关键：** 脱敏必须同时满足：(1) 不可逆 (2) 保持确定性唯一性 (3) 替换原列（不留原始 PII）。

---

### Q313 -- 固定长度哈希脱敏（SQL sha1）

**原题：**

> A data engineer is masking the provided data column containing the email address. The goal is to have an output of the same length for all rows, while keeping different outputs for different values.
>
> Which SQL function should be used to achieve this?

**选项：**
- A. hash(email)
- B. mask(email, '?')
- C. sha1('email')
- D. sha2(email, 0)

**我的答案：** X（未答） | **正确答案：** C

**解析：**
- 要求两个条件：(1) 所有行输出长度相同 (2) 不同值不同输出
- SHA-1 产生固定 40 字符十六进制字符串，碰撞概率极低，满足两个条件
- `hash()` 返回整数，不是固定长度字符串
- `mask()` 只是字符替换，输出长度随输入变化
- `sha2(email, 0)` 等价于 SHA-256（64 字符），也满足条件，但题目答案选 C

**注意 sha1 vs sha2 的题目答案差异可能与考试版本有关，核心是理解哈希函数的固定长度特性。**

---

## 核心对比表

### 包安装方式

| 方式 | 作用域 | 分发到 Workers | 需重启 |
|------|--------|---------------|--------|
| `%pip install` | Notebook | 自动分发 | 自动重启解释器 |
| `%sh pip install` | Driver shell | 不分发 | 不自动 |
| Cluster UI | Cluster | 自动 | 需重启集群 |

### PII 脱敏函数

| 函数 | 输出长度 | 确定性 | 可逆 | 保留唯一性 |
|------|----------|--------|------|-----------|
| sha1 | 固定40字符 | 是 | 否 | 是 |
| sha2 | 固定64字符 | 是 | 否 | 是 |
| hash | 整数（变长） | 是 | 否 | 是 |
| uuid | 固定36字符 | 否 | N/A | 否 |
| mask | 变长 | 否 | 否 | 否 |

### 跨语言互操作

| Python 操作结果 | SQL 可见？ | 正确传值方式 |
|----------------|-----------|-------------|
| Python list/string | 不可见 | spark.sql(f"...") |
| PySpark DataFrame | 不可见 | createOrReplaceTempView() |
| Temp View (注册后) | 可见 | 直接 SQL 查询 |

---

## 自测清单

- [ ] Python 变量（list 或 DataFrame）能直接在 %sql cell 中使用吗？（不能）
- [ ] `.collect()` + list comprehension 的结果类型是什么？（Python list，不是 DataFrame）
- [ ] 跨语言传值的正确方式是什么？（createOrReplaceTempView 或 spark.sql(f"...")）
- [ ] Databricks Python notebook 源文件第一行是什么？（# Databricks notebook source）
- [ ] `%pip install` 和 `%sh pip install` 的关键区别是什么？（%pip 分发到所有节点，%sh 只在 driver）
- [ ] MLflow pyfunc 模型在 Spark 中怎么调用？（model(*columns) 在 df.select 中使用）
- [ ] `sys.path` 是什么？（Python 搜索模块的目录列表）
- [ ] sha1 的输出特征是什么？（固定 40 字符，不同输入不同输出）
- [ ] PySpark 中脱敏 email 列应该用什么？（F.sha2("user_email")，替换原列）
- [ ] 为什么不能用 uuid() 脱敏？（随机值，失去确定性唯一标识）
- [ ] DBFS 的数据存储在哪里？（对象存储，不是 driver 临时磁盘）
- [ ] DBFS 默认谁能访问？（所有工作区用户）
- [ ] Compute Policies 能设置什么？（Spark 配置、系统属性、环境变量）
- [ ] Init Scripts 的作用是什么？（集群启动时安装依赖、设置环境）
- [ ] View 中 WHERE age > 17 过滤的结果是什么？（age >= 18 的行保留，其余行删除，不是置 null）
