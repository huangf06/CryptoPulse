# 11. Git, Repos 与测试

**题量：4 题 | 全错（0/4）| 状态：严重薄弱**

---

## 概念框架

### Databricks Repos（Git Folders）

Databricks Repos 是 Databricks 与 Git 的集成，允许：
- 将 Git 仓库克隆到 Databricks workspace
- 在 Databricks UI 中进行分支管理、commit、push、pull
- 合并冲突解决（可视化 UI）

### Files in Repos

Files in Repos 允许在 Repos 中存放**非 notebook 文件**（.py, .txt, .yaml 等），关键能力：
- 将函数定义在 `.py` 文件中（而非 notebook）
- 用 `import` 语句导入这些模块
- 用 **pytest/unittest** 等标准测试框架进行单元测试
- 实现代码模块化、版本控制、测试与生产代码分离

### %sh 的本质

```
%sh 命令 → 在 DRIVER 节点的 shell 中执行
         → 单节点，串行处理
         → 不利用 worker 节点
         → 不利用 Spark 分布式引擎
         → 1GB 数据可能需要 20+ 分钟

正确替代：
  spark.read → 分布式读取
  spark.write → 分布式写入
  DataFrame API → 分布式处理
```

### DataFrame.transform 模式

```python
# 模块化、可测试的 ETL 最佳实践
def clean_data(df: DataFrame) -> DataFrame:
    return df.filter(col("value").isNotNull())

def add_timestamp(df: DataFrame) -> DataFrame:
    return df.withColumn("processed_at", current_timestamp())

# 链式调用
result = (raw_df
    .transform(clean_data)
    .transform(add_timestamp)
)
```

每个转换函数：
- 接收 DataFrame，返回 DataFrame（纯函数）
- 可独立单元测试
- 无副作用，不依赖外部状态
- 可自由组合

---

## 错题精析

### Q66 -- %sh 性能问题

**原题：**

> The following code has been migrated to a Databricks notebook from a legacy workload:
> The code executes successfully and provides the logically correct results, however, it takes over 20 minutes to extract and load around 1 GB of data.
>
> Which statement is a possible explanation for this behavior?

**选项：**
- A. %sh triggers a cluster restart to collect and install Git. Most of the latency is related to cluster startup time.
- B. Instead of cloning, the code should use %sh pip install so that the Python code can get executed in parallel across all nodes in a cluster.
- C. %sh does not distribute file moving operations; the final line of code should be updated to use %fs instead.
- D. Python will always execute slower than Scala on Databricks. The run.py script should be refactored to Scala.
- E. %sh executes shell code on the driver node. The code does not take advantage of the worker nodes or Databricks optimized Spark.

**我的答案：** C | **正确答案：** E

**解析：**
- `%sh` 只在 **driver 节点**上执行 shell 命令，是单节点串行操作
- 1GB 数据在单节点处理自然很慢，完全没有利用集群的并行计算能力
- C 选项虽然提到了 `%sh` 不分布式，但给出的解决方案 `%fs` 也主要在 driver 执行，且问题根源是**整个代码块**都是单节点执行（clone, run.py, mv），不仅仅是文件移动操作
- E 选项准确指出了根本原因：`%sh` = driver only = 无法利用 worker + Spark 优化

**错因：** 选了一个"半对"的选项。C 看到了分布式问题但误判了症结；E 才是根因分析。

---

### Q188 -- %sh 只在 driver 执行（重复考点）

**原题：**

> The following code has been migrated to a Databricks notebook from a legacy workload:
> The code executes successfully and provides the logically correct results, however, it takes over 20 minutes to extract and load around 1 GB of data.
>
> Which statement is a possible explanation for this behavior?

**选项：**
- A. %sh triggers a cluster restart to collect and install Git. Most of the latency is related to cluster startup time.
- B. Instead of cloning, the code should use %sh pip install so that the Python code can get executed in parallel across all nodes in a cluster.
- C. %sh does not distribute file moving operations; the final line of code should be updated to use %fs instead.
- D. %sh executes shell code on the driver node. The code does not take advantage of the worker nodes or Databricks optimized Spark.

**我的答案：** C | **正确答案：** D

**解析：**
- 与 Q66 几乎完全相同的题目（选项顺序略有不同），同样考察 `%sh` 的执行范围
- **两次都选了 C**，说明对 `%sh` 的理解有系统性偏差
- `%sh` 是 shell magic command，在 driver 的操作系统 shell 中执行，与 Spark 引擎无关
- `%fs` 是 DBFS 文件系统命令，虽然不在 shell 中执行，但也不是分布式的
- 核心区分：`%sh`/`%fs` 都是单节点操作；只有 **Spark DataFrame API** 才是分布式的

**教训：** 同一个知识点错了两次，说明不是"粗心"而是认知错误。必须修正：`%sh` = driver only。

---

### Q226 -- Files in Repos 单元测试

**原题：**

> A Data Engineer wants to run unit tests using common Python testing frameworks on Python functions defined across several Databricks notebooks currently used in production.
>
> How can the data engineer run unit tests against functions that work with data in production?

**选项：**
- A. Define and import unit test functions from a separate Databricks notebook
- B. Define and unit test functions using Files in Repos
- C. Run unit tests against non-production data that closely mirrors production
- D. Define unit tests and functions within the same notebook

**我的答案：** A | **正确答案：** B

**解析：**
- **Files in Repos** 允许将函数抽取到 `.py` 文件中，与 notebook 分离
- 这样可以用标准 Python 测试框架（pytest, unittest）直接测试这些 `.py` 文件中的函数
- A 选项虽然也涉及 notebook 导入（`%run`），但 notebook 之间的导入不支持标准测试框架的运行方式。`%run` 是 Databricks 特有的，无法与 pytest 集成
- C 选项讨论的是测试数据问题，不是测试方法问题
- D 选项将测试和代码混在一起，违反关注点分离

**关键：** 题目关键词是"common Python testing frameworks"，这意味着需要标准的 `.py` 文件，而不是 notebook。

---

### Q247 -- DataFrame.transform 模块化 ETL

**原题：**

> Which approach demonstrates a modular and testable way to use DataFrame transform for ETL code in PySpark?

**选项：**
- A. (代码选项 A)
- B. (代码选项 B)
- C. (使用 DataFrame.transform 配合纯转换函数)
- D. (代码选项 D)

**我的答案：** A | **正确答案：** C

**解析：**
- `DataFrame.transform()` 接受一个函数作为参数，该函数签名为 `(DataFrame) -> DataFrame`
- 最佳实践：每个转换是独立的纯函数，无副作用，可单独测试
- 链式调用：`df.transform(f1).transform(f2).transform(f3)` -- 清晰、可组合、可测试
- 其他选项可能使用了类方法、闭包或直接操作，不符合"模块化可测试"的要求

**这道题因 PDF 提取限制无法看到代码选项细节，但核心概念明确：** DataFrame.transform + 纯函数 = 模块化可测试 ETL。

---

## 核心对比表

| 命令/方法 | 执行位置 | 是否分布式 | 适用场景 |
|-----------|----------|-----------|----------|
| `%sh` | Driver shell | 否 | 简单 shell 命令（安装工具等） |
| `%fs` | Driver（DBFS API） | 否 | DBFS 文件操作 |
| `%pip install` | Driver + Workers | 是（自动分发） | 安装 Python 包 |
| Spark DataFrame API | Driver + Workers | 是 | 数据处理（ETL） |

| 测试方法 | 支持标准框架 | 代码分离 | 推荐程度 |
|----------|-------------|----------|----------|
| Files in Repos (.py) | pytest/unittest | 完全分离 | 最佳实践 |
| %run notebook 导入 | 不支持 | 部分分离 | 不推荐 |
| 同一 notebook 内测试 | 不方便 | 无分离 | 不推荐 |

---

## 自测清单

- [ ] `%sh` 在哪里执行？（driver 节点的 shell）
- [ ] `%sh` 执行的代码能利用 worker 节点吗？（不能）
- [ ] 1GB 数据用 `%sh` 处理为什么慢？（单节点串行，不利用 Spark 分布式引擎）
- [ ] 要在 Databricks 中用 pytest 做单元测试，应该把函数定义在哪里？（Files in Repos 的 .py 文件中）
- [ ] `%run` 导入 notebook 能与 pytest 集成吗？（不能）
- [ ] DataFrame.transform() 接受什么类型的参数？（接收 DataFrame 返回 DataFrame 的函数）
- [ ] 模块化 ETL 的最佳实践是什么？（纯函数 + DataFrame.transform 链式调用）
