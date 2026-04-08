# 11. Git, Repos 与测试 -- 深度复习辅导材料

**题量：4 题 | 全错（0/4）| 状态：严重薄弱**

---

## 一、错误模式诊断

### 模式 1："%sh 半对陷阱"（Q66, Q188 -- 同一错误犯了两次）

你两次都选了 C（"%sh 不分布式文件操作，应改用 %fs"），而正确答案是 E/D（"%sh 在 driver 执行，未利用 worker 和 Spark"）。

**根因分析：**
- 你识别出了"不分布式"这个关键词，但把症结定位在了**最后一步（文件移动）**而非**整个执行模型**
- C 选项是"局部正确"：它正确指出 %sh 不分布式，但给出的修复方案（%fs）本身也不是分布式的
- 你的思维路径是"找到一个能改的具体操作"，而考题要的是"找到根本原因的解释"
- **核心认知偏差：把"哪一行代码有问题"当成了"为什么整体慢"**

**修正规则：** 遇到性能问题的解释题，先问"执行模型是什么"，再问"哪一行有问题"。根因 > 症状。

### 模式 2：Notebook 导入 vs 标准测试框架（Q226）

选了 A（从另一个 notebook 导入测试函数），正确答案是 B（Files in Repos）。

**根因分析：**
- 题目关键词是 "common Python testing frameworks"，这直接排除了 notebook 方案
- `%run` 是 Databricks 特有的执行机制，不是 Python import，pytest 无法发现和运行 notebook 中的测试
- 你可能混淆了"代码复用"和"可测试性"：notebook 之间可以通过 %run 复用代码，但这不等于支持标准测试框架

**修正规则：** 看到 "standard/common testing frameworks" 就锁定 Files in Repos（.py 文件）。pytest 需要 .py 文件，不认 notebook。

### 模式 3：DataFrame.transform 模式识别不足（Q247）

选了 A 而非 C。虽然无法看到代码选项细节，但说明对 `DataFrame.transform()` 的标准用法不够熟悉。

**修正规则：** `df.transform(func)` 中的 func 必须是 `DataFrame -> DataFrame` 的纯函数。看到"modular and testable ETL"就选 transform + 纯函数模式。

---

## 二、子主题分解与学习方法

### 子主题 A：%sh / %fs / %pip 的执行模型

**核心知识：**

| 命令 | 执行位置 | 分布式？ | 本质 |
|------|----------|----------|------|
| `%sh` | Driver 节点的 OS shell（bash） | 否 | 等同于 SSH 到 driver 机器执行命令 |
| `%fs` | Driver 节点通过 DBFS API | 否 | Databricks 文件系统操作，单节点 |
| `%pip install` | Driver + 自动分发到 Workers | 是 | 安装 Python 包到整个集群 |
| `%run` | 当前 notebook 上下文 | 否 | 在当前 notebook 的命名空间中执行另一个 notebook |
| Spark DataFrame API | Driver 协调 + Workers 执行 | 是 | 真正的分布式计算 |

**学习方法：** 不要死记表格。理解底层原理：
- `%sh` 打开的是 driver 机器上的 bash shell。你在里面执行的一切（git clone, python script, mv）都是 Linux 单机操作，和 Spark 完全无关
- `%fs` 调用的是 DBFS REST API，driver 发起请求，数据经过 driver 中转
- 只有通过 SparkSession 提交的操作（spark.read, df.write, df.transform 等）才会被 Spark 引擎分解为 task 并分发到 worker

### 子主题 B：Files in Repos 与测试

**核心知识：**

Databricks Repos 中可以放两类文件：
1. **Notebook 文件**（.py notebook, .sql notebook 等）-- Databricks 特有格式
2. **普通文件**（.py, .yaml, .json, .txt 等）-- 标准文件，可被标准工具处理

Files in Repos 的测试价值链：
```
函数定义在 .py 文件 → 可以 import → 可以被 pytest 发现 → 可以用 pytest.fixture 等全部功能
函数定义在 notebook → 只能 %run → pytest 无法发现 → 无法用标准测试框架
```

**学习方法：** 动手验证。在 Databricks workspace 创建一个 Repo，放一个 `utils.py` 和一个 `test_utils.py`，跑一次 pytest，体会 notebook 做不到这件事。

### 子主题 C：DataFrame.transform 与模块化 ETL

**核心知识：**

`DataFrame.transform(func)` 的语义：
- `func` 签名：`(DataFrame) -> DataFrame`
- 可以接受额外参数：`df.transform(func, arg1, arg2)` -- func 签名为 `(DataFrame, arg1_type, arg2_type) -> DataFrame`
- 链式调用：`df.transform(f1).transform(f2).transform(f3)` 等价于 `f3(f2(f1(df)))`

为什么这是"模块化可测试"的：
- 每个 func 是独立的纯函数，可以单独用 pytest 测试
- 输入输出都是 DataFrame，天然支持 mock 和 assertion
- 无副作用：不修改全局状态，不依赖外部变量
- 可组合：像乐高积木一样自由拼装 pipeline

**反模式识别：**
- 把所有转换写在一个大函数里 -- 不可测试
- 用类方法封装（self.df = ...）-- 引入状态，不纯
- 在函数内部读写外部表 -- 副作用，不可测试
- 直接操作全局变量 -- 耦合，无法隔离测试

### 子主题 D：Databricks Repos Git 集成

**核心知识：**
- Repos 支持 GitHub, GitLab, Azure DevOps, Bitbucket 等主流 Git 提供商
- 操作：clone, pull, push, commit, branch, merge
- 合并冲突可在 Databricks UI 中解决
- Repos 是 workspace 级别的，每个用户有自己的 Repo 副本
- CI/CD 集成：可以通过 Databricks Repos API 在 CI pipeline 中自动更新 Repo

---

## 三、苏格拉底式教学问题集

### 子主题 A：%sh 执行模型

1. 如果你在 %sh 中执行 `python process_data.py`，这个 Python 进程运行在哪里？它能访问 Spark 的 worker 节点吗？为什么？
2. `%sh pip install numpy` 和 `%pip install numpy` 有什么区别？前者安装后 worker 节点能用吗？
3. 假设你用 `%sh curl` 下载了一个 2GB 文件到 driver 本地，然后用 `%sh python` 处理它。整个集群的 100 个 worker 在做什么？这合理吗？
4. 有人说"%fs 是分布式的因为它操作的是分布式文件系统 DBFS"。这个推理哪里有问题？
5. 在什么场景下 %sh 是合理的选择？（提示：不涉及大数据处理的场景）

### 子主题 B：Files in Repos 与测试

1. 为什么 pytest 无法测试 notebook 中定义的函数？从 pytest 的发现机制角度解释。
2. 如果团队坚持把所有代码写在 notebook 里，他们能实现自动化单元测试吗？代价是什么？
3. Files in Repos 中的 .py 文件和 notebook 中的 Python cell 有什么本质区别？
4. `%run ./utils` 和 `from utils import func` 有什么区别？哪个更接近标准 Python 生态？
5. 如果你在 Repo 中有 `src/transforms.py` 和 `tests/test_transforms.py`，你如何在 Databricks 中运行测试？

### 子主题 C：DataFrame.transform

1. `df.transform(func)` 和 `func(df)` 在结果上有区别吗？那为什么还要用 transform？
2. 如果一个转换函数内部调用了 `spark.read.table("some_table")`，它还是纯函数吗？这对测试有什么影响？
3. 如何为 `def add_tax(df: DataFrame, rate: float) -> DataFrame` 编写单元测试？你需要连接 Spark 集群吗？
4. transform 链中某个函数出错了，调试难度和一个 500 行的大函数相比如何？为什么？

---

## 四、场景判断题

### 子主题 A：%sh 执行模型

**题 A1：**
一个团队将遗留的 bash ETL 脚本迁移到 Databricks。他们用 `%sh bash etl.sh` 执行，脚本内部用 `awk/sed` 处理 CSV 文件。集群有 1 driver + 10 workers。处理 500MB 数据耗时 30 分钟。最可能的原因是什么？

A. awk/sed 不兼容 Databricks 环境，导致解析错误和重试
B. bash 脚本需要先用 %pip 安装依赖
C. %sh 只在 driver 节点执行，10 个 worker 完全空闲，未利用分布式计算
D. CSV 格式不适合 Databricks 处理，应转换为 Parquet

**答案：C**
解析：%sh 在 driver 的 shell 中执行，awk/sed 是单节点工具。10 个 worker 在整个过程中什么都没做。正确做法是用 `spark.read.csv()` 读取数据，用 DataFrame API 替代 awk/sed 的处理逻辑。D 虽然是好建议但不是"最可能的原因"。

---

**题 A2：**
以下哪个操作会利用集群的 worker 节点？

A. `%sh python process.py`
B. `%fs ls /mnt/data/`
C. `spark.read.parquet("/mnt/data/").filter(col("status") == "active").write.parquet("/mnt/output/")`
D. `%sh hadoop fs -cp /source /dest`

**答案：C**
解析：只有通过 Spark API 提交的操作才会被分发到 worker。A 是 driver shell 中的 Python 进程；B 是 driver 发起的 DBFS API 调用；D 虽然用了 hadoop 命令但仍在 driver shell 中执行。C 是完整的 Spark 读取-过滤-写入 pipeline，会利用全部 worker。

---

**题 A3：**
开发者在 notebook 中执行 `%sh pip install scikit-learn`，然后在后续 cell 中 `import sklearn`。在 driver 上 import 成功，但 UDF 在 worker 上执行时报 ModuleNotFoundError。原因是什么？

A. scikit-learn 不兼容 Spark
B. %sh pip install 只在 driver 安装，worker 没有该包
C. UDF 不支持第三方库
D. 需要重启集群才能生效

**答案：B**
解析：`%sh pip install` 是在 driver 的 shell 中执行 pip，只在 driver 本地安装。worker 节点没有执行这个命令，所以没有安装。应该用 `%pip install scikit-learn`，它会自动在所有节点安装并重启 Python 解释器。

---

### 子主题 B：Files in Repos 与测试

**题 B1：**
团队需要对 ETL pipeline 中的数据清洗函数进行自动化单元测试，要求：(1) 使用 pytest (2) 集成到 CI/CD (3) 测试覆盖率报告。最佳方案是什么？

A. 在 notebook 中编写测试，用 %run 调用被测函数
B. 将清洗函数定义在 Repos 的 .py 文件中，配套 test_*.py 文件，用 pytest 运行
C. 在 notebook 中使用 assert 语句手动验证
D. 使用 Databricks SQL 查询验证数据质量

**答案：B**
解析：三个要求（pytest、CI/CD、覆盖率）都指向标准 Python 项目结构。只有 .py 文件才能被 pytest 发现、执行、生成覆盖率报告。A 中 %run 无法与 pytest 集成；C 不是自动化测试；D 是数据质量检查不是单元测试。

---

**题 B2：**
一个 .py 文件定义在 Databricks Repo 的 `src/transforms.py` 中。notebook 中如何使用这个文件中的函数？

A. `%run ./src/transforms`
B. `%sh python src/transforms.py`
C. `from src.transforms import my_function`
D. `%fs cat /Repos/user/project/src/transforms.py`

**答案：C**
解析：Files in Repos 支持标准 Python import。`from src.transforms import my_function` 就像普通 Python 项目一样工作。A 是 notebook 级别的执行，不是 import；B 在 shell 中执行脚本不会把函数引入 notebook 命名空间；D 只是读取文件内容。

---

### 子主题 C：DataFrame.transform

**题 C1：**
以下哪段代码体现了"模块化、可测试的 ETL"最佳实践？

A.
```python
def pipeline(input_path, output_path):
    df = spark.read.parquet(input_path)
    df = df.filter(col("v").isNotNull())
    df = df.withColumn("ts", current_timestamp())
    df.write.parquet(output_path)
```

B.
```python
def remove_nulls(df): return df.filter(col("v").isNotNull())
def add_ts(df): return df.withColumn("ts", current_timestamp())

result = spark.read.parquet(path).transform(remove_nulls).transform(add_ts)
```

C.
```python
class ETL:
    def __init__(self, path):
        self.df = spark.read.parquet(path)
    def process(self):
        self.df = self.df.filter(col("v").isNotNull())
```

D.
```python
df = spark.read.parquet(path)
df.createOrReplaceTempView("raw")
result = spark.sql("SELECT * FROM raw WHERE v IS NOT NULL")
```

**答案：B**
解析：B 中每个函数是 `DataFrame -> DataFrame` 的纯函数，无副作用，可独立测试，通过 transform 链式组合。A 把读取、处理、写入混在一起，无法单独测试转换逻辑；C 引入了可变状态（self.df）；D 用 SQL 实现但没有模块化。

---

**题 C2：**
你要为 `def clean(df: DataFrame) -> DataFrame` 写单元测试。以下哪种方式最合适？

A. 在测试中连接生产 Spark 集群，读取生产表，调用 clean() 后检查结果
B. 在测试中创建一个小的 DataFrame（用 `spark.createDataFrame()`），传入 clean()，验证输出
C. 在 notebook 中手动运行 clean() 并目视检查结果
D. 用 %run 执行包含 clean() 的 notebook 后 assert

**答案：B**
解析：单元测试的原则是快速、隔离、可重复。B 用小数据集构造输入，验证输出，不依赖外部数据源。A 依赖生产数据，不可重复且有安全风险；C 不是自动化测试；D 无法与 pytest 集成。

---

## 五、关键知识网络

```
                         Databricks 代码执行生态
                                  |
                 +----------------+----------------+
                 |                |                |
            Magic Commands    Spark API      Repos/Files
                 |                |                |
         +-------+-------+       |          +-----+-----+
         |       |       |       |          |           |
        %sh     %fs    %pip   DataFrame   Notebook    .py Files
         |       |       |    .transform()    |           |
     driver   driver   all      |          %run      import
      shell   DBFS    nodes     |        (不可测试)   (可测试)
    (单节点) (单节点) (分布式)    |                      |
                              纯函数              pytest/unittest
                           DataFrame->DataFrame
                              可测试
                              可组合
```

**关键连接：**
- `%sh` 和 `%fs` 都是单节点操作，区别只是 %sh 在 OS shell，%fs 在 DBFS API
- `%pip` 是唯一一个自动分发到全部节点的 magic command
- 测试能力取决于代码载体：.py 文件支持标准测试框架，notebook 不支持
- DataFrame.transform 是连接"可测试的 .py 函数"和"Spark 分布式执行"的桥梁

---

## 六、Anki 卡片

**卡片 1**
Q: `%sh` 命令在 Databricks 集群的哪里执行？
A: 在 driver 节点的操作系统 shell（bash）中执行，是单节点操作，不利用 worker 节点，不利用 Spark 引擎。

**卡片 2**
Q: `%sh` 和 `%fs` 都是单节点操作吗？
A: 是。`%sh` 在 driver 的 OS shell 执行，`%fs` 通过 driver 调用 DBFS API。两者都不分布式。

**卡片 3**
Q: `%sh pip install` 和 `%pip install` 有什么关键区别？
A: `%sh pip install` 只在 driver 安装；`%pip install` 在 driver 和所有 worker 上安装，并自动重启 Python 解释器。

**卡片 4**
Q: 要用 pytest 测试 Databricks 中的 Python 函数，函数应该定义在哪里？
A: 定义在 Files in Repos 的 .py 文件中。pytest 只能发现和运行 .py 文件中的测试，无法处理 notebook。

**卡片 5**
Q: `%run ./other_notebook` 能与 pytest 集成吗？
A: 不能。`%run` 是 Databricks 特有的执行机制，不是标准 Python import，pytest 无法发现或运行通过 %run 导入的代码。

**卡片 6**
Q: `DataFrame.transform(func)` 中的 func 签名是什么？
A: `func(df: DataFrame) -> DataFrame`，接收一个 DataFrame，返回一个新的 DataFrame。也可接受额外参数。

**卡片 7**
Q: 为什么 `df.transform(f1).transform(f2)` 比把 f1、f2 逻辑写在一个大函数里更好？
A: 每个函数可独立单元测试、无副作用、可自由组合。大函数无法隔离测试各个转换步骤。

**卡片 8**
Q: 遗留 bash 脚本用 %sh 在 Databricks 执行，处理 1GB 数据需要 20+ 分钟。根本原因是什么？
A: %sh 只在 driver 节点执行，整个脚本是单节点串行处理，完全没有利用集群的 worker 节点和 Spark 分布式引擎。

**卡片 9**
Q: Files in Repos 支持哪些文件类型？
A: 支持任意非 notebook 文件：.py, .yaml, .json, .txt, .cfg 等。这些文件可以被 import、读取、用标准工具处理。

**卡片 10**
Q: 模块化可测试 ETL 的"三要素"是什么？
A: (1) 纯函数（DataFrame -> DataFrame，无副作用） (2) DataFrame.transform() 链式调用 (3) 函数定义在 .py 文件中（可被 pytest 测试）

**卡片 11**
Q: 在 Databricks 中，哪些操作真正利用了 worker 节点进行分布式计算？
A: 通过 Spark API 提交的操作：spark.read, df.write, df.filter, df.transform 等 DataFrame/Dataset 操作。%sh、%fs、%run 都不是分布式的。

---

## 七、掌握度自测

**题 1：** 以下哪个说法正确？
A. %fs 是分布式操作，因为 DBFS 是分布式文件系统
B. %sh 在所有节点上并行执行命令
C. %pip install 只在 driver 安装包
D. %sh 只在 driver 节点的 shell 中执行

**答案：D** -- %sh 是 driver-only 的 shell 执行。A 错在"分布式文件系统"不等于"分布式操作"；B 完全错误；C 描述的是 %sh pip install 而非 %pip install。

---

**题 2：** 开发者想用 pytest 对 Databricks 中的数据转换函数进行自动化测试。以下哪种做法可行？
A. 在 notebook cell 中写 `def test_func(): assert ...`，然后 `%sh pytest`
B. 将函数定义在 Repo 的 .py 文件中，编写 test_*.py，运行 pytest
C. 用 %run 导入被测 notebook，然后在另一个 notebook 中 assert
D. 在 notebook 中 `import unittest` 并手写 test runner

**答案：B** -- 只有 .py 文件才能被 pytest 正常发现和执行。A 中 pytest 无法发现 notebook cell 中的函数；C 的 %run 不是 import；D 虽然技术上可行但不是标准做法且题目问的是 pytest。

---

**题 3：** 以下代码体现了什么设计模式？
```python
def remove_duplicates(df): return df.dropDuplicates()
def cast_types(df): return df.withColumn("id", col("id").cast("int"))
result = raw_df.transform(remove_duplicates).transform(cast_types)
```
A. 装饰器模式
B. 策略模式
C. 管道/链式转换模式（Pipeline pattern with DataFrame.transform）
D. 观察者模式

**答案：C** -- DataFrame.transform 实现的是管道模式，每个纯函数是管道中的一个步骤，通过链式调用组合。

---

**题 4：** 以下哪种情况下 %sh 是合理的选择？
A. 处理 10GB CSV 数据的 ETL
B. 在 driver 上安装一个 Linux 系统工具（如 `apt-get install jq`）
C. 将数据从一个 DBFS 路径复制到另一个
D. 运行一个处理大量数据的 Python 脚本

**答案：B** -- %sh 适合不涉及大数据处理的系统级操作，如安装工具、查看系统信息、管理进程等。A/C/D 都涉及数据处理，应该用 Spark API。

---

**题 5：** `from src.utils import clean_data` 在 Databricks 中能工作的前提是什么？
A. src/utils.py 存在于 DBFS 中
B. src/utils.py 存在于 Databricks Repos（Git Folders）中
C. 已经用 %run 执行过 src/utils notebook
D. 已经用 %pip install 安装了 src 包

**答案：B** -- Files in Repos 支持标准 Python import。文件必须在当前 Repo 的目录结构中，Databricks 会自动将 Repo 根目录加入 Python path。

---

**题 6：** 一个 transform 函数 `def enrich(df): df.write.saveAsTable("tmp"); return df` 违反了什么原则？
A. 类型安全
B. 纯函数/无副作用
C. 单一职责
D. 开闭原则

**答案：B** -- 函数内部写入了表（副作用），不再是纯函数。纯函数只应该接收 DataFrame 并返回新的 DataFrame，不应该产生任何外部可观测的变化。这使得函数不可测试（测试时会真的写入表）、不可重复执行。

---

**题 7：** %sh pip install pandas 执行成功后，以下哪个场景会失败？
A. 在后续 cell 中 `import pandas`
B. 在 Spark UDF 中使用 pandas（worker 执行）
C. 在 driver 上运行的 Python 函数中使用 pandas
D. 在同一 notebook 的后续 %python cell 中使用 pandas

**答案：B** -- %sh pip install 只在 driver 安装。UDF 在 worker 上执行，worker 没有安装 pandas，会报 ModuleNotFoundError。A/C/D 都在 driver 上执行所以没问题。

---

**题 8：** 关于 Databricks Repos，以下哪个说法是错误的？
A. 支持 GitHub, GitLab, Azure DevOps 等 Git 提供商
B. 每个用户有自己独立的 Repo 副本
C. Repos 中的 notebook 修改会自动 commit 并 push 到远程仓库
D. 可以在 Databricks UI 中解决合并冲突

**答案：C** -- Repos 不会自动 commit/push。用户需要手动（或通过 API）执行 commit 和 push 操作。这是 Git 的基本工作模式：修改是本地的，必须显式提交。
