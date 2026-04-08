# 10. Databricks CLI, API 与部署 -- 深度复习辅导材料

**题量：6 题 | 全错（0/6）| 优先级：最高**

---

## 一、错误模式诊断

### 1. 命令名称混淆（Q68, Q232）

两道题的错误本质相同：对 Databricks CLI 的子命令组和操作动词记忆不准确。

- Q68：将"上传文件到 DBFS"（文件系统操作）误判为 `libraries`（集群库管理操作）。根本原因是没有区分"把文件放到存储系统"和"在集群上安装依赖"这两个完全不同的动作。
- Q232：选了不存在的 `compute` 子命令组。CLI 中管理集群的子命令组是 `clusters`，不是 `compute`（`compute` 是 Terraform provider 和部分 SDK 的术语，CLI 中不用）。

**模式总结：** 你在 CLI 命令上依赖直觉推测而非精确记忆，容易被"听起来合理"的干扰项骗到。

### 2. 工作流步骤遗漏（Q263）

Asset Bundle 接管现有 job 需要三步：`generate` -> `bind` -> `deploy`。你跳过了 `bind`，直接从手动创建 YAML 到 `deploy`。这暴露两个问题：
- 不知道 `bundle generate` 命令的存在（选了手动创建 YAML）
- 不知道 `bind` 是"接管"而非"新建"的关键区别

**模式总结：** 对 Asset Bundles 这个较新的功能体系完全陌生，需要从零建立认知。

### 3. 安全机制细节盲区（Q46）

选了"Secrets 存储在 Hive Metastore"这个完全错误的选项，说明对 Secrets 的存储和保护机制缺乏了解。正确答案（逐字符遍历绕过 REDACTED 保护）是一个经典的已知限制。

**模式总结：** 安全相关的题需要记忆具体的实现细节和已知漏洞，不能靠推理。

### 4. 参数顺序记忆缺失（Q295）

`put-secret SCOPE KEY` 和 `dbutils.secrets.get(scope, key)` 的参数顺序都是 scope 在前、key 在后。这道题未作答，说明考试时对这个知识点完全没有信心。

### 5. 过度激进的解决方案（Q316）

Git 合并冲突选了"删除文件重建"，这是最极端的方案。正确做法是在 UI 中手动解决冲突。这个错误更像是考试心态问题——遇到不确定的题倾向于选"彻底"的方案。

---

## 二、子主题分解与学习方法

### 子主题 A：CLI 命令体系

**核心知识：** 记住以下子命令组及其职责边界

| 子命令组 | 职责 | 典型操作动词 |
|----------|------|-------------|
| `fs` | DBFS 文件系统操作 | `cp`, `ls`, `rm`, `mkdirs` |
| `clusters` | 集群生命周期管理 | `create`, `delete`, `list`, `get` |
| `jobs` | Job 生命周期管理 | `create`, `list`, `run-now`, `delete` |
| `secrets` | 密钥管理 | `create-scope`, `put-secret`, `list-secrets` |
| `workspace` | Notebook/文件夹管理 | `import`, `export`, `list` |
| `pipelines` | DLT Pipeline 管理 | `create`, `get`, `start`, `stop` |
| `bundle` | Asset Bundle 管理 | `generate`, `deploy`, `bind` |

**学习方法：** 不要死记列表，而是建立"操作对象 -> 子命令组"的映射。问自己：这个操作的对象是什么？文件 -> `fs`，集群 -> `clusters`，密钥 -> `secrets`。

**关键区分：**
- 上传 .whl 文件到 DBFS = 文件操作 = `fs cp`
- 在集群上安装 .whl 库 = 集群库管理 = `libraries install`
- 操作动词遵循 CRUD 模式：`create`, `get/list`, `delete`（没有 `add`）

### 子主题 B：Secrets 管理

**核心知识：**
1. 参数顺序：`put-secret SCOPE KEY`，`dbutils.secrets.get(scope, key)` -- 都是 scope 在前
2. ACL 权限粒度：scope 级别（不能对单个 key 设权限）
3. 安全限制：`print(secret)` 显示 `[REDACTED]`，但 `for c in secret: print(c)` 泄露明文
4. REST API 只能列出 key 名称，不能返回明文值

**学习方法：** 记忆口诀"容器在前，条目在后"。scope 是容器（文件夹），key 是条目（文件），描述路径时自然是先说文件夹再说文件。

### 子主题 C：Asset Bundles 工作流

**核心知识：接管现有 job 的三步流程**

```
1. bundle generate job --existing-job-id <ID>
   -> 自动生成 YAML + 下载引用文件

2. bundle deployment bind
   -> 建立 bundle resource 与现有 job 的持久链接

3. bundle deploy
   -> 部署更新（更新已绑定的 job，而非创建新 job）
```

**关键概念：** `bind` 的作用是告诉 Databricks "这个 bundle resource 对应的是那个已经存在的 job"。没有 bind，deploy 不知道要更新哪个 job，只能创建新的。

**学习方法：** 类比 Git：`generate` 相当于 `git clone`（把现有东西拉下来），`bind` 相当于 `git remote add`（建立对应关系），`deploy` 相当于 `git push`（推送更新）。

### 子主题 D：Git Folders 冲突解决

**核心知识：** Databricks Git Folders UI 提供完整的可视化冲突解决能力，操作流程与标准 Git 工具一致：
1. 打开冲突文件，查看 conflict markers
2. 手动选择要保留的代码
3. 移除 `<<<<<<<`, `=======`, `>>>>>>>` 标记
4. 标记冲突已解决
5. 完成合并提交

**学习方法：** 记住一个原则——Databricks 在 Git 操作上不搞特殊化，遵循标准 Git 工作流。冲突解决 = 手动编辑 + 标记 resolved，不需要任何极端操作。

---

## 三、苏格拉底式教学问题集

### 子主题 A：CLI 命令体系

1. `databricks fs cp` 和 `databricks libraries install` 的操作对象分别是什么？为什么上传 .whl 到 DBFS 不用 `libraries`？
2. CLI 中为什么用 `clusters` 而不是 `compute` 作为子命令组名？这两个术语分别在 Databricks 生态的哪些地方出现？
3. 如果需要在 DBFS 上创建一个目录然后上传文件，完整的命令序列是什么？
4. `databricks workspace import` 和 `databricks fs cp` 有什么区别？分别用于什么场景？
5. CLI 的操作动词为什么是 `create/delete/list` 而不是 `add/remove/show`？这跟 REST API 的设计有什么关系？

### 子主题 B：Secrets 管理

1. 为什么 Databricks 选择在 scope 级别而非 key 级别设置 ACL？这样设计的优缺点是什么？
2. `[REDACTED]` 保护为什么能被逐字符遍历绕过？这说明保护机制是在哪一层实现的？
3. 如果你需要在一个 notebook 中使用来自两个不同 scope 的 secret，代码怎么写？
4. `databricks secrets list-secrets` 返回的是 key 名称还是 key 值？为什么这样设计？

### 子主题 C：Asset Bundles 工作流

1. 如果跳过 `bind` 直接 `deploy`，会发生什么？为什么 Databricks 不自动根据 job 名称匹配？
2. `bundle generate` 生成的 YAML 文件包含哪些内容？"下载引用文件"具体指什么？
3. 如果一个 job 已经被一个 bundle bind 了，另一个 bundle 能否再 bind 同一个 job？
4. `bundle deploy` 和 `jobs create` 的区别是什么？Asset Bundles 解决了什么 `jobs create` 解决不了的问题？
5. 为什么接管现有 job 需要 `generate` 而不是直接手动写 YAML？

### 子主题 D：Git Folders 冲突解决

1. 在 Databricks Git Folders 中解决冲突和在本地 IDE 中解决冲突，流程有什么区别？
2. 为什么 `git push --force` 不是解决合并冲突的正确方式？它实际上做了什么？
3. 如果两个人同时修改了同一个 notebook 的不同 cell，会产生冲突吗？

---

## 四、场景判断题

### 子主题 A：CLI 命令体系

**A1.** 一个数据工程师需要将本地的 `etl_utils-1.0-py3-none-any.whl` 上传到 DBFS 的 `/mnt/libs/` 路径下。应该使用哪个命令？

A. `databricks libraries install --whl /mnt/libs/etl_utils-1.0-py3-none-any.whl`
B. `databricks fs cp etl_utils-1.0-py3-none-any.whl dbfs:/mnt/libs/`
C. `databricks workspace import etl_utils-1.0-py3-none-any.whl /mnt/libs/`
D. `databricks clusters install-library --whl etl_utils-1.0-py3-none-any.whl`

**答案：B**
- 上传文件到 DBFS 是文件系统操作，用 `fs cp`
- A 是在集群上安装库，不是上传文件
- C 是导入 notebook/文件到 workspace，不是 DBFS
- D 的命令格式不存在

---

**A2.** 数据工程师想查看当前工作区所有正在运行的集群的信息。应该使用哪个命令？

A. `databricks compute list --status RUNNING`
B. `databricks clusters list`
C. `databricks clusters get --all`
D. `databricks workspace list /clusters`

**答案：B**
- `clusters list` 列出所有集群（可通过过滤器筛选状态）
- A 使用了不存在的 `compute` 子命令组
- C 的 `get` 用于获取单个集群详情，需要 cluster-id
- D 完全错误，workspace 是管理 notebook 的

---

**A3.** 需要删除 DBFS 上 `/data/temp/` 目录及其所有内容。正确命令是？

A. `databricks fs rm dbfs:/data/temp/`
B. `databricks fs rm -r dbfs:/data/temp/`
C. `databricks fs delete dbfs:/data/temp/ --recursive`
D. `databricks workspace rm -r /data/temp/`

**答案：B**
- `fs rm -r` 递归删除目录，与 Linux 的 `rm -r` 语法一致
- A 没有 `-r` 标志，不能删除非空目录
- C 的 `delete` 不是正确的操作动词（应为 `rm`）
- D 是 workspace 操作，不是 DBFS 操作

---

### 子主题 B：Secrets 管理

**B1.** 数据工程师需要为团队设置一个存储数据库连接信息的 secret scope，包含 host、port、password 三个 key。团队成员只需要读取权限，团队负责人需要写入权限。以下哪个方案正确？

A. 创建三个 scope（host_scope, port_scope, password_scope），每个 scope 一个 key，分别设置 ACL
B. 创建一个 scope，存三个 key，在 scope 级别设置 ACL（团队成员 Read，负责人 Write）
C. 创建一个 scope，存三个 key，对 password key 单独设置更严格的 ACL
D. 创建一个 scope，将所有信息合并为一个 JSON 字符串存为一个 key

**答案：B**
- ACL 在 scope 级别设置，不能对单个 key 设权限（排除 C）
- 一个 scope 存多个相关的 key 是最佳实践（排除 A 的过度拆分）
- D 虽然技术上可行，但违反了每个 secret 独立管理的设计意图

---

**B2.** 以下哪段代码会泄露 secret 的明文值？

A. `print(dbutils.secrets.get(scope="s", key="k"))`
B. `display(dbutils.secrets.get(scope="s", key="k"))`
C. `secret = dbutils.secrets.get(scope="s", key="k"); [print(c) for c in secret]`
D. `spark.conf.set("key", dbutils.secrets.get(scope="s", key="k")); print(spark.conf.get("key"))`

**答案：C**
- A 和 B 都会显示 `[REDACTED]`
- C 逐字符遍历打印，绕过了字符串级别的 REDACTED 保护
- D 中 `spark.conf.get()` 同样受 REDACTED 保护

---

**B3.** 创建一个名为 `prod-db` 的 scope 并存入 key 为 `api-token` 的 secret，然后在 notebook 中读取。正确的命令和代码组合是？

A. `databricks secrets create-scope prod-db` + `databricks secrets put-secret api-token prod-db` + `dbutils.secrets.get(key="api-token", scope="prod-db")`
B. `databricks secrets create-scope prod-db` + `databricks secrets put-secret prod-db api-token` + `dbutils.secrets.get(scope="prod-db", key="api-token")`
C. `databricks secrets new-scope prod-db` + `databricks secrets add-secret prod-db api-token` + `dbutils.secrets.get(scope="prod-db", key="api-token")`
D. `databricks secrets create-scope prod-db` + `databricks secrets put-secret prod-db api-token` + `dbutils.secrets.read(scope="prod-db", key="api-token")`

**答案：B**
- `create-scope` 创建 scope，`put-secret SCOPE KEY`（scope 在前）
- `dbutils.secrets.get(scope, key)` 读取
- A 的 `put-secret` 参数顺序颠倒（key 在 scope 前面）
- C 使用了不存在的 `new-scope` 和 `add-secret`
- D 使用了不存在的 `dbutils.secrets.read`

---

### 子主题 C：Asset Bundles 工作流

**C1.** 一个数据工程师需要将生产环境中已运行 6 个月的 Job（ID: 12345）纳入 Asset Bundle 管理，且不能中断现有 job。正确的步骤是？

A. 手动查看 job 配置，编写对应的 YAML 文件，运行 `databricks bundle deploy`
B. 运行 `databricks bundle generate job --existing-job-id 12345`，然后直接运行 `databricks bundle deploy`
C. 运行 `databricks bundle generate job --existing-job-id 12345`，然后运行 `databricks bundle deployment bind`，最后运行 `databricks bundle deploy`
D. 使用 REST API 导出 job JSON，转换为 YAML，运行 `databricks bundle deploy`

**答案：C**
- `generate` 自动生成 YAML 和下载引用文件（比手动更准确）
- `bind` 建立 bundle resource 与现有 job 的链接（关键步骤）
- 没有 `bind`，`deploy` 会创建新 job 而非更新现有 job（B 的致命错误）
- A 和 D 都是手动操作且缺少 bind 步骤

---

**C2.** 一个团队有两个环境（dev, prod），使用 Asset Bundles 管理。Dev 中的 job 是全新创建的，prod 中的 job 是从已有 job 接管的。以下哪个说法正确？

A. 两个环境都需要执行 `bundle deployment bind`
B. 只有 prod 环境需要执行 `bundle deployment bind`，dev 环境直接 `deploy` 即可
C. 两个环境都可以直接 `deploy`，`bind` 只是可选优化
D. `bind` 操作需要在每次 `deploy` 之前执行

**答案：B**
- `bind` 的目的是将 bundle resource 绑定到**已有的** job/resource
- Dev 环境的 job 是全新创建的，不需要 bind，直接 deploy 会创建新 job
- Prod 环境的 job 是已有的，需要 bind 来建立对应关系
- `bind` 只需执行一次，后续 deploy 自动识别绑定关系

---

### 子主题 D：Git Folders 冲突解决

**D1.** 两个数据工程师在不同分支修改了同一个 notebook 的同一段代码。合并时出现冲突。以下哪个做法正确？

A. 在 Databricks Git Folders UI 中打开冲突文件，手动编辑选择要保留的代码，移除 conflict markers，标记已解决
B. 使用集群的 Web Terminal 运行 `git merge --abort`，然后让一方先提交，另一方在其基础上修改
C. 在 Git Folders UI 中点击"Accept Theirs"覆盖自己的更改
D. 通过 Databricks REST API 强制推送本地版本

**答案：A**
- 标准做法是手动解决冲突：选择代码 + 移除 markers + 标记 resolved
- B 是回避问题，不是解决问题
- C 会丢失自己的修改（如果只想保留对方的更改则可以，但题目问的是"正确做法"）
- D 强制推送会覆盖他人工作

---

**D2.** 在 Databricks Git Folders 中解决合并冲突后，需要执行什么操作来完成合并？

A. 运行 `git merge --continue` 命令
B. 在 Git Folders UI 中标记冲突已解决并提交合并
C. 删除冲突文件然后重新从远程拉取
D. 使用 `databricks workspace import` 重新导入解决后的文件

**答案：B**
- Databricks Git Folders 提供 UI 操作完成整个合并流程
- 标记已解决 + 提交合并是标准的最后一步
- A 是命令行操作，Git Folders UI 不需要手动运行 git 命令
- C 和 D 都是不必要的破坏性操作

---

## 五、关键知识网络

```
Databricks CLI 命令体系
|
+-- 文件操作层
|   +-- fs: DBFS 文件 (cp, ls, rm, mkdirs)
|   +-- workspace: Notebook/文件夹 (import, export, list)
|
+-- 计算资源层
|   +-- clusters: 集群管理 (create, delete, list, get)
|   +-- libraries: 集群库管理 (install, uninstall, list)
|   +-- jobs: Job 管理 (create, list, run-now)
|   +-- pipelines: DLT Pipeline 管理 (create, get, start)
|
+-- 安全层
|   +-- secrets: Secret 管理
|   |   +-- create-scope -> put-secret SCOPE KEY -> dbutils.secrets.get(scope, key)
|   |   +-- ACL: scope 级别 (Read/Write/Manage)
|   |   +-- 限制: 逐字符遍历可绕过 REDACTED 保护
|   +-- tokens: Personal Access Token 管理
|
+-- 部署管理层
|   +-- bundle: Asset Bundle
|       +-- generate: 从现有资源生成 YAML
|       +-- deployment bind: 绑定 bundle resource 到现有资源
|       +-- deploy: 部署/更新
|       +-- 接管流程: generate -> bind -> deploy
|
+-- 版本控制层
    +-- Git Folders
        +-- 冲突解决: UI 手动编辑 -> 移除 markers -> 标记 resolved -> 提交
        +-- 原则: 遵循标准 Git 工作流，不搞特殊化
```

**跨概念关联：**
- `fs cp` (上传 .whl) -> `libraries install` (安装到集群) -> `jobs create` (创建使用该库的 job) -> `bundle generate` (纳入 bundle 管理)
- Secrets 贯穿所有层：Job 中读取凭据、Pipeline 中连接外部系统、Notebook 中调用 API

---

## 六、Anki 卡片

**卡片 1**
Q: 上传 Python Wheel 文件到 DBFS 应该使用哪个 CLI 子命令？
A: `databricks fs cp my_package.whl dbfs:/path/`。`fs` 负责文件系统操作，`libraries` 是在集群上安装库，两者不同。

**卡片 2**
Q: `databricks secrets put-secret` 的参数顺序是什么？
A: `put-secret SCOPE KEY`，scope 在前，key 在后。记忆：容器（scope）在前，条目（key）在后。

**卡片 3**
Q: `dbutils.secrets.get()` 的参数顺序是什么？
A: `dbutils.secrets.get(scope="SCOPE", key="KEY")`，与 CLI 一致，都是 scope 在前。

**卡片 4**
Q: `print(dbutils.secrets.get(...))` 输出什么？如何绕过？
A: 输出 `[REDACTED]`。绕过方式：`for c in secret: print(c)` 逐字符遍历打印明文。

**卡片 5**
Q: Secrets ACL 的权限粒度是什么级别？有哪三种权限？
A: Scope 级别（不能对单个 key 设权限）。三种权限：Read、Write、Manage。

**卡片 6**
Q: 创建集群的正确 CLI 命令是什么？
A: `databricks clusters create`。不是 `compute create`（CLI 中没有 `compute` 子命令组），不是 `clusters add`（没有 `add` 操作动词）。

**卡片 7**
Q: Asset Bundle 接管现有 production job 的三步流程是什么？
A: (1) `bundle generate job --existing-job-id <ID>` 生成 YAML (2) `bundle deployment bind` 建立绑定 (3) `bundle deploy` 部署更新。没有 bind 会创建新 job。

**卡片 8**
Q: `bundle deployment bind` 的作用是什么？省略会怎样？
A: 将 bundle resource 绑定到已有的 Databricks job，确保后续 deploy 更新同一个 job。省略 bind，deploy 会创建新 job 而非更新现有 job。

**卡片 9**
Q: Databricks Git Folders 中遇到合并冲突的正确解决步骤？
A: 在 UI 中打开冲突文件 -> 手动选择要保留的代码 -> 移除 conflict markers -> 标记冲突已解决 -> 完成合并提交。

**卡片 10**
Q: `databricks fs` 和 `databricks workspace` 分别操作什么？
A: `fs` 操作 DBFS 文件系统（数据文件、wheel 包等），`workspace` 操作 Databricks 工作区对象（notebook、文件夹）。

**卡片 11**
Q: REST API 能否列出 secret 的明文值？
A: 不能。REST API 的 `list-secrets` 只返回 key 名称，不返回值。管理员也不能在控制台看到明文。

**卡片 12**
Q: `databricks bundle generate job` 做了哪两件事？
A: (1) 从现有 job 自动生成完整的 YAML 配置文件 (2) 下载该 job 引用的所有文件（如 notebook、Python 脚本等）。

---

## 七、掌握度自测

**T1.** 数据工程师需要将本地 CSV 文件上传到 DBFS 的 `/mnt/data/` 路径。正确命令是？

A. `databricks workspace import data.csv /mnt/data/`
B. `databricks fs cp data.csv dbfs:/mnt/data/`
C. `databricks files upload data.csv /mnt/data/`
D. `databricks data cp data.csv dbfs:/mnt/data/`

---

**T2.** 以下哪个关于 Databricks Secrets 的说法是**错误**的？

A. ACL 权限在 scope 级别设置
B. `put-secret` 的参数顺序是 scope 在前、key 在后
C. REST API 的 `list-secrets` 可以返回 secret 明文
D. 逐字符遍历 secret 值可以绕过 REDACTED 保护

---

**T3.** 将现有 production job 纳入 Asset Bundle 管理，以下哪个步骤组合正确？

A. `bundle init` -> `bundle deploy`
B. `bundle generate` -> `bundle deploy`
C. `bundle generate` -> `bundle deployment bind` -> `bundle deploy`
D. `bundle create` -> `bundle deployment bind` -> `bundle deploy`

---

**T4.** 两个工程师在不同分支修改了同一个 notebook，合并时冲突。以下哪个做法**最不恰当**？

A. 在 Git Folders UI 中手动编辑并解决冲突
B. 使用 `git push --force` 强制推送
C. 回退合并，协商后重新合并
D. 选择保留一方的版本，放弃另一方的修改

---

**T5.** `databricks clusters create` 和 `databricks compute create` 有什么区别？

A. 功能相同，`compute` 是新版命令
B. `clusters` 用于 all-purpose 集群，`compute` 用于 job 集群
C. CLI 中只有 `clusters` 子命令组，`compute` 不存在
D. `compute` 需要额外的配置文件参数

---

**T6.** 以下哪段代码**不会**泄露 secret 明文？

A. `[print(c) for c in dbutils.secrets.get(scope="s", key="k")]`
B. `print(dbutils.secrets.get(scope="s", key="k"))`
C. `secret = dbutils.secrets.get(scope="s", key="k"); print(''.join([c for c in secret]))`
D. `for i in range(len(secret)): print(secret[i])`

---

**T7.** 关于 `databricks bundle deployment bind`，以下哪个说法**正确**？

A. 每次 deploy 之前都需要执行 bind
B. bind 只需执行一次，后续 deploy 自动识别绑定关系
C. bind 会立即更新现有 job 的配置
D. bind 只能用于 job resource，不能用于其他 resource 类型

---

**T8.** 一个新的 scope `analytics` 需要存入三个 key：`db_host`、`db_port`、`db_password`。正确的 CLI 命令序列是？

A. `create-scope analytics` -> `put-secret analytics db_host` -> `put-secret analytics db_port` -> `put-secret analytics db_password`
B. `create-scope analytics` -> `put-secret db_host analytics` -> `put-secret db_port analytics` -> `put-secret db_password analytics`
C. `create-scope analytics` -> `put-secret analytics db_host db_port db_password`
D. `create-scope analytics db_host db_port db_password`

---

### 自测答案

| 题号 | 答案 | 关键理由 |
|------|------|----------|
| T1 | B | `fs cp` 是 DBFS 文件操作的标准命令 |
| T2 | C | REST API 只返回 key 名称，不返回明文 |
| T3 | C | generate -> bind -> deploy，bind 是接管现有 job 的关键步骤 |
| T4 | B | force-push 覆盖他人工作，是破坏性操作 |
| T5 | C | CLI 中没有 `compute` 子命令组，只有 `clusters` |
| T6 | B | `print()` 直接输出 `[REDACTED]`，其他三个都通过逐字符操作泄露明文 |
| T7 | B | bind 只需一次，建立持久绑定关系 |
| T8 | A | `put-secret SCOPE KEY`，scope 在前，每个 key 单独一条命令 |
