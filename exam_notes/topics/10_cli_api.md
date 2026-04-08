# 10. Databricks CLI, API 与部署

**题量：6 题 | 全错（0/6）| 状态：严重薄弱**

---

## 概念框架

### Databricks CLI 命令体系

| 子命令组 | 功能 | 常用操作 |
|----------|------|----------|
| `databricks fs` | DBFS 文件操作 | `cp`, `ls`, `rm`, `mkdirs` |
| `databricks clusters` | 集群管理 | `create`, `delete`, `list`, `get` |
| `databricks jobs` | Job 管理 | `create`, `list`, `run-now` |
| `databricks secrets` | Secret 管理 | `create-scope`, `put-secret`, `list-secrets` |
| `databricks pipelines` | DLT 管理 | `get`, `create`, `start` |
| `databricks workspace` | Notebook/文件夹管理 | `import`, `export`, `list` |
| `databricks bundle` | Asset Bundle 管理 | `generate`, `deploy`, `bind` |

### Databricks Secrets 体系

```
Secret Scope（作用域）
  |
  +-- Secret Key 1 = value1
  +-- Secret Key 2 = value2
  
CLI 操作：
  databricks secrets create-scope SCOPE_NAME
  databricks secrets put-secret SCOPE KEY        # 注意顺序：先 scope 后 key
  
Notebook 读取：
  dbutils.secrets.get(scope="SCOPE", key="KEY")  # 注意顺序：先 scope 后 key

ACL 权限（scope 级别，不能对单个 key 设权限）：
  - Read：读取 secret 值
  - Write：写入/修改 secret
  - Manage：管理 scope 的 ACL
```

**安全限制：** `print(secret)` 显示 `[REDACTED]`，但逐字符遍历打印可绕过保护显示明文。

### Asset Bundles 工作流

将现有 production job 纳入 Asset Bundle 管理的正确流程：

```
Step 1: databricks bundle generate job --existing-job-id <ID>
        → 生成 YAML 配置 + 下载引用的文件

Step 2: databricks bundle deployment bind
        → 将 bundle 的 job resource 绑定到现有的 Databricks job
        → 确保后续 deploy 更新同一个 job（而不是创建新 job）

Step 3: databricks bundle deploy
        → 部署更新（更新已绑定的 job）
```

**关键：** 没有 `bind` 步骤，`deploy` 会创建新 job 而不是更新现有 job。

### Git Folders 合并冲突解决

Databricks Git Folders UI 提供可视化冲突解决：
1. 打开冲突文件
2. 手动选择两个版本中需要保留的代码
3. 移除 conflict markers（`<<<<<<<`, `=======`, `>>>>>>>`）
4. 标记冲突已解决
5. 完成合并提交

---

## 错题精析

### Q46 -- Databricks Secrets 限制

**原题：**

> Although the Databricks Utilities Secrets module provides tools to store sensitive credentials and avoid accidentally displaying them in plain text users should still be careful with which credentials are stored here and which users have access to using these secrets.
>
> Which statement describes a limitation of Databricks Secrets?

**选项：**
- A. Because the SHA256 hash is used to obfuscate stored secrets, reversing this hash will display the value in plain text.
- B. Account administrators can see all secrets in plain text by logging on to the Databricks Accounts console.
- C. Secrets are stored in an administrators-only table within the Hive Metastore; database administrators have permission to query this table by default.
- D. Iterating through a stored secret and printing each character will display secret contents in plain text.
- E. The Databricks REST API can be used to list secrets in plain text if the personal access token has proper credentials.

**我的答案：** C | **正确答案：** D

**解析：**
- `dbutils.secrets.get()` 返回的值在直接 `print()` 时显示 `[REDACTED]`，这是 Databricks 的保护机制
- 但这个保护可以被绕过：`for c in secret: print(c)` 逐字符打印会显示明文
- Secrets 不存储在 Hive Metastore（C 错误）
- REST API 不能列出明文（E 错误），只能列出 key 名称
- 管理员也不能在控制台看到明文（B 错误）

**错因：** 不了解 Databricks Secrets 的具体实现细节和已知安全限制。

---

### Q68 -- Databricks CLI 上传文件

**原题：**

> Assuming that the Databricks CLI has been installed and configured correctly, which Databricks CLI command can be used to upload a custom Python Wheel to object storage mounted with the DBFS for use with a production job?

**选项：**
- A. configure
- B. fs
- C. jobs
- D. libraries
- E. workspace

**我的答案：** D | **正确答案：** B

**解析：**
- 上传文件到 DBFS 用 `databricks fs cp my_package.whl dbfs:/path/to/wheel/`
- `fs` 子命令负责所有 DBFS 文件操作（上传、下载、列出、删除）
- `libraries` 子命令用于管理集群上已安装的库（安装、卸载、查看状态），不是上传文件
- `workspace` 用于 notebook 和文件夹操作，不是 DBFS 文件

**错因：** 混淆了"上传文件到 DBFS"和"在集群上安装库"两个不同操作。上传是文件系统操作（fs），安装是集群操作（libraries）。

---

### Q232 -- Databricks CLI 创建集群命令

**原题：**

> A data engineer wants to create a cluster using the Databricks CLI for a big ETL pipeline. The cluster should have five workers and one driver of type i3.xlarge and should use the '14.3.x-scala2.12' runtime.
>
> Which command should the data engineer use?

**选项：**
- A. databricks compute add 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
- B. databricks clusters create 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
- C. databricks compute create 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster
- D. databricks clusters add 14.3.x-scala2.12 --num-workers 5 --node-type-id i3.xlarge --cluster-name Data Engineer_cluster

**我的答案：** A | **正确答案：** B

**解析：**
- 正确的子命令组是 `clusters`（不是 `compute`），操作动词是 `create`（不是 `add`）
- `databricks clusters create` 是标准的集群创建命令
- A/C 用了不存在的 `compute` 子命令组
- D 用了不存在的 `add` 操作动词

**记忆：** clusters create, clusters delete, clusters list -- 子命令组 + CRUD 动词。

---

### Q263 -- Asset Bundle 管理现有生产 Job

**原题：**

> A data engineer is bringing an existing production Databricks job under asset bundle management and wants to ensure that:
> - The job's current configuration is captured as YAML, and all referenced files are included in their bundle project.
> - Future changes to the bundle's YAML will update the existing job in-place (not create a new job)
>
> How should the data engineer successfully move the production job under asset bundle management?

**选项：**
- A. Run `databricks bundle generate job --existing-job-id` to generate the YAML and download referenced files. Then, run `databricks bundle deploy` to deploy the bundle, which will always update the existing job automatically.
- B. Export the job definition as JSON, convert it to YAML, and place it in your bundle. Then, run `databricks bundle deploy` to update the existing job.
- C. Manually create the YAML configuration for the job in your bundle project, ensuring all settings match the existing job. Then, run `databricks bundle deploy` the bundle, which will update the existing job in your workspace.
- D. Run `databricks bundle generate job --existing-job-id` to generate the YAML and download referenced files. Then, run `databricks bundle deployment bind` to link the bundle's job resource to the existing job in Databricks.

**我的答案：** C | **正确答案：** D

**解析：**
- `bundle generate` 自动从现有 job 生成完整的 YAML 配置并下载所有引用文件，比手动创建更准确
- `bundle deployment bind` 建立 bundle resource 与现有 job 之间的持久链接，确保后续 `deploy` 更新同一个 job
- **没有 bind 步骤，deploy 会创建新 job**，这是 A 选项的致命错误
- C 选项手动创建 YAML 容易遗漏配置，且没有 bind 步骤

**错因：** 不了解 Asset Bundle 的 `bind` 机制——这是"接管现有资源"的关键步骤。

---

### Q295 -- Secrets CLI 命令语法（pending）

**原题：**

> A Data Engineer is building a fraud detection pipeline that calls out to OpenAI, via a Python library, and needs to include an access token when using the API.
>
> Which Databricks CLI command should the Data Engineer use to create the secret?

**选项：**
- A. databricks secrets put-secret KEY SCOPE; dbutils.secrets.get(KEY, SCOPE)
- B. databricks tokens put-token SCOPE KEY; dbutils.tokens.get(SCOPE, KEY)
- C. databricks secrets put-secret SCOPE KEY; dbutils.secrets.get(SCOPE, KEY)
- D. databricks tokens put-token KEY SCOPE; dbutils.secrets.get(KEY, SCOPE)

**我的答案：** X（未答） | **正确答案：** C

**解析：**
- CLI 命令格式：`databricks secrets put-secret <SCOPE> <KEY>` -- **先 scope 后 key**
- Python 读取格式：`dbutils.secrets.get(scope=<SCOPE>, key=<KEY>)` -- **先 scope 后 key**
- 两者参数顺序一致，都是 scope 在前、key 在后
- B/D 使用了不存在的 `tokens put-token` 和 `dbutils.tokens.get` API
- A 的参数顺序颠倒（KEY 在 SCOPE 前面）

**记忆口诀：** scope 是容器，key 是具体条目。先说容器再说条目，符合直觉。

---

### Q316 -- Git Folders 合并冲突解决

**原题：**

> Two data engineers are working on the same Databricks notebook in separate branches. Both have edited the same section of code. When one tries to merge the other's branch into their own using the Databricks Git folders UI, a merge conflict occurs on that notebook file. The UI highlights the conflict and presents options for resolution.
>
> How should the data engineers resolve this merge conflict using Databricks Git folders?

**选项：**
- A. Use the Git CLI in the cluster's web terminal to force-push the conflicted merge (git push --force), overriding the remote branch with local version and discarding changes.
- B. Use the Git folders UI to manually edit the notebook file, selecting the desired lines from both versions and removing the conflict markers, then mark the conflict as resolved.
- C. Abort the merge, discard all local changes, and try the merge operation again without reviewing the conflicting code.
- D. Delete the conflicted notebook file via the Databricks workspace UI, commit the deletion, and recreate the notebook from scratch in a new commit to bypass the conflict entirely.

**我的答案：** D | **正确答案：** B

**解析：**
- Databricks Git Folders UI 提供了完整的可视化冲突解决功能
- 正确做法：在 UI 中打开冲突文件 -> 手动选择两个版本中要保留的代码 -> 移除 conflict markers -> 标记已解决 -> 完成合并
- A（force-push）会覆盖他人工作，是破坏性操作
- C（丢弃重试）不解决问题，冲突还在
- D（删除重建）丢失历史记录，且完全没必要

**错因：** 选了最极端的方案。合并冲突是日常操作，用 UI 手动解决即可。

---

## 核心对比表

| CLI 操作 | 正确命令 | 常见混淆 |
|----------|----------|----------|
| 上传文件到 DBFS | `databricks fs cp` | `libraries`（安装库，不是上传文件） |
| 创建集群 | `databricks clusters create` | `compute create/add`（不存在） |
| 存储 secret | `databricks secrets put-secret SCOPE KEY` | 参数顺序颠倒 |
| 读取 secret | `dbutils.secrets.get(scope, key)` | `dbutils.tokens.get`（不存在） |
| 接管现有 job | `bundle generate` + `bundle deployment bind` | 只 `generate` + `deploy`（会创建新 job） |

---

## 自测清单

- [ ] 上传 wheel 文件到 DBFS 用哪个 CLI 子命令？（fs）
- [ ] `databricks secrets put-secret` 的参数顺序是什么？（先 scope 后 key）
- [ ] 逐字符遍历 secret 值会发生什么？（绕过保护显示明文）
- [ ] Secrets ACL 的权限粒度是什么级别？（scope 级别，不能对单个 key 设权限）
- [ ] 创建集群的正确 CLI 命令是什么？（databricks clusters create）
- [ ] Asset Bundle 接管现有 job 的关键步骤是什么？（generate + bind，不能只 deploy）
- [ ] Git Folders 中遇到合并冲突应该怎么做？（UI 中手动编辑、选择代码、移除 markers、标记 resolved）
