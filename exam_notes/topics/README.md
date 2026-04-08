# Databricks Data Engineer Professional -- 错题深度复习

> 121 道错题 | 12 个主题 | 按概念框架 + 错题精析 + 对比表 + 自测清单组织
> 生成日期：2026-04-08

## 复习优先级

### P0 -- 核心盲区（必须先攻克）

| # | 主题 | 错题数 | 文件 |
|---|------|--------|------|
| 01 | [Delta Lake 核心机制](01_delta_lake.md) | 21 | 概念框架：Transaction Log 驱动一切 |
| 02 | [Unity Catalog 与权限管理](02_unity_catalog.md) | 17 | 概念框架：权限检查链 USE CATALOG -> USE SCHEMA -> SELECT |

### P1 -- 高频考点（强化掌握）

| # | 主题 | 错题数 | 文件 |
|---|------|--------|------|
| 03 | [Structured Streaming](03_structured_streaming.md) | 13 | 概念框架：微批处理循环模型 |
| 04 | [性能优化与 Spark 调优](04_performance.md) | 14 | 概念框架：Spark UI 各 tab 用途 |
| 05 | [Jobs & Workflows](05_jobs_workflows.md) | 11 | 概念框架：API endpoint 功能划分 |

### P2 -- 专项突破

| # | 主题 | 错题数 | 文件 |
|---|------|--------|------|
| 06 | [Change Data Feed / CDC](06_cdf_cdc.md) | 6 | CDF 消费模式 + MERGE INTO |
| 07 | [Auto Loader](07_auto_loader.md) | 7 | directory listing vs notification |
| 08 | [DLT / Lakeflow Pipelines](08_dlt_lakeflow.md) | 6 | LIVE TABLE vs STREAMING LIVE TABLE |
| 09 | [Delta Sharing](09_delta_sharing.md) | 3 | Provider/Recipient/Share 模型 |

### P3 -- 记忆性知识

| # | 主题 | 错题数 | 文件 |
|---|------|--------|------|
| 10 | [Databricks CLI, API 与部署](10_cli_api.md) | 6 | CLI 命令 + Asset Bundles |
| 11 | [Git, Repos 与测试](11_git_repos.md) | 4 | Files in Repos + DataFrame.transform |
| 12 | [平台操作与工具](12_platform_ops.md) | 13 | dbutils, secrets, %pip, SQL Alert 等 |

## 每个文件的结构

```
# 主题名

## 一、概念框架
（正确的心智模型，不是知识点罗列）

## 二、错题精析
### 子主题 A
Q{n} -- 题目标题
  原题（英文）+ 选项
  我的答案 vs 正确答案
  解析（中文）：为什么对、为什么错、关联概念

### 子主题 B
...

## 三、核心对比表
（容易混淆的概念并排对比）

## 四、自测清单
- [ ] 能否解释 X？
- [ ] 能否区分 A 和 B？
```

## 使用建议

1. **按 P0 -> P1 -> P2 -> P3 顺序复习**
2. 每个主题先读概念框架，建立心智模型
3. 然后逐题分析错题，理解自己为什么错
4. 用自测清单检验是否真正掌握
5. 对比表可以打印出来随时翻看
