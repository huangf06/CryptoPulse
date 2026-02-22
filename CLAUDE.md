# CLAUDE.md - Project Instructions

## 权限
- 对本项目所有文件拥有完全读写权限
- 可以自由执行 bash 命令，无需确认
- 可以自由创建、修改、删除文件，无需确认
- 不要在任务执行过程中打断用户询问权限或确认

## 项目背景
Databricks Certified Data Engineer Professional 认证考试备考项目。
题目来源：examtopics.com，共 327 题。

## 工作流程
用户会发送 examtopics 截图 + 自己的答案（如 "DBDACBBABB"），我需要：
1. 自动拆分长截图为小块图片以便识别
2. 读取每道题的完整内容和正确答案
3. 与用户答案逐题对比批改
4. 将结果（含完整原题、选项、解析）追加到 `mock_exam/review_notes.md`
5. 完成后清理临时文件

## 文件结构
- `mock_exam/review_notes.md` — 错题复习笔记（持续追加）
- `mock_exam/*.png` — examtopics 截图原件

## 注意事项
- 所有题目原文保留英文
- 解析用中文
- 临时拆分的图片用完即删
- 不要中途打断用户，遇到问题自行解决
