# CLAUDE.md - Project Instructions

## 权限
- 对本项目所有文件拥有完全读写权限
- 可以自由执行 bash 命令，无需确认
- 可以自由创建、修改、删除文件，无需确认
- 不要在任务执行过程中打断用户询问权限或确认

## 项目背景
Databricks Certified Data Engineer Professional 认证考试备考项目。
题库来源：examtopics.com 完整版 PDF，共 327 题。

## 题库文件
- `mock_exam/Certified Data Engineer Professional  327题.pdf` — 完整题库（含答案）
- `mock_exam/327题_无答案版.pdf` — 去答案版（用户刷题用）

## 题库读取方法
使用 PyMuPDF (fitz) 从 PDF 提取题目文本：
```python
import fitz, re
doc = fitz.open(r'mock_exam/Certified Data Engineer Professional  327题.pdf')
full_text = ''
for page in doc:
    full_text += page.get_text()
# 提取所有正确答案
answers = re.findall(r'Correct Answer: ([A-E])', full_text)
# 提取题目内容：按 "QUESTION \d+" 分割
questions = re.split(r'(?=QUESTION \d+\n)', full_text)
```
注意：运行 Python 时需设置 `PYTHONIOENCODING=utf-8` 避免 Windows 编码错误。

## 工作流程（PDF 版）
用户发送题号范围 + 答案字符串（如 "Q31-Q40, ABDCEADBCE"），我需要：
1. 用 PyMuPDF 从完整版 PDF 提取对应题号的题目内容和正确答案
2. 与用户答案逐题对比批改
3. 将结果（含完整原题、选项、解析）追加到 `mock_exam/review_notes.md`
4. 更新统计表

## 旧工作流程（截图版，已弃用）
如果用户发送 examtopics 截图而非题号范围：
1. 用 `mock_exam/split_screenshot.py` 拆分长截图
2. 逐块读取识别题目内容和正确答案
3. 批改后清理临时文件

## review_notes.md 格式规范
- 统计表在文件顶部，每轮追加一行
- 每道题格式：`### Q{n} ❌/✅ — 简短标题`
- 错题：完整原题 + 选项 + 我的答案 + 正确答案 + 中文解析 + 知识点标签
- 对题：简要记录（一行）
- 每个 Page 末尾附高频易错知识点汇总表

## 文件结构
- `mock_exam/review_notes.md` — 错题复习笔记（持续追加）
- `mock_exam/split_screenshot.py` — 截图拆分工具（旧流程）
- `mock_exam/*.png` — examtopics 截图原件（旧流程）

## 注意事项
- 所有题目原文保留英文
- 解析用中文
- 不要中途打断用户，遇到问题自行解决
