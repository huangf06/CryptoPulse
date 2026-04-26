#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
提取所有错题的完整内容并分析知识点
"""

import fitz
import re
import json

def extract_questions_from_pdf(pdf_path):
    """从 PDF 提取所有题目"""
    doc = fitz.open(pdf_path)
    full_text = ''
    for page in doc:
        full_text += page.get_text()

    # 提取所有正确答案
    answers = re.findall(r'Correct Answer:\s*([A-E]+)', full_text)

    # 按 QUESTION 分割
    questions = re.split(r'(?=QUESTION \d+)', full_text)

    return questions, answers

def parse_question(q_text):
    """解析单个题目，提取题干和选项"""
    # 提取题号
    q_num_match = re.search(r'QUESTION (\d+)', q_text)
    q_num = int(q_num_match.group(1)) if q_num_match else None

    # 提取题干（QUESTION 行之后到第一个选项之前）
    lines = q_text.split('\n')
    question_text = []
    options = {}

    in_question = False
    in_options = False

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.startswith('QUESTION'):
            in_question = True
            continue

        # 检测选项开始
        option_match = re.match(r'^([A-E])\.\s+(.+)$', line)
        if option_match:
            in_options = True
            in_question = False
            option_letter = option_match.group(1)
            option_text = option_match.group(2)
            options[option_letter] = option_text
            continue

        # 检测答案行（结束）
        if line.startswith('Correct Answer:'):
            break

        if in_question:
            question_text.append(line)
        elif in_options and options:
            # 继续上一个选项的文本
            last_option = list(options.keys())[-1]
            options[last_option] += ' ' + line

    return {
        'num': q_num,
        'question': '\n'.join(question_text),
        'options': options
    }

def main():
    pdf_path = r'mock_exam/Certified Data Engineer Professional  327题.pdf'

    print('正在提取 PDF 内容...')
    questions, answers = extract_questions_from_pdf(pdf_path)

    # 定义所有错题（基于之前的批改结果）
    # 这里先处理灾难性 Page
    disaster_pages = {
        'Page 1': list(range(1, 11)),
        'Page 11': list(range(101, 111)),
        'Page 30': list(range(291, 301)),
        'Page 32': list(range(311, 321))
    }

    # 用户答案（灾难性 Page）
    user_answers = {
        # Page 1
        1: 'D', 2: 'E', 3: 'A', 4: 'C', 5: 'A', 6: 'C', 7: 'D', 8: 'E', 9: 'C', 10: 'A',
        # Page 11
        101: 'X', 102: 'A', 103: 'D', 104: 'E', 105: 'E', 106: 'C', 107: 'B', 108: 'E', 109: 'E', 110: 'C',
        # Page 30
        291: 'C', 292: 'X', 293: 'B', 294: 'D', 295: 'X', 296: 'B', 297: 'C', 298: 'A', 299: 'A', 300: 'B',
        # Page 32
        311: 'C', 312: 'D', 313: 'X', 314: 'D', 315: 'D', 316: 'D', 317: 'B', 318: 'B', 319: 'A', 320: 'B'
    }

    results = {}

    for page_name, q_nums in disaster_pages.items():
        print(f'\n处理 {page_name}...')
        results[page_name] = []

        for q_num in q_nums:
            if q_num >= len(questions) or q_num > len(answers):
                continue

            correct = answers[q_num - 1]
            user = user_answers.get(q_num, 'X')
            is_correct = (user != 'X' and user == correct)

            if not is_correct:
                # 解析题目
                parsed = parse_question(questions[q_num])

                results[page_name].append({
                    'num': q_num,
                    'user_answer': user,
                    'correct_answer': correct,
                    'question': parsed['question'][:500],  # 限制长度
                    'options': parsed['options']
                })

                print(f'  Q{q_num}: 你答 {user}, 正确 {correct}')

    # 保存结果
    output_file = 'mock_exam/disaster_pages_analysis.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f'\n结果已保存到: {output_file}')

    # 统计
    total_wrong = sum(len(v) for v in results.values())
    print(f'\n灾难性 Page 错题总数: {total_wrong}')

if __name__ == '__main__':
    main()
