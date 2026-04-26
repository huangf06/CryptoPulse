"""
模拟考试抽题器
用法：python mock_exam/generate_mock_exam.py [--exam 1|2] [--seed N]

从 327 题中随机抽 60 题（排除重复题和无效题），生成考试题号列表。
两次考试不重叠（exam 1 和 exam 2 各 60 题，互不重复）。
"""

import random
import argparse

DUPLICATE_QUESTIONS = {203, 204, 205, 206, 207, 208, 209, 213, 214, 215, 216, 217, 219, 220, 222, 223}
INVALID_QUESTIONS = {246, 285}
EXCLUDED = DUPLICATE_QUESTIONS | INVALID_QUESTIONS

ALL_QUESTIONS = [q for q in range(1, 328) if q not in EXCLUDED]

def generate_exam(exam_num: int, seed: int = 42):
    rng = random.Random(seed)
    shuffled = ALL_QUESTIONS.copy()
    rng.shuffle(shuffled)

    if exam_num == 1:
        questions = sorted(shuffled[:60])
    elif exam_num == 2:
        questions = sorted(shuffled[60:120])
    else:
        raise ValueError("exam_num must be 1 or 2")

    return questions

def main():
    parser = argparse.ArgumentParser(description="Generate mock exam question list")
    parser.add_argument("--exam", type=int, default=1, choices=[1, 2], help="Exam number (1 or 2)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    questions = generate_exam(args.exam, args.seed)

    print(f"\n{'='*60}")
    print(f"  Mock Exam #{args.exam} — 60 Questions")
    print(f"  Time Limit: 120 minutes")
    print(f"  Pass: 48/60 (80%) | Target: 51/60 (85%)")
    print(f"{'='*60}\n")

    for i, q in enumerate(questions, 1):
        print(f"  {i:2d}. Q{q}")

    print(f"\n{'='*60}")
    print(f"  Total: {len(questions)} questions")
    print(f"  Write your answers as a single string (e.g., 'ABCDEA...')")
    print(f"  Then use Claude to grade: 'Q列表, 答案字符串'")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()
