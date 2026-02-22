"""
ExamTopics 截图拆分工具
用法: python split_screenshot.py <screenshot.png> [parts_count]

将长截图拆分为多个小块，方便 AI 识别题目内容。
默认拆分为 13 块，可通过第二个参数自定义。
输出到同目录下的 parts/ 文件夹。
"""

import sys
import os
from PIL import Image


def split_screenshot(image_path, num_parts=13):
    img = Image.open(image_path)
    w, h = img.size
    print(f"原图尺寸: {w} x {h}")

    out_dir = os.path.join(os.path.dirname(image_path), "parts")
    os.makedirs(out_dir, exist_ok=True)

    part_h = h // num_parts
    for i in range(num_parts):
        top = i * part_h
        bottom = min((i + 1) * part_h, h)
        part = img.crop((0, top, w, bottom))
        out_path = os.path.join(out_dir, f"part_{i+1:02d}.png")
        part.save(out_path)
        print(f"  Part {i+1:2d}: y={top}-{bottom} -> {out_path}")

    print(f"\n完成！共 {num_parts} 块，保存在 {out_dir}")


def cleanup(image_path):
    """清理拆分产生的临时文件"""
    parts_dir = os.path.join(os.path.dirname(image_path), "parts")
    if os.path.exists(parts_dir):
        import shutil
        shutil.rmtree(parts_dir)
        print(f"已清理: {parts_dir}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("用法: python split_screenshot.py <screenshot.png> [parts]")
        sys.exit(1)

    path = sys.argv[1]
    parts = int(sys.argv[2]) if len(sys.argv) > 2 else 13
    split_screenshot(path, parts)
