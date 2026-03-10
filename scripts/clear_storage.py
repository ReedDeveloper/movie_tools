"""
清空项目内所有历史存储与缓存，使下次运行从零开始保存数据。

会删除：
  - output/movie_tools.db     StateStore（推送记录、影片库、用户反馈、抓取审计）
  - output/cache/             HTTP 与 Playwright 缓存（豆瓣列表/抽象/详情、TMDB）
  - output/history.json       旧版历史文件（若存在）

不会删除：
  - output/*.csv, output/*.md, output/*.xlsx  生成的导出文件（可按需手动删）

用法：
  python scripts/clear_storage.py           # 仅清空 DB + 缓存
  python scripts/clear_storage.py --all     # 同时删除 output 下所有导出文件
"""

import argparse
import os
import shutil
import sys

# 项目根目录
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main():
    parser = argparse.ArgumentParser(description="清空历史存储与缓存")
    parser.add_argument(
        "--all",
        action="store_true",
        help="同时删除 output 下的所有导出文件（csv/md/xlsx）",
    )
    args = parser.parse_args()

    os.chdir(ROOT)
    removed = []

    # StateStore
    db_path = "output/movie_tools.db"
    if os.path.isfile(db_path):
        os.remove(db_path)
        removed.append(db_path)

    # JsonFileCache
    cache_dir = "output/cache"
    if os.path.isdir(cache_dir):
        shutil.rmtree(cache_dir)
        removed.append(cache_dir + "/")

    # 旧版 history.json
    history_file = "output/history.json"
    if os.path.isfile(history_file):
        os.remove(history_file)
        removed.append(history_file)

    if args.all:
        out_dir = "output"
        if os.path.isdir(out_dir):
            for name in os.listdir(out_dir):
                if name in ("cache", "movie_tools.db", "history.json"):
                    continue
                path = os.path.join(out_dir, name)
                if name.endswith((".csv", ".md", ".xlsx", ".db")):
                    try:
                        os.remove(path)
                        removed.append(path)
                    except OSError:
                        pass

    if removed:
        print("已删除:", ", ".join(removed))
    else:
        print("没有需要清理的存储或缓存文件。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
