"""
Playwright spider smoke tests.

Usage
-----
# 只检查环境
python test_playwright_spider.py --check

# 抓单部影片（默认：奥本海默 36951397）
python test_playwright_spider.py

# 抓指定豆瓣 ID
python test_playwright_spider.py --id 26363254

# 批量修复测试（多部已知影片）
python test_playwright_spider.py --batch

# 完整测试流程（环境检测 + 单片 + 批量）
python test_playwright_spider.py --all
"""

import argparse
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("test_playwright")

# ── 参考数据（title, douban_id, expected_prefix YYYY-MM） ─────────────────────
# 这些 ID 已经通过实际抓取验证存在且有上映日期
KNOWN_MOVIES = [
    {"title": "哪吒2之魔童闹海", "douban_id": "34780991", "expected_prefix": "2025-01"},
]


# ── helpers ───────────────────────────────────────────────────────────────────

def check_env() -> bool:
    """Verify that Playwright package and Chromium browser are both ready."""
    ok = True

    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
        logger.info("✓ playwright 包已安装")
    except ImportError:
        logger.error("✗ playwright 未安装 — 请执行：pip install playwright")
        return False

    logger.info("  正在启动 Chromium 做可用性检测 …")
    try:
        from playwright.sync_api import sync_playwright
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            version = browser.version
            browser.close()
        logger.info("✓ Chromium 可用（版本 %s）", version)
    except Exception as exc:
        logger.error("✗ Chromium 不可用：%s", exc)
        logger.error("  请执行：playwright install chromium")
        ok = False

    return ok


def test_single(douban_id: str) -> bool:
    """Fetch release_date for one movie and print the result."""
    from spiders.playwright_spider import PlaywrightDetailSpider

    logger.info("── 单片测试 douban_id=%s ────────────────────", douban_id)
    with PlaywrightDetailSpider(headless=True) as spider:
        date = spider.fetch_release_date(douban_id)

    if date and len(date) >= 7:
        logger.info("✓ release_date = %s", date)
        return True
    else:
        logger.warning("✗ 未能获取月份级日期（result=%r）", date)
        return False


def test_batch() -> bool:
    """Run repair_movies on KNOWN_MOVIES and validate each result."""
    from spiders.playwright_spider import PlaywrightDetailSpider

    logger.info("── 批量修复测试（%d 部影片）────────────────", len(KNOWN_MOVIES))

    # Start with year-only dates to simulate the repair scenario
    input_movies = [
        {
            "douban_id": m["douban_id"],
            "title": m["title"],
            "release_date": m["expected_prefix"][:4],   # year only
            "release_date_source": "douban_abstract",
            "release_date_confidence": "low",
            "movie_key": f"douban:{m['douban_id']}",
        }
        for m in KNOWN_MOVIES
    ]

    with PlaywrightDetailSpider(headless=True) as spider:
        repaired = spider.repair_movies(input_movies)

    passed = 0
    for movie, ref in zip(
        sorted(repaired, key=lambda x: x["douban_id"]),
        sorted(KNOWN_MOVIES, key=lambda x: x["douban_id"]),
    ):
        date = movie.get("release_date", "")
        source = movie.get("release_date_source", "")
        if len(date) >= 7 and date.startswith(ref["expected_prefix"]):
            logger.info("  ✓ %-10s  %s  (source=%s)", movie["title"], date, source)
            passed += 1
        else:
            logger.warning(
                "  ✗ %-10s  %r  期望前缀=%s", movie["title"], date, ref["expected_prefix"]
            )

    logger.info("── 批量结果: %d / %d 通过 ─────────────────", passed, len(KNOWN_MOVIES))
    return passed == len(KNOWN_MOVIES)


# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Playwright spider 功能测试",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--check",  action="store_true", help="仅检查 Playwright 环境")
    parser.add_argument("--id",     default="34780991",  help="要测试的豆瓣 ID（默认：哪吒2之魔童闹海）")
    parser.add_argument("--batch",  action="store_true", help="批量修复测试")
    parser.add_argument("--all",    action="store_true", help="完整测试（环境 + 单片 + 批量）")
    args = parser.parse_args()

    results: list[bool] = []

    run_check  = args.check or args.all
    run_single = not args.check and not args.batch or args.all
    run_batch  = args.batch or args.all

    if run_check:
        ok = check_env()
        results.append(ok)
        if not ok:
            sys.exit(1)

    if run_single and not args.check:
        results.append(test_single(args.id))

    if run_batch:
        results.append(test_batch())

    overall = all(results)
    logger.info("══ 总体结果: %s ══", "PASS ✓" if overall else "FAIL ✗")
    sys.exit(0 if overall else 1)


if __name__ == "__main__":
    main()
