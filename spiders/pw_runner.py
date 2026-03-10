"""
Standalone Playwright repair worker – must be run as __main__.

Reads a JSON array of movie dicts from stdin, uses PlaywrightDetailSpider to
fill in missing month-level release dates, and writes the repaired list as JSON
to stdout.

Running as a *subprocess* completely isolates Playwright's asyncio event loop
from the parent process (Streamlit / Tornado).  On Windows + Python 3.8 the
default SelectorEventLoop does not implement subprocess transport; this script
sets WindowsProactorEventLoopPolicy before anything else so Playwright can
launch Chromium without a NotImplementedError.
"""

import asyncio
import sys

# ── MUST come before any other asyncio or playwright import ──────────────────
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import io
import json
import logging
import os

# Force UTF-8 for stderr so Windows console encoding doesn't corrupt log lines
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Add project root so the spiders / utils packages are importable
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    stream=sys.stderr,   # keep stdout clean for JSON output
)
logger = logging.getLogger("pw_runner")


def main() -> None:
    raw = sys.stdin.read().strip()
    if not raw:
        print("[]")
        return

    repair_queue = json.loads(raw)
    logger.info("pw_runner: 收到 %d 部影片待修复", len(repair_queue))

    from spiders.playwright_spider import PlaywrightDetailSpider
    from utils.cache import JsonFileCache

    cache = JsonFileCache()
    # state_store is not passed – audit logging is handled by the parent process
    with PlaywrightDetailSpider(cache=cache, headless=True) as spider:
        result = spider.repair_movies(repair_queue)

    logger.info("pw_runner: 修复完成，输出 %d 部", len(result))
    # Only write to stdout at the very end so nothing corrupts the JSON
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
