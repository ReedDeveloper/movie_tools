"""
Playwright-based repair spider for Douban movie detail pages.

Used *only* as a fallback for movies whose release_date is still missing or
year-only after the fast path (Abstract API + TMDB).  A real Chromium browser
executes JavaScript, handles cookies and session state, and bypasses the
sec.douban.com anti-bot redirect that defeats plain requests-based scraping.

Prerequisites (one-time setup):
    pip install playwright
    playwright install chromium
"""

import logging
import random
import re
import string
import time
from typing import List, Optional

from utils.cache import JsonFileCache
from utils.processor import maybe_update_release_date


logger = logging.getLogger(__name__)

# Injected before every page load – hides the most common Playwright fingerprints
_STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins',   {get: () => [1, 2, 3, 4, 5]});
Object.defineProperty(navigator, 'languages', {get: () => ['zh-CN', 'zh', 'en']});
window.chrome = {runtime: {}};
const getParameter = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(parameter) {
  if (parameter === 37445) return 'Intel Inc.';
  if (parameter === 37446) return 'Intel Iris OpenGL Engine';
  return getParameter(parameter);
};
"""

# ── availability check (lazy, cached) ────────────────────────────────────────
_PLAYWRIGHT_AVAILABLE: Optional[bool] = None


def playwright_available() -> bool:
    global _PLAYWRIGHT_AVAILABLE
    if _PLAYWRIGHT_AVAILABLE is None:
        try:
            from playwright.sync_api import sync_playwright  # noqa: F401
            _PLAYWRIGHT_AVAILABLE = True
        except ImportError:
            _PLAYWRIGHT_AVAILABLE = False
    return _PLAYWRIGHT_AVAILABLE


def _random_bid() -> str:
    return "".join(random.sample(string.ascii_letters + string.digits, 11))


# ── main class ────────────────────────────────────────────────────────────────

class PlaywrightDetailSpider:
    """
    Context-manager based Playwright spider.
    One browser / one context is reused across all page fetches to reduce
    overhead and make the session look like normal human browsing.

    Usage:
        with PlaywrightDetailSpider(headless=True) as spider:
            date = spider.fetch_release_date("12345678")
            repaired = spider.repair_movies(movie_list)
    """

    DETAIL_URL = "https://movie.douban.com/subject/{}/"

    def __init__(self, cache=None, state_store=None, headless: bool = True):
        self.cache = cache or JsonFileCache()
        self.state_store = state_store
        self.headless = headless
        self._pw = None
        self._browser = None
        self._context = None

    # ── context manager ───────────────────────────────────────────────────────

    def __enter__(self) -> "PlaywrightDetailSpider":
        t0 = time.time()
        if not playwright_available():
            raise RuntimeError(
                "Playwright 未安装，请先执行：\n"
                "  pip install playwright\n"
                "  playwright install chromium"
            )
        from playwright.sync_api import sync_playwright

        logger.info("[playwright] 正在启动浏览器 …")
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(
            headless=self.headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            locale="zh-CN",
            timezone_id="Asia/Shanghai",
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
        )
        self._context.add_cookies([
            {
                "name": "bid",
                "value": _random_bid(),
                "domain": ".douban.com",
                "path": "/",
            }
        ])
        # Inject stealth JS before every page load
        self._context.add_init_script(_STEALTH_JS)
        # Warm up: visit the homepage once to establish a natural session
        self._warmup()
        elapsed = time.time() - t0
        logger.info("[playwright] 浏览器启动完成 耗时 %.2fs", elapsed)
        return self

    def __exit__(self, *args) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()

    def _warmup(self) -> None:
        """Visit Douban homepage briefly to seed cookies and look like a real user."""
        page = self._context.new_page()
        try:
            page.goto("https://www.douban.com/", wait_until="domcontentloaded", timeout=20_000)
            time.sleep(random.uniform(1.0, 2.0))
            logger.debug("Playwright warmup complete (title=%r)", page.title())
        except Exception as exc:
            logger.debug("Warmup failed (non-fatal): %s", exc)
        finally:
            page.close()

    # Region preference order for Chinese users (lower index = higher priority)
    _REGION_PRIORITY: List[str] = [
        "中国大陆",
        "中国香港",
        "香港",
        "中国台湾",
        "台湾",
    ]

    # ── internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_date_entries(tags) -> List[tuple]:
        """
        Parse all v:initialReleaseDate tags into (date_str, region) tuples.

        Tag text examples:
            "2026-03-07(中国大陆)"
            "2026-02-11(英国)"
            "2026-02-13"          ← no region
        """
        entries: List[tuple] = []
        for tag in tags:
            text = (tag.text_content() or "").strip()
            # Extract date and optional region from text like "2026-03-07(中国大陆)"
            m = re.search(r"(\d{4}-\d{2}-\d{2}|\d{4}-\d{2})\s*(?:\(([^)]+)\))?", text)
            if m:
                date_str = m.group(1)
                region = (m.group(2) or "").strip()
                entries.append((date_str, region))
        return entries

    def _best_date(self, entries: List[tuple]) -> str:
        """
        Choose the most relevant date from (date_str, region) pairs.

        Priority:
          1. Preferred Chinese region, by _REGION_PRIORITY order
          2. Earliest full date (YYYY-MM-DD) across all regions
          3. Earliest partial date (YYYY-MM) across all regions
        """
        if not entries:
            return ""

        # Build lookup: region → date_str  (keep earliest per region)
        region_map: dict = {}
        for date_str, region in entries:
            key = region or "__none__"
            if key not in region_map or date_str < region_map[key]:
                region_map[key] = date_str

        # Priority 1: preferred Chinese regions
        for preferred in self._REGION_PRIORITY:
            if preferred in region_map:
                logger.debug("Using %s release date: %s", preferred, region_map[preferred])
                return region_map[preferred]

        # Priority 2: earliest full date (YYYY-MM-DD)
        full = sorted(
            d for d, _ in entries if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d)
        )
        if full:
            return full[0]

        # Priority 3: earliest partial date (YYYY-MM)
        partial = sorted(
            d for d, _ in entries if re.fullmatch(r"\d{4}-\d{2}", d)
        )
        return partial[0] if partial else ""

    def _extract_date(self, page) -> str:
        """
        Extract the best release date from a rendered Douban movie page.

        Strategy:
          1. Parse all v:initialReleaseDate microdata tags (with region info).
          2. Prefer Chinese mainland / HK / TW dates over other regions.
          3. Fall back to the earliest available date.
          4. If no microdata, scan the #info text block.
        """
        # Method 1: structured microdata tags (most reliable when present)
        tags = page.query_selector_all("[property='v:initialReleaseDate']")
        entries = self._parse_date_entries(tags)
        if entries:
            return self._best_date(entries)

        # Method 2: plain text scan inside #info block
        info = page.query_selector("#info")
        if info:
            info_text = (info.text_content() or "").replace("\n", " ")
            m = re.search(r"上映日期[：:]\s*(\d{4}-\d{2}-\d{2})", info_text)
            if m:
                return m.group(1)
            m = re.search(r"上映日期[：:]\s*(\d{4}-\d{2})", info_text)
            if m:
                return m.group(1)

        return ""

    # ── public API ────────────────────────────────────────────────────────────

    def fetch_release_date(self, douban_id: str) -> str:
        """
        Fetch the release date for one movie.

        Returns a normalized date string (YYYY-MM-DD or YYYY-MM) or "".
        Results are cached locally for 30 days so re-runs are instant.
        """
        cache_ns, cache_key = "pw_detail", str(douban_id)
        cached = self.cache.get(cache_ns, cache_key, ttl_hours=24 * 30)
        if cached is not None:
            logger.debug("[playwright] douban_id=%s 命中缓存", douban_id)
            return cached.get("release_date", "")

        t0 = time.time()
        logger.info("[playwright] 正在获取日期 douban_id=%s", douban_id)

        if self._context is None:
            raise RuntimeError(
                "PlaywrightDetailSpider must be used as a context manager."
            )

        url = self.DETAIL_URL.format(douban_id)
        release_date = ""
        page = self._context.new_page()

        try:
            # Human-like random pre-delay
            time.sleep(random.uniform(1.5, 3.5))

            # Use "commit" so we get control immediately and can react to the URL
            page.goto(url, wait_until="commit", timeout=30_000)

            # Douban sometimes redirects to sec.douban.com for a JS challenge.
            # With a real browser the challenge resolves automatically – we just
            # have to wait long enough for the subsequent redirect back.
            if "sec.douban.com" in page.url:
                logger.debug(
                    "sec.douban.com challenge for douban_id=%s; waiting for redirect …",
                    douban_id,
                )
                try:
                    # Wait until the URL no longer points to the security page
                    page.wait_for_function(
                        "!window.location.href.includes('sec.douban.com')",
                        timeout=20_000,
                    )
                except Exception:
                    logger.warning(
                        "sec.douban.com redirect did not resolve for douban_id=%s",
                        douban_id,
                    )
                    if self.state_store:
                        self.state_store.record_fetch_event(
                            "pw_detail", douban_id, "sec_redirect", page.url
                        )
                    return ""

            # Wait for #info to appear (avoids slow networkidle on ad-heavy pages)
            try:
                page.wait_for_selector("#info", timeout=15_000)
            except Exception:
                title = page.title() or ""
                logger.warning(
                    "No #info block (douban_id=%s, title=%r, url=%s)",
                    douban_id, title, page.url,
                )
                return ""

            # Detect hard login / block pages (check after JS has run)
            title = page.title() or ""
            blocked_keywords = ("禁止访问", "Verify", "captcha")
            if any(kw in title for kw in blocked_keywords):
                logger.warning("Blocked (douban_id=%s, title=%r)", douban_id, title)
                if self.state_store:
                    self.state_store.record_fetch_event(
                        "pw_detail", douban_id, "blocked", title
                    )
                return ""

            release_date = self._extract_date(page)
            status = "ok" if release_date else "no_date"
            elapsed = time.time() - t0
            logger.info(
                "[playwright] douban_id=%s 完成 耗时 %.2fs  %s → %r",
                douban_id, elapsed, status, release_date,
            )
            # Cache even empty results so we don't retry the same ID repeatedly
            self.cache.set(cache_ns, cache_key, {"release_date": release_date})
            if self.state_store:
                self.state_store.record_fetch_event(
                    "pw_detail", douban_id, status, release_date
                )

        except Exception as exc:
            elapsed = time.time() - t0
            logger.error(
                "[playwright] douban_id=%s 失败 耗时 %.2fs  error=%s",
                douban_id, elapsed, exc,
            )
            if self.state_store:
                self.state_store.record_fetch_event(
                    "pw_detail", douban_id, "failed", str(exc)
                )
        finally:
            page.close()

        return release_date

    def repair_movies(self, movies: List[dict]) -> List[dict]:
        """
        Fill in missing month-level release dates for a batch of movies.

        Only fetches pages for movies that still have year-only or no dates.
        Returns new dicts (originals are not mutated).
        """
        repaired: List[dict] = []
        needs_repair = [
            m for m in movies
            if len(str(m.get("release_date") or "")) < 7 and m.get("douban_id")
        ]
        skip_repair = [
            m for m in movies
            if len(str(m.get("release_date") or "")) >= 7 or not m.get("douban_id")
        ]

        total = len(needs_repair)
        logger.info(
            "[playwright] 开始日期补全  待处理 %d 部  已有日期跳过 %d 部",
            total, len(skip_repair),
        )
        repair_t0 = time.time()

        for idx, movie in enumerate(needs_repair, 1):
            item_t0 = time.time()
            douban_id = str(movie["douban_id"])
            current_date = str(movie.get("release_date") or "")

            fetched = self.fetch_release_date(douban_id)
            movie = dict(movie)

            if fetched and len(fetched) >= 7:
                maybe_update_release_date(movie, fetched, "playwright_detail", "high")
                movie["decision_status"] = "repaired"
                item_elapsed = time.time() - item_t0
                logger.info(
                    "[playwright] 进度 %d/%d  %s  %r → %r  本条耗时 %.2fs",
                    idx, total, douban_id, current_date, fetched, item_elapsed,
                )
            else:
                item_elapsed = time.time() - item_t0
                logger.info(
                    "[playwright] 进度 %d/%d  %s  未获取到日期 (仍: %r)  本条耗时 %.2fs",
                    idx, total, douban_id, current_date, item_elapsed,
                )

            repaired.append(movie)
            # Inter-page delay to avoid burst patterns
            time.sleep(random.uniform(1.0, 2.5))

        total_elapsed = time.time() - repair_t0
        logger.info(
            "[playwright] 日期补全结束  共处理 %d 部  总耗时 %.2fs",
            total, total_elapsed,
        )
        return skip_repair + repaired
