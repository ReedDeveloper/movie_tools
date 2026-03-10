import logging
import random
import re
import string
import time
from datetime import date

import requests
from bs4 import BeautifulSoup

from utils.cache import JsonFileCache
from utils.processor import maybe_update_release_date


logger = logging.getLogger(__name__)

# ── 熔断器阈值 ────────────────────────────────────────────
_CIRCUIT_FAIL_THRESHOLD = 3      # 连续失败 N 次后断路
_CIRCUIT_RESET_SECONDS  = 180.0  # 断路后多久自动恢复
_BACKOFF_BASE           = 6.0    # 初始退避秒数
_BACKOFF_MAX            = 60.0   # 最长退避秒数
_HTML_MIN_INTERVAL      = 3.5    # HTML 详情页最短请求间隔


def _random_bid() -> str:
    """生成随机 BID cookie，模拟正常浏览器访问。"""
    return "".join(random.sample(string.ascii_letters + string.digits, 11))


class DoubanSpider:
    BASE_URL = "https://movie.douban.com/j/new_search_subjects"
    DETAIL_URL_TEMPLATE = "https://movie.douban.com/subject/{}/"

    def __init__(self, cache=None, state_store=None):
        self.cache = cache or JsonFileCache()
        self.state_store = state_store
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://movie.douban.com/explore",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Connection": "keep-alive",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self._refresh_bid()

        self.last_request_at = 0.0
        # 熔断器状态（仅用于 HTML 详情页，JSON 接口单独管控）
        self._html_fail_count = 0
        self._html_circuit_open_until = 0.0

    # ── Cookie ────────────────────────────────────────────
    def _refresh_bid(self) -> None:
        self.session.cookies.set("bid", _random_bid(), domain=".douban.com")

    # ── 限速 ──────────────────────────────────────────────
    def _respect_rate_limit(self, min_interval: float = 1.5) -> None:
        elapsed = time.time() - self.last_request_at
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed + random.uniform(0.2, 0.8))
        self.last_request_at = time.time()

    # ── 熔断器 ────────────────────────────────────────────
    def _html_circuit_is_open(self) -> bool:
        if time.time() < self._html_circuit_open_until:
            remaining = int(self._html_circuit_open_until - time.time())
            logger.warning("HTML 熔断器开路，还有 %ds，跳过详情页请求", remaining)
            return True
        return False

    def _html_record_failure(self) -> None:
        self._html_fail_count += 1
        if self._html_fail_count >= _CIRCUIT_FAIL_THRESHOLD:
            self._html_circuit_open_until = time.time() + _CIRCUIT_RESET_SECONDS
            logger.warning(
                "连续 %d 次 429/阻断，HTML 熔断器已开路，暂停 %.0fs",
                self._html_fail_count, _CIRCUIT_RESET_SECONDS,
            )

    def _html_record_success(self) -> None:
        self._html_fail_count = 0
        self._html_circuit_open_until = 0.0

    # ── 工具：检测 sec.douban.com 重定向 ─────────────────
    @staticmethod
    def _has_sec_redirect(response: requests.Response) -> bool:
        return any("sec.douban.com" in (r.headers.get("Location", "") or r.url)
                   for r in response.history)

    # ── JSON 请求（列表/摘要接口）────────────────────────
    def _request_json(self, url, params=None, headers=None,
                      cache_namespace=None, cache_key=None, ttl_hours=24):
        if cache_namespace and cache_key:
            cached = self.cache.get(cache_namespace, cache_key, ttl_hours=ttl_hours)
            if cached is not None:
                logger.debug("[douban] 命中缓存 %s/%s", cache_namespace, cache_key)
                return cached

        self._respect_rate_limit(min_interval=1.5)
        t0 = time.time()
        response = self.session.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        elapsed = time.time() - t0

        if cache_namespace and cache_key:
            self.cache.set(cache_namespace, cache_key, data)
        logger.info("[douban] JSON 请求完成 耗时 %.2fs  key=%s", elapsed, cache_key or url)
        return data

    # ── HTML 请求（详情页）带退避 + 熔断 ─────────────────
    def _request_text(self, url, cache_namespace=None, cache_key=None, ttl_hours=24):
        if cache_namespace and cache_key:
            cached = self.cache.get(cache_namespace, cache_key, ttl_hours=ttl_hours)
            if cached is not None:
                self._html_record_success()
                logger.debug("[douban] HTML 命中缓存 %s/%s", cache_namespace, cache_key)
                return cached

        if self._html_circuit_is_open():
            raise RuntimeError("HTML circuit open – skipping")

        t0 = time.time()
        backoff = _BACKOFF_BASE
        last_exc: Exception = RuntimeError("no attempt")

        for attempt in range(3):
            try:
                self._respect_rate_limit(min_interval=_HTML_MIN_INTERVAL)
                response = self.session.get(url, timeout=15)

                # sec.douban.com 重定向 = 被风控，当作软限频处理
                if self._has_sec_redirect(response):
                    logger.warning(
                        "sec.douban.com 重定向（attempt %d）: %s", attempt + 1, url
                    )
                    self._html_record_failure()
                    self._refresh_bid()
                    wait = backoff + random.uniform(0, backoff * 0.4)
                    logger.info("退避 %.1fs …", wait)
                    time.sleep(wait)
                    backoff = min(backoff * 2, _BACKOFF_MAX)
                    last_exc = RuntimeError("sec.douban.com redirect")
                    continue

                response.raise_for_status()
                text = response.text
                self._html_record_success()
                elapsed = time.time() - t0
                logger.info("[douban] HTML 请求完成 耗时 %.2fs  key=%s", elapsed, cache_key or url)

                if cache_namespace and cache_key:
                    self.cache.set(cache_namespace, cache_key, text)
                return text

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else 0
                if status == 429:
                    self._html_record_failure()
                    self._refresh_bid()
                    wait = backoff + random.uniform(0, backoff * 0.4)
                    logger.warning(
                        "429 Too Many Requests（attempt %d），退避 %.1fs: %s",
                        attempt + 1, wait, url,
                    )
                    time.sleep(wait)
                    backoff = min(backoff * 2, _BACKOFF_MAX)
                    last_exc = exc
                    if self._html_circuit_is_open():
                        raise
                else:
                    self._html_record_failure()
                    raise

        raise last_exc

    def get_top_movies_by_year(self, year, limit=20, min_rating=0.0, with_details=False):
        t0 = time.time()
        logger.info("[douban] 开始抓取年份 %s (limit=%s, min_rating=%s)", year, limit, min_rating)
        params = {
            "sort": "S",
            "range": "0,10",
            "tags": "电影",
            "start": 0,
            "year_range": f"{year},{year}",
            "limit": limit,
        }

        try:
            data = self._request_json(
                self.BASE_URL,
                params=params,
                cache_namespace="douban_list",
                cache_key=f"{year}:{limit}:{min_rating}",
                ttl_hours=24 * 7,
            )
            movies = data.get("data", [])
            results = []
            for movie in movies:
                rating = float(movie.get("rate") or 0)
                if rating < min_rating:
                    continue

                item = {
                    "douban_id": movie.get("id"),
                    "title": movie.get("title"),
                    "rating": rating,
                    "cover": movie.get("cover"),
                    "url": movie.get("url"),
                    "year": year,
                    "directors": movie.get("directors", []),
                    "casts": movie.get("casts", []),
                    "genres": [],
                    "summary": "",
                    "countries": [],
                    "release_date": "",
                    "release_date_source": "",
                    "release_date_confidence": "unknown",
                }

                if with_details and item["douban_id"]:
                    details = self.get_movie_details(item["douban_id"])
                    if details:
                        item.update(details)
                results.append(item)

            elapsed = time.time() - t0
            logger.info("[douban] 年份 %s 完成 耗时 %.2fs  共 %d 部", year, elapsed, len(results))
            if self.state_store:
                self.state_store.record_fetch_event("douban_list", str(year), "ok", f"{len(results)} items")
            return results
        except Exception as error:
            elapsed = time.time() - t0
            logger.error("[douban] 年份 %s 失败 耗时 %.2fs  error=%s", year, elapsed, error)
            if self.state_store:
                self.state_store.record_fetch_event("douban_list", str(year), "failed", str(error))
            return []

    def collect_candidate_pool(self, months_window, per_year_limit=60, min_rating=0.0):
        t0 = time.time()
        current_year = date.today().year
        year_candidates = sorted({current_year, current_year - 1}, reverse=True)
        total_years = len(year_candidates)
        logger.info("[douban] 开始收集候选池 当年+去年 共 %d 年 (per_year_limit=%s)", total_years, per_year_limit)
        movies = []
        for idx, year in enumerate(year_candidates, 1):
            year_movies = self.get_top_movies_by_year(year, limit=per_year_limit, min_rating=min_rating, with_details=False)
            movies.extend(year_movies)
            logger.info("[douban] 进度 %d/%d 年  %s 本批 %d 部 累计 %d 部", idx, total_years, year, len(year_movies), len(movies))
        elapsed = time.time() - t0
        logger.info("[douban] 候选池收集完成 总耗时 %.2fs  去重前 %d 部", elapsed, len(movies))
        return movies

    def collect_candidate_pool_by_years(self, years_window: int = 2, per_year_limit: int = 60, min_rating: float = 0.0):
        """场景一（批量查询）：按最近 years_window 年范围抓取候选，合并去重。

        遍历 current_year 到 current_year - (years_window - 1)，
        每年调用 get_top_movies_by_year，以 movie_key 去重（后出现覆盖先出现）。
        """
        t0 = time.time()
        current_year = date.today().year
        start_year = current_year - max(years_window - 1, 0)
        year_list = list(range(start_year, current_year + 1))
        total_years = len(year_list)
        logger.info("[douban] 开始按年抓取 范围 %s~%s 共 %d 年 (per_year_limit=%s)", start_year, current_year, total_years, per_year_limit)
        movies_by_key: dict = {}
        for idx, year in enumerate(year_list, 1):
            year_movies = self.get_top_movies_by_year(
                year, limit=per_year_limit, min_rating=min_rating, with_details=False
            )
            for movie in year_movies:
                key = f"douban:{movie.get('douban_id')}"
                movies_by_key[key] = movie
            logger.info("[douban] 进度 %d/%d 年  %s 本批 %d 部 去重后累计 %d 部", idx, total_years, year, len(year_movies), len(movies_by_key))
        result = list(movies_by_key.values())
        elapsed = time.time() - t0
        logger.info("[douban] 按年抓取完成 总耗时 %.2fs  去重后 %d 部", elapsed, len(result))
        return result

    def get_movie_abstract(self, douban_id):
        t0 = time.time()
        url = f"https://movie.douban.com/j/subject_abstract?subject_id={douban_id}"
        headers = self.headers.copy()
        headers["X-Requested-With"] = "XMLHttpRequest"
        try:
            data = self._request_json(
                url,
                headers=headers,
                cache_namespace="douban_abstract",
                cache_key=str(douban_id),
                ttl_hours=24 * 14,
            )
            elapsed = time.time() - t0
            subject = data.get("subject", {})
            if not subject:
                logger.info("[douban] abstract douban_id=%s 无 subject 耗时 %.2fs", douban_id, elapsed)
                return None

            result = {
                "genres": subject.get("types", []),
                "countries": [subject.get("region")] if subject.get("region") else [],
                "duration": subject.get("duration"),
                "release_year": subject.get("release_year"),
                "summary": subject.get("short_comment", {}).get("content", ""),
                "release_date": "",
                "release_date_source": "",
                "release_date_confidence": "unknown",
            }
            if result["release_year"]:
                maybe_update_release_date(result, str(result["release_year"]), "douban_abstract", "low")
            logger.info("[douban] abstract douban_id=%s 耗时 %.2fs", douban_id, elapsed)
            return result
        except Exception as error:
            elapsed = time.time() - t0
            logger.error("[douban] abstract douban_id=%s 失败 耗时 %.2fs  error=%s", douban_id, elapsed, error)
            if self.state_store:
                self.state_store.record_fetch_event("douban_abstract", str(douban_id), "failed", str(error))
            return None

    def _extract_release_date(self, info_text, soup):
        release_date_tag = soup.find("span", property="v:initialReleaseDate")
        if release_date_tag:
            raw_date = release_date_tag.get_text(strip=True)
        else:
            raw_date = " ".join(line.strip() for line in info_text.splitlines() if "上映日期" in line)

        match = re.search(r"\d{4}-\d{2}-\d{2}", raw_date)
        if match:
            return match.group(0)

        match = re.search(r"\d{4}-\d{2}", raw_date)
        if match:
            return match.group(0)
        return ""

    def get_movie_details(self, douban_id, include_html=True):
        t0 = time.time()
        result = self.get_movie_abstract(douban_id) or {
            "genres": [],
            "countries": [],
            "duration": "",
            "summary": "",
            "release_date": "",
            "release_date_source": "",
            "release_date_confidence": "unknown",
        }

        if not include_html:
            logger.info("[douban] details douban_id=%s (仅abstract) 耗时 %.2fs", douban_id, time.time() - t0)
            return result

        # 熔断器开路时直接跳过 HTML 请求，用 abstract 结果即可
        if self._html_circuit_is_open():
            logger.info("HTML 熔断器开路，跳过详情页: %s", douban_id)
            if self.state_store:
                self.state_store.record_fetch_event(
                    "douban_detail", str(douban_id), "circuit_open", "skipped"
                )
            return result

        try:
            html = self._request_text(
                self.DETAIL_URL_TEMPLATE.format(douban_id),
                cache_namespace="douban_detail",
                cache_key=str(douban_id),
                ttl_hours=24 * 30,
            )
            soup = BeautifulSoup(html, "html.parser")
            page_title = soup.title.string if soup.title else ""
            if "登录" in page_title or "禁止访问" in page_title:
                if self.state_store:
                    self.state_store.record_fetch_event(
                        "douban_detail", str(douban_id), "blocked", page_title
                    )
                return result

            info = soup.find("div", id="info")
            if not info:
                return result

            info_text = info.get_text("\n")
            imdb_link = info.find("a", href=lambda v: v and "imdb.com/title/tt" in v)
            summary_tag = soup.find("span", property="v:summary")
            duration_tag = soup.find("span", property="v:runtime")
            genres = [tag.get_text(strip=True) for tag in info.find_all("span", property="v:genre")]

            country_line = next(
                (line for line in info_text.splitlines() if "制片国家/地区:" in line), ""
            )
            countries = (
                country_line.replace("制片国家/地区:", "").strip().split(" / ")
                if country_line else []
            )

            if summary_tag and not result.get("summary"):
                result["summary"] = summary_tag.get_text(strip=True)
            if duration_tag and not result.get("duration"):
                result["duration"] = duration_tag.get_text(strip=True)
            if genres:
                result["genres"] = genres
            if countries:
                result["countries"] = countries
            if imdb_link:
                result["imdb_id"] = imdb_link.get_text(strip=True)

            maybe_update_release_date(
                result, self._extract_release_date(info_text, soup), "douban_detail", "high"
            )

            if self.state_store:
                self.state_store.record_fetch_event(
                    "douban_detail", str(douban_id), "ok", "html parsed"
                )
            elapsed = time.time() - t0
            logger.info("[douban] details douban_id=%s 耗时 %.2fs", douban_id, elapsed)
            return result
        except Exception as error:
            elapsed = time.time() - t0
            logger.error("[douban] details douban_id=%s 失败 耗时 %.2fs  error=%s", douban_id, elapsed, error)
            if self.state_store:
                self.state_store.record_fetch_event(
                    "douban_detail", str(douban_id), "failed", str(error)
                )
            return result


if __name__ == "__main__":
    spider = DoubanSpider()
    print(spider.get_top_movies_by_year(2025, limit=2, min_rating=5.0))
