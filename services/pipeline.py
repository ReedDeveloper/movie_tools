import json
import logging
import os
import subprocess
import sys
import time
from dataclasses import asdict
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Union

from models import BatchQueryConfig, DigestRunResult, MonthlyDigestConfig, ScheduledRecommendConfig
from spiders.douban import DoubanSpider
from services.feishu_push import FeishuCardPushService
from services.push import PushService
from services.tmdb import TMDBService
from utils.date_utils import (
    display_release_date,
    is_in_strict_window,
    is_in_year_range,
    window_start,
)
from utils.processor import clean_movie_data
from utils.state_store import StateStore
from utils.storage import save_digest_markdown, save_to_csv, save_to_excel


logger = logging.getLogger(__name__)

AnyConfig = Union[BatchQueryConfig, ScheduledRecommendConfig, MonthlyDigestConfig]


# ═══════════════════════════════════════════════════════════════════════════════
# 场景一：快速查询（BatchQueryService）——不生成推荐记录，直接返回排序结果
# ═══════════════════════════════════════════════════════════════════════════════

class BatchQueryService:
    """快速查询高分影片，按评分降序返回，不写入 digest 表。

    支持追加模式：传入 exclude_keys 可排除已展示的影片，实现「继续加载」。
    """

    def __init__(self, config: BatchQueryConfig, state_store: Optional[StateStore] = None):
        self.config = config
        self.state_store = state_store or StateStore()
        self.spider = DoubanSpider(state_store=self.state_store)

    def query(self, exclude_keys: Optional[List[str]] = None) -> List[Dict]:
        """
        查询满足条件的影片列表（评分降序）。

        Args:
            exclude_keys: 需要排除的 movie_key 列表（已展示过的）。

        Returns:
            不超过 config.max_candidates 部影片的列表。
        """
        t0 = time.time()
        config = self.config
        exclude_count = len(exclude_keys) if exclude_keys else 0
        logger.info(
            "[pipeline] 快速查询开始  years_window=%s min_rating=%s max_candidates=%s  排除已展示=%d 部",
            config.years_window, config.min_rating, config.max_candidates, exclude_count,
        )
        raw_movies = self.spider.collect_candidate_pool_by_years(
            years_window=config.years_window,
            per_year_limit=config.per_year_limit,
            min_rating=config.min_rating,
        )

        # 去重、清洗
        deduped: Dict[str, Dict] = {}
        for movie in raw_movies:
            normalized = clean_movie_data(movie)
            key = normalized["movie_key"]
            # 保留评分最高的（按豆瓣列表顺序，一般已排好，直接取最后覆盖）
            if key not in deduped or float(normalized.get("rating") or 0) > float(deduped[key].get("rating") or 0):
                deduped[key] = normalized

        # 地区过滤
        candidates = list(deduped.values())
        if config.region_scope != "all":
            candidates = [
                m for m in candidates
                if config.region_scope.lower() in str(m.get("countries", "")).lower()
            ]

        # 年份范围过滤
        candidates = [
            m for m in candidates
            if is_in_year_range(
                str(m.get("year") or m.get("release_date") or ""),
                config.years_window,
            )
        ]

        # 按评分降序
        candidates.sort(key=lambda m: float(m.get("rating") or 0), reverse=True)

        # 排除已展示的
        if exclude_keys:
            exclude_set = set(exclude_keys)
            candidates = [m for m in candidates if m["movie_key"] not in exclude_set]

        result = candidates[: config.max_candidates]

        # 写入 movies_enriched（仅缓存，不入 digest）
        for movie in result:
            self.state_store.upsert_movie(movie)

        elapsed = time.time() - t0
        logger.info("[pipeline] 快速查询完成  返回 %d 部  总耗时 %.2fs", len(result), elapsed)
        return result


# ═══════════════════════════════════════════════════════════════════════════════
# 共享子组件
# ═══════════════════════════════════════════════════════════════════════════════

class CandidateCollector:
    def __init__(self, spider: DoubanSpider):
        self.spider = spider

    def collect(self, config: AnyConfig) -> List[Dict]:
        months_window = getattr(config, "months_window", 3)
        raw_movies = self.spider.collect_candidate_pool(
            months_window=months_window,
            per_year_limit=config.per_year_limit,
            min_rating=config.min_rating,
        )
        deduped: Dict[str, Dict] = {}
        for movie in raw_movies:
            normalized = clean_movie_data(movie)
            deduped[normalized["movie_key"]] = normalized
        return list(deduped.values())


class MetadataEnricher:
    def __init__(self, spider: DoubanSpider, tmdb_service: TMDBService, state_store: StateStore):
        self.spider = spider
        self.tmdb_service = tmdb_service
        self.state_store = state_store

    def enrich(self, movies: List[Dict], include_html_detail: bool = False) -> Tuple[List[Dict], List[Dict]]:
        t0 = time.time()
        total = len(movies)
        logger.info("[pipeline] 元数据补全开始  共 %d 部", total)
        enriched_movies = []
        repair_queue = []

        for idx, movie in enumerate(movies, 1):
            movie_key = clean_movie_data(movie)["movie_key"]
            cached = self.state_store.get_movie(movie_key)
            current = dict(cached or movie)
            current.update(movie)

            release_confidence = current.get("release_date_confidence", "unknown")
            release_date_val = current.get("release_date", "") or ""
            date_incomplete = (
                not release_date_val
                or len(release_date_val) < 7
                or release_confidence in {"unknown", "low"}
            )
            should_fetch_details = date_incomplete and current.get("douban_id")
            if should_fetch_details:
                details = self.spider.get_movie_details(
                    current["douban_id"], include_html=include_html_detail
                )
                if details:
                    current.update(details)

            release_after = current.get("release_date", "") or ""
            confidence_after = current.get("release_date_confidence", "unknown")
            date_still_incomplete = (
                not release_after
                or len(release_after) < 7
                or confidence_after in {"unknown", "low"}
            )
            should_fetch_tmdb = (
                date_still_incomplete
                or not current.get("genres")
                or not current.get("duration")
            )
            if should_fetch_tmdb:
                current = self.tmdb_service.enrich_movie_data(current)
            current = clean_movie_data(current)

            if len(str(current.get("release_date") or "")) < 7:
                current["decision_status"] = "repair"
                repair_queue.append(current)

            self.state_store.upsert_movie(current)
            enriched_movies.append(current)
            if idx % 10 == 0 or idx == total:
                logger.info("[pipeline] 元数据补全进度  %d/%d", idx, total)

        elapsed = time.time() - t0
        logger.info(
            "[pipeline] 元数据补全完成  耗时 %.2fs  待 Playwright 补全 %d 部",
            elapsed, len(repair_queue),
        )
        return enriched_movies, repair_queue


class PlaywrightRepairWorker:
    """对仍缺月份级 release_date 的影片通过子进程运行 Playwright 补全。"""

    def __init__(self, cache, state_store: StateStore):
        self.cache = cache
        self.state_store = state_store

    def repair(self, enriched_movies: List[Dict], repair_queue: List[Dict]) -> List[Dict]:
        from spiders.playwright_spider import playwright_available

        if not repair_queue:
            return enriched_movies

        if not playwright_available():
            logger.warning(
                "Playwright 未安装，跳过日期补全（%d 部影片仍缺月份日期）。"
                "安装方法：pip install playwright && playwright install chromium",
                len(repair_queue),
            )
            return enriched_movies

        logger.info("[pipeline] 启动 Playwright 子进程补全日期，共 %d 部 …", len(repair_queue))
        pw_t0 = time.time()
        enriched_index: Dict[str, Dict] = {m["movie_key"]: dict(m) for m in enriched_movies}

        try:
            repaired_list = self._run_in_subprocess(repair_queue)
            logger.info("[pipeline] Playwright 子进程完成  耗时 %.2fs", time.time() - pw_t0)
            for movie in repaired_list:
                key = movie.get("movie_key")
                if key and len(str(movie.get("release_date") or "")) >= 7:
                    enriched_index[key] = movie
                    self.state_store.upsert_movie(movie)
        except Exception as exc:
            logger.error("Playwright 补全失败: %s", exc)

        return list(enriched_index.values())

    def _run_in_subprocess(self, repair_queue: List[Dict]) -> List[Dict]:
        runner = os.path.normpath(
            os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "..", "spiders", "pw_runner.py"
            )
        )
        timeout_secs = 60 + len(repair_queue) * 25
        proc = subprocess.run(
            [sys.executable, runner],
            input=json.dumps(repair_queue, ensure_ascii=False).encode("utf-8"),
            capture_output=True,
            timeout=timeout_secs,
        )
        stdout_text = (proc.stdout or b"").decode("utf-8", errors="replace").strip()
        stderr_text = (proc.stderr or b"").decode("utf-8", errors="replace")

        if proc.returncode != 0:
            raise RuntimeError(
                f"pw_runner 退出码 {proc.returncode}:\n{stderr_text[-2000:]}"
            )
        for line in stderr_text.splitlines():
            if line.strip():
                logger.info("[pw_runner] %s", line.strip())
        return json.loads(stdout_text) if stdout_text else []


class DecisionEngine:
    def __init__(self, state_store: StateStore):
        self.state_store = state_store

    def select(self, movies: List[Dict], config: AnyConfig) -> Tuple[List[Dict], List[Dict]]:
        months_window = getattr(config, "months_window", 3)
        allow_repeat = getattr(config, "allow_repeat", False)

        # allow_repeat=True 时不过滤历史已推送
        use_only_unseen = not allow_repeat
        sent_keys: set = set()
        if use_only_unseen:
            sent_keys = set(self.state_store.get_sent_movie_keys(digest_type="scheduled"))

        feedback_map = self.state_store.get_feedback_map([m["movie_key"] for m in movies])

        selected = []
        skipped = []

        for movie in movies:
            movie = dict(movie)
            feedback_status = feedback_map.get(movie["movie_key"], "new")
            movie["feedback_status"] = feedback_status

            if use_only_unseen and (
                movie["movie_key"] in sent_keys or feedback_status in {"seen", "skip"}
            ):
                skipped.append(movie)
                continue

            if movie.get("rating", 0) < config.min_rating:
                skipped.append(movie)
                continue

            # 严格时间窗：必须有月/日级日期
            if not is_in_strict_window(movie.get("release_date"), months_window):
                skipped.append(movie)
                continue

            if config.region_scope != "all":
                if config.region_scope.lower() not in str(movie.get("countries", "")).lower():
                    skipped.append(movie)
                    continue

            movie["decision_score"] = self._score(movie, months_window)
            movie["decision_status"] = "selected"
            selected.append(movie)

        selected.sort(key=lambda m: m["decision_score"], reverse=True)
        return selected[: config.max_candidates], skipped

    def _score(self, movie: Dict, months_window: int) -> float:
        freshness = 30 if is_in_strict_window(movie.get("release_date"), max(months_window, 1)) else 0
        confidence_bonus = {"high": 10, "medium": 6, "low": 2}.get(
            movie.get("release_date_confidence", "unknown"), 0
        )
        vote_bonus = float(movie.get("tmdb_votes") or 0) / 1000
        return float(movie.get("rating") or 0) * 10 + freshness + confidence_bonus + vote_bonus


class DigestBuilder:
    def build(self, movies: List[Dict], config: AnyConfig) -> str:
        months_window = getattr(config, "months_window", 3)
        today = datetime.now().strftime("%Y-%m-%d")
        start_date = window_start(months_window).strftime("%Y-%m-%d")

        lines = [
            f"# 定时电影推荐 - {today}",
            "",
            f"- 时间窗口: 最近 {months_window} 个月（{start_date} ~ {today}）",
            f"- 最低评分: 豆瓣 {config.min_rating}+",
            f"- 推荐数量: {len(movies)}",
            "",
        ]

        if not movies:
            lines.append("本期没有满足条件的新片。")
            return "\n".join(lines)

        for index, movie in enumerate(movies, start=1):
            lines.extend([
                f"## {index}. {movie['title']}",
                f"- 豆瓣评分: {movie.get('rating', 0)}",
                f"- 上映日期: {display_release_date(movie.get('release_date'), movie.get('year'))}",
                f"- 类型: {movie.get('genres') or '待补全'}",
                f"- 地区: {movie.get('countries') or '待补全'}",
                f"- 一句话简介: {(movie.get('summary') or '暂无简介')[:120]}",
                f"- 豆瓣链接: {movie.get('url') or '暂无'}",
                "",
            ])
        return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# 场景二：定时推荐（MovieDigestService）——Playwright 内置，飞书卡片推送
# ═══════════════════════════════════════════════════════════════════════════════

class MovieDigestService:
    """定时推荐主服务：Playwright 作为主要日期获取手段，结果入 digest，支持飞书卡片推送。"""

    def __init__(self, config: AnyConfig, state_store: Optional[StateStore] = None):
        self.config = config
        self.state_store = state_store or StateStore()
        self.spider = DoubanSpider(state_store=self.state_store)
        self.tmdb_service = TMDBService(
            api_key=getattr(config, "tmdb_api_key", None),
            state_store=self.state_store,
        )
        self.collector = CandidateCollector(self.spider)
        self.enricher = MetadataEnricher(self.spider, self.tmdb_service, self.state_store)
        self.pw_repair_worker = PlaywrightRepairWorker(self.spider.cache, self.state_store)
        self.decision_engine = DecisionEngine(self.state_store)
        self.digest_builder = DigestBuilder()
        self.push_service = PushService()

    def run(self) -> DigestRunResult:
        config = self.config
        months_window = getattr(config, "months_window", 3)
        push_interval = getattr(config, "push_interval", "1month")

        # ── 1. 候选收集 ──────────────────────────────────────────────────────
        phase_t0 = time.time()
        logger.info("[pipeline] 阶段 1/6 候选收集 开始")
        candidates = self.collector.collect(config)
        logger.info("[pipeline] 阶段 1/6 候选收集 完成  共 %d 部  耗时 %.2fs", len(candidates), time.time() - phase_t0)

        # ── 2. 元数据补全（TMDB + 抽象 API）─────────────────────────────────
        phase_t0 = time.time()
        logger.info("[pipeline] 阶段 2/6 元数据补全 开始")
        enriched_movies, repair_queue = self.enricher.enrich(candidates, include_html_detail=False)
        logger.info("[pipeline] 阶段 2/6 元数据补全 完成  耗时 %.2fs  待 Playwright %d 部", time.time() - phase_t0, len(repair_queue))

        # ── 3. Playwright 补全日期（定时推荐始终执行，对有 repair_queue 的影片）────
        if repair_queue:
            phase_t0 = time.time()
            logger.info("[pipeline] 阶段 3/6 Playwright 日期补全 开始  待处理 %d 部", len(repair_queue))
            enriched_movies = self.pw_repair_worker.repair(enriched_movies, repair_queue)
            logger.info("[pipeline] 阶段 3/6 Playwright 日期补全 完成  耗时 %.2fs", time.time() - phase_t0)
            # 重新统计仍缺日期的（供记录用，不影响决策逻辑）
            repair_queue = [
                m for m in enriched_movies
                if len(str(m.get("release_date") or "")) < 7
            ]
            logger.info("[pipeline] Playwright 修复后仍缺月份日期: %d 部", len(repair_queue))

        # ── 4. 决策筛选（严格时间窗 + 评分排序）─────────────────────────────
        phase_t0 = time.time()
        logger.info("[pipeline] 阶段 4/6 决策筛选 开始")
        selected_movies, skipped_movies = self.decision_engine.select(enriched_movies, config)
        logger.info(
            "[pipeline] 阶段 4/6 决策筛选 完成  入选 %d 部 跳过 %d 部  耗时 %.2fs",
            len(selected_movies), len(skipped_movies), time.time() - phase_t0,
        )

        # ── 5. 构建摘要 & 持久化 ─────────────────────────────────────────────
        phase_t0 = time.time()
        logger.info("[pipeline] 阶段 5/6 构建摘要与持久化 开始")
        digest_id = datetime.now().strftime("%Y%m%d%H%M%S")
        tw_start = window_start(months_window).strftime("%Y-%m-%d")
        tw_end = datetime.now().strftime("%Y-%m-%d")
        title = f"定时推荐 最近{months_window}个月高分电影"

        markdown = self.digest_builder.build(selected_movies, config)
        config_dict = asdict(config)  # type: ignore[arg-type]
        export_fmt = getattr(config, "output_format", "csv")
        if export_fmt == "excel":
            export_path = save_to_excel(selected_movies, f"digest_{digest_id}.xlsx")
        else:
            export_path = save_to_csv(selected_movies, f"digest_{digest_id}.csv")
        markdown_path = save_digest_markdown(markdown, f"digest_{digest_id}.md")
        logger.info("[pipeline] 阶段 5/6 构建摘要与持久化 完成  耗时 %.2fs", time.time() - phase_t0)

        payload = {
            "title": title,
            "digest_type": "scheduled",
            "movies": selected_movies,
            "skipped_movies": skipped_movies,
            "repair_queue": repair_queue,
            "config": config_dict,
            "time_window_start": tw_start,
            "time_window_end": tw_end,
        }
        status = "pushed" if getattr(config, "push_enabled", False) else "generated"
        self.state_store.create_digest(
            digest_id=digest_id,
            config=config_dict,
            movies=selected_movies,
            markdown_path=markdown_path,
            export_path=export_path,
            payload=payload,
            status=status,
            digest_type="scheduled",
            time_window_start=tw_start,
            time_window_end=tw_end,
            push_interval=push_interval,
        )

        # ── 6. 推送（飞书卡片优先，回退到通用推送）────────────────────────────
        pushed_channels: List[str] = []
        if getattr(config, "push_enabled", False):
            push_t0 = time.time()
            logger.info("[pipeline] 阶段 6/6 推送 开始")
            feishu_webhook = (
                getattr(config, "feishu_webhook_url", None)
                or os.getenv("FEISHU_WEBHOOK_URL")
            )
            feishu_app_id = getattr(config, "feishu_app_id", None) or os.getenv("FEISHU_APP_ID")
            feishu_app_secret = (
                getattr(config, "feishu_app_secret", None)
                or os.getenv("FEISHU_APP_SECRET")
            )

            if feishu_webhook:
                feishu_svc = FeishuCardPushService(
                    webhook_url=feishu_webhook,
                    app_id=feishu_app_id,
                    app_secret=feishu_app_secret,
                )
                time_window_str = f"{tw_start} ~ {tw_end}"
                if feishu_svc.send_movies(
                    title=f"🎬 {title}",
                    movies=selected_movies,
                    time_window=time_window_str,
                    min_rating=config.min_rating,
                ):
                    pushed_channels.append("feishu")
            else:
                # 回退通用推送（webhook/serverchan/bark）
                push_channel = getattr(config, "push_channel", "file")
                if self.push_service.send(push_channel, title, markdown):
                    pushed_channels.append(push_channel)
            logger.info("[pipeline] 阶段 6/6 推送 完成  通道=%s  耗时 %.2fs", pushed_channels or "无", time.time() - push_t0)

        return DigestRunResult(
            digest_id=digest_id,
            title=title,
            digest_type="scheduled",
            movies=selected_movies,
            skipped_movies=skipped_movies,
            repair_queue=repair_queue,
            markdown=markdown,
            export_path=export_path,
            markdown_path=markdown_path,
            pushed_channels=pushed_channels,
            time_window_start=tw_start,
            time_window_end=tw_end,
        )


# 向后兼容别名
MonthlyDigestService = MovieDigestService
