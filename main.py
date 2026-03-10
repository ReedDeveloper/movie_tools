import argparse
import logging
import os
import sys

from dotenv import load_dotenv

load_dotenv()

from models import BatchQueryConfig, MonthlyDigestConfig, ScheduledRecommendConfig
from services.pipeline import BatchQueryService, MovieDigestService
from services.tmdb import TMDBService
from spiders.douban import DoubanSpider
from utils.processor import clean_movie_data
from utils.storage import save_to_csv, save_to_excel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("movie_tools.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def run_catalog(args):
    """旧式按年目录抓取（保留向后兼容）。"""
    spider = DoubanSpider()
    tmdb_service = TMDBService(api_key=args.tmdb_api_key)
    all_movies = []

    for year in range(args.end_year, args.start_year - 1, -1):
        logger.info("Processing year %s...", year)
        douban_movies = spider.get_top_movies_by_year(
            year,
            limit=args.limit,
            min_rating=args.min_rating,
            with_details=args.include_html_detail,
        )
        for movie in douban_movies:
            try:
                all_movies.append(clean_movie_data(tmdb_service.enrich_movie_data(movie)))
            except Exception as error:
                logger.error("Error processing movie %s: %s", movie.get("title"), error)

    if not all_movies:
        logger.warning("No movies were processed.")
        return

    output_file = (
        save_to_excel(all_movies) if args.format == "excel" else save_to_csv(all_movies)
    )
    logger.info(
        "Successfully processed %s movies. Saved to %s", len(all_movies), output_file
    )


def run_batch(args):
    """场景一：快速查询高分影片（按年范围，不生成推荐记录）。"""
    config = BatchQueryConfig(
        years_window=args.years_window,
        min_rating=args.min_rating,
        max_candidates=args.max_candidates,
        per_year_limit=args.per_year_limit,
        region_scope=args.region_scope,
        tmdb_api_key=args.tmdb_api_key or os.getenv("TMDB_API_KEY"),
    )
    svc = BatchQueryService(config)
    movies = svc.query()
    logger.info("Batch query returned %s movies.", len(movies))
    for i, m in enumerate(movies, 1):
        logger.info(
            "%2d. %-40s  ★ %s  (%s)",
            i,
            m.get("title", ""),
            m.get("rating", ""),
            m.get("year") or str(m.get("release_date", ""))[:4],
        )


def run_scheduled(args):
    """场景二：定时推荐（Playwright 日期补全、严格月份时间窗、飞书卡片推送）。"""
    config = ScheduledRecommendConfig(
        months_window=args.months_window,
        min_rating=args.min_rating,
        max_candidates=args.max_candidates,
        per_year_limit=args.per_year_limit,
        output_format=args.format,
        push_channel=args.push_channel,
        push_enabled=args.push,
        push_interval=args.push_interval,
        region_scope=args.region_scope,
        allow_repeat=args.allow_repeat,
        feishu_webhook_url=args.feishu_webhook or os.getenv("FEISHU_WEBHOOK_URL"),
        feishu_app_id=args.feishu_app_id or os.getenv("FEISHU_APP_ID"),
        feishu_app_secret=args.feishu_app_secret or os.getenv("FEISHU_APP_SECRET"),
        tmdb_api_key=args.tmdb_api_key or os.getenv("TMDB_API_KEY"),
    )
    result = MovieDigestService(config).run()
    logger.info(
        "Scheduled digest %s generated with %s movies.", result.digest_id, len(result.movies)
    )
    if result.time_window_start:
        logger.info("Time window: %s ~ %s", result.time_window_start, result.time_window_end)
    if result.pushed_channels:
        logger.info("Pushed to: %s", ", ".join(result.pushed_channels))
    if result.export_path:
        logger.info("Export saved to %s", result.export_path)
    if result.markdown_path:
        logger.info("Digest markdown saved to %s", result.markdown_path)
    if result.repair_queue:
        logger.info("Movies still missing exact date: %s", len(result.repair_queue))


def main():
    parser = argparse.ArgumentParser(description="Movie tools task runner")
    subparsers = parser.add_subparsers(dest="command")

    # ── digest 子命令（双模式）──────────────────────────────────
    digest_parser = subparsers.add_parser(
        "digest", help="生成电影片单（batch=批量查询 / scheduled=定时推荐）"
    )
    digest_parser.add_argument(
        "--mode",
        choices=["batch", "scheduled"],
        default="scheduled",
        help="运行模式：batch=批量查询（按年）、scheduled=定时推荐（严格月份窗）",
    )

    # ── batch 专属参数
    digest_parser.add_argument(
        "--years-window", type=int, default=2, metavar="1-5",
        help="[batch] 最近 N 年（1~5，默认 2）",
    )

    # ── scheduled 专属参数
    digest_parser.add_argument(
        "--months-window", type=int, default=1, metavar="1-5",
        help="[scheduled] 最近 N 个月（1~5，默认 1）",
    )
    digest_parser.add_argument(
        "--push-interval",
        choices=["1week", "2weeks", "1month", "2months"],
        default="1month",
        help="[scheduled] 推送周期（1week / 2weeks / 1month / 2months）",
    )
    # 飞书推送参数（scheduled）
    digest_parser.add_argument("--feishu-webhook", default=None, help="[scheduled] 飞书 Webhook URL")
    digest_parser.add_argument("--feishu-app-id", default=None, help="[scheduled] 飞书 App ID（图片上传）")
    digest_parser.add_argument("--feishu-app-secret", default=None, help="[scheduled] 飞书 App Secret")

    # ── 通用参数
    digest_parser.add_argument(
        "--min-rating", type=float, default=None,
        help="最低豆瓣评分（batch 默认 5.0，scheduled 默认 6.0）",
    )
    digest_parser.add_argument(
        "--max-candidates", type=int, default=None,
        help="推荐/展示数量上限（batch 默认 15，scheduled 默认 5）",
    )
    digest_parser.add_argument("--per-year-limit", type=int, default=60)
    digest_parser.add_argument("--format", choices=["excel", "csv"], default="csv")
    digest_parser.add_argument(
        "--push-channel",
        choices=["file", "console", "webhook", "serverchan", "bark"],
        default="file",
        help="[scheduled] 回退推送通道（无飞书配置时使用）",
    )
    digest_parser.add_argument("--push", action="store_true", help="[scheduled] 执行推送")
    digest_parser.add_argument("--region-scope", default="all")
    digest_parser.add_argument(
        "--allow-repeat", action="store_true",
        help="[scheduled] 允许重复推送（始终选当前最高分）",
    )
    digest_parser.add_argument("--tmdb-api-key", default=None)

    # ── catalog 子命令（向后兼容）────────────────────────────────
    catalog_parser = subparsers.add_parser("catalog", help="旧式按年目录抓取")
    catalog_parser.add_argument("--start-year", type=int, default=2024)
    catalog_parser.add_argument("--end-year", type=int, default=2025)
    catalog_parser.add_argument("--limit", type=int, default=20)
    catalog_parser.add_argument("--min-rating", type=float, default=0.0)
    catalog_parser.add_argument("--format", choices=["excel", "csv"], default="excel")
    catalog_parser.add_argument("--include-html-detail", action="store_true")
    catalog_parser.add_argument("--tmdb-api-key", default=None)

    raw_args = sys.argv[1:]
    if raw_args and raw_args[0] in {"digest", "catalog"}:
        args = parser.parse_args(raw_args)
    else:
        args = parser.parse_args(["digest", *raw_args])

    command = args.command or "digest"

    if command == "catalog":
        run_catalog(args)
        return

    # digest 分发
    mode = getattr(args, "mode", "scheduled")

    # 填入 mode 对应的默认值（未显式传参时）
    if args.min_rating is None:
        args.min_rating = 5.0 if mode == "batch" else 6.0
    if args.max_candidates is None:
        args.max_candidates = 15 if mode == "batch" else 5

    if mode == "batch":
        run_batch(args)
    else:
        run_scheduled(args)


if __name__ == "__main__":
    main()
