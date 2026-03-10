from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

# "batch"  = 快速查询：按年抓取、评分排序、不强调具体日期、不生成推荐记录
# "scheduled" = 定时推荐：严格时间窗、Playwright 补日期、飞书卡片推送
DigestType = Literal["batch", "scheduled"]

# 推送间隔选项
PushInterval = Literal["1week", "2weeks", "1month", "2months"]


@dataclass
class BatchQueryConfig:
    """场景一：快速查询——按年范围抓取高分影片，海报展示仅年份，不生成推荐记录。"""
    digest_type: DigestType = "batch"
    years_window: int = 2                # 最近 1~5 年
    min_rating: float = 5.0              # 最低评分（>5 起）
    max_candidates: int = 15             # 单次展示数量（支持追加）
    per_year_limit: int = 60             # 每年豆瓣抓取条数
    region_scope: str = "all"
    tmdb_api_key: Optional[str] = None


@dataclass
class ScheduledRecommendConfig:
    """场景二：定时推荐——Playwright 获取日期、严格月份时间窗、飞书卡片推送。"""
    digest_type: DigestType = "scheduled"
    months_window: int = 1               # 最近 1~5 个月（严格范围筛选）
    min_rating: float = 6.0              # 最低评分（≥6）
    max_candidates: int = 5              # 推荐数量 1~10
    per_year_limit: int = 60             # 每年豆瓣抓取条数
    output_format: str = "csv"
    push_channel: str = "file"
    push_enabled: bool = False
    push_interval: PushInterval = "1month"   # 1week / 2weeks / 1month / 2months
    region_scope: str = "all"
    allow_repeat: bool = False           # 允许重复推送（始终选最高分）
    feishu_webhook_url: Optional[str] = None  # 飞书 Webhook 地址
    feishu_app_id: Optional[str] = None      # 可选，用于图片上传
    feishu_app_secret: Optional[str] = None  # 可选，用于图片上传
    tmdb_api_key: Optional[str] = None


# ── 向后兼容：原有 MonthlyDigestConfig 保留 ──────────────────────────────────
@dataclass
class MonthlyDigestConfig:
    """兼容旧接口；新代码请直接使用 BatchQueryConfig / ScheduledRecommendConfig。"""
    digest_type: DigestType = "scheduled"
    months_window: int = 3
    min_rating: float = 5.0
    max_candidates: int = 12
    per_year_limit: int = 60
    output_format: str = "csv"
    push_channel: str = "file"
    push_enabled: bool = False
    push_interval: str = "1month"
    region_scope: str = "all"
    include_html_detail: bool = False
    use_playwright_repair: bool = False
    only_unseen: bool = True
    allow_repeat: bool = False
    tmdb_api_key: Optional[str] = None
    years_window: int = 2


@dataclass
class DigestRunResult:
    digest_id: str
    title: str
    digest_type: DigestType = "scheduled"
    movies: List[Dict[str, Any]] = field(default_factory=list)
    skipped_movies: List[Dict[str, Any]] = field(default_factory=list)
    repair_queue: List[Dict[str, Any]] = field(default_factory=list)
    markdown: str = ""
    export_path: Optional[str] = None
    markdown_path: Optional[str] = None
    pushed_channels: List[str] = field(default_factory=list)
    time_window_start: Optional[str] = None
    time_window_end: Optional[str] = None
