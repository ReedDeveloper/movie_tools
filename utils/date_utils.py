from datetime import date, datetime, timedelta
from typing import Optional


CONFIDENCE_ORDER = {
    "unknown": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}


def parse_release_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y":
                return date(parsed.year, 1, 1)
            if fmt == "%Y-%m":
                return date(parsed.year, parsed.month, 1)
            return parsed.date()
        except ValueError:
            continue
    return None


def normalize_release_date(value: Optional[str]) -> str:
    if not value:
        return ""

    text = str(value).strip()
    if len(text) >= 10:
        return text[:10]
    if len(text) >= 7:
        return text[:7]
    if len(text) >= 4:
        return text[:4]
    return ""


def window_start(months_window: int, today: Optional[date] = None) -> date:
    current = today or date.today()
    days = max(months_window, 1) * 31
    return current - timedelta(days=days)


def is_recent_release(value: Optional[str], months_window: int, today: Optional[date] = None) -> bool:
    """宽松判断：仅年份时放宽，只要年份在窗内范围即认为满足。用于批量查询场景。"""
    if not value:
        return False
    text = str(value).strip()
    if not text:
        return False

    current = today or date.today()
    ws = window_start(months_window, current)

    # 仅年份：放宽判断
    if len(text) == 4 and text.isdigit():
        year = int(text)
        return ws.year <= year <= current.year

    release_date = parse_release_date(value)
    if not release_date:
        return False
    return ws <= release_date <= current


def is_in_strict_window(
    value: Optional[str],
    months_window: int,
    today: Optional[date] = None,
) -> bool:
    """严格判断：必须有月份级（≥7 字符）或日期级日期，且落在 [window_start, today] 内。
    仅年份的条目返回 False，用于定时推荐场景——避免把年份级日期误判为在窗内。
    """
    if not value:
        return False
    text = str(value).strip()
    if len(text) < 7:
        # 仅有年份或空：严格模式不接受
        return False

    current = today or date.today()
    ws = window_start(months_window, current)
    release_date = parse_release_date(value)
    if not release_date:
        return False
    return ws <= release_date <= current


def is_in_year_range(value: Optional[str], years_window: int, today: Optional[date] = None) -> bool:
    """判断影片是否在最近 years_window 年范围内，用于批量查询场景的年份过滤。"""
    if not value:
        return False
    text = str(value).strip()
    if not text:
        return False

    current = today or date.today()
    start_year = current.year - (years_window - 1)

    # 优先从日期字段取年份
    if len(text) >= 4 and text[:4].isdigit():
        year = int(text[:4])
        return start_year <= year <= current.year
    return False


def display_release_date(value: Optional[str], fallback_year: Optional[int] = None) -> str:
    normalized = normalize_release_date(value)
    if normalized:
        return normalized
    if fallback_year:
        return str(fallback_year)
    return "待确认"


def confidence_rank(value: Optional[str]) -> int:
    return CONFIDENCE_ORDER.get((value or "unknown").lower(), 0)
