import ast
import base64
import os

import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

from models import BatchQueryConfig, ScheduledRecommendConfig
from services.pipeline import BatchQueryService, MovieDigestService
from utils.date_utils import display_release_date, window_start
from utils.processor import clean_movie_data
from utils.state_store import StateStore

st.set_page_config(page_title="电影工具", page_icon="🎬", layout="wide")

state_store = StateStore()

# ──────────────────────────────────────────────────────────────────────────────
# CSS - 海报卡片样式
# ──────────────────────────────────────────────────────────────────────────────
st.markdown(
    """
<meta name="referrer" content="no-referrer" />
<style>
    .movie-container {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
        gap: 20px;
        padding: 10px;
    }
    .movie-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        overflow: hidden;
        transition: transform 0.2s, box-shadow 0.2s;
        display: flex;
        flex-direction: column;
        height: 100%;
        border: 1px solid #e0e0e0;
    }
    .movie-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.1);
    }
    .movie-poster-container {
        width: 100%;
        padding-top: 150%;
        position: relative;
        background-color: #ddd;
    }
    .movie-poster {
        position: absolute;
        top: 0; left: 0;
        width: 100%; height: 100%;
        object-fit: cover;
    }
    .movie-content {
        padding: 8px 10px;
        display: flex;
        flex-direction: column;
        flex-grow: 1;
        gap: 3px;
        font-size: 13px;
        line-height: 1.3;
    }
    .movie-title {
        font-weight: bold;
        font-size: 15px;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        margin-bottom: 2px;
        color: #333;
    }
    .movie-meta {
        display: flex;
        justify-content: space-between;
        align-items: center;
        font-size: 12px;
        color: #666;
    }
    .movie-rating       { color: #ff9900; font-weight: bold; font-size: 13px; }
    .movie-rating.high  { color: #e50914; }
    .movie-duration     { font-size: 11px; color: #888; }
    .movie-crew {
        font-size: 11px; color: #555;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        margin-bottom: 2px;
    }
    .movie-tags {
        display: flex; flex-wrap: nowrap; gap: 4px;
        margin-top: auto; padding-top: 4px;
        align-items: center; overflow: hidden;
    }
    .tag-container { display: flex; flex-wrap: nowrap; gap: 3px; flex-shrink: 0; }
    .movie-region {
        font-size: 10px; color: #777;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
        flex-grow: 1; min-width: 0;
    }
    .tag-badge {
        background-color: #e0e0e0; color: #444;
        padding: 1px 5px; border-radius: 3px;
        font-size: 10px; white-space: nowrap;
    }
    .movie-link {
        font-size: 11px; color: #0068c9;
        text-decoration: none; white-space: nowrap;
        flex-shrink: 0; margin-left: auto;
    }
    .movie-link:hover { text-decoration: underline; }
    @media (prefers-color-scheme: dark) {
        .movie-card        { background-color: #262730; border-color: #444; }
        .movie-title       { color: #eee; }
        .movie-meta        { color: #aaa; }
        .movie-crew        { color: #888; }
        .tag-badge         { background-color: #333; color: #ccc; }
    }
</style>
""",
    unsafe_allow_html=True,
)


# ──────────────────────────────────────────────────────────────────────────────
# 图片辅助：服务端 base64 绕过防盗链
# ──────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_image_as_base64(url: str) -> str:
    if not url or isinstance(url, float) or "placeholder" in url:
        return "https://via.placeholder.com/200x300?text=No+Image"
    if url.startswith("data:image"):
        return url
    try:
        resp = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://movie.douban.com/",
            },
            timeout=3,
        )
        if resp.status_code == 200:
            b64 = base64.b64encode(resp.content).decode("utf-8")
            return f"data:image/jpeg;base64,{b64}"
    except Exception:
        pass
    return url


def _parse_list_field(value) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return []
    if text.startswith("["):
        try:
            return ast.literal_eval(text)
        except Exception:
            text = text.strip("[]").replace("'", "").replace('"', "")
    return [item.strip() for item in text.split(",") if item.strip()]


def _parse_person_list(value) -> list:
    items = _parse_list_field(value)
    names = []
    for item in items:
        if isinstance(item, dict):
            names.append(item.get("name", str(item)))
        else:
            names.append(str(item))
    return names


def render_movie_cards(movies: list, show_date: bool = True) -> None:
    """渲染海报卡片网格。show_date=False 时只显示年份。"""
    if not movies:
        st.warning("暂无符合条件的影片。")
        return

    grid_html = '<div class="movie-container">'
    for row in movies:
        if not isinstance(row, dict):
            continue

        cover = row.get("cover") or ""
        display_url = (
            fetch_image_as_base64(cover)
            if cover
            else "https://via.placeholder.com/200x300?text=No+Image"
        )

        rating = float(row.get("rating") or 0)
        rating_class = "high" if rating >= 8.0 else ""

        directors = _parse_person_list(row.get("directors", []))
        casts = _parse_person_list(row.get("casts", []))
        crew_info = f"🎬 {' '.join(directors)}"
        if casts:
            crew_info += f" / 👤 {' '.join(casts[:2])}"

        tags = _parse_list_field(row.get("genres") or row.get("smart_tags") or "")
        tags = [t.strip() for t in tags if t.strip() and t.strip().lower() != "nan"][:3]
        tags_html = "".join(f'<span class="tag-badge">{t}</span>' for t in tags)

        region_str = "/".join(_parse_list_field(row.get("countries") or ""))
        duration_raw = str(row.get("duration") or "").strip()
        duration = (
            ""
            if duration_raw.lower() in ("", "nan", "none")
            else duration_raw.replace("minutes", "m").replace("分钟", "m")
        )

        if show_date:
            date_display = display_release_date(row.get("release_date"), row.get("year"))
        else:
            # 快速查询：只显示年份
            year_val = row.get("year") or (
                str(row.get("release_date", ""))[:4] if row.get("release_date") else ""
            )
            date_display = str(year_val) if year_val else "待确认"

        title = str(row.get("title") or "")
        link = str(row.get("url") or "#")

        grid_html += f"""
<div class="movie-card">
  <div class="movie-poster-container">
    <img src="{display_url}" class="movie-poster" loading="lazy" alt="{title}">
  </div>
  <div class="movie-content">
    <div class="movie-title" title="{title}">{title}</div>
    <div class="movie-meta">
      <span>{date_display}</span>
      <span class="movie-duration">{duration}</span>
      <span class="movie-rating {rating_class}">★ {rating}</span>
    </div>
    <div class="movie-crew" title="{crew_info}">{crew_info}</div>
    <div class="movie-tags">
      <div class="tag-container">{tags_html}</div>
      <div class="movie-region" title="{region_str}">{region_str}</div>
      <a href="{link}" target="_blank" class="movie-link">详情 &gt;</a>
    </div>
  </div>
</div>"""

    grid_html += "</div>"
    st.markdown(grid_html, unsafe_allow_html=True)


def load_digest_payload(digest_id):
    return state_store.get_digest_payload(digest_id) or {
        "movies": [],
        "repair_queue": [],
        "skipped_movies": [],
        "digest_type": "scheduled",
        "time_window_start": None,
        "time_window_end": None,
    }


def update_feedback(movie_key, status):
    state_store.set_feedback(movie_key, status)


# ──────────────────────────────────────────────────────────────────────────────
# Session state 初始化
# ──────────────────────────────────────────────────────────────────────────────
# 场景一：快速查询状态
if "batch_movies" not in st.session_state:
    st.session_state["batch_movies"] = []      # 已展示的影片列表（含历次追加）
if "batch_query_params" not in st.session_state:
    st.session_state["batch_query_params"] = {}  # 上次查询参数（用于检测变更）

# 场景二：定时推荐当前 digest
if "selected_digest_id" not in st.session_state:
    recent = state_store.list_recent_digests(limit=1, digest_type="scheduled")
    st.session_state["selected_digest_id"] = recent[0]["digest_id"] if recent else None


# ──────────────────────────────────────────────────────────────────────────────
# 侧栏：场景选择 + 参数配置
# ──────────────────────────────────────────────────────────────────────────────
st.sidebar.title("电影工具")

scene = st.sidebar.radio(
    "功能场景",
    ["快速查询", "定时推荐"],
    index=0,
    help="快速查询：按年份和评分即时查看高分片单；定时推荐：严格月份范围筛选，生成推荐并推送飞书。",
)

st.sidebar.markdown("---")
tmdb_api_key = st.sidebar.text_input(
    "TMDB API Key（可选）", type="password", value=os.getenv("TMDB_API_KEY", "")
)

# ── 场景一侧栏 ─────────────────────────────────────────────────────────────────
if scene == "快速查询":
    st.sidebar.subheader("查询参数")
    years_window = st.sidebar.selectbox("年份范围（最近 N 年）", [1, 2, 3, 4, 5], index=1)
    min_rating = st.sidebar.slider("最低评分", min_value=5.0, max_value=9.5, value=5.0, step=0.5)
    max_candidates = st.sidebar.slider("单次展示数量", min_value=5, max_value=20, value=10)
    per_year_limit = st.sidebar.slider("每年候选抓取量", min_value=20, max_value=120, value=60, step=10)
    region_scope_batch = st.sidebar.text_input("地区筛选（all 为不限）", value="all", key="region_batch")

    current_params = {
        "years_window": years_window,
        "min_rating": min_rating,
        "max_candidates": max_candidates,
        "per_year_limit": per_year_limit,
        "region_scope": region_scope_batch,
    }
    params_changed = current_params != st.session_state.get("batch_query_params")

    col_btn1, col_btn2 = st.sidebar.columns(2)
    do_fresh = col_btn1.button(
        "查询" if params_changed else "重新查询",
        type="primary",
        use_container_width=True,
    )
    do_append = col_btn2.button(
        "继续加载",
        type="secondary",
        use_container_width=True,
        disabled=len(st.session_state["batch_movies"]) == 0,
    )

    def _run_batch_query(append: bool = False):
        config = BatchQueryConfig(
            years_window=years_window,
            min_rating=min_rating,
            max_candidates=max_candidates,
            per_year_limit=per_year_limit,
            region_scope=region_scope_batch,
            tmdb_api_key=tmdb_api_key or None,
        )
        svc = BatchQueryService(config, state_store=state_store)
        exclude = (
            [m["movie_key"] for m in st.session_state["batch_movies"]] if append else None
        )
        with st.spinner("正在查询..." if not append else "继续加载..."):
            new_movies = svc.query(exclude_keys=exclude)

        if append:
            st.session_state["batch_movies"].extend(new_movies)
        else:
            st.session_state["batch_movies"] = new_movies
            st.session_state["batch_query_params"] = current_params

        return len(new_movies)

    if do_fresh:
        count = _run_batch_query(append=False)
        if count:
            st.sidebar.success(f"已展示 {count} 部，点击「继续加载」追加更多")
        else:
            st.sidebar.warning("未找到符合条件的影片，可尝试放宽条件。")

    if do_append:
        count = _run_batch_query(append=True)
        if count:
            st.sidebar.success(f"已追加 {count} 部")
        else:
            st.sidebar.info("没有更多新影片了。")

# ── 场景二侧栏 ─────────────────────────────────────────────────────────────────
else:
    st.sidebar.subheader("定时推荐参数")
    months_window = st.sidebar.selectbox("时间窗口（最近 N 个月）", [1, 2, 3, 4, 5], index=0)
    min_rating_sched = st.sidebar.slider(
        "最低评分", min_value=6.0, max_value=9.5, value=6.0, step=0.5, key="rating_sched"
    )
    max_candidates_sched = st.sidebar.slider(
        "推荐数量上限", min_value=1, max_value=10, value=5, key="cand_sched"
    )
    per_year_limit_sched = st.sidebar.slider(
        "每年候选抓取量", min_value=20, max_value=120, value=60, step=10, key="pyl_sched"
    )

    push_interval = st.sidebar.radio(
        "推送周期",
        ["1week", "2weeks", "1month", "2months"],
        index=2,
        format_func=lambda x: {"1week": "每 1 周", "2weeks": "每 2 周", "1month": "每 1 月", "2months": "每 2 月"}[x],
        horizontal=True,
    )
    allow_repeat = st.sidebar.checkbox("允许重复推送（始终选当前最高分）", value=False)

    st.sidebar.markdown("**飞书推送配置**")
    feishu_webhook = st.sidebar.text_input(
        "飞书 Webhook URL",
        value=os.getenv("FEISHU_WEBHOOK_URL", ""),
        help="飞书自定义机器人 Webhook 地址",
    )
    with st.sidebar.expander("飞书图片上传（可选，用于卡片展示海报）"):
        feishu_app_id = st.text_input(
            "飞书 App ID",
            value=os.getenv("FEISHU_APP_ID", ""),
            key="feishu_app_id",
        )
        feishu_app_secret = st.text_input(
            "飞书 App Secret",
            value=os.getenv("FEISHU_APP_SECRET", ""),
            type="password",
            key="feishu_app_secret",
        )

    push_enabled = st.sidebar.checkbox("生成后立即推送到飞书", value=False)
    region_scope_sched = st.sidebar.text_input(
        "地区筛选（all 为不限）", value="all", key="region_sched"
    )

    if st.sidebar.button("生成本期推荐", type="primary", use_container_width=True):
        config = ScheduledRecommendConfig(
            months_window=months_window,
            min_rating=min_rating_sched,
            max_candidates=max_candidates_sched,
            per_year_limit=per_year_limit_sched,
            output_format="csv",
            push_channel="file",
            push_enabled=push_enabled,
            push_interval=push_interval,
            region_scope=region_scope_sched,
            allow_repeat=allow_repeat,
            feishu_webhook_url=feishu_webhook or None,
            feishu_app_id=feishu_app_id or None,
            feishu_app_secret=feishu_app_secret or None,
            tmdb_api_key=tmdb_api_key or None,
        )
        with st.spinner("正在生成推荐（含 Playwright 日期补全，请稍候）..."):
            result = MovieDigestService(config, state_store=state_store).run()

        st.session_state["selected_digest_id"] = result.digest_id
        pushed_info = f"  已推送到：{', '.join(result.pushed_channels)}" if result.pushed_channels else ""
        st.sidebar.success(
            f"已生成 {len(result.movies)} 部推荐（编号 {result.digest_id}）{pushed_info}"
        )
        if result.repair_queue:
            st.sidebar.warning(f"仍有 {len(result.repair_queue)} 部影片缺少具体日期（已按年份展示）")


# ──────────────────────────────────────────────────────────────────────────────
# 主区域
# ──────────────────────────────────────────────────────────────────────────────
st.title("电影工具 · 快速查询 & 定时推荐")

if scene == "快速查询":
    # ── 快速查询主区域 ──────────────────────────────────────────────────────
    batch_movies = st.session_state.get("batch_movies", [])
    if not batch_movies:
        st.info("请在左侧配置参数，点击「查询」开始查看高分片单。")
    else:
        params = st.session_state.get("batch_query_params", {})
        st.caption(
            f"最近 {params.get('years_window', '?')} 年 | "
            f"评分 ≥ {params.get('min_rating', '?')} | "
            f"已展示 {len(batch_movies)} 部（按评分从高到低）"
        )
        render_movie_cards(batch_movies, show_date=False)

        # 继续加载提示
        st.markdown("---")
        st.caption("如需查看更多，点击左侧「继续加载」追加下一批。")

else:
    # ── 定时推荐主区域 ──────────────────────────────────────────────────────
    tab_current, tab_history, tab_library = st.tabs(["当前推荐", "历史记录", "影片库"])

    with tab_current:
        selected_digest_id = st.session_state.get("selected_digest_id")
        if not selected_digest_id:
            st.info("还没有生成任何推荐，请先在左侧点击「生成本期推荐」。")
        else:
            payload = load_digest_payload(selected_digest_id)
            movies = payload.get("movies", [])
            repair_queue = payload.get("repair_queue", [])
            tw_start = payload.get("time_window_start")
            tw_end = payload.get("time_window_end")

            m1, m2, m3 = st.columns(3)
            m1.metric("推荐数量", len(movies))
            m2.metric("待日期补全", len(repair_queue))
            m3.metric("已缓存影片", len(state_store.list_movies(limit=500)))

            if tw_start:
                st.caption(f"时间窗口：{tw_start} ~ {tw_end}")

            render_movie_cards(movies, show_date=True)

            # 观看决策
            if movies:
                st.markdown("---")
                st.subheader("观看决策")
                feedback_map = state_store.get_feedback_map([m["movie_key"] for m in movies])
                for movie in movies:
                    current_status = feedback_map.get(movie["movie_key"], "new")
                    status_badge = {
                        "wishlist": "💚 想看", "seen": "✅ 已看",
                        "skip": "⏭ 跳过", "hold": "⏳ 待定",
                    }.get(current_status, "")
                    with st.expander(f"{movie['title']}  {status_badge}", expanded=False):
                        info_col, action_col = st.columns([3, 2])
                        with info_col:
                            st.write(f"豆瓣评分：{movie.get('rating', 0)}")
                            st.write(f"上映日期：{display_release_date(movie.get('release_date'), movie.get('year'))}")
                            st.write(f"类型：{movie.get('genres') or '待补全'}")
                            st.write(f"地区：{movie.get('countries') or '待补全'}")
                            st.write(f"简介：{(movie.get('summary') or '暂无简介')[:200]}")
                            if movie.get("url"):
                                st.markdown(f"[打开豆瓣详情]({movie['url']})")
                        with action_col:
                            kb = movie["movie_key"]
                            if st.button("想看", key=f"wish_{kb}"):
                                update_feedback(kb, "wishlist"); st.rerun()
                            if st.button("已看", key=f"seen_{kb}"):
                                update_feedback(kb, "seen"); st.rerun()
                            if st.button("跳过", key=f"skip_{kb}"):
                                update_feedback(kb, "skip"); st.rerun()
                            if st.button("待定", key=f"hold_{kb}"):
                                update_feedback(kb, "hold"); st.rerun()

            if repair_queue:
                st.markdown("---")
                st.subheader("日期待补全影片（仅按年份展示）")
                render_movie_cards(repair_queue, show_date=True)

    with tab_history:
        col_filter, _ = st.columns([2, 3])
        with col_filter:
            history_type_filter = st.selectbox(
                "筛选类型", ["全部", "定时推荐"], index=0, key="history_type_filter"
            )
        filter_arg = "scheduled" if history_type_filter == "定时推荐" else None
        digests = state_store.list_recent_digests(limit=20, digest_type=filter_arg)

        if not digests:
            st.info("还没有历史记录。")
        else:
            digest_options = {
                f"{item['created_at'][:19]} | {item['digest_id']}": item["digest_id"]
                for item in digests
            }
            selected_label = st.selectbox("选择历史记录", list(digest_options.keys()))
            selected_history_id = digest_options[selected_label]
            if st.button("切换到该记录"):
                st.session_state["selected_digest_id"] = selected_history_id
                st.rerun()

            history_payload = load_digest_payload(selected_history_id)
            history_movies = history_payload.get("movies", [])
            hist_tw_start = history_payload.get("time_window_start")
            hist_tw_end = history_payload.get("time_window_end")
            if hist_tw_start:
                st.caption(f"时间窗口：{hist_tw_start} ~ {hist_tw_end}")
            render_movie_cards(history_movies, show_date=True)

    with tab_library:
        library_movies = state_store.list_movies(limit=200)
        if not library_movies:
            st.info("数据库里还没有影片缓存。")
        else:
            lib_col1, lib_col2 = st.columns([3, 1])
            with lib_col1:
                search_term = st.text_input("搜索片名", "")
            with lib_col2:
                min_rating_lib = st.slider("最低评分", 0.0, 10.0, 0.0, 0.5, key="lib_rating")
            filtered = [
                m for m in library_movies
                if (not search_term or search_term.lower() in str(m.get("title", "")).lower())
                and float(m.get("rating") or 0) >= min_rating_lib
            ]
            st.caption(f"显示 {len(filtered)} / {len(library_movies)} 部")
            render_movie_cards(filtered, show_date=True)
