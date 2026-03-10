"""
飞书自定义机器人卡片推送服务。

支持两种模式：
  1. 纯 Webhook 模式（只需 FEISHU_WEBHOOK_URL）：
     发送不含图片的文字卡片，每部影片含评分、日期、类型、豆瓣链接。
  2. 带图片模式（需额外配置 FEISHU_APP_ID + FEISHU_APP_SECRET）：
     先通过飞书 API 上传海报图片获取 img_key，再构建含图片的 column_set 卡片。

飞书 Webhook 消息格式：
    POST https://open.feishu.cn/open-apis/bot/v2/hook/{token}
    Body: {"msg_type": "interactive", "card": { ... }}
"""

import io
import logging
import os
from typing import Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

# 飞书 API 端点
_AUTH_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
_IMG_UPLOAD_URL = "https://open.feishu.cn/open-apis/im/v1/images"

# 卡片颜色主题
_HEADER_COLOR = "blue"

# 每部影片在卡片中展示的简介最大字数
_SUMMARY_MAX = 80


class FeishuCardPushService:
    """飞书机器人卡片推送，多影片拼接为单张交互卡片（可选图片）。"""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        app_id: Optional[str] = None,
        app_secret: Optional[str] = None,
    ):
        self.webhook_url = (
            webhook_url
            or os.getenv("FEISHU_WEBHOOK_URL")
            or os.getenv("MOVIE_PUSH_WEBHOOK_URL")
        )
        self.app_id = app_id or os.getenv("FEISHU_APP_ID")
        self.app_secret = app_secret or os.getenv("FEISHU_APP_SECRET")
        self._access_token: Optional[str] = None
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    # ── 图片上传（可选，需 app_id + app_secret）─────────────────────────────

    def _get_access_token(self) -> Optional[str]:
        if self._access_token:
            return self._access_token
        if not self.app_id or not self.app_secret:
            return None
        try:
            resp = self.session.post(
                _AUTH_URL,
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data.get("tenant_access_token")
            if token:
                self._access_token = token
            return token
        except Exception as exc:
            logger.warning("获取飞书 access token 失败: %s", exc)
            return None

    def _download_cover(self, url: str) -> Optional[bytes]:
        """下载豆瓣海报图片（加 Referer 绕过防盗链）。"""
        if not url:
            return None
        try:
            resp = requests.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                    "Referer": "https://movie.douban.com/",
                },
                timeout=8,
            )
            if resp.status_code == 200:
                return resp.content
        except Exception as exc:
            logger.debug("下载海报失败 %s: %s", url, exc)
        return None

    def _upload_image(self, image_bytes: bytes) -> Optional[str]:
        """上传图片到飞书，返回 img_key；失败时返回 None。"""
        token = self._get_access_token()
        if not token:
            return None
        try:
            resp = requests.post(
                _IMG_UPLOAD_URL,
                headers={"Authorization": f"Bearer {token}"},
                files={
                    "image_type": (None, "message"),
                    "image": ("poster.jpg", io.BytesIO(image_bytes), "image/jpeg"),
                },
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == 0:
                return data.get("data", {}).get("image_key")
            logger.warning("飞书上传图片失败: %s", data)
        except Exception as exc:
            logger.warning("飞书上传图片异常: %s", exc)
        return None

    def _get_img_key(self, cover_url: str) -> Optional[str]:
        """尝试下载并上传海报，返回 img_key（无凭证或失败时返回 None）。"""
        if not self.app_id or not self.app_secret:
            return None
        image_bytes = self._download_cover(cover_url)
        if not image_bytes:
            return None
        return self._upload_image(image_bytes)

    # ── 卡片构建 ────────────────────────────────────────────────────────────

    def _movie_elements(self, movie: Dict, index: int) -> list:
        """为单部影片构建卡片元素（含/不含图片均适配）。"""
        title = movie.get("title") or "未知片名"
        rating = float(movie.get("rating") or 0)
        cover_url = movie.get("cover") or ""
        douban_url = movie.get("url") or ""
        release_date = str(movie.get("release_date") or movie.get("year") or "待确认")
        genres = movie.get("genres") or []
        if isinstance(genres, list):
            genres_str = " / ".join(str(g) for g in genres[:3]) or "待补全"
        else:
            genres_str = str(genres)[:30] or "待补全"
        countries = movie.get("countries") or []
        if isinstance(countries, list):
            countries_str = " / ".join(str(c) for c in countries[:2]) or "待补全"
        else:
            countries_str = str(countries)[:20] or "待补全"
        summary = str(movie.get("summary") or "暂无简介")
        if len(summary) > _SUMMARY_MAX:
            summary = summary[:_SUMMARY_MAX] + "…"

        rating_stars = "⭐" * min(int(rating / 2), 5) if rating else ""
        info_md = (
            f"**{index}. {title}**\n"
            f"评分：{rating_stars} **{rating}**\n"
            f"上映：{release_date} | {countries_str}\n"
            f"类型：{genres_str}\n"
            f"{summary}"
        )

        # 尝试上传图片
        img_key = self._get_img_key(cover_url) if cover_url else None

        if img_key:
            # 带图片的 column_set 布局
            col_img = {
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "vertical_align": "top",
                "elements": [
                    {
                        "tag": "img",
                        "img_key": img_key,
                        "alt": {"tag": "plain_text", "content": title},
                        "preview": True,
                    }
                ],
            }
            col_info = {
                "tag": "column",
                "width": "weighted",
                "weight": 3,
                "vertical_align": "top",
                "elements": [
                    {"tag": "div", "text": {"tag": "lark_md", "content": info_md}},
                    {
                        "tag": "action",
                        "actions": [
                            {
                                "tag": "button",
                                "text": {"tag": "plain_text", "content": "豆瓣详情"},
                                "type": "primary",
                                "url": douban_url,
                            }
                        ],
                    },
                ],
            }
            elements = [
                {
                    "tag": "column_set",
                    "flex_mode": "none",
                    "background_style": "grey",
                    "columns": [col_img, col_info],
                },
                {"tag": "hr"},
            ]
        else:
            # 纯文字布局（无图片）
            action = (
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "豆瓣详情"},
                            "type": "primary",
                            "url": douban_url,
                        }
                    ],
                }
                if douban_url
                else None
            )
            block = {
                "tag": "div",
                "text": {"tag": "lark_md", "content": info_md},
            }
            row: list = [block]
            if action:
                row.append(action)
            elements = row + [{"tag": "hr"}]

        return elements

    def _build_card(
        self,
        title: str,
        movies: List[Dict],
        time_window: str = "",
        min_rating: float = 0,
    ) -> Dict:
        """构建完整的飞书交互卡片 payload。"""
        count = len(movies)
        subtitle_parts = []
        if time_window:
            subtitle_parts.append(f"📅 {time_window}")
        if min_rating:
            subtitle_parts.append(f"⭐ 最低 {min_rating} 分")
        subtitle_parts.append(f"共 {count} 部精选")
        subtitle_md = " | ".join(subtitle_parts)

        elements: list = [
            {"tag": "div", "text": {"tag": "lark_md", "content": subtitle_md}},
            {"tag": "hr"},
        ]
        for i, movie in enumerate(movies, start=1):
            elements.extend(self._movie_elements(movie, i))

        return {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {"content": title, "tag": "plain_text"},
                "template": _HEADER_COLOR,
            },
            "elements": elements,
        }

    # ── 公开接口 ─────────────────────────────────────────────────────────────

    def send_movies(
        self,
        title: str,
        movies: List[Dict],
        time_window: str = "",
        min_rating: float = 0,
    ) -> bool:
        """发送多影片拼接的飞书卡片，返回是否成功。"""
        if not self.webhook_url:
            logger.warning("FEISHU_WEBHOOK_URL 未配置，跳过飞书推送。")
            return False
        if not movies:
            logger.info("无影片，跳过飞书推送。")
            return False

        card = self._build_card(title, movies, time_window, min_rating)
        try:
            resp = self.session.post(
                self.webhook_url,
                json={"msg_type": "interactive", "card": card},
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            # 飞书群机器人成功时 code=0，StatusCode=0
            code = data.get("code", data.get("StatusCode", 0))
            if code not in (0, None):
                logger.error("飞书卡片推送失败，返回: %s", data)
                return False
            logger.info("飞书卡片推送成功，共 %d 部影片。", len(movies))
            return True
        except Exception as exc:
            logger.error("飞书卡片推送异常: %s", exc)
            return False
