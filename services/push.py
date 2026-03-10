import logging
import os
from typing import Dict, Optional

import requests


logger = logging.getLogger(__name__)


class PushService:
    def __init__(self):
        self.session = requests.Session()

    def send(self, channel: str, title: str, content: str) -> bool:
        channel = (channel or "file").lower()
        if channel in {"file", "none"}:
            logger.info("Push skipped for channel=%s", channel)
            return True
        if channel == "console":
            logger.info("%s\n%s", title, content)
            return True
        if channel == "webhook":
            return self._send_webhook(title, content)
        if channel == "serverchan":
            return self._send_serverchan(title, content)
        if channel == "bark":
            return self._send_bark(title, content)

        logger.warning("Unsupported push channel: %s", channel)
        return False

    def _send_webhook(self, title: str, content: str) -> bool:
        webhook_url = os.getenv("MOVIE_PUSH_WEBHOOK_URL")
        if not webhook_url:
            logger.warning("MOVIE_PUSH_WEBHOOK_URL is not configured.")
            return False

        response = self.session.post(
            webhook_url,
            json={"title": title, "content": content},
            timeout=10,
        )
        response.raise_for_status()
        return True

    def _send_serverchan(self, title: str, content: str) -> bool:
        sendkey = os.getenv("SERVERCHAN_SENDKEY")
        if not sendkey:
            logger.warning("SERVERCHAN_SENDKEY is not configured.")
            return False

        response = self.session.post(
            f"https://sctapi.ftqq.com/{sendkey}.send",
            data={"title": title, "desp": content},
            timeout=10,
        )
        response.raise_for_status()
        return True

    def _send_bark(self, title: str, content: str) -> bool:
        bark_url = os.getenv("BARK_PUSH_URL")
        if not bark_url:
            logger.warning("BARK_PUSH_URL is not configured.")
            return False

        response = self.session.post(
            bark_url.rstrip("/") + "/",
            json={"title": title, "body": content},
            timeout=10,
        )
        response.raise_for_status()
        return True
