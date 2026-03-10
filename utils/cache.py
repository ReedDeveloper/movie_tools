import hashlib
import json
import os
from datetime import datetime, timedelta
from typing import Any, Optional


class JsonFileCache:
    def __init__(self, cache_dir: str = "output/cache"):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def _cache_path(self, namespace: str, key: str) -> str:
        safe_key = hashlib.sha1(key.encode("utf-8")).hexdigest()
        namespace_dir = os.path.join(self.cache_dir, namespace)
        os.makedirs(namespace_dir, exist_ok=True)
        return os.path.join(namespace_dir, f"{safe_key}.json")

    def get(self, namespace: str, key: str, ttl_hours: Optional[int] = None) -> Optional[Any]:
        path = self._cache_path(namespace, key)
        if not os.path.exists(path):
            return None

        try:
            with open(path, "r", encoding="utf-8") as file:
                payload = json.load(file)
        except Exception:
            return None

        if ttl_hours is not None:
            created_at = payload.get("_cached_at")
            if created_at:
                try:
                    created_at_dt = datetime.fromisoformat(created_at)
                    if datetime.now() - created_at_dt > timedelta(hours=ttl_hours):
                        return None
                except ValueError:
                    return None

        return payload.get("data")

    def set(self, namespace: str, key: str, data: Any) -> None:
        path = self._cache_path(namespace, key)
        payload = {
            "_cached_at": datetime.now().isoformat(),
            "data": data,
        }
        with open(path, "w", encoding="utf-8") as file:
            json.dump(payload, file, ensure_ascii=False, indent=2)
