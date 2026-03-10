import logging
import os

import requests
from dotenv import load_dotenv

from utils.cache import JsonFileCache
from utils.processor import maybe_update_release_date, normalize_list_field


load_dotenv()

logger = logging.getLogger(__name__)


class TMDBService:
    BASE_URL = "https://api.themoviedb.org/3"

    def __init__(self, api_key=None, cache=None, state_store=None):
        self.api_key = api_key or os.getenv("TMDB_API_KEY")
        self.cache = cache or JsonFileCache()
        self.state_store = state_store
        self.session = requests.Session()
        if not self.api_key:
            logger.warning("TMDB_API_KEY is not set. Enrichment will be limited.")

    def _cached_get(self, namespace, key, url, params):
        cached = self.cache.get(namespace, key, ttl_hours=24 * 7)
        if cached is not None:
            return cached

        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        self.cache.set(namespace, key, data)
        return data

    def search_movie(self, title, year=None):
        if not self.api_key:
            return None

        params = {
            "api_key": self.api_key,
            "query": title,
            "language": "zh-CN",
            "year": year,
        }

        try:
            key = f"{title}:{year}"
            data = self._cached_get("tmdb_search", key, f"{self.BASE_URL}/search/movie", params)
            results = data.get("results", [])
            return results[0] if results else None
        except Exception as error:
            logger.error("Error searching TMDB for %s: %s", title, error)
            if self.state_store:
                self.state_store.record_fetch_event("tmdb_search", title, "failed", str(error))
            return None

    def get_movie_details(self, tmdb_id):
        if not self.api_key:
            return None

        params = {
            "api_key": self.api_key,
            "language": "zh-CN",
            "append_to_response": "keywords,release_dates",
        }

        try:
            data = self._cached_get("tmdb_detail", str(tmdb_id), f"{self.BASE_URL}/movie/{tmdb_id}", params)
            keywords = [item["name"] for item in data.get("keywords", {}).get("keywords", [])]
            genres = [item["name"] for item in data.get("genres", [])]
            return {
                "tmdb_id": tmdb_id,
                "tmdb_rating": data.get("vote_average"),
                "vote_count": data.get("vote_count"),
                "original_title": data.get("original_title"),
                "release_date": data.get("release_date"),
                "runtime": data.get("runtime"),
                "tmdb_tags": keywords[:10],
                "tmdb_genres": genres,
            }
        except Exception as error:
            logger.error("Error getting TMDB details for %s: %s", tmdb_id, error)
            if self.state_store:
                self.state_store.record_fetch_event("tmdb_detail", str(tmdb_id), "failed", str(error))
            return None

    def enrich_movie_data(self, movie):
        movie = dict(movie)
        if not self.api_key:
            return movie

        tmdb_movie = self.search_movie(movie["title"], movie.get("year"))
        if not tmdb_movie:
            return movie

        details = self.get_movie_details(tmdb_movie["id"])
        if not details:
            return movie

        movie["tmdb_rating"] = details.get("tmdb_rating")
        movie["tmdb_votes"] = details.get("vote_count")
        movie["original_title"] = details.get("original_title")
        movie["tmdb_tags"] = details.get("tmdb_tags", [])

        if not normalize_list_field(movie.get("genres")):
            movie["genres"] = details.get("tmdb_genres", [])

        if not movie.get("duration") and details.get("runtime"):
            movie["duration"] = f"{details['runtime']}分钟"

        if not movie.get("smart_tags"):
            movie["smart_tags"] = details.get("tmdb_tags", [])

        maybe_update_release_date(movie, details.get("release_date"), "tmdb_primary", "medium")
        return movie
