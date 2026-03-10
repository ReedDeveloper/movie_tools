import json
import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional


class StateStore:
    def __init__(self, db_path: str = "output/movie_tools.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_schema()
        self._migrate_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS movies_enriched (
                    movie_key TEXT PRIMARY KEY,
                    douban_id TEXT,
                    title TEXT,
                    rating REAL,
                    release_date TEXT,
                    release_date_source TEXT,
                    release_date_confidence TEXT,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS digests (
                    digest_id TEXT PRIMARY KEY,
                    created_at TEXT NOT NULL,
                    digest_type TEXT NOT NULL DEFAULT 'scheduled',
                    months_window INTEGER,
                    years_window INTEGER,
                    min_rating REAL NOT NULL,
                    max_candidates INTEGER NOT NULL,
                    push_channel TEXT NOT NULL,
                    push_interval TEXT,
                    time_window_start TEXT,
                    time_window_end TEXT,
                    status TEXT NOT NULL,
                    markdown_path TEXT,
                    export_path TEXT,
                    payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS digest_items (
                    digest_id TEXT NOT NULL,
                    movie_key TEXT NOT NULL,
                    rank_no INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    rating REAL,
                    release_date TEXT,
                    PRIMARY KEY (digest_id, movie_key)
                );

                CREATE TABLE IF NOT EXISTS user_feedback (
                    movie_key TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    note TEXT,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS fetch_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    identifier TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT,
                    attempts INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL
                );
                """
            )

    def _migrate_schema(self) -> None:
        """向后兼容迁移：对旧 digests 表补充新字段（旧记录 NULL 兼容）。"""
        with self._connect() as connection:
            existing = {
                row[1]
                for row in connection.execute("PRAGMA table_info(digests)").fetchall()
            }
            new_columns = {
                "digest_type": "TEXT NOT NULL DEFAULT 'scheduled'",
                "years_window": "INTEGER",
                "push_interval": "TEXT",
                "time_window_start": "TEXT",
                "time_window_end": "TEXT",
            }
            for col, col_def in new_columns.items():
                if col not in existing:
                    connection.execute(
                        f"ALTER TABLE digests ADD COLUMN {col} {col_def}"
                    )

    def upsert_movie(self, movie: Dict) -> None:
        now = datetime.now().isoformat()
        payload = json.dumps(movie, ensure_ascii=False)
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO movies_enriched (
                    movie_key, douban_id, title, rating, release_date,
                    release_date_source, release_date_confidence, payload, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(movie_key) DO UPDATE SET
                    douban_id=excluded.douban_id,
                    title=excluded.title,
                    rating=excluded.rating,
                    release_date=excluded.release_date,
                    release_date_source=excluded.release_date_source,
                    release_date_confidence=excluded.release_date_confidence,
                    payload=excluded.payload,
                    updated_at=excluded.updated_at
                """,
                (
                    movie.get("movie_key"),
                    movie.get("douban_id"),
                    movie.get("title"),
                    movie.get("rating"),
                    movie.get("release_date"),
                    movie.get("release_date_source", ""),
                    movie.get("release_date_confidence", ""),
                    payload,
                    now,
                ),
            )

    def get_movie(self, movie_key: str) -> Optional[Dict]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM movies_enriched WHERE movie_key = ?",
                (movie_key,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row["payload"])

    def list_movies(self, limit: int = 200) -> List[Dict]:
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT payload FROM movies_enriched ORDER BY updated_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [json.loads(row["payload"]) for row in rows]

    def create_digest(
        self,
        digest_id: str,
        config: Dict,
        movies: List[Dict],
        markdown_path: Optional[str],
        export_path: Optional[str],
        payload: Dict,
        status: str,
        digest_type: str = "scheduled",
        time_window_start: Optional[str] = None,
        time_window_end: Optional[str] = None,
        push_interval: Optional[str] = None,
    ) -> None:
        now = datetime.now().isoformat()
        months_window = config.get("months_window") or None
        years_window = config.get("years_window") or None
        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO digests (
                    digest_id, created_at, digest_type, months_window, years_window,
                    min_rating, max_candidates, push_channel, push_interval,
                    time_window_start, time_window_end,
                    status, markdown_path, export_path, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    digest_id,
                    now,
                    digest_type,
                    months_window,
                    years_window,
                    config["min_rating"],
                    config["max_candidates"],
                    config["push_channel"],
                    push_interval,
                    time_window_start,
                    time_window_end,
                    status,
                    markdown_path,
                    export_path,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
            connection.execute("DELETE FROM digest_items WHERE digest_id = ?", (digest_id,))
            for rank, movie in enumerate(movies, start=1):
                connection.execute(
                    """
                    INSERT INTO digest_items (digest_id, movie_key, rank_no, title, rating, release_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        digest_id,
                        movie.get("movie_key"),
                        rank,
                        movie.get("title"),
                        movie.get("rating"),
                        movie.get("release_date"),
                    ),
                )

    def list_recent_digests(
        self, limit: int = 10, digest_type: Optional[str] = None
    ) -> List[Dict]:
        query = "SELECT * FROM digests"
        params: List = []
        if digest_type:
            query += " WHERE digest_type = ?"
            params.append(digest_type)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def get_digest_payload(self, digest_id: str) -> Optional[Dict]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload FROM digests WHERE digest_id = ?",
                (digest_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row["payload"])

    def get_sent_movie_keys(self, digest_type: Optional[str] = None) -> List[str]:
        """返回历史已推送的 movie_key 列表，可按 digest_type 过滤。"""
        if digest_type:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT DISTINCT di.movie_key
                    FROM digest_items di
                    JOIN digests d ON d.digest_id = di.digest_id
                    WHERE d.digest_type = ?
                    """,
                    (digest_type,),
                ).fetchall()
        else:
            with self._connect() as connection:
                rows = connection.execute(
                    "SELECT DISTINCT movie_key FROM digest_items"
                ).fetchall()
        return [row["movie_key"] for row in rows]

    def set_feedback(self, movie_key: str, status: str, note: str = "") -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO user_feedback (movie_key, status, note, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(movie_key) DO UPDATE SET
                    status=excluded.status,
                    note=excluded.note,
                    updated_at=excluded.updated_at
                """,
                (movie_key, status, note, datetime.now().isoformat()),
            )

    def get_feedback_map(self, movie_keys: Optional[List[str]] = None) -> Dict[str, str]:
        query = "SELECT movie_key, status FROM user_feedback"
        params: List = []
        if movie_keys:
            placeholders = ",".join(["?"] * len(movie_keys))
            query += f" WHERE movie_key IN ({placeholders})"
            params.extend(movie_keys)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return {row["movie_key"]: row["status"] for row in rows}

    def record_fetch_event(self, source: str, identifier: str, status: str, detail: str = "", attempts: int = 1) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO fetch_audit (source, identifier, status, detail, attempts, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (source, identifier, status, detail, attempts, datetime.now().isoformat()),
            )
