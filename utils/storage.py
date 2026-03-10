import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd


logger = logging.getLogger(__name__)


DEFAULT_COLUMNS = [
    "title",
    "rating",
    "release_date",
    "release_date_source",
    "release_date_confidence",
    "year",
    "countries",
    "genres",
    "summary",
    "smart_tags",
    "url",
    "imdb_id",
    "tmdb_rating",
    "tmdb_tags",
]


def movies_to_dataframe(data: List[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(data)
    cols = [column for column in DEFAULT_COLUMNS if column in df.columns]
    cols.extend([column for column in df.columns if column not in cols])
    if cols:
        df = df[cols]
    return df


def _build_output_path(filename: str) -> str:
    os.makedirs("output", exist_ok=True)
    return os.path.join("output", filename)


def save_to_excel(data, filename: Optional[str] = None):
    if not data:
        logger.warning("No data to save.")
        return None

    if not filename:
        filename = f"movie_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    try:
        filepath = _build_output_path(filename)
        movies_to_dataframe(data).to_excel(filepath, index=False)
        logger.info("Data saved to %s", filepath)
        return filepath
    except Exception as error:
        logger.error("Error saving data: %s", error)
        return None


def save_to_csv(data, filename: Optional[str] = None):
    if not data:
        logger.warning("No data to save.")
        return None

    if not filename:
        filename = f"movie_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    try:
        filepath = _build_output_path(filename)
        movies_to_dataframe(data).to_csv(filepath, index=False, encoding="utf-8-sig")
        logger.info("Data saved to %s", filepath)
        return filepath
    except Exception as error:
        logger.error("Error saving data: %s", error)
        return None


def save_digest_markdown(markdown: str, filename: Optional[str] = None) -> Optional[str]:
    if not filename:
        filename = f"monthly_digest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md"

    try:
        filepath = _build_output_path(filename)
        with open(filepath, "w", encoding="utf-8") as file:
            file.write(markdown)
        logger.info("Digest markdown saved to %s", filepath)
        return filepath
    except Exception as error:
        logger.error("Error saving digest markdown: %s", error)
        return None
