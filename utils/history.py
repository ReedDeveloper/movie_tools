import json
import os
from datetime import datetime
import pandas as pd
import logging
from utils.state_store import StateStore

logger = logging.getLogger(__name__)

class HistoryManager:
    def __init__(self, history_file="output/history.json"):
        self.history_file = history_file
        self.output_dir = os.path.dirname(history_file)
        os.makedirs(self.output_dir, exist_ok=True)
        self.state_store = StateStore()
        self.history = self._load_history()

    def _load_history(self):
        if not os.path.exists(self.history_file):
            return []
        try:
            with open(self.history_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading history: {e}")
            return []

    def _save_history(self):
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.history, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Error saving history: {e}")

    def add_record(self, params, result_file, count):
        """
        Add a new search record.
        """
        record = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "params": params,
            "file_path": result_file,
            "count": count,
            "id": datetime.now().strftime("%Y%m%d%H%M%S")
        }
        
        # Add to beginning
        self.history.insert(0, record)
        
        # Keep only last 10 records
        if len(self.history) > 10:
            # Optionally delete old files? No, keep them for now or let user delete.
            # But let's remove from history list.
            self.history = self.history[:10]
            
        self._save_history()
        return record

    def get_history(self):
        digest_history = []
        for digest in self.state_store.list_recent_digests(limit=10):
            payload = self.state_store.get_digest_payload(digest["digest_id"]) or {}
            digest_history.append(
                {
                    "timestamp": digest["created_at"].replace("T", " ")[:19],
                    "params": f"{digest['months_window']} Months, Rating {digest['min_rating']}+, Top {digest['max_candidates']}",
                    "file_path": digest.get("export_path"),
                    "count": len(payload.get("movies", [])),
                    "id": digest["digest_id"],
                }
            )

        if digest_history:
            return digest_history
        return self.history

    def load_data(self, file_path):
        """
        Load data from a history file path.
        """
        if os.path.exists(file_path):
            try:
                if file_path.endswith('.csv'):
                    return pd.read_csv(file_path)
                elif file_path.endswith('.xlsx'):
                    return pd.read_excel(file_path)
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
                return None
        return None
