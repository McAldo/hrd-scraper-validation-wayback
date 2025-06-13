# file: wayback_checker.py

import time
import logging
from typing import List, Dict, Any

import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from db import URL

logger = logging.getLogger(__name__)

class WaybackChecker:
    def __init__(self, db_session: Session, delay: float = 1.0,
                 api_endpoint: str = "http://archive.org/wayback/available"):
        """
        Args:
            db_session (Session): Active SQLAlchemy session.
            delay (float): Delay between API calls.
            api_endpoint (str): Base Wayback availability API URL.
        """
        self.db = db_session
        self.delay = delay
        self.api_endpoint = api_endpoint

    def check_all(self, url_ids: List[int]) -> Dict[str, Any]:
        """
        Check archival status for a list of URL record IDs.

        Args:
            url_ids (List[int]): List of URL record primary keys.

        Returns:
            Dict[str, Any]: Summary report of results.
        """
        report = {"total": len(url_ids), "archived": 0, "not_archived": 0, "errors": []}
        for uid in url_ids:
            try:
                record = self.db.query(URL).get(uid)
                archived = self.check_single(record)
                if archived:
                    report["archived"] += 1
                else:
                    report["not_archived"] += 1
                self.db.commit()
            except (SQLAlchemyError, Exception) as e:
                self.db.rollback()
                report["errors"].append({"url_id": uid, "error": str(e)})
            time.sleep(self.delay)
        return report

    def check_single(self, record: URL) -> bool:
        """
        Check if a single URL is archived.

        Args:
            record (URL): URL ORM instance.

        Returns:
            bool: True if archived, else False.
        """
        try:
            params = {"url": record.url}
            response = requests.get(self.api_endpoint, params=params, timeout=10)
            data = response.json()
            archived_snapshots = data.get("archived_snapshots", {})
            if "closest" in archived_snapshots:
                snapshot = archived_snapshots["closest"]
                record.is_archived = True
                record.archived_url = snapshot.get("url")
                record.archived_timestamp = snapshot.get("timestamp")
                self.db.add(record)
                return True
            else:
                record.is_archived = False
                self.db.add(record)
                return False
        except requests.RequestException as e:
            logger.warning(f"Wayback API request failed for {record.url}: {e}")
            record.is_archived = False
            self.db.add(record)
            return False
