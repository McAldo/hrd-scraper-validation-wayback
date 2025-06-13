# file: text_scraper.py

import time
import logging
from typing import List, Dict, Any

import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from db import URL  

logger = logging.getLogger(__name__)

class TextScraper:
    def __init__(self, db_session: Session, delay: float = 1.0):
        """
        Args:
            db_session (Session): Active SQLAlchemy session.
            delay (float): Delay between HTTP GET requests.
        """
        self.db = db_session
        self.delay = delay

    def scrape_all(self, url_ids: List[int]) -> Dict[str, Any]:
        """
        Fetch and store text for multiple URLs.

        Args:
            url_ids (List[int]): List of URL record primary keys.

        Returns:
            Dict[str, Any]: Summary report.
        """
        report = {"total": len(url_ids), "fetched": 0, "errors": []}
        for uid in url_ids:
            try:
                record = self.db.query(URL).get(uid)
                if self.scrape_single(record):
                    report["fetched"] += 1
                self.db.commit()
            except (SQLAlchemyError, Exception) as e:
                self.db.rollback()
                report["errors"].append({"url_id": uid, "error": str(e)})
            time.sleep(self.delay)
        return report

    def scrape_single(self, record: URL) -> bool:
        """
        Fetch the HTML/text of a live URL and store it.

        Args:
            record (URL): URL ORM instance with `url` and `is_active`.

        Returns:
            bool: True if fetched and saved, False otherwise.
        """
        if not record.is_active:
            return False
        try:
            response = requests.get(record.url, timeout=15)
            response.raise_for_status()
            record.page_text = response.text
            self.db.add(record)
            return True
        except requests.RequestException as e:
            logger.warning(f"Text fetch failed for {record.url}: {e}")
            return False
