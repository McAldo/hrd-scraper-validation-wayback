# file: wayback_submitter.py

import time
import logging
from typing import List, Dict, Any

import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from db import URL  

logger = logging.getLogger(__name__)

class WaybackSubmitter:
    def __init__(self, db_session: Session, delay: float = 5.0,
                 api_endpoint: str = "https://web.archive.org/save", 
                 max_retries: int = 3):
        """
        Args:
            db_session (Session): Active SQLAlchemy session.
            delay (float): Delay between API calls.
            api_endpoint (str): SavePageNow endpoint.
            max_retries (int): Number of retry attempts on failure.
        """
        self.db = db_session
        self.delay = delay
        self.api_endpoint = api_endpoint
        self.max_retries = max_retries

    def submit_all(self, url_ids: List[int]) -> Dict[str, Any]:
        """
        Submit a list of URL records for archiving.

        Args:
            url_ids (List[int]): List of URL record primary keys.

        Returns:
            Dict[str, Any]: Summary report.
        """
        report = {"total": len(url_ids), "submitted": 0, "errors": []}
        for uid in url_ids:
            try:
                record = self.db.query(URL).get(uid)
                if self.submit_single(record):
                    report["submitted"] += 1
                self.db.commit()
            except (SQLAlchemyError, Exception) as e:
                self.db.rollback()
                report["errors"].append({"url_id": uid, "error": str(e)})
            time.sleep(self.delay)
        return report

    def submit_single(self, record: URL) -> bool:
        """
        Submit a single URL to SavePageNow.

        Args:
            record (URL): URL ORM instance.

        Returns:
            bool: True on successful submission, False otherwise.
        """
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(self.api_endpoint, params={"url": record.url}, timeout=30)
                if response.status_code in (200, 302):
                    # Archive URL redirect or status indicates success
                    record.is_archived = True
                    record.archived_url = response.headers.get("Content-Location") or record.url
                    self.db.add(record)
                    return True
                else:
                    logger.warning(f"Unexpected status {response.status_code} for {record.url}")
            except requests.RequestException as e:
                logger.warning(f"Error submitting {record.url}: {e}")
            retries += 1
            time.sleep(self.delay)
        return False
