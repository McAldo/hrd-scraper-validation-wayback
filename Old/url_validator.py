import time
import logging
from typing import List, Dict, Any

import requests
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

# Import ORM class
# from db import URL

from db import URL  

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class URLValidator:
    def __init__(self, db_session: Session, delay: float = 0.5, method: str = "HEAD"):
        """
        Initialize the URLValidator.

        Args:
            db_session (Session): Active SQLAlchemy session.
            delay (float): Delay between HTTP requests in seconds.
            method (str): HTTP method to use for validation ("HEAD" or "GET").
        """
        self.db = db_session
        self.delay = delay
        self.method = method.upper()

    def validate_all(self, url_ids: List[int]) -> Dict[str, Any]:
        """
        Validate multiple URL records by their database IDs.

        Args:
            url_ids (List[int]): List of URL record primary keys.

        Returns:
            Dict[str, Any]: Summary report of validation.
        """
        report = {"total": len(url_ids), "active": 0, "inactive": 0, "errors": []}
        for uid in url_ids:
            try:
                record = self.db.query(URL).get(uid)
                active = self.validate_single(record)
                if active:
                    report["active"] += 1
                else:
                    report["inactive"] += 1
                self.db.commit()
            except (SQLAlchemyError, Exception) as e:
                self.db.rollback()
                logger.error(f"Error validating URL id {uid}: {e}")
                report["errors"].append({"url_id": uid, "error": str(e)})
            time.sleep(self.delay)
        return report

    def validate_single(self, record: URL) -> bool:
        """
        Validate a single URL record.

        Args:
            record (URL): URL ORM instance with attributes 'url', 'is_active', etc.

        Returns:
            bool: True if the URL is active (status code < 400), else False.
        """
        try:
            response = requests.request(self.method, record.url, timeout=10)
            # If HEAD not allowed, fallback to GET
            if self.method == "HEAD" and response.status_code == 405:
                response = requests.get(record.url, timeout=10)

            record.is_active = response.status_code < 400
            record.last_status_code = response.status_code
            # Optionally store page content
            if self.method == "GET" and record.is_active:
                record.page_text = response.text

            self.db.add(record)
            return record.is_active
        except requests.RequestException as e:
            logger.warning(f"Validation request failed for {record.url}: {e}")
            record.is_active = False
            record.last_status_code = None
            self.db.add(record)
            return False
