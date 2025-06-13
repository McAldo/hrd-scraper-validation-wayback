import time
import logging
import re
from datetime import datetime
from typing import Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from tqdm import tqdm
from rapidfuzz import fuzz   # new import

from db import URL

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


class URLValidator:
    def __init__(self, db_session: Session, delay: float = 1.0):
        self.db = db_session
        self.delay = delay

        # Session with retries
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=frozenset(['HEAD', 'GET', 'OPTIONS'])
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/114.0.0.0 Safari/537.36'
            )
        })

    def validate_batch(self,
                       limit: Optional[int] = None,
                       force: bool = False
                       ):
        """
        Validate URLs:

        - If force=False (default), only URLs with checked_at IS NULL.
        - If force=True, re-validate all URLs.
        """
        q = self.db.query(URL)
        if not force:
            q = q.filter(URL.checked_at.is_(None))
        if limit:
            q = q.limit(limit)

        pending: List[URL] = q.all()
        logger.info("Validating %d URLs (limit=%s, force=%s)",
                    len(pending), limit, force)

        for url_rec in tqdm(pending, desc="Validating URLs", unit="url"):
            now = datetime.utcnow()

            # 1) Existence check: HEAD -> GET fallback
            try:
                resp = self.session.head(url_rec.url, timeout=10, allow_redirects=True)
                status = resp.status_code
                if status >= 400:
                    logger.debug("HEAD %d for %s; falling back to GET", status, url_rec.url)
                    resp_get = self.session.get(url_rec.url, timeout=10, stream=True)
                    status = resp_get.status_code
                    resp_get.close()
                url_rec.is_active = status < 400
                logger.debug("URL %s status %d → is_active=%s",
                             url_rec.url, status, url_rec.is_active)
            except Exception as e:
                logger.debug("HEAD/GET existence check failed for %s: %s",
                             url_rec.url, e)
                url_rec.is_active = False

            # Reset name & text
            url_rec.contains_name = False
            url_rec.page_text     = None

            # 2) Content & name check
            if url_rec.is_active:
                try:
                    full_html = self.session.get(url_rec.url, timeout=10).text
                    soup      = BeautifulSoup(full_html, 'html.parser')
                    raw_text  = soup.get_text(separator=' ', strip=True)
                    norm_text = " ".join(raw_text.split()).lower()

                    raw_name  = url_rec.profile.name or ""
                    norm_name = " ".join(raw_name.split()).lower()
                    found = False

                    # A) Exact full-name
                    if norm_name and norm_name in norm_text:
                        found = True

                    # B) Surname only
                    if not found:
                        surname = norm_name.split()[-1] if norm_name else ""
                        if surname and surname in norm_text:
                            found = True

                    # C) Fuzzy match on full-name
                    if not found and norm_name:
                        score = fuzz.partial_ratio(norm_name, norm_text)
                        logger.debug("Fuzzy score %d for '%s' in %s",
                                     score, norm_name, url_rec.url)
                        if score >= 75:
                            found = True

                    # D) Regex any token match
                    if not found and norm_name:
                        tokens = norm_name.split()
                        pattern = r"\b(" + "|".join(re.escape(tok) for tok in tokens) + r")\b"
                        if re.search(pattern, norm_text, flags=re.IGNORECASE):
                            found = True

                    if found:
                        url_rec.contains_name = True
                        url_rec.page_text     = raw_text

                except Exception as e:
                    logger.debug("Content fetch/search failed for %s: %s",
                                 url_rec.url, e)

            # 3) Timestamp
            url_rec.checked_at = now

            # Commit & delay
            try:
                self.db.commit()
            except SQLAlchemyError as e:
                logger.error("DB commit failed for %s: %s", url_rec.url, e)
                self.db.rollback()

            time.sleep(self.delay)

        logger.info("Batch complete")
