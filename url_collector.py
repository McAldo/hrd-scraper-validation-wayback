import time
import logging
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from db import URL, Profile

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class URLCollector:
    def __init__(self,
                 base_url: str,
                 db_session: Session,
                 delay: float = 2.0,
                 start_page: int = 1,
                 max_pages: Optional[int] = 20):  ## Max number of profiles to be collected, set to None for no limit
        """
        Crawl paginated listing pages to collect profile URLs.

        Args:
            base_url (str): Root listing page URL.
            db_session (Session): Active SQLAlchemy session.
            delay (float): Delay between requests.
            start_page (int): Page number to start/resume.
            max_pages (Optional[int]): Optional limit to number of pages. None for no limit.
        """
        self.base_url = base_url.rstrip('/')
        self.delay = delay
        self.start_page = start_page
        self.max_pages = max_pages
        self.db = db_session

        # Use a Session and set a browser-like User-Agent to avoid 403s
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/114.0.0.0 Safari/537.36'
            )
        })

    def collect(self) -> List[str]:
        page = self.start_page
        collected: List[str] = []

        while True:
            if self.max_pages and page > self.max_pages:
                logger.info("Reached max_pages limit: %s", self.max_pages)
                break

            url = (
                f"{self.base_url}/page/{page}/"
                if page > 1 else f"{self.base_url}/"
            )

            try:
                resp = self.session.get(url, timeout=10)
                if resp.status_code == 404:
                    logger.info("No more pages: %s returned 404", url)
                    break
                resp.raise_for_status()
            except requests.RequestException as e:
                logger.error("Error fetching page %s: %s", page, e)
                break

            soup = BeautifulSoup(resp.text, 'html.parser')
            links = soup.select('div.hrd-listing a[href]')
            if not links:
                logger.info("No listing links found on page %s", page)
                break

            new_count = 0
            for a in links:
                href = a['href']
                if '/hrdrecord/' in href and href not in collected:
                    collected.append(href)
                    new_count += 1

                    # Example DB upsert stub:
                    # try:
                    #     obj = URL(profile_url=href)
                    #     self.db.add(obj)
                    #     self.db.commit()
                    # except SQLAlchemyError:
                    #     self.db.rollback()

            logger.info("Page %s: collected %d new URLs", page, new_count)

            page += 1
            time.sleep(self.delay)

        return collected
