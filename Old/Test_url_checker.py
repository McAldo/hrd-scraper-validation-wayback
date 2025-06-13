import logging
import time
from typing import Optional, Tuple
import requests
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse
import re

from db import URL, Profile, init_db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class URLChecker:
    def __init__(self, db_session: Session, delay: float = 1.0):
        self.db_session = db_session
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })

    def check_url(self, url: str, defender_name: str) -> Tuple[bool, bool, Optional[str]]:
        """
        Check if a URL is active and if it contains the defender's name.
        Returns: (is_active, contains_name, archived_url)
        """
        try:
            response = self.session.get(url, timeout=10, allow_redirects=True)
            response.raise_for_status()
            
            # Check if we got redirected to an archive
            final_url = response.url
            if 'archive.org' in final_url or 'web.archive.org' in final_url:
                return True, False, final_url
            
            # Check if the page contains the defender's name
            soup = BeautifulSoup(response.text, 'html.parser')
            text_content = soup.get_text().lower()
            name_present = defender_name.lower() in text_content
            
            return True, name_present, None
            
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {url}: {str(e)}")
            return False, False, None

    def process_urls(self):
        """Process all URLs in the database"""
        urls = self.db_session.query(URL).all()
        total = len(urls)
        processed = 0
        
        for url_record in urls:
            processed += 1
            if processed % 10 == 0:
                logger.info(f"Processed {processed}/{total} URLs")
            
            # Skip if already checked
            if url_record.is_active is not None:
                continue
                
            # Get the defender's name from the associated profile
            profile = self.db_session.query(Profile).filter_by(profile_id=url_record.profile_id).first()
            if not profile or not profile.name:
                logger.warning(f"No profile or name found for URL {url_record.url}")
                continue
            
            # Check the URL
            is_active, contains_name, archived_url = self.check_url(url_record.url, profile.name)
            
            # Update the database
            url_record.is_active = is_active
            url_record.is_archived = archived_url is not None
            url_record.archived_url = archived_url
            
            # Add a new column for name presence if it doesn't exist
            if not hasattr(url_record, 'contains_name'):
                self.db_session.execute('ALTER TABLE urls ADD COLUMN contains_name BOOLEAN')
            
            url_record.contains_name = contains_name
            
            # Commit after each URL to avoid losing progress
            self.db_session.commit()
            
            # Respect rate limiting
            time.sleep(self.delay)

def main():
    # Initialize database connection
    SessionLocal = init_db("sqlite:///hrd.db", echo=False)
    
    with SessionLocal() as session:
        checker = URLChecker(session, delay=2.0)
        checker.process_urls()

if __name__ == "__main__":
    main() 