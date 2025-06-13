# Alternative to run_pipeline.py.
# This script starts from step 2 of the pipeline (Scrape profiles, validate URLs, etc.) skipping steps 1 (collect URLs)
# file: run_pipeline_from_step2.py

import os
import json
import logging
from sqlalchemy.orm import Session

from db import init_db, URL
from profile_scraper import ProfileScraper
from url_validator import URLValidator
from wayback_checker import WaybackChecker
from wayback_submitter import WaybackSubmitter
from text_scraper import TextScraper
from export_module import Exporter

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )

def main():
    configure_logging()
    logger = logging.getLogger('pipeline')

    # Initialize DB & session
    SessionLocal = init_db('sqlite:///hrd.db', echo=False)

    # Load pre‐collected URLs
    urls_path = 'profile_urls.json'
    if not os.path.exists(urls_path):
        logger.error(f"Profile URLs file not found: {urls_path}")
        return

    with open(urls_path, 'r', encoding='utf-8') as f:
        profile_urls = json.load(f)
    logger.info(f"Loaded {len(profile_urls)} profile URLs from {urls_path}")

    with SessionLocal() as session:  # type: Session
        logger.info("Starting pipeline from Step 2: Scrape profiles")

        # 2. Scrape profile pages
        delay = 1.0
        scraper = ProfileScraper(db_session=session, delay=delay)
        scrape_report = scraper.scrape_profiles(profile_urls)
        logger.info(f"Scraped profiles: {scrape_report}")

        # 3. Validate URLs
        url_ids = [u.url_id for (u,) in session.query(URL.url_id).all()]
        validator = URLValidator(db_session=session, delay=0.5, method='HEAD')
        validate_report = validator.validate_all(url_ids)
        logger.info(f"URL validation report: {validate_report}")

        # 4. Check Wayback Machine
        checker = WaybackChecker(db_session=session, delay=1.0)
        wayback_report = checker.check_all(url_ids)
        logger.info(f"Wayback availability report: {wayback_report}")

        # 5. Submit missing to Wayback
        missing_ids = [
            rec.url_id
            for rec in session.query(URL).filter(URL.is_archived == False).all()
        ]
        submitter = WaybackSubmitter(db_session=session, delay=5.0)
        submit_report = submitter.submit_all(missing_ids)
        logger.info(f"Wayback submission report: {submit_report}")

        # 6. Scrape text for active URLs
        active_ids = [
            rec.url_id
            for rec in session.query(URL).filter(URL.is_active == True).all()
        ]
        text_scraper = TextScraper(db_session=session, delay=1.0)
        text_report = text_scraper.scrape_all(active_ids)
        logger.info(f"Text scraping report: {text_report}")

        # 7. Export data
        exporter = Exporter(db_session=session, output_dir='./output')
        export_report = exporter.export(include_profiles=True, include_urls=True)
        logger.info(f"Export report: {export_report}")

    logger.info("Pipeline complete")

if __name__ == '__main__':
    main()
