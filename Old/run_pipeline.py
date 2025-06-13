import time
import logging
from sqlalchemy.orm import Session

from db import init_db, Profile, URL
from url_collector import URLCollector
from profile_scraper import ProfileScraper
from url_validator import URLValidator
from wayback_checker import WaybackChecker
from wayback_submitter import WaybackSubmitter
from text_scraper import TextScraper
from export_module import Exporter

from db import URL  

# Configure logging
def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(name)s: %(message)s'
    )


def main():
    configure_logging()
    logger = logging.getLogger('pipeline')

    # Initialize database and session factory
    SessionLocal = init_db('sqlite:///hrd.db', echo=False)

    # Begin session
    with SessionLocal() as session:  # type: Session
        logger.info("Starting pipeline")

        # 1. Collect profile URLs
        collector = URLCollector(
            base_url='https://hrdmemorial.org/hrdrecord/',
            db_session=session,
            delay=2.0
        )
        profile_urls = collector.collect()
        logger.info(f"Collected {len(profile_urls)} profile URLs")

        # 2. Scrape profile pages
        delay=1.0 # Set delay time to avoid overloading the server
        logger.info(f"Starting profiles scraping with delay time {delay}")
        scraper = ProfileScraper(db_session=session, delay=delay)
        scrape_report = scraper.scrape_profiles(profile_urls)
        logger.info(f"Scraped profiles: {scrape_report}")

        # 3. Validate URLs
        # Fetch all URL IDs
        url_ids = [u.url_id for u in session.query(URL.url_id).all()]
        validator = URLValidator(db_session=session, delay=0.5, method='HEAD')
        validate_report = validator.validate_all(url_ids)
        logger.info(f"URL validation report: {validate_report}")

        # 4. Check Wayback Machine
        checker = WaybackChecker(db_session=session, delay=1.0)
        wayback_report = checker.check_all(url_ids)
        logger.info(f"Wayback availability report: {wayback_report}")

        # 5. Submit missing to Wayback
        missing_ids = [uid for uid, rec in 
                       [(u.url_id, u) for u in session.query(URL).all()]
                       if not rec.is_archived]
        submitter = WaybackSubmitter(db_session=session, delay=5.0)
        submit_report = submitter.submit_all(missing_ids)
        logger.info(f"Wayback submission report: {submit_report}")

        # 6. Scrape text for active URLs
        active_ids = [u.url_id for u in session.query(URL).filter(URL.is_active == True).all()]
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
