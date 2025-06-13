## A simplified pipeline which only collect the profile urls, scrape the profiles, and export the profiles and URLs as csv
## filename: run_1_pipeline_collect_scrape_ToCSV_profiles.py

#!/usr/bin/env python3
import os
import logging
from sqlalchemy.orm import Session

from db import init_db
from url_collector import URLCollector
from profile_scraper import ProfileScraper
from export_module import Exporter

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )

def main():
    configure_logging()
    logger = logging.getLogger("pipeline")

    # 1. Initialize the database (SQLite file hrd.db)
    SessionLocal = init_db("sqlite:///hrd.db", echo=False)

    with SessionLocal() as session:  # type: Session
        logger.info("Step 1: Collect profile URLs")
        collector = URLCollector(
            base_url="https://hrdmemorial.org/hrdrecord/",
            db_session=session,
            delay=2.0
        )
        profile_urls = collector.collect()
        logger.info(f"Collected {len(profile_urls)} profile URLs")

        logger.info("Step 2: Scrape profile pages")
        scraper = ProfileScraper(db_session=session, delay=1.0)
        report = scraper.scrape_profiles(profile_urls)
        logger.info(f"Scraping report: {report}")

        logger.info("Step 3: Export profiles to CSV")
        # Ensure output directory exists
        output_dir = "./output_profiles"
        os.makedirs(output_dir, exist_ok=True)

        exporter = Exporter(db_session=session, output_dir=output_dir)
        export_report = exporter.export(
            include_profiles=True,
            include_urls=True,   # skip exporting URLs table
            to_pandas=True
        )
        logger.info(f"Export report: {export_report}")

    logger.info("Pipeline complete (profiles only)")

if __name__ == "__main__":
    main()
