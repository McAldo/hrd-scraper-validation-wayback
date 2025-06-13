#!/usr/bin/env python3
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from db import Base
from phase2_validator import URLValidator

logger = logging.getLogger("phase2")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s"
)

def ensure_url_columns(engine):
    needed = {
        'is_active':     "BOOLEAN",
        'contains_name': "BOOLEAN",
        'page_text':     "TEXT",
        'checked_at':    "DATETIME"
    }
    with engine.connect() as conn:
        existing = {
            row['name']
            for row in conn.execute(text("PRAGMA table_info(urls);")).mappings()
        }
        for col, col_def in needed.items():
            if col not in existing:
                logger.info("Adding column `%s` to urls", col)
                conn.execute(text(f"ALTER TABLE urls ADD COLUMN {col} {col_def};"))
        conn.commit()

def main(limit=None, force=False):
    # 0) Setup
    DB_URL = "sqlite:///hrd.db"
    engine = create_engine(DB_URL, echo=False)
    Base.metadata.create_all(engine)
    ensure_url_columns(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    validator = URLValidator(db_session=session, delay=1.0)
    validator.validate_batch(limit=limit, force=force)

    logger.info("Phase II complete")

if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Phase II URL Validation")
    p.add_argument("--limit", type=int, default=None,
                   help="Max URLs to process")
    p.add_argument("--force", action="store_true",
                   help="Re-validate all URLs, ignoring prior checks")
    args = p.parse_args()

    main(limit=args.limit, force=args.force)
