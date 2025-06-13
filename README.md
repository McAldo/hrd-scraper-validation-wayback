---

## Human Rights Defenders Profile Scraper

### About the HRD Memorial

The **HRD Memorial** ([https://hrdmemorial.org](https://hrdmemorial.org)) is an online archive that honors human rights defenders who were killed in the course of their work. It provides searchable, country- and issue-specific records—including biographies, dates, locations, and external sources—to document their legacies and support research, advocacy, and remembrance.

### 1. Project Description

A one-off Python suite, broken into discrete phases:

1. **Phase I: Profile Collection & Scraping**

   1. **Collect** all individual profile URLs of human rights defenders (≈ 1 700) via paginated lists.
   2. **Scrape** each profile page for personal details (name, image, dates, location, biography, contact, external links).
   3. **Store** the data in an SQLite database (`profiles` & `urls` tables).
   4. **Export** the `profiles` table to a UTF-8-SIG CSV for downstream analysis (e.g., in Excel).

2. **Phase II: Link Validation**

   1. **Schema migration**: add `is_active`, `contains_name`, `page_text`, `checked_at` columns to the existing `urls` table without losing Phase I data.
   2. **Validate** each external URL (≈ 5 000) by:

      * Performing HEAD→GET fallback requests to detect live vs. dead links.
      * Scraping full page text for live URLs.
      * Normalizing whitespace and performing a cascade of name-matching strategies (exact full-name, surname only, fuzzy matching via RapidFuzz, regex token match).
      * Recording matches and full text in the database, along with timestamps.
   3. **Support** batch limits, resumable runs (`--limit`, `--force`).

3. **Phase III: Wayback Machine Archiving**

   * For all inactive but important URLs, check and submit missing pages to the Internet Archive’s Wayback Machine via its API.
   * Compile and share an archive report with HRD management.

### 2. Motivation

* **Data preservation**: capture structured records of defenders from a static site.
* **Link longevity**: ensure the cited external resources are active or archived for future reference.
* **Analysis readiness**: clean, tabular data for mapping, timelines, and link-rollchecks.
* **Ethical scraping**: rate-limit with delays, use a browser-like User-Agent, and stagger API calls to minimize load.
* **Reproducibility**: modular, scriptable steps allow resuming or re-running at any phase.

### 3. Architecture & Structure

```
project/
├── db.py                         # SQLAlchemy ORM models & DB init
├── url_collector.py              # Phase I: paginate & collect profile URLs
├── profile_scraper.py            # Phase I: fetch & parse each profile into DB
├── run_pipeline_profiles.py      # Phase I orchestrator: collect → scrape → export
├── export_module.py              # Phase I: CSV export (UTF-8-SIG for Excel)
├── phase2_validator.py           # Phase II: URL validation logic
└── run_phase2.py                 # Phase II orchestrator: schema migration → validate
```

* **Phase I pipeline** manages profiles and initial link collection.
* **Phase II pipeline** reads existing `urls`, augments schema, validates and annotates links.

### 4. Key Libraries Used

* **Requests & BeautifulSoup**: HTTP fetch + HTML parsing
* **SQLAlchemy**: ORM-based database modeling (SQLite)
* **Pandas**: DataFrame manipulation & CSV export
* **TQDM**: Command-line progress bars
* **RapidFuzz**: fuzzy string matching
* **urllib3 Retry & HTTPAdapter**: robust request retries
* **Datetime, JSON, re**: field normalization and serialization

### 5. Development Workflow

1. **Iterative Prompt Design**

   * Collaboratively with ChatGPT, crafted a multi-stage prompt: from high-level objectives to detailed module specs.
2. **Specification-Driven Coding**

   * ChatGPT generated skeletons for each module (collector, scraper, exporter, validator).
3. **Debugging & Enhancements**

   * **Phase I**:

     * Added realistic **User-Agent** to avoid 403s.
     * JSON-encoded multi-valued fields; unwrapped singletons.
     * Scraped correct `<div class="entry-content">` for biographies.
     * Exported CSVs with `utf-8-sig` for Excel.
     * Added missing `region` & `country` columns to `profiles`.
     * Implemented respectful **rate-limiting** (delays).
   * **Phase II**:

     * Migrated `urls` schema via `ALTER TABLE` without data loss.
     * Fallback HEAD→GET checks, with retries for server errors.
     * Cascading name-matching: exact, surname-only, fuzzy matching (RapidFuzz), regex token match.
     * Full page text storage and timestamping.
     * Batch limits and resumable runs (`--limit`, `--force`).

### 6. Ethical Considerations

* **Rate limiting** and **delays** ensure minimal impact on both HRD’s website and external domains.
* **Selective retries** avoid hammering unstable servers.
* **Archival via Wayback Machine** preserves inaccessible resources for public benefit.

---

## USAGE


* **Phase I pipeline** manages profiles and initial link collection.  
* **Phase II pipeline** reads existing `urls`, augments schema, validates and annotates links.  
* **Phase III** (future) handles Wayback Machine archiving.

---

### Key Libraries Used

* **Requests & BeautifulSoup**: HTTP fetch + HTML parsing  
* **SQLAlchemy**: ORM-based database modeling (SQLite)  
* **Pandas**: DataFrame manipulation & CSV export  
* **TQDM**: Command-line progress bars  
* **RapidFuzz**: fuzzy string matching  
* **urllib3 Retry & HTTPAdapter**: robust request retries  
* **Datetime, JSON, re**: field normalization and serialization  

---

### Setup & Requirements

1. **Clone** the repo and `cd` in:
   ```bash
   git clone https://github.com/YOUR_USER/hrd-scraper.git
   cd hrd-scraper

2. Create & activate a Python virtual environment:

```
python -m venv .venv

# On macOS/Linux or Git Bash:
source .venv/Scripts/activate

# On PowerShell:
.\.venv\Scripts\Activate.ps1
```

3. Install dependencies

```
pip install -r requirements.txt
```

### Phase I: profile collection and scraping

In the file `url_collector.py` set the max profile number to a low value (already set to 20)

```
class URLCollector:
    def __init__(self,
                 base_url: str,
                 db_session: Session,
                 delay: float = 2.0,
                 start_page: int = 1,
                 max_pages: Optional[int] = 20):  ## Max number of profiles to be collected, set to None for no limit
```
then run the pipeline via command line
```

# Scrape all ≈1 700 profiles:
python run_1_pipeline_collect_scrape_ToCSV_profiles
```
By default, this will create/append to hrd.db and output output_profiles/profiles.csv (UTF-8-SIG).

### Phase II: URL Validation
Via command line:

```
# Validate next 100 URLs (only unchecked):
python run_phase2.py --limit 100

# Validate all remaining URLs:
python run_phase2.py

# Re-validate every URL from scratch:
python run_phase2.py --force
```
Fields written back into hrd.db's urls table:
is_active, contains_name, page_text, checked_at.

### Ethical Considerations & Best Practice

⚠️ Do not run the full pipelines unbounded against the live HRD server.

    Always limit the number of profiles (e.g. 10-30 items per run) while testing or harvesting.

    Respectful rate-limiting and User-Agent headers protect the site’s bandwidth.

    Coordinate with site maintainers before large-scale runs.

> **Note**: This end-to-end pipeline was planned and refined through an LLM-guided iterative prompt process, with ChatGPT 04-mini as a collaborative design partner at each stage.
