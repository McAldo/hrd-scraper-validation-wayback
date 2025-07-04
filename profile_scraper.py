import time
import logging
import json
from datetime import datetime
from typing import List, Tuple, Dict, Optional, Any

import requests
from bs4 import BeautifulSoup, Tag
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError

from tqdm import tqdm               # ← new import
from db import Profile, URL

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class ProfileScraper:
    def __init__(self, db_session: Session, delay: float = 1.0):
        self.db_session = db_session
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/114.0.0.0 Safari/537.36'
            )
        })

    def scrape_profiles(self, profile_urls: List[str]) -> Dict[str, Any]:
        """
        Scrape a list of profile URLs with a tqdm progress bar.
        """
        total = len(profile_urls)
        report = {'total': total, 'success': 0, 'failures': []}

        # Wrap the list in tqdm to show progress & remaining count
        for url in tqdm(profile_urls,
                        desc="Scraping profiles",
                        unit="profile",
                        leave=True):
            success, error = self.scrape_single_profile(url)
            if success:
                report['success'] += 1
            else:
                report['failures'].append({'url': url, 'error': error})
            time.sleep(self.delay)

        return report

    def scrape_single_profile(self, url: str) -> Tuple[bool, Optional[str]]:
        try:
            resp = self.session.get(url, timeout=10)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return False, str(e)

        try:
            profile_data, url_records = self.extract_profile_data(resp.text, url)
        except Exception as e:
            logger.error(f"Extraction failed for {url}: {e}")
            return False, str(e)

        try:
            pid = self._upsert_profile(profile_data)
            self._insert_urls(pid, url_records)
            self.db_session.commit()
            return True, None
        except SQLAlchemyError as e:
            self.db_session.rollback()
            logger.error(f"Database error for {url}: {e}")
            return False, str(e)

    # ... rest of your methods (_normalize_date, extract_profile_data, _upsert_profile, _insert_urls) unchanged ...


    def extract_profile_data(self, html: str, url: str) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        soup = BeautifulSoup(html, 'html.parser')
        slug = url.rstrip('/').split('/')[-1]

        data: Dict[str, Any] = {
            'slug': slug,
            'profile_url': url,
            'created_at': datetime.utcnow()
        }

        # Name & Image
        title = soup.find('h1', class_='entry-title')
        data['name'] = title.get_text(strip=True) if title else None
        img = soup.select_one('div.thumbnail img')
        data['image_url'] = img['src'] if img and img.has_attr('src') else None

        # Parse all <p class="basic-info-item">
        for p in soup.select('p.basic-info-item'):
            label = p.find('span').get_text(strip=True).rstrip(':')
            a = p.find('a')
            if a:
                val = a.get_text(strip=True)
            else:
                txt = p.get_text(strip=True)
                val = txt.split(':',1)[1].strip() if ':' in txt else None

            if label == 'Region':
                data['region'] = val
            elif label == 'Country':
                data['country'] = val
            elif label in ('Department/Province/State',):
                data['state'] = val
            elif label.startswith('Sex'):
                data['sex'] = val
            elif label == 'Date of Killing':
                data['date_of_killing'] = self._normalize_date(val) if val else None
            elif label == 'Previous Threats':
                data['previous_threats'] = (val or '').lower() == 'yes'
            elif label == 'Type of Work':
                data['type_of_work'] = val
            elif label.startswith('Sector or Type of Rights'):
                data['sector'] = val
            elif label == 'Sector Detail':
                details = [a_.get_text(strip=True) for a_ in p.find_all('a')]
                data['sector_detail'] = json.dumps(details)
            elif label == 'More information':
                data['more_information'] = val

        # Defaults
        data.setdefault('region', None)
        data.setdefault('country', None)
        data.setdefault('state', None)
        data.setdefault('sex', None)
        data.setdefault('type_of_work', None)
        data.setdefault('sector', None)
        data.setdefault('previous_threats', False)
        data.setdefault('sector_detail', json.dumps([]))
        data.setdefault('more_information', None)

        # Source & Author
        src = soup.find('strong', string=lambda t: t and 'Source:' in t)
        if src:
            link = src.find_next('a')
            data['source_name'] = link.get_text(strip=True) if link else None
            data['source_url']  = link['href'] if link else None

        meta = soup.find('p', class_='meta')
        if meta and 'Written by' in meta.get_text():
            data['author'] = meta.get_text(strip=True).replace('Written by', '').strip()

        # Description: target the <div class="entry-content">
        desc_div = soup.find('div', class_='entry-content')
        if desc_div:
            # remove iframes so only text remains
            for iframe in desc_div.find_all('iframe'):
                iframe.decompose()
            data['description_html'] = str(desc_div)
            paragraphs = [
                p.get_text(separator=' ', strip=True)
                for p in desc_div.find_all('p')
                if p.get_text(strip=True)
            ]
            data['description_text'] = "\n\n".join(paragraphs).strip()
        else:
            data['description_html'] = None
            data['description_text'] = None

        # Contact email
        contact = soup.find('h5', string=lambda t: t and 'contact' in t.lower())
        if contact:
            p_mail = contact.find_next_sibling('p')
            if p_mail and p_mail.find('a', href=True):
                data['contact_email'] = p_mail.find('a')['href'].split('mailto:')[-1]
        data.setdefault('contact_email', None)

        # URLs of Interest
        url_records: List[Dict[str, Any]] = []
        section = soup.find('h5', string=lambda t: t and 'URLs' in t)
        if section:
            dl = section.find_next_sibling('dl')
            if dl:
                for dt in dl.find_all('dt'):
                    dd = dt.find_next_sibling('dd')
                    a = dd.find('a') if dd else None
                    if a and a.has_attr('href'):
                        url_records.append({
                            'label': dt.get_text(strip=True),
                            'url': a['href'],
                            'is_active': None,
                            'is_archived': None,
                            'archived_url': None
                        })

        return data, url_records

    def _normalize_date(self, s: str) -> Optional[datetime.date]:
        try:
            return datetime.strptime(s, '%d/%m/%Y').date()
        except Exception:
            logger.warning(f"Date parse failed: {s}")
            return None

    def _upsert_profile(self, pd: Dict[str, Any]) -> int:
        existing = self.db_session.query(Profile).filter_by(slug=pd['slug']).one_or_none()
        if existing:
            for k, v in pd.items():
                setattr(existing, k, v)
            obj = existing
        else:
            obj = Profile(**pd)
            self.db_session.add(obj)
        self.db_session.flush()
        return obj.profile_id

    def _insert_urls(self, profile_id: int, records: List[Dict[str, Any]]):
        objs = [URL(profile_id=profile_id, **r) for r in records]
        self.db_session.bulk_save_objects(objs)
