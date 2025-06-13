#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import json

def scrape_single_profile(url: str):
    # Set up a session with a realistic User-Agent
    session = requests.Session()
    session.headers.update({
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/114.0.0.0 Safari/537.36'
        )
    })
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    data = {
        'slug': url.rstrip('/').split('/')[-1],
        'profile_url': url
    }

    # Name
    title = soup.find('h1', class_='entry-title')
    data['name'] = title.get_text(strip=True) if title else None

    # Basic Information fields
    for p in soup.select('p.basic-info-item'):
        label = p.find('span').get_text(strip=True).rstrip(':')
        # Value might be in an <a> or as plain text
        a = p.find('a')
        if a:
            value = a.get_text(strip=True)
        else:
            parts = p.get_text(strip=True).split(':', 1)
            value = parts[1].strip() if len(parts) > 1 else None
        data[label] = value

    # Description (article body)
    content_div = soup.find('div', class_='entry-content')
    data['description_text'] = (
        content_div.get_text('\n', strip=True) if content_div else None
    )

    # URLs of Interest
    urls = []
    heading = soup.find('h5', string=lambda t: 'URLs' in t)
    if heading:
        dl = heading.find_next_sibling('dl')
        if dl:
            for dt in dl.find_all('dt'):
                dd = dt.find_next_sibling('dd')
                link = dd.find('a') if dd else None
                urls.append({
                    'label': dt.get_text(strip=True),
                    'url': link['href'] if link else None
                })
    data['urls'] = urls

    print("\n=== SCRAPED PROFILE DATA ===")
    print(json.dumps(data, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    test_url = "https://hrdmemorial.org/hrdrecord/mateo-chaman-paau/"
    scrape_single_profile(test_url)
