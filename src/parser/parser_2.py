import requests
import xml.etree.ElementTree as ET
from bs4 import BeautifulSoup
import json
import time
import random
from datetime import datetime, timezone
from urllib.parse import urlparse

# Configuration
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15'
]

CATEGORY_MAP = {
    'science': ['science', 'environment', 'climate'],
    'technology': ['technology', 'tech', 'innovation'],
    'business': ['business', 'economy', 'markets'],
    'entertainment': ['entertainment', 'arts', 'culture'],
    'other': ['other'],
}

SITEMAP_INDEX = 'https://www.bbc.com/sitemaps/https-index-com-news.xml'
MAX_ARTICLES = 1000
REQUEST_DELAY = (1, 3)
NS = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

session = requests.Session()
session.headers.update({'User-Agent': random.choice(USER_AGENTS)})

def fetch_xml(url):
    """Fetch XML content with error handling"""
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {url}: {str(e)}")
        return None

def parse_sitemap_index(index_url):
    """Parse sitemap index and return valid news sitemaps"""
    print(f"Processing index: {index_url}")
    content = fetch_xml(index_url)
    if not content:
        return []
    
    try:
        root = ET.fromstring(content)
        return [elem.text for elem in root.findall('.//sm:sitemap/sm:loc', NS)]
    except ET.ParseError as e:
        print(f"XML parse error: {str(e)}")
        return []

def process_news_sitemap(sitemap_url):
    """Extract recent news URLs from a news sitemap"""
    print(f"Processing news sitemap: {sitemap_url}")
    content = fetch_xml(sitemap_url)
    if not content:
        return []
    
    try:
        root = ET.fromstring(content)
        urls = []
        
        for url_elem in root.findall('.//sm:url', NS):
            loc = url_elem.find('sm:loc', NS).text
            lastmod = url_elem.find('sm:lastmod', NS)
            
            if lastmod is not None:
                try:
                    # Parse as UTC datetime
                    article_date = datetime.fromisoformat(
                        lastmod.text.replace('Z', '+00:00')
                    ).astimezone(timezone.utc)
                    
                    # Get current UTC time
                    now_utc = datetime.now(timezone.utc)
                    
                    # Compare both timezone-aware datetimes
                    if article_date > now_utc:
                        continue
                except ValueError:
                    pass
                
            urls.append(loc)
        
        return urls
    except ET.ParseError as e:
        print(f"XML parse error: {str(e)}")
        return []

def categorize_url(url):
    """Categorize URL using multiple matching strategies"""
    path = urlparse(url).path.lower()
    
    # Direct category matching
    for category, keywords in CATEGORY_MAP.items():
        if any(kw in path for kw in keywords):
            return category
    
    # Structural analysis for news URLs
    parts = [p for p in path.split('/') if p]
    if len(parts) >= 3 and parts[1] == 'news':
        if parts[2].isdigit():  # Article ID format
            return parts[3] if len(parts) > 3 else 'general'
        return parts[2]
    
    return 'other'

def collect_articles():
    """Main collection function with progress tracking"""
    articles = {cat: [] for cat in CATEGORY_MAP}
    articles['other'] = []
    
    # Step 1: Get all news sitemaps
    sitemaps = parse_sitemap_index(SITEMAP_INDEX)
    print(f"Found {len(sitemaps)} news sitemaps")
    
    # Step 2: Process sitemaps until we reach target counts
    for sitemap_url in sitemaps:
        # if all(len(v) >= MAX_ARTICLES for v in articles.values() if v is not 'other'):
        if all(len(v) >= MAX_ARTICLES for v in articles.values()):
            break
            
        urls = process_news_sitemap(sitemap_url)
        random.shuffle(urls)  # Avoid detection patterns
        
        for url in urls:
            category = categorize_url(url)
            if category in articles and len(articles[category]) < MAX_ARTICLES:
                articles[category].append(url)
                
            # Early exit check
            # if all(len(v) >= MAX_ARTICLES for v in articles.values() if v is not 'other'):
            if all(len(v) >= MAX_ARTICLES for v in articles.values()):
                break
            
        time.sleep(random.uniform(*REQUEST_DELAY))
    
    print("\nCollection results:")
    for cat, urls in articles.items():
        print(f"{cat.capitalize()}: {len(urls)} URLs")
    
    return articles

def parse_article(url):
    """Parse article content with multiple fallback strategies"""
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Extract JSON-LD metadata
        metadata = {}
        script = soup.find('script', type='application/ld+json')
        if script:
            try:
                metadata = json.loads(script.string)
                if isinstance(metadata, list):
                    metadata = metadata[0]
            except json.JSONDecodeError:
                pass
        
        # Title extraction
        title = metadata.get('headline') or soup.find('h1').get_text(strip=True) if soup.find('h1') else ''
        
        # Main content extraction
        body = soup.find('main', {'id': 'main-content'}) or soup.find('article')
        paragraphs = body.find_all('p') if body else []
        text = '\n'.join([p.get_text(strip=True) for p in paragraphs if p.get_text(strip=True)])
        
        # Tags extraction
        tags = metadata.get('keywords', [])
        if not tags:
            tags_section = soup.find('div', {'data-component': 'topic-list'})
            if tags_section:
                tags = [tag.get_text(strip=True) for tag in tags_section.find_all('li')]
        
        return {
            "article_id": url,
            "title": title,
            "category": categorize_url(url),
            "tags": ", ".join(tags),
            "text": text,
            "source": "BBC News",
            "date_published": metadata.get('datePublished', '')
        }
    
    except Exception as e:
        print(f"Error processing {url}: {str(e)}")
        return None

def main():
    # Collect articles
    articles = collect_articles()
    
    # Process and save articles
    final_data = []
    for category in CATEGORY_MAP:
        print(f"\nProcessing {len(articles[category])} {category} articles...")
        for i, url in enumerate(articles[category], 1):
            article = parse_article(url)
            if article:
                final_data.append(article)
                print(f"Processed {i}/{len(articles[category])}: {article['title'][:50]}...")
            
            time.sleep(random.uniform(*REQUEST_DELAY))  # Respectful delay
    
    # Save results
    filename = f"bbc_news_{datetime.now().strftime('%Y%m%d_%H%M')}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(final_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nSuccessfully saved {len(final_data)} articles to {filename}")

if __name__ == "__main__":
    main()