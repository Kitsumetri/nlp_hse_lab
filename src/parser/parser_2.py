import requests
import json
import time
import random
import logging
from src.logger import setup_logger
from urllib.parse import quote
from bs4 import BeautifulSoup
import regex as re

# Updated settings
categories = ['world', 'business', 'technology', 'markets']
# categories = ['business']
articles_per_category = 1000
articles_per_request = 20
connection_batch_size = 100
output_links_file = 'data/reuters_links.jsonl'
output_articles_file = 'data/reuters_articles.json'

# Modern browser headers template
HEADERS_TEMPLATE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'Connection': 'keep-alive',
    'Cache-Control': 'max-age=0',
    'DNT': '1'
}

class ReutersScraper:
    def __init__(self):
        self.session = requests.Session()
        self._init_session()

    def _init_session(self):
        """Initialize session with proper cookies and headers"""
        # First request to establish basic cookies
        self.session.get('https://www.reuters.com/', headers=HEADERS_TEMPLATE)
        time.sleep(random.uniform(3, 5))
        
        # Second request to simulate browser warmup
        self.session.get('https://www.reuters.com/world/', headers=HEADERS_TEMPLATE)
        time.sleep(random.uniform(3, 5))

    def _make_request(self, url, category=None):
        """Make a request with proper headers and delays"""
        headers = HEADERS_TEMPLATE.copy()
        if category:
            headers['Referer'] = f'https://www.reuters.com/{category}/'
        
        time.sleep(random.uniform(0.5, 2))
        
        try:
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                logging.warning("Blocking detected - regenerating session...")
                self._init_session()
                return self._make_request(url, category)
            raise

    def fetch_links(self):
        """Collect article links using the API"""
        base_api_url = 'https://www.reuters.com/pf/api/v3/content/fetch/articles-by-section-alias-or-id-v1?query='
        
        for category in categories:
            offset = 0
            collected = 0
            
            while collected < articles_per_category:
                query = {
                    "section_id": f"/{category}",
                    "size": articles_per_request,
                    "offset": offset,
                    "website": "reuters"
                }
                encoded_query = quote(json.dumps(query))
                url = base_api_url + encoded_query
                
                try:
                    response = self._make_request(url, category)
                    data = response.json()
                    articles = data.get('result', {}).get('articles', [])
                    
                    if not articles:
                        break
                    
                    for article in articles:
                        article_url = 'https://www.reuters.com' + article['canonical_url']
                        article_data = {
                            'url': article_url,
                            'category': category,
                            'title': article.get('title', ''),
                            'tags': [tag['slug'] for tag in article.get('taxonomy', {}).get('tags', [])]
                        }
                        
                        with open(output_links_file, 'a', encoding='utf-8') as f:
                            f.write(json.dumps(article_data, ensure_ascii=False) + '\n')
                        
                        collected += 1
                        logging.info(f"Collected {collected}/{articles_per_category} in {category}")
                        if collected >= articles_per_category:
                            break
                    
                    offset += articles_per_request
                    
                except Exception as e:
                    logging.error(f"Error fetching {category}: {e}")
                    break

    def _extract_article_content(self, soup):
        """Direct text extraction from paragraph containers"""
        paragraphs = soup.find_all('div', {
            'data-testid': lambda x: x and x.startswith('paragraph-')
        })
        
        clean_text = []
        for p in paragraphs[:2]:
            # Skip non-content containers
            if p.find(['aside', 'figure', 'div[data-testid="signup-prompt"]']):
                continue
                
            # Directly extract text from the div itself
            text = p.get_text(separator=' ', strip=True)
            if text:
                clean_text.append(text)
        
        return ' '.join(clean_text)

    # def _extract_tags_from_meta(self, soup):
    #     """Improved tag extraction from multiple sources"""
    #     # From keywords meta
    #     meta_keywords = soup.find('meta', {'name': 'keywords'})
    #     if meta_keywords:
    #         return [tag.strip() for tag in meta_keywords.get('content', '').split(',')]
        
    #     # From JSON-LD data
    #     script = soup.find('script', type='application/ld+json')
    #     if script:
    #         try:
    #             data = json.loads(script.string)
    #             return data.get('keywords', '').split(',')
    #         except json.JSONDecodeError:
    #             pass
        
    #     return []

    def parse_articles(self):
        """Updated parsing with proper text/tag handling"""
        articles_data = []
        
        with open(output_links_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for idx, line in enumerate(lines):
            article_info = json.loads(line)
            url = article_info['url']
            category = article_info['category']
            tags = article_info['tags']  # Preserve API-provided tags
            
            try:
                response = self._make_request(url, category)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract title using direct text access
                title_element = soup.find('h1')
                title = title_element.text.strip() if title_element else article_info['title']
                
                # Extract article text
                text = self._extract_article_content(soup)
                
                # Clean up Reuters-specific trailing content
                text = re.sub(r'\s*Sign up here\..*$', '', text, flags=re.DOTALL)
                text = re.sub(r'\s*Our Standards:.*$', '', text, flags=re.DOTALL)
                # Fallback tag extraction
                if not tags:
                    meta_keywords = soup.find('meta', {'name': 'keywords'})
                    if meta_keywords:
                        tags = [tag.strip() for tag in meta_keywords.get('content', '').split(',') if (not "DEST" in tag)]
                
                article_entry = {
                    "article_id": url,
                    "title": title,
                    "category": category.replace('-', '_'),
                    "tags": tags,
                    "text": text.strip()
                }
                
                articles_data.append(article_entry)
                logging.info(f"Successfully parsed: {title[:50]} ({idx+1}/{articles_per_category})...")
                
            except Exception as e:
                logging.error(f"Error parsing {url}: {e}")
                continue

            if idx % connection_batch_size == 0:
                logging.info("Safe reconnecting...")
                self._init_session()

                with open(output_articles_file, 'a', encoding='utf-8') as f:
                    json.dump(articles_data, f, ensure_ascii=False, indent=4)
                    articles_data.clear()
                    logging.info(f"Saved {idx+1} articles from {articles_per_category*len(categories)}")
        
        with open(output_articles_file, 'a', encoding='utf-8') as f:
            json.dump(articles_data, f, ensure_ascii=False, indent=4)
            articles_data.clear()
            logging.info(f"All {articles_per_category * len(categories)} articles saved!")
        
    def _remove_trailing_junk(self, text):
        """Remove common trailing elements like sign-up prompts"""
        patterns = [
            r'\s*Sign up here\..*$',
            r'\s*Our Standards:.*$',
            r'\s*Reporting by.*$',
            r'\s*Editing by.*$',
            r'\s*Thomson Reuters.*$'
        ]
        for pattern in patterns:
            text = re.sub(pattern, '', text, flags=re.DOTALL|re.IGNORECASE)
        return text.strip()

    def _extract_meta_description(self, soup):
        """Extract description from meta tags"""
        meta = soup.find('meta', {'name': 'description'}) or \
            soup.find('meta', {'property': 'og:description'})
        return meta.get('content', '').strip() if meta else ''

    def _extract_tags_from_meta(self, soup):
        """Improved tag extraction from multiple sources"""
        # From keywords meta
        meta_keywords = soup.find('meta', {'name': 'keywords'})
        if meta_keywords:
            return [tag.strip() for tag in meta_keywords.get('content', '').split(',')]
        
        # From JSON-LD data
        script = soup.find('script', type='application/ld+json')
        if script:
            try:
                data = json.loads(script.string)
                return data.get('keywords', '').split(',')
            except json.JSONDecodeError:
                pass
        
        return []

if __name__ == '__main__':
    with open(output_articles_file, 'w', encoding='utf-8') as f:
        f.write("")
    
    with open(output_links_file, 'w', encoding='utf-8') as f:
        f.write("")
    
    setup_logger(logging.INFO, stdout_log=True, file_log=False)
    scraper = ReutersScraper()
    scraper.fetch_links()
    scraper.parse_articles()