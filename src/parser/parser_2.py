import requests
import json
import time
import random
from urllib.parse import quote
from bs4 import BeautifulSoup

# Updated settings
categories = ['world', 'business', 'technology', 'politics']
articles_per_category = 20
articles_per_request = 20
output_links_file = 'reuters_links.jsonl'
output_articles_file = 'reuters_articles.json'

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
        time.sleep(random.uniform(1, 2))
        
        # Second request to simulate browser warmup
        self.session.get('https://www.reuters.com/politics/', headers=HEADERS_TEMPLATE)
        time.sleep(random.uniform(1, 2))

    def _make_request(self, url, category=None):
        """Make a request with proper headers and delays"""
        headers = HEADERS_TEMPLATE.copy()
        if category:
            headers['Referer'] = f'https://www.reuters.com/{category}/'
        
        time.sleep(random.uniform(1.5, 3.5))  # Randomized delay
        
        try:
            response = self.session.get(url, headers=headers)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                print("Blocking detected - regenerating session...")
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
                    response = self._make_request(url)
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
                        print(f"Collected {collected}/{articles_per_category} in {category}")
                        if collected >= articles_per_category:
                            break
                    
                    offset += articles_per_request
                    
                except Exception as e:
                    print(f"Error fetching {category}: {e}")
                    break

    def parse_articles(self):
        """Parse individual articles with enhanced protection bypass"""
        articles_data = []
        
        with open(output_links_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        for line in lines:
            article_info = json.loads(line)
            url = article_info['url']
            category = article_info['category']
            tags = article_info['tags']
            
            try:
                response = self._make_request(url, category)
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Extract title
                title = soup.find('h1').text.strip() if soup.find('h1') else article_info['title']
                
                # Extract article body
                body = soup.select_one('div.article-body__content')
                text = ' '.join([p.text.strip() for p in body.find_all('p')]) if body else ''
                
                # Fallback tag extraction
                if not tags:
                    meta_keywords = soup.find('meta', {'name': 'keywords'})
                    if meta_keywords:
                        tags = [tag.strip() for tag in meta_keywords.get('content', '').split(',')]
                
                article_entry = {
                    "article_id": url,
                    "title": title,
                    "category": category.replace('-', '_'),
                    "tags": ",".join(tags),
                    "text": text.strip()
                }
                
                articles_data.append(article_entry)
                print(f"Successfully parsed: {title[:60]}...")
                
            except Exception as e:
                print(f"Error parsing {url}: {e}")
                continue
        
        with open(output_articles_file, 'w', encoding='utf-8') as f:
            json.dump(articles_data, f, ensure_ascii=False, indent=4)

if __name__ == '__main__':
    scraper = ReutersScraper()
    scraper.fetch_links()
    scraper.parse_articles()