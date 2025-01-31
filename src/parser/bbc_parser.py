import time
import random
import requests
import json
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urlparse
from tqdm.asyncio import tqdm
from dataclasses import dataclass

@dataclass
class ScraperConfig:
    base_url: str = "https://www.bbc.com/news"
    headers: list = (
        {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"},
        {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"},
    )
    delay_range: tuple = (1, 5)
    output_file: str = "bbc_articles.json"
    categories: tuple = ("world", "business", "technology", "science_and_environment")
    min_articles: int = 4500

CONFIG = ScraperConfig()

async def get_soup(session, url):
    """Fetches and parses a URL safely."""
    headers = random.choice(CONFIG.headers)
    await asyncio.sleep(random.uniform(*CONFIG.delay_range))
    try:
        async with session.get(url, headers=headers, timeout=100) as response:
            response.raise_for_status()
            text = await response.text()
            return BeautifulSoup(text, "html.parser")
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

async def get_all_news_links():
    """Fetches all news article links from selected BBC News categories."""
    async with aiohttp.ClientSession() as session:
        links = set()
        tasks = []

        for category in CONFIG.categories:
            page = 1
            while len(links) < CONFIG.min_articles:
                category_url = f"{CONFIG.base_url}/{category}?page={page}"
                tasks.append(get_soup(session, category_url))
                page += 1
                if page > 40:  # Prevent infinite loops
                    break
        
        soups = await asyncio.gather(*tasks)
        
        for soup in tqdm(soups, desc="Fetching news links", unit="category"):
            if not soup:
                continue
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/news/" in href and not href.startswith("http"):
                    links.add("https://www.bbc.com" + href)
                if len(links) >= CONFIG.min_articles:
                    break
        
        print(f"Total links collected: {len(links)}")
        return list(links)


async def extract_article_data(session, url):
    """Extracts text data from a BBC newspaper page."""
    soup = await get_soup(session, url)
    if not soup:
        return None
    
    title = soup.find("h1").text if soup.find("h1") else "Unknown Title"
    category = urlparse(url).path.split("/")[2] if len(urlparse(url).path.split("/")) > 2 else "Unknown"
    
    tags = [tag.text for tag in soup.find_all("li", class_="bbc-1msyfg1 e1hq59l0")]  # Adjust selector as needed
    text = " ".join([p.text for p in soup.find_all("p")])
    
    # Extracting small article text (summary)
    article_summary = soup.find("meta", attrs={"name": "description"})
    article_summary = article_summary["content"] if article_summary else "No Summary Available"
    
    return {
        "article_id": url,
        "title": title,
        "category": category.replace("-", "_"),
        "tags": ", ".join(tags) if tags else "No Tags Available",
        "summary": article_summary,
        "text": text.strip()
    }

async def scrape_bbc_articles():
    """Scrapes multiple BBC articles safely and saves to JSON."""
    article_urls = await get_all_news_links()
    results = []
    async with aiohttp.ClientSession() as session:
        tasks = [extract_article_data(session, url) for url in article_urls]
        for future in tqdm(asyncio.as_completed(tasks), total=len(tasks), desc="Scraping BBC News", unit="article"):
            data = await future
            if data:
                results.append(data)
    
    with open(CONFIG.output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=4, ensure_ascii=False)
    
    print(f"Data saved to {CONFIG.output_file}")
    return results

# Example usage
if __name__ == "__main__":
    asyncio.run(scrape_bbc_articles())