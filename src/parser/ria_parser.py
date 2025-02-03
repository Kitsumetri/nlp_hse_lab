import os
import re
import json
import time
import logging
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/90.0.4430.93 Safari/537.36")
}

CATEGORIES = {
    "Политика": "https://ria.ru/politics/",
    "В мире": "https://ria.ru/v-mire/",
    "Экономика": "https://ria.ru/ekonomika/",
    "Общество": "https://ria.ru/obshchestvo/",
    "Происшествия": "https://ria.ru/proisshestviya/",
    "Армия": "https://ria.ru/armiya/",
    "Наука": "https://ria.ru/nauka/",
    "Культура": "https://ria.ru/kultura/",
    "Спорт": "https://rsport.ria.ru/",
    "Туризм": "https://ria.ru/turizm/",
    "Религия": "https://ria.ru/religiya/"
}

def get_article_links(category_url, max_articles=1000):
    """
    Собирает ссылки на статьи из заданной категории с помощью автоматической прокрутки (infinite scroll).
    Используется Selenium для эмуляции браузера с поддержкой JavaScript.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(f'user-agent={HEADERS["User-Agent"]}')
    
    # Используем webdriver_manager для автоматической установки chromedriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.get(category_url)
    
    SCROLL_PAUSE_TIME = 1.0
    links = set()
    
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while len(links) < max_articles:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        for tag in soup.find_all('a', href=True):
            href = urljoin(category_url, tag['href'])
            if re.search(r'/20\d{2}', href):
                links.add(href)
        
        new_height = driver.execute_script("return document.body.scrollHeight")
        logging.info(f"Найдено {len(links)} ссылок на текущем этапе")
        if new_height == last_height:
            logging.info("Больше страниц не подгружается")
            break
        last_height = new_height

    driver.quit()
    logging.info(f"Итог: найдено {len(links)} ссылок для категории {category_url}")
    return list(links)[:max_articles]

def parse_article(url, category_name):
    try:
        import requests
        response = requests.get(url, headers=HEADERS, timeout=10)
    except Exception as e:
        logging.error(f"Ошибка запроса статьи {url}: {e}")
        return None
    if response.status_code != 200:
        logging.error(f"Статус {response.status_code} для {url}")
        return None
    soup = BeautifulSoup(response.text, 'html.parser')
    title_tag = soup.find('h1')
    title = title_tag.get_text(strip=True) if title_tag else ''
    content = ''
    content_div = soup.find('div', class_=re.compile(r'article[-_]text'))
    if content_div:
        paragraphs = content_div.find_all('p')
        content = "\n".join(p.get_text(strip=True) for p in paragraphs)
    else:
        content = soup.get_text(separator="\n", strip=True)
    tags = ''
    tags_div = soup.find('div', class_=re.compile(r'article[-_]tags'))
    if tags_div:
        tag_list = [a.get_text(strip=True) for a in tags_div.find_all('a')]
        tags = ",".join(tag_list)
    else:
        meta_keywords = soup.find('meta', attrs={'name': 'keywords'})
        if meta_keywords:
            tags = meta_keywords.get('content', '')
    if not title or not content:
        return None
    return {
        "article_id": url,
        "title": title,
        "category": category_name,
        "tags": tags,
        "text": content
    }

def save_articles(articles, output_filename):
    with open(output_filename, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)
    logging.info(f"Сохранено {len(articles)} статей в {output_filename}")

def main():
    all_articles = []
    os.makedirs("data", exist_ok=True)
    output_filename = os.path.join("data", "ria_articles.json")
    for category_name, category_url in CATEGORIES.items():
        logging.info(f"Начало сбора для категории: {category_name} ({category_url})")
        article_links = get_article_links(category_url, max_articles=1000)
        logging.info(f"Найдено {len(article_links)} ссылок для категории {category_name}")
        count = 0
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_url = {executor.submit(parse_article, link, category_name): link for link in article_links}
            for future in as_completed(future_to_url):
                article = future.result()
                if article:
                    all_articles.append(article)
                    count += 1
                    logging.info(f"Спарсена статья: {article['title']}")
                else:
                    logging.info(f"Не удалось спарсить: {future_to_url[future]}")
                if len(all_articles) % 100 == 0:
                    save_articles(all_articles, output_filename)
        logging.info(f"Завершено для категории {category_name}: спарсено {count} статей")
    save_articles(all_articles, output_filename)
    logging.info(f"Общий итог: {len(all_articles)} статей сохранено в {output_filename}")

if __name__ == "__main__":
    main()
