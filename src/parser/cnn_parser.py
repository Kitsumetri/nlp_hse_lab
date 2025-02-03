import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import json
import re
import concurrent.futures
import os
import logging
from urllib.parse import urlparse
import time
import random

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

# Конфигурация
TARGET_CATEGORIES = [
    "world", "politics", "business", "health", "entertainment",
    "tech", "travel", "opinion", "sports", "science", "style", "living"
]
MIN_ARTICLES_PER_CATEGORY = 1000
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/115.0'
]
REQUEST_DELAY = 0.2
OUTPUT_DIR = os.path.join(os.getcwd(), 'cnn_data')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'cnn_articles.json')

def get_random_agent():
    return random.choice(USER_AGENTS)

def create_directory(path):
    """Создает директорию если не существует"""
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        logging.error(f"Ошибка создания директории {path}: {str(e)}")
        raise

def extract_text(soup):
    """Улучшенный метод извлечения текста"""
    # Сохранение оригинальной логики извлечения текста
    # ... (оставить без изменений)

def normalize_category(category):
    """Обновленный маппинг категорий"""
    # ... (оставить без изменений)

def parse_article(url):
    """Добавлена обработка новых структу CNN"""
    headers = {
        'User-Agent': get_random_agent(),
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        logging.warning(f"Ошибка запроса {url}: {str(e)}")
        return None

    # Остальная логика парсинга
    # ... (оставить без изменений)

def get_sitemap_links(sitemap_url):
    """Обновленный парсер sitemap с обработкой CDATA"""
    headers = {'User-Agent': get_random_agent()}
    try:
        response = requests.get(sitemap_url, headers=headers, timeout=20)
        response.raise_for_status()
    except Exception as e:
        logging.error(f"Ошибка sitemap: {sitemap_url} - {str(e)}")
        return []

    try:
        # Обработка CDATA секций
        content = response.content.replace(b'<![CDATA[', b'').replace(b']]>', b'')
        root = ET.fromstring(content)
    except ET.ParseError as e:
        logging.error(f"XML Parse Error: {str(e)}")
        return []

    namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
    links = []

    # Обработка вложенных sitemap
    for sitemap in root.findall('ns:sitemap', namespace):
        loc = sitemap.find('ns:loc', namespace)
        if loc is not None:
            links += get_sitemap_links(loc.text.strip())

    # Сбор URL статей с новым фильтром
    for url in root.findall('ns:url', namespace):
        loc = url.find('ns:loc', namespace)
        if loc is not None:
            url_str = loc.text.strip()
            if re.search(r'/(20\d{2}|articles?|news)/', url_str):
                links.append(url_str)
                time.sleep(0.01)  # Небольшая задержка

    return links

def save_progress(data):
    """Надежное сохранение данных"""
    try:
        create_directory(OUTPUT_DIR)
        temp_file = OUTPUT_FILE + '.tmp'
        
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            
        os.replace(temp_file, OUTPUT_FILE)
        logging.info(f"Данные сохранены в {OUTPUT_FILE}")
        
    except Exception as e:
        logging.error(f"Ошибка сохранения: {str(e)}")
        raise

def main():
    create_directory(OUTPUT_DIR)
    
    try:
        # Получение URL
        main_sitemaps = [
            'https://edition.cnn.com/sitemaps/sitemap.xml',
            'https://edition.cnn.com/sitemaps/sitemap-news.xml'
        ]
        
        all_urls = set()
        for sitemap_url in main_sitemaps:
            logging.info(f"Загрузка sitemap: {sitemap_url}")
            urls = get_sitemap_links(sitemap_url)
            all_urls.update(urls)
            logging.info(f"Найдено URL: {len(urls)} в {sitemap_url}")
            time.sleep(1)

        logging.info(f"Всего собрано URL: {len(all_urls)}")
        if not all_urls:
            raise ValueError("Не найдено URL для обработки")

        # Инициализация структур данных
        articles = {category: [] for category in TARGET_CATEGORIES}
        articles['other'] = []

        # Параллельная обработка
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(parse_article, url): url for url in all_urls}
            
            for i, future in enumerate(concurrent.futures.as_completed(futures)):
                try:
                    article = future.result()
                except Exception as e:
                    logging.warning(f"Ошибка обработки: {str(e)}")
                    continue
                
                if article:
                    category = article['category']
                    if category in articles:
                        articles[category].append(article)
                    else:
                        articles['other'].append(article)

                # Сохранение каждые 100 статей
                if (i + 1) % 100 == 0:
                    save_progress(articles)
                    logging.info(f"Обработано {i+1}/{len(all_urls)} статей")

        # Финальное сохранение
        save_progress(articles)
        logging.info("Сбор данных завершен успешно")

    except Exception as e:
        logging.error(f"Критическая ошибка: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Прервано пользователем")
    except Exception as e:
        logging.error(f"Фатальная ошибка: {str(e)}")