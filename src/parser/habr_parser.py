import os
import requests
import random
import logging
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
}

CATEGORY_MAP = {
    "Программирование": ["programming", "python", "c++", "go", "javascript", "typescript", "react", "postgresql", "c#"],
    "Искусственный Интеллект": ["artificial_intelligence", "machine_learning", "research", "neural-networks", "bigdata"],
    "Кибербезопасность": ["infosecurity", "reverse-engineering", "cryptography", "pentest"],
    "Веб-разработка": ["webdev", "weban", "frontend", "backend", "nodejs", "typescript", "reactjs"],
    "Электроника": ["electronics", "circuit-design", "raspberry", "arduino", "easyelectronics"],
    "Мобильные технологии": ["smartphones", "cellular", "android", "ios"],
    "Менеджмент": ["pm", "productpm", "growthhacking", "sales", "project-management"],
    "Научпоп": ["popular_science", "futurenow", "brain", "antikvariat"],
    "Облачные технологии": ["cloud_services", "docker", "kubernetes", "aws"],
    "Базы данных": ["db_admins", "dwh", "postgresql", "mysql", "mongodb"],
    "Дизайн": ["analysis_design", "apps_design", "design"],
    "Разное": ["asterisk", "health", "interviews", "read"]
}

REQUEST_DELAY = 0.77
ARTICLES_PER_HUB = 300
MAX_PAGES_PER_HUB = 1000
TARGET_ARTICLES_PER_CATEGORY = 1000


def safe_request(session, url, retries=3, backoff_factor=1.5):
    """
    Безопасный запрос с повторными попытками.
    """
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=15)
            if response.status_code in [403, 429]:
                logging.warning(f"Блокировка: {response.status_code} для {url}")
                time.sleep(10 * (attempt + 1))
                continue
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка (попытка {attempt + 1}) для {url}: {e}")
            if attempt == retries - 1:
                return None
            sleep_time = backoff_factor * (2 ** attempt) + random.uniform(0, 1)
            time.sleep(sleep_time)
    return None


def categorize_hub(hub_url):
    path = urlparse(hub_url).path.lower()
    for category, keywords in CATEGORY_MAP.items():
        for keyword in keywords:
            if keyword in path:
                return category
    return "Разное"


def get_hub_urls(session):
    base_url = "https://habr.com/ru/"
    response = safe_request(session, base_url)
    if not response:
        return {}

    # Используем lxml для более быстрого парсинга
    soup = BeautifulSoup(response.content, 'lxml')
    hubs = set()

    # Сбор основных хабов
    for link in soup.select('a[href*="/hubs/"]:not([href*="/all/"])'):
        href = link.get('href', '')
        full_url = urljoin(base_url, href.split('/posts')[0])
        hubs.add(full_url.lower().rstrip('/') + '/')

    # Сбор популярных хабов
    for link in soup.select('a.tm-hubs-list__hub-link[href*="/hubs/"]'):
        href = link.get('href', '')
        full_url = urljoin(base_url, href.split('/posts')[0])
        hubs.add(full_url.lower().rstrip('/') + '/')

    categorized = defaultdict(list)
    for hub_url in hubs:
        category = categorize_hub(hub_url)
        categorized[category].append(hub_url)

    logging.info(f"Найдено хабов: {len(hubs)}")
    return dict(categorized)


def get_articles_from_hub(hub_url, max_articles, session):
    """
    Получает URL статей из конкретного хаба.
    """
    articles = []
    page = 1

    while len(articles) < max_articles and page <= MAX_PAGES_PER_HUB:
        if page == 1:
            page_url = urljoin(hub_url, 'articles/')
        else:
            page_url = urljoin(hub_url, f'articles/page{page}/')

        response = safe_request(session, page_url)
        if not response or response.status_code != 200:
            break

        soup = BeautifulSoup(response.content, 'lxml')
        new_links = []

        for article in soup.select('article.tm-articles-list__item:not(.tm-articles-list__item_sponsored)'):
            link_tag = article.select_one('a.tm-title__link')
            if link_tag:
                full_url = urljoin('https://habr.com', link_tag.get('href', '').split('?')[0])
                new_links.append(full_url)

        if not new_links:
            break

        articles.extend(new_links)
        logging.info(f"Хаб: {hub_url.split('/')[-2]} | Страница {page}: +{len(new_links)} статей")
        page += 1
        time.sleep(REQUEST_DELAY + random.uniform(0, 0.3))

    return articles[:max_articles]


def parse_article(url, category, session):
    """
    Получает содержимое статьи и возвращает словарь с данными.
    """
    response = safe_request(session, url)
    if not response:
        return None

    soup = BeautifulSoup(response.content, 'lxml')
    title_tag = soup.find('h1', class_='tm-title')
    title = title_tag.get_text(strip=True) if title_tag else 'Без названия'
    tags = [tag.get_text(strip=True) for tag in soup.select('a.tm-tags-list__link')]
    body = soup.find('div', class_='tm-article-body')
    text = ' '.join(p.get_text(strip=True) for p in body.find_all(['p', 'li', 'h2', 'h3'])) if body else ''

    return {
        "article_id": url,
        "title": title,
        "category": category,
        "tags": ", ".join(tags),
        "text": text
    }


def main():
    output_file = os.path.join('data', 'habr_articles.json')
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Загрузка существующих данных
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
            processed_urls = {item['article_id'] for item in existing_data}
            category_counts = defaultdict(int)
            for item in existing_data:
                category_counts[item['category']] += 1
        logging.info(f"Загружено существующих статей: {len(existing_data)}")
    except (FileNotFoundError, json.JSONDecodeError):
        existing_data = []
        processed_urls = set()
        category_counts = defaultdict(int)

    with requests.Session() as session:
        session.headers.update(HEADERS)

        # Получение хабов
        categorized_hubs = get_hub_urls(session)
        if not categorized_hubs:
            logging.error("Не удалось получить список хабов")
            return

        # Используем пул потоков для параллельного парсинга статей
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Сбор статей по категориям
            for category, hubs in categorized_hubs.items():
                needed = TARGET_ARTICLES_PER_CATEGORY - category_counts[category]
                if needed <= 0:
                    logging.info(f"Категория {category} уже заполнена")
                    continue

                logging.info(f"\n{'='*40}\nСбор категории: {category} (осталось: {needed})\n{'='*40}")
                collected = 0

                for hub_url in hubs:
                    if collected >= needed:
                        break

                    try:
                        # Ограничиваем количество статей, получаемых из одного хаба
                        articles = get_articles_from_hub(hub_url, min(needed - collected, ARTICLES_PER_HUB), session)
                        new_articles = [url for url in articles if url not in processed_urls]
                        if not new_articles:
                            continue

                        # Параллельный парсинг статей
                        futures = {
                            executor.submit(parse_article, url, category, session): url
                            for url in new_articles
                        }
                        for future in as_completed(futures):
                            article_data = future.result()
                            if article_data:
                                existing_data.append(article_data)
                                processed_urls.add(article_data["article_id"])
                                collected += 1
                                category_counts[category] += 1
                                logging.info(f"[{category}] Собрано: {collected}/{needed} | {article_data['title'][:50]}...")
                                # Сохраняем каждые 10 статей
                                if collected % 10 == 0:
                                    with open(output_file, 'w', encoding='utf-8') as f:
                                        json.dump(existing_data, f, ensure_ascii=False, indent=2)

                            # Добавляем небольшую задержку между задачами
                            time.sleep(REQUEST_DELAY * 0.1)
                            if collected >= needed:
                                break

                    except Exception as e:
                        logging.error(f"Ошибка в хабе {hub_url}: {e}")

        # Финальное сохранение
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, ensure_ascii=False, indent=2)

    logging.info(f"\n{'='*40}\nИтоговый отчет:")
    for cat, count in category_counts.items():
        logging.info(f"{cat}: {count} статей")
    logging.info(f"Всего собрано: {len(existing_data)} статей")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Прерывание пользователем. Сохранение данных...")
        exit(0)
