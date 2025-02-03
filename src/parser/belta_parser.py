import json
import logging
import random
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry

BASE_URL = "https://www.belta.by/"
CATEGORIES = [
    "economics/",
    "tech/",
    "incident/", 
    "regions/",
    "politics/",
    "president/",
    "society/",
    "kaleidoscope/",
    "events/",
    "sport/",
    "culture/",
    "world/",
]
MAX_PAGES = 100
TARGET_COUNT = 1000
OUTPUT_FILE = Path("data/belta_articles.json")
SAVE_BATCH = 100

HEADERS = {
    "Accept": "*/*",
    "User-Agent": "Mozilla/5.0 (compatible; MSIE 5.0; Windows 98; Trident/3.1)"
}

# Настройка логирования
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),
    ]
)

# ==================== Функции ====================

def create_session() -> requests.Session:
    """
    Создает сессию requests с настройками повторных попыток (retry).
    """
    session = requests.Session()
    retries = Retry(
        total=3,                # Общее число повторов
        backoff_factor=1,       # Задержка между повторными попытками: 1, 2, 4 сек.
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_page(session: requests.Session, url: str) -> str:
    """
    Получает HTML-страницу по URL с обработкой ошибок.
    """
    try:
        logging.debug(f"Запрос: {url}")
        response = session.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе {url}: {e}")
        return ""


def parse_article(session: requests.Session, article_path: str) -> tuple[str, str]:
    """
    Парсит страницу отдельной статьи.
    Возвращает кортеж (текст статьи, теги).
    """
    article_url = BASE_URL + article_path.lstrip("/")
    html = fetch_page(session, article_url)
    if not html:
        return "None", "None"

    soup = BeautifulSoup(html, "lxml")

    # Извлечение основного текста статьи
    text_block = soup.find(class_="js-mediator-article")
    article_text = text_block.get_text(strip=True) if text_block else "None"

    # Извлечение тегов статьи
    tags_block = soup.find(class_="news_tags_block")
    tags = []
    if tags_block:
        for tag in tags_block.find_all("a"):
            title = tag.get("title")
            if title:
                tags.append(title)
    tags_str = ",".join(tags) if tags else "None"

    logging.debug(f"Статья: {article_url} | Теги: {tags_str[:50]}...")
    return article_text, tags_str


def save_data(catalog: list, output_file: Path):
    """
    Сохраняет данные в JSON-файл.
    Итоговая структура файла — массив объектов.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=4)
    logging.info(f"Данные сохранены в {output_file} (всего статей: {len(catalog)})")


def parse_category(session: requests.Session, category: str, catalog: list) -> int:
    """
    Парсит статьи для заданной категории и добавляет их в общий список catalog.
    Процесс останавливается, как только накопится TARGET_COUNT статей для этой категории.
    
    Возвращает количество статей, спарсенных для категории.
    """
    cat_count = 0
    for page in range(MAX_PAGES):
        # Если уже достигнуто целевое количество, прекращаем парсинг этой категории.
        if cat_count >= TARGET_COUNT:
            logging.info(f"Достигнуто {cat_count} статей для категории '{category.rstrip('/')}' (цель: {TARGET_COUNT}).")
            break

        # Формирование URL: первая страница без "page/", далее с указанием номера страницы.
        page_url = BASE_URL + category if page == 0 else BASE_URL + category + "page/" + str(page)
        logging.info(f"Парсинг категории '{category}', страница {page}: {page_url}")

        html = fetch_page(session, page_url)
        if not html:
            logging.warning(f"Пустой ответ для {page_url}. Пропускаем страницу.")
            continue

        soup = BeautifulSoup(html, "lxml")
        news_items = soup.find_all(class_="news_item")
        if not news_items:
            logging.info(f"Нет новостей на странице {page_url}. Возможно, достигнут конец раздела.")
            break  # Если новостей нет, прекращаем обработку страниц

        links, titles = [], []
        # Извлечение ссылок и заголовков
        for item in news_items:
            for tag in item.find_all("a", href=True, title=True):
                links.append(tag.get("href"))
                titles.append(tag.get("title"))

        if not links:
            logging.info(f"Не найдены ссылки на новости на странице {page_url}")
            continue

        # Обработка каждой найденной статьи
        for link, title in zip(links, titles):
            # Если уже достигнуто целевое количество, прерываем обработку
            if cat_count >= TARGET_COUNT:
                break

            article_text, article_tags = parse_article(session, link)
            article_data = {
                "article_id": link,
                "title": title,
                "category": category.rstrip("/"),
                "tags": article_tags,
                "text": article_text
            }
            catalog.append(article_data)
            cat_count += 1
            logging.debug(f"Добавлена статья: {title} ({cat_count} из {TARGET_COUNT})")

            # Сохранение данных каждые SAVE_BATCH статей
            if len(catalog) % SAVE_BATCH == 0:
                save_data(catalog, OUTPUT_FILE)

            # Небольшая случайная задержка, чтобы не нагружать сервер
            time.sleep(random.uniform(0.5, 1.5))

        logging.info(f"Страница {page} категории '{category.rstrip('/')}' обработана, спарсено {cat_count} из {TARGET_COUNT} статей.")
        # Если на странице новостей меньше ожидаемого — возможно, достигнут конец раздела.
        # В этом случае завершаем парсинг категории, даже если целевое количество не достигнуто.
    logging.info(f"Для категории '{category.rstrip('/')}' спарсено {cat_count} из {TARGET_COUNT} статей.")
    return cat_count


def main():
    logging.info("Начало парсинга.")
    session = create_session()
    catalog = []  # Итоговый список объектов-статей
    category_counts = {}  # Подсчет статей по категориям

    try:
        for category in CATEGORIES:
            logging.info(f"Обработка категории: {category.rstrip('/')}")
            cat_count = parse_category(session, category, catalog)
            category_counts[category.rstrip("/")] = cat_count
    except KeyboardInterrupt:
        logging.info("Парсинг прерван пользователем (Ctrl+C).")
    finally:
        # Сохраняем накопленные данные
        save_data(catalog, OUTPUT_FILE)
        total_articles = len(catalog)
        logging.info("Парсинг завершён.")
        logging.info(f"Общее количество статей: {total_articles}")

        # Проверка минимальных требований
        if total_articles < 4000:
            logging.error(f"Общее количество статей недостаточно: {total_articles} < 4000")
        for cat, count in category_counts.items():
            logging.info(f"Категория '{cat}': {count} из {TARGET_COUNT} статей спарсено.")
            if count < 950:
                logging.error(f"Количество статей для категории '{cat}' вне допустимого диапазона: {count} (ожидалось 1000±50)")
        if total_articles >= 4000 and all(950 <= count for count in category_counts.values()):
            logging.info("Требования по количеству статей выполнены.")

if __name__ == "__main__":
    main()
