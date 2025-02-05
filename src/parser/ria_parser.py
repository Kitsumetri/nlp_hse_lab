import json
import time
import logging
import os
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import concurrent.futures

# ========== Конфигурационные переменные ==========
TARGET_LINKS = 1000
CHUNK_SIZE_LINKS = 50
CHUNK_SIZE_ARTICLES = 50
LINKS_OUTPUT_FILE = "data/ria_links.json"
ARTICLES_OUTPUT_FILE = "data/ria_articles.json"
# ===================================================

# Полный список категорий
CATEGORIES = [
    {"url": "https://ria.ru/politics", "category": "politics"},
    {"url": "https://ria.ru/world", "category": "world"},
    {"url": "https://ria.ru/economy", "category": "economy"},
    {"url": "https://ria.ru/society", "category": "society"},
    {"url": "https://ria.ru/incidents", "category": "incidents"},
    {"url": "https://ria.ru/defense_safety", "category": "defense_safety"},
    {"url": "https://ria.ru/science", "category": "science"},
    {"url": "https://ria.ru/culture", "category": "culture"},
    # {"url": "https://ria.ru/tourism", "category": "tourism"},
    {"url": "https://ria.ru/religion", "category": "religion"}
]

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")
chrome_options.add_argument("--headless=new")

driver = webdriver.Chrome(options=chrome_options)
wait = WebDriverWait(driver, 20)

def scroll_page():
    logger.debug("Прокрутка страницы вниз.")
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(1)

def save_links(links):
    with open(LINKS_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(links, f, ensure_ascii=False, indent=4)
    logger.info("Обновлено ссылок: %d в %s", len(links), LINKS_OUTPUT_FILE)

def save_articles(articles):
    with open(ARTICLES_OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)
    logger.info("Обновлено статей: %d в %s", len(articles), ARTICLES_OUTPUT_FILE)

def collect_links_for_category(category_url, category_name, min_links=1000):
    logger.debug("Начало сбора ссылок для категории '%s' по URL: %s", category_name, category_url)
    links_data = set()
    driver.get(category_url)
    time.sleep(1)
    while True:
        scroll_page()
        try:
            article_elements = driver.find_elements(By.CSS_SELECTOR, "a.list-item__title")
            for elem in article_elements:
                url = elem.get_attribute("href")
                if url:
                    links_data.add(url)
            logger.debug("Для категории '%s' собрано ссылок: %d", category_name, len(links_data))
        except Exception as e:
            logger.error("Ошибка при извлечении ссылок для категории '%s': %s", category_name, e)
        current_links = [{"url": url, "category": category_name} for url in links_data]
        save_links(current_links)
        if len(links_data) >= min_links:
            logger.info("Достигнут лимит ссылок для категории '%s': %d", category_name, len(links_data))
            break
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, "div.list-more.color-btn-second-hover")
            logger.debug("Нажатие кнопки 'Еще 20 материалов' для категории '%s'.", category_name)
            driver.execute_script("arguments[0].click();", more_button)
            time.sleep(2)
        except Exception as e:
            logger.info("Кнопка 'Еще 20 материалов' не найдена для категории '%s'. Error: %s", category_name, e)
            break
    return [{"url": url, "category": category_name} for url in links_data]

def get_with_backoff(url, headers, max_retries=5, backoff_factor=1):
    """Выполняет запрос с экспоненциальным бэкоффом при получении кода 429."""
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers)
            if response.status_code != 429:
                return response
            wait_time = backoff_factor * (4 ** retries)
            logger.warning("Ошибка 429 для %s. Повтор через %d секунд...", url, wait_time)
            time.sleep(wait_time)
            retries += 1
        except Exception as e:
            logger.error("Исключение при запросе %s: %s", url, e)
            return None
    return response

def extract_full_text(soup):
    # Попытка найти контейнер с основным текстом
    container = soup.find("div", class_="article__body")
    if container:
        text_parts = []
        for tag in container.find_all(["div", "p"]):
            classes = tag.get("class", [])
            # Собираем, если элемент имеет один из нужных классов или является простым абзацем
            if ("article__text" in classes or "article__quote" in classes or tag.name == "p") and tag.get_text(strip=True):
                text_parts.append(tag.get_text(" ", strip=True))
        if text_parts:
            return "\n".join(text_parts)
    # Если контейнер не найден – fallback: собрать все блоки после заголовка
    header = soup.select_one("h1.article__title")
    if not header:
        header = soup.find("h1")
    text_parts = []
    for tag in header.find_all_next():
        # Если встретили специальный якорный элемент (например, с классом "article__anchor"), завершаем сбор
        if tag.name == "a" and tag.get("class") and "article__anchor" in tag.get("class"):
            break
        classes = tag.get("class", [])
        if ("article__text" in classes or "article__quote" in classes or tag.name == "p") and tag.get_text(strip=True):
            text_parts.append(tag.get_text(" ", strip=True))
    return "\n".join(text_parts)

def parse_article(article_url, category):
    logger.debug("Парсинг статьи: %s", article_url)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    try:
        response = get_with_backoff(article_url, headers=headers, max_retries=7, backoff_factor=2)
        if not response or response.status_code != 200:
            logger.error("Ошибка запроса %s: код %s", article_url, response.status_code if response else "None")
            return None
        soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        logger.error("Ошибка запроса %s: %s", article_url, e)
        return None

    try:
        title_element = soup.select_one("h1.article__title")
        if not title_element:
            logger.debug("Элемент 'h1.article__title' не найден, используем fallback поиск по <h1> для %s", article_url)
            title_element = soup.find("h1")
        title = title_element.get_text(strip=True) if title_element else ""
    except Exception as e:
        logger.error("Ошибка при извлечении заголовка для %s: %s", article_url, e)
        return None

    try:
        text = extract_full_text(soup)
    except Exception as e:
        logger.error("Ошибка при извлечении полного текста для %s: %s", article_url, e)
        text = ""

    try:
        tags_elements = soup.select("div.article__tags a")
        tags = ", ".join([tag.get_text(strip=True) for tag in tags_elements]) if tags_elements else ""
    except Exception as e:
        logger.error("Ошибка при извлечении тегов для %s: %s", article_url, e)
        tags = ""

    article_data = {
        "article_id": article_url,
        "title": title,
        "category": category,
        "tags": tags,
        "text": text
    }
    logger.debug("Извлечены данные статьи: %s", article_data)
    return article_data


def main():
    collected_links = []
    collected_articles = []

    # 1) Работа с файлом ria_links.json
    if os.path.exists(LINKS_OUTPUT_FILE) and os.path.getsize(LINKS_OUTPUT_FILE) > 0:
        with open(LINKS_OUTPUT_FILE, "r", encoding="utf-8") as f:
            collected_links = json.load(f)
        logger.info("Считано %d ссылок из %s", len(collected_links), LINKS_OUTPUT_FILE)
    else:
        for cat in CATEGORIES:
            logger.info("Начало сбора ссылок для категории: %s", cat["category"])
            cat_links = collect_links_for_category(cat["url"], cat["category"], min_links=TARGET_LINKS)
            logger.info("Для категории '%s' собрано %d ссылок", cat["category"], len(cat_links))
            collected_links.extend(cat_links)
            save_links(collected_links)

    # 2) Работа с файлом ria_articles.json
    if os.path.exists(ARTICLES_OUTPUT_FILE) and os.path.getsize(ARTICLES_OUTPUT_FILE) > 0:
        with open(ARTICLES_OUTPUT_FILE, "r", encoding="utf-8") as f:
            collected_articles = json.load(f)
        logger.info("Считано %d статей из %s", len(collected_articles), ARTICLES_OUTPUT_FILE)
    else:
        collected_articles = []

    # Определяем, какие ссылки ещё не спарсены (по article_id == url)
    existing_article_ids = {article["article_id"] for article in collected_articles}
    missing_links = [link for link in collected_links if link["url"] not in existing_article_ids]
    logger.info("Будет спаршено %d новых статей из %d ссылок", len(missing_links), len(collected_links))

    # Параллельный сбор статей для ускорения
    def worker(link_item):
        url = link_item.get("url")
        category = link_item.get("category")
        logging.info(f"Начинаем парсинг статьи {missing_links.index(link_item)+1}/{len(missing_links)}")
        return parse_article(url, category)

    results = []
    hard_chunk_count = len(missing_links) // CHUNK_SIZE_ARTICLES
    for idx in range(hard_chunk_count):
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results += list(executor.map(worker, missing_links[idx*CHUNK_SIZE_ARTICLES:(idx+1)*CHUNK_SIZE_ARTICLES]))
            save_articles(collected_articles + results)
            logging.info(f"Сохраняем {len(results)}/{len(missing_links)} статей.")
    if hard_chunk_count != len(missing_links) / CHUNK_SIZE_ARTICLES:
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results += list(executor.map(worker, missing_links[hard_chunk_count*CHUNK_SIZE_ARTICLES:-1]))
            save_articles(collected_articles + results)
            logging.info(f"Сохраняем {len(results)}/{len(missing_links)} статей.")

    new_articles = [article for article in results if article is not None]
    logger.info("Спаршено %d новых статей", len(new_articles))
    collected_articles.extend(new_articles)
    save_articles(collected_articles)

if __name__ == "__main__":
    try:
        main()
    finally:
        logger.info("Закрытие драйвера")
        driver.quit()
