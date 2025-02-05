import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

# ========== Конфигурационные переменные ==========
TARGET_LINKS = 10
CHUNK_SIZE_LINKS = 5
CHUNK_SIZE_ARTICLES = 5
LINKS_OUTPUT_FILE = "data/ria_links.json"
ARTICLES_OUTPUT_FILE = "data/ria_articles.json"
# ===================================================

# Полный список категорий
CATEGORIES = [
    {"url": "https://ria.ru/politics", "category": "politics"},
    # {"url": "https://ria.ru/world", "category": "world"},
    # {"url": "https://ria.ru/economy", "category": "economy"},
    # {"url": "https://ria.ru/society", "category": "society"},
    # {"url": "https://ria.ru/incidents", "category": "incidents"},
    # {"url": "https://ria.ru/defense_safety", "category": "defense_safety"},
    # {"url": "https://ria.ru/science", "category": "science"},
    # {"url": "https://ria.ru/culture", "category": "culture"},
    # # {"url": "https://ria.ru/tourism", "category": "tourism"},
    # {"url": "https://ria.ru/religion", "category": "religion"}
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
            logger.info("Для категории '%s' собрано ссылок: %d", category_name, len(links_data))
        except Exception as e:
            logger.error("Ошибка при извлечении ссылок: %s", e)
        # Сохранение в реальном времени
        current_links = [{"url": url, "category": category_name} for url in links_data]
        save_links(current_links)
        if len(links_data) >= min_links:
            break
        try:
            more_button = driver.find_element(By.CSS_SELECTOR, "div.list-more.color-btn-second-hover")
            driver.execute_script("arguments[0].click();", more_button)
            time.sleep(2)
        except Exception as e:
            logger.info("Кнопка 'Еще 20 материалов' не найдена для категории '%s'. Error: %s", category_name, e)
            break
    return [{"url": url, "category": category_name} for url in links_data]

def parse_article(article_url, category):
    try:
        response = requests.get(article_url, headers={'User-Agent': 'Mozilla/5.0'})
        if response.status_code != 200:
            logger.error("Ошибка запроса %s: код %d", article_url, response.status_code)
            return None
        soup = BeautifulSoup(response.content, "html.parser")
    except Exception as e:
        logger.error("Ошибка запроса %s: %s", article_url, e)
        return None

    try:
        # Попытка извлечь заголовок по селектору "h1.article__title"
        title_element = soup.select_one("h1.article__title")
        # Если не найден, fallback к первому тегу <h1>
        if not title_element:
            title_element = soup.find("h1")
        title = title_element.get_text(strip=True) if title_element else ""
    except Exception as e:
        logger.error("Ошибка при извлечении заголовка для %s: %s", article_url, e)
        return None

    try:
        text_element = soup.select_one("div.article__text")
        text = text_element.get_text(strip=True) if text_element else ""
    except Exception as e:
        logger.error("Ошибка при извлечении текста для %s: %s", article_url, e)
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
    # Сбор ссылок для всех категорий
    for cat in CATEGORIES:
        logger.info("Начало сбора ссылок для категории: %s", cat["category"])
        cat_links = collect_links_for_category(cat["url"], cat["category"], min_links=TARGET_LINKS)
        logger.info("Для категории %s собрано %d ссылок", cat["category"], len(cat_links))
        collected_links.extend(cat_links)
        save_links(collected_links)
    
    logger.info("Начало парсинга статей. Всего ссылок: %d", len(collected_links))
    for idx, item in enumerate(collected_links, start=1):
        url = item.get("url")
        category = item.get("category")
        article_data = parse_article(url, category)
        if article_data:
            collected_articles.append(article_data)
            if idx % CHUNK_SIZE_ARTICLES == 0:
                save_articles(collected_articles)
        time.sleep(0.5)
    save_articles(collected_articles)

if __name__ == "__main__":
    try:
        main()
    finally:
        logger.info("Закрытие драйвера")
        driver.quit()
