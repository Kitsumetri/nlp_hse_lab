import requests
from bs4 import BeautifulSoup
import json
import re
import concurrent.futures
import os
import logging
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)

def extract_text(soup):
    selectors = [
        ("div", re.compile("zn-body__paragraph")),
        ("div", re.compile("l-container")),
        ("article", None),
        ("div", re.compile("Article__body"))
    ]
    for tag, cls in selectors:
        if cls:
            containers = soup.find_all(tag, class_=cls)
            for container in containers:
                paragraphs = container.find_all("p")
                text = "\n".join(p.get_text(strip=True) for p in paragraphs)
                if text.strip():
                    return text.strip()
        else:
            container = soup.find(tag)
            if container:
                paragraphs = container.find_all("p")
                text = "\n".join(p.get_text(strip=True) for p in paragraphs)
                if text.strip():
                    return text.strip()
    paragraphs = soup.find_all("p")
    return "\n".join(p.get_text(strip=True) for p in paragraphs).strip()


def parse_article(url, forced_category=None):
    logging.debug(f"Парсинг статьи: {url}")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            logging.error(f"Ошибка запроса {url}: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Исключение при запросе {url}: {e}")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    og_title = soup.find("meta", property="og:title")
    title = (og_title.get("content") if og_title and og_title.get("content")
             else (soup.find("h1").get_text(strip=True) if soup.find("h1") else None))
    if not title:
        logging.error(f"Заголовок не найден для {url}")
        return None

    # Извлечение тегов статьи
    tags = ""
    meta_keywords = soup.find("meta", attrs={"name": "news_keywords"})
    if meta_keywords and meta_keywords.get("content"):
        tags = meta_keywords.get("content").strip()
    else:
        tag_elements = soup.find_all("meta", property="article:tag")
        if tag_elements:
            tags_list = [elem.get("content").strip() for elem in tag_elements if elem.get("content")]
            tags = ", ".join(tags_list)
        else:
            meta_keywords = soup.find("meta", attrs={"name": "keywords"})
            if meta_keywords and meta_keywords.get("content"):
                tags = meta_keywords.get("content").strip()

    article_text = extract_text(soup)
    if not article_text or len(article_text) < 50:
        logging.error(f"Текст не найден или слишком короткий для {url}")
        return None

    # Если принудительно задана категория, используем её
    if forced_category:
        category = forced_category
    else:
        # Логика извлечения категории (meta-тег или по URL)
        category = ""
        meta_section = soup.find("meta", property="article:section")
        if not meta_section:
            meta_section = soup.find("meta", attrs={"name": "section"})
        if meta_section and meta_section.get("content"):
            category = meta_section.get("content").strip().lower().replace(" ", "_")
        else:
            path = urlparse(url).path
            parts = [part for part in path.split("/") if part]
            if len(parts) >= 4:
                category = parts[3].lower()
            else:
                category = "uncategorized"
    logging.debug(f"Категория определена как: {category} для {url}")

    logging.info(f"Успешно спарсена статья: {title}")
    return {
        "article_id": url,
        "title": title,
        "category": category,
        "tags": tags,
        "text": article_text
    }


def parse_category_page(category_url):
    logging.debug(f"Парсинг страницы категории: {category_url}")
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(category_url, headers=headers, timeout=10)
        if response.status_code != 200:
            logging.error(f"Ошибка запроса {category_url}: {response.status_code}")
            return [], None
    except Exception as e:
        logging.error(f"Исключение при запросе {category_url}: {e}")
        return [], None

    soup = BeautifulSoup(response.content, 'html.parser')
    article_links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r'/202\d/', href):
            if href.startswith("/"):
                href = "https://edition.cnn.com" + href
            article_links.add(href)
    logging.debug(f"Найдено {len(article_links)} ссылок на статьи на странице {category_url}")

    next_page_url = None
    next_page = soup.find("a", string=re.compile("Next"))
    if next_page and next_page.get("href"):
        next_link = next_page["href"]
        if next_link.startswith("/"):
            next_link = "https://edition.cnn.com" + next_link
        next_page_url = next_link
        logging.debug(f"Найдена ссылка на следующую страницу: {next_page_url}")
    else:
        logging.debug("Ссылка на следующую страницу не найдена.")
    return list(article_links), next_page_url


def get_articles_from_category(category_url, forced_category, target_count=1000):
    logging.info(f"Начало сбора статей из: {category_url} (цель: {target_count})")
    articles = []
    visited_urls = set()
    next_page_url = category_url
    try:
        while len(articles) < target_count and next_page_url:
            logging.debug(f"Обработка страницы: {next_page_url}")
            article_urls, next_page_url = parse_category_page(next_page_url)
            new_urls = [url for url in article_urls if url not in visited_urls]
            visited_urls.update(new_urls)
            logging.debug(f"Новых ссылок: {len(new_urls)}")
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                # Передаем forced_category в parse_article
                futures = {executor.submit(parse_article, url, forced_category): url for url in new_urls}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        article = future.result()
                    except KeyboardInterrupt:
                        logging.info("Получен KeyboardInterrupt в потоках, прерывание...")
                        executor.shutdown(wait=False, cancel_futures=True)
                        raise
                    if article:
                        articles.append(article)
                        logging.info(f"Статья добавлена: {article['title']}")
                    if len(articles) >= target_count:
                        break
            logging.info(f"Собрано {len(articles)} статей из {category_url}")
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt в get_articles_from_category")
        raise
    return articles


def save_articles(articles, filename="data/cnn_articles.json"):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)
    logging.info(f"Сохранено {len(articles)} статей в файл {filename}")


def main():
    categories = {
        "world": "https://edition.cnn.com/world",
        "politics": "https://edition.cnn.com/politics",
        "business": "https://edition.cnn.com/business",
        "health": "https://edition.cnn.com/health",
        "entertainment": "https://edition.cnn.com/entertainment",
        "travel": "https://edition.cnn.com/travel",
        "opinion": "https://edition.cnn.com/opinion",
        "sports": "https://edition.cnn.com/sport",
        "science": "https://edition.cnn.com/science",
        "style": "https://edition.cnn.com/style",
        "living": "https://edition.cnn.com/living"
    }
    all_articles = []
    target_per_category = 1000
    save_interval = 100
    try:
        for cat_name, cat_url in categories.items():
            logging.info(f"Запуск сбора статей для категории: {cat_name}")
            # Передаём cat_name как принудительную категорию
            articles = get_articles_from_category(cat_url, cat_name, target_count=target_per_category)
            all_articles.extend(articles)
            if len(all_articles) >= save_interval:
                save_articles(all_articles)
        save_articles(all_articles)
    except KeyboardInterrupt:
        logging.info("Получен KeyboardInterrupt. Сохранение текущих результатов...")
        save_articles(all_articles)
        logging.info("Выход из приложения по Ctrl+C")
    except Exception as e:
        logging.error(f"Неожиданная ошибка: {e}")
        save_articles(all_articles)
        logging.info("Выход из приложения из-за ошибки.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Принудительный выход по Ctrl+C")
