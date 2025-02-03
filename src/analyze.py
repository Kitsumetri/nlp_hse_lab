import os
import json
import random
import string
from collections import Counter

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from faker import Faker

# Импорт spaCy и NLTK (для стоп-слов, если потребуется)
import spacy
import nltk

# Если стоп-слова NLTK понадобятся, раскомментируйте следующую строку:
# nltk.download("stopwords")

# Задаём стиль для графиков
sns.set(style="whitegrid", font_scale=1.1)


# ======================================
# 1. Генерация JSON-датасета с 4000+ записями
# ======================================
def generate_news_dataset(filename="news_dataset.json"):
    """
    Генерирует JSON-файл с новостными статьями в формате:
    {
        "article_id": "https://www.bbc.com/news/<category>-<uuid>",
        "title": "<заголовок>",
        "category": "<категория>",
        "tags": "tag1,tag2,...",
        "text": "<текст статьи>"
    }
    
    Для каждой статьи с некоторой вероятностью вставляются аномалии:
      - очень короткий или очень длинный текст,
      - добавление лишних символов,
      - пустое поле тегов.
    """
    fake = Faker('en_US')
    Faker.seed(42)
    random.seed(42)

    # Задаём категории и число записей для каждой
    categories_counts = {
        "science_and_environment": 1000,
        "technology": 1500,
        "politics": 800,
        "health": 700
    }

    # Возможные теги для каждой категории
    tags_dict = {
        "science_and_environment": ["Astronomy", "Space", "Climate", "Biology", "Environment", "Physics"],
        "technology": ["AI", "Gadgets", "Software", "Hardware", "Innovation", "Cybersecurity"],
        "politics": ["Elections", "Policy", "Government", "Diplomacy", "Debate", "Congress"],
        "health": ["Medicine", "Outbreak", "Nutrition", "Wellness", "Research", "Fitness"]
    }

    data = []

    for category, count in categories_counts.items():
        for _ in range(count):
            article_id = f"https://www.bbc.com/news/{category}-{fake.uuid4()}"
            title = fake.sentence(nb_words=random.randint(5, 12))
            normal_text = fake.text(max_nb_chars=random.randint(150, 300))

            # Вносим аномалии:
            # 5% — очень короткий текст
            if random.random() < 0.05:
                text = fake.word()
            # 5% — очень длинный текст (повтор обычного текста 5-7 раз)
            elif random.random() < 0.05:
                repeat_times = random.randint(5, 7)
                text = " ".join([normal_text] * repeat_times)
            else:
                text = normal_text

            # 5% — вставка лишних символов/цифр в текст
            if random.random() < 0.05:
                anomaly_chars = ''.join(random.choices(string.punctuation + string.digits, k=10))
                insert_pos = random.randint(0, len(text))
                text = text[:insert_pos] + anomaly_chars + text[insert_pos:]

            # Генерация тегов: 2-4 случайных тега
            if random.random() < 0.03:
                tags = ""  # аномалия: пустые теги
            else:
                tags_list = random.sample(tags_dict[category], k=random.randint(2, 4))
                tags = ",".join(tags_list)

            record = {
                "article_id": article_id,
                "title": title,
                "category": category,
                "tags": tags,
                "text": text
            }
            data.append(record)

    random.shuffle(data)
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Сгенерировано {len(data)} записей и сохранено в {filename}")


# ======================================
# 2. Базовый анализ данных
# ======================================
def basic_text_analysis(df):
    """
    Выводит базовую статистику:
      - общее число статей,
      - распределение длины текстов и заголовков,
      - количество статей по категориям.
    Строит соответствующие графики.
    """
    df["text_length"] = df["text"].apply(lambda x: len(x.split()))
    df["title_length"] = df["title"].apply(lambda x: len(x.split()))

    print("Общее количество статей:", len(df))
    print("\nСтатистика по длине текстов (в словах):")
    print(df["text_length"].describe())
    print("\nСтатистика по длине заголовков (в словах):")
    print(df["title_length"].describe())

    # График распределения статей по категориям
    plt.figure(figsize=(8, 6))
    sns.countplot(data=df, x="category", order=sorted(df["category"].unique()))
    plt.title("Количество статей по категориям")
    plt.xlabel("Категория")
    plt.ylabel("Количество статей")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("articles_by_category.png")
    plt.show()

    # Гистограмма длины текстов
    plt.figure(figsize=(8, 6))
    sns.histplot(df["text_length"], bins=20, kde=True)
    plt.title("Распределение длины текстов (в словах)")
    plt.xlabel("Количество слов")
    plt.ylabel("Частота")
    plt.tight_layout()
    plt.savefig("text_length_distribution.png")
    plt.show()

    # Boxplot длины текстов по категориям
    plt.figure(figsize=(8, 6))
    sns.boxplot(data=df, x="category", y="text_length", order=sorted(df["category"].unique()))
    plt.title("Длина текстов по категориям")
    plt.xlabel("Категория")
    plt.ylabel("Количество слов")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("text_length_by_category.png")
    plt.show()


# ======================================
# 3. Предобработка текстов с использованием spaCy
# ======================================
def preprocess_texts(text_series, nlp):
    """
    Выполняет предобработку текстов:
      - токенизация,
      - лемматизация,
      - удаление стоп-слов, пунктуации и пробельных символов.
    Возвращает серию обработанных текстов.
    """
    processed_texts = []
    for doc in nlp.pipe(text_series, batch_size=50, n_process=1):
        tokens = []
        for token in doc:
            if token.is_stop or token.is_punct or token.is_space:
                continue
            tokens.append(token.lemma_.lower())
        processed_texts.append(" ".join(tokens))
    return processed_texts


# ======================================
# 4. Анализ ключевых слов по категориям
# ======================================
def analyze_keywords_by_category(df, processed_texts):
    """
    Для каждой категории:
      - объединяет все обработанные тексты,
      - вычисляет общее число слов и число уникальных слов,
      - рассчитывает коэффициент разнообразия ключевых слов (уникальные/все),
      - определяет топ-10 наиболее часто встречающихся слов,
      - строит график для топ-10 слов.
    Также строится сравнительный график коэффициента разнообразия для всех категорий.
    """
    df = df.copy()
    df["processed_text"] = processed_texts

    diversity_data = {}
    categories = sorted(df["category"].unique())

    for cat in categories:
        texts_cat = " ".join(df[df["category"] == cat]["processed_text"].tolist())
        words = texts_cat.split()
        total_words = len(words)
        unique_words = len(set(words))
        diversity_ratio = unique_words / total_words if total_words > 0 else 0
        diversity_data[cat] = diversity_ratio

        counter = Counter(words)
        top10 = counter.most_common(10)

        print(f"\nКатегория: {cat}")
        print(f"Общее число слов: {total_words}")
        print(f"Число уникальных слов: {unique_words}")
        print(f"Коэффициент разнообразия (уникальные/все): {diversity_ratio:.3f}")
        print("Топ-10 ключевых слов:")
        for word, freq in top10:
            print(f"  {word}: {freq}")

        # График топ-10 слов для категории
        if top10:
            words_list, freq_list = zip(*top10)
        else:
            words_list, freq_list = ([], [])
        plt.figure(figsize=(8, 4))
        sns.barplot(x=list(words_list), y=list(freq_list), palette="magma")
        plt.title(f"Топ-10 ключевых слов для категории: {cat}")
        plt.xlabel("Ключевое слово")
        plt.ylabel("Частота")
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(f"top_keywords_{cat}.png")
        plt.show()

    # Сравнительный график коэффициента разнообразия по категориям
    plt.figure(figsize=(8, 4))
    ratios = [diversity_data[cat] for cat in categories]
    sns.barplot(x=categories, y=ratios, palette="viridis")
    plt.title("Коэффициент разнообразия ключевых слов по категориям")
    plt.xlabel("Категория")
    plt.ylabel("Доля уникальных слов")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("keyword_diversity_ratio.png")
    plt.show()


# ======================================
# Основная функция
# ======================================
def main():
    dataset_filename = os.path.join("data", "news_dataset.json")
    
    # Если датасет не существует – генерируем его
    if not os.path.exists(dataset_filename):
        generate_news_dataset(filename=dataset_filename)
    else:
        print(f"Файл {dataset_filename} уже существует. Пропускаем генерацию.")

    # Загружаем данные из JSON в DataFrame
    with open(dataset_filename, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    
    # Базовый анализ данных
    basic_text_analysis(df)

    # Загрузка модели spaCy (en_core_web_sm)
    try:
        nlp = spacy.load("en_core_web_sm")
    except Exception as e:
        print("Ошибка загрузки модели spaCy. Убедитесь, что en_core_web_sm установлен.")
        return

    print("\nПредобработка текстов (токенизация, лемматизация, удаление стоп-слов и пунктуации)...")
    processed_texts = preprocess_texts(df["text"], nlp)

    # Анализ ключевых слов по категориям
    analyze_keywords_by_category(df, processed_texts)

    # Возможные выводы (пример):
    conclusions = """
    Возможные выводы из анализа:
    1. Распределение статей по категориям может быть неравномерным, что следует учитывать при дальнейшем анализе.
    2. Статистика длины текстов и заголовков помогает выявить аномалии (очень короткие или длинные статьи).
    3. Предобработка (токенизация, лемматизация, удаление стоп-слов) существенно приводит тексты к единому виду.
    4. Анализ ключевых слов показывает, какие термины доминируют в каждой категории. Коэффициент разнообразия (уникальные/все слова) позволяет оценить лексическую вариативность текстов – более высокий показатель говорит о большем разнообразии используемых терминов.
    5. Топ-10 ключевых слов для каждой категории может быть использован для дальнейшего feature engineering или тематического анализа.
    """
    print(conclusions)


if __name__ == "__main__":
    main()
