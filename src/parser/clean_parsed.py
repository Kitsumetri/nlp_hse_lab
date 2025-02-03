import pandas as pd
import json

# Пути к файлам (замените на ваши реальные пути)
articles_file = 'data/reuters_links.jsonl' 
parsed_articles_file = 'data/reuters_articles.json'
output_file = 'data/reuters_links.jsonl'

articles_df = pd.read_json(articles_file, lines=True)
parsed_df = pd.read_json(parsed_articles_file)

articles_df = articles_df.drop_duplicates(subset='url', keep='first')
parsed_urls = set(parsed_df['article_id'])
articles_df = articles_df[~articles_df['url'].isin(parsed_urls)]

with open(output_file, 'w', encoding='utf-8') as f:
    for record in articles_df.to_dict(orient='records'):
        json_line = json.dumps(record, ensure_ascii=False)
        json_line = json_line.replace('\\/', '/')
        f.write(json_line + "\n")

print(f"Очищенные данные сохранены в файле: {output_file}")
