import os
import json
import glob
import pandas as pd
import numpy as np


def split_json(input_file, output_prefix, max_size_mb=30):
    max_size = max_size_mb * 1024 * 1024
    df = pd.read_json(input_file)
    
    part = 1
    current_size = 0
    current_data = []
    
    for index, row in df.iterrows():
        item = row.to_dict()
        item_size = len(json.dumps(item, ensure_ascii=False).encode('utf-8'))
        
        if current_size + item_size > max_size:
            pd.DataFrame(current_data).to_json(f"{output_prefix}_part{part}.json", orient='records', force_ascii=False, indent=2)
            part += 1
            current_data = []
            current_size = 0
        
        current_data.append(item)
        current_size += item_size
    
    if current_data:
        pd.DataFrame(current_data).to_json(f"{output_prefix}_part{part}.json", orient='records', force_ascii=False, indent=2)
    
    print(f"Разделение завершено, создано {part} файлов.")


def merge_json(output_file, input_pattern):
    input_files = sorted(glob.glob(input_pattern))
    merged_df = pd.concat([pd.read_json(file) for file in input_files], ignore_index=True)
    merged_df.to_json(output_file, orient='records', force_ascii=False, indent=2)
    
    print(f"Объединение завершено, записан файл {output_file}.")


def habr_split():
    split_json(os.path.join("data", "habr_articles.json"),
               os.path.join("data", "habr_articles"))


def habr_merge():
    merge_json(os.path.join("data", "habr_articles.json"),
               os.path.join("data", "habr_articles_part*.json"))