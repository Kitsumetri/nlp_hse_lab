import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM
import argparse

def inference(
    prompt: str,
    model_dir: str,
    n: int = 1,
    repetition_penalty: float = 1.0,
    temperature: float = 1.0,
    top_p: float = 1.0,
    top_k: int = -1,
    max_tokens: int = 50,
    min_tokens: int = 0,
    skip_special_tokens: bool = True,
):
    # Загружаем токенайзер и модель из указанной директории
    tokenizer = AutoTokenizer.from_pretrained(model_dir)
    model = AutoModelForCausalLM.from_pretrained(model_dir)

    # Переводим модель на нужное устройство (GPU, если доступен)
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)

    # Токенизируем входной текст
    inputs = tokenizer(prompt, return_tensors="pt")
    inputs = {key: val.to(device) for key, val in inputs.items()}

    # Если top_k равен -1, отключаем top-k фильтрацию
    if top_k <= 0:
        top_k = None

    # Вычисляем параметры генерации.
    # Используем max_new_tokens и min_new_tokens, чтобы задать число генерируемых токенов
    do_sample = temperature > 0.0  # при temperature == 0 происходит жадное декодирование

    generate_kwargs = {
        "do_sample": do_sample,
        "num_return_sequences": n,
        "repetition_penalty": repetition_penalty,
        "top_p": top_p,
        "max_new_tokens": max_tokens,
        "min_new_tokens": min_tokens,
        "eos_token_id": tokenizer.eos_token_id,
    }
    if do_sample:
        generate_kwargs["temperature"] = temperature
    if top_k is not None:
        generate_kwargs["top_k"] = top_k

    # Генерация последовательностей
    outputs = model.generate(**inputs, **generate_kwargs)

    # Декодируем результаты
    responses = [
        tokenizer.decode(output, skip_special_tokens=skip_special_tokens)
        for output in outputs
    ]
    return responses

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inference script for text generation")
    parser.add_argument("--model_dir", type=str, required=True, help="Путь к директории модели")
    parser.add_argument("--prompt", type=str, required=True, help="Начальный текст (prompt)")
    parser.add_argument("--n", type=int, default=1, help="Число возвращаемых последовательностей")
    parser.add_argument("--repetition_penalty", type=float, default=1.0, help="Параметр штрафа за повторения")
    parser.add_argument("--temperature", type=float, default=1.0, help="Параметр температуры (0 для жадного декодирования)")
    parser.add_argument("--top_p", type=float, default=1.0, help="Параметр top_p для nucleus sampling")
    parser.add_argument("--top_k", type=int, default=-1, help="Параметр top_k (установите -1, чтобы отключить)")
    parser.add_argument("--max_tokens", type=int, default=50, help="Максимальное число генерируемых токенов")
    parser.add_argument("--min_tokens", type=int, default=0, help="Минимальное число генерируемых токенов до EOS")
    parser.add_argument("--skip_special_tokens", action="store_true", help="Пропускать специальные токены в выводе")
    args = parser.parse_args()

    responses = inference(
        prompt=args.prompt,
        model_dir=args.model_dir,
        n=args.n,
        repetition_penalty=args.repetition_penalty,
        temperature=args.temperature,
        top_p=args.top_p,
        top_k=args.top_k,
        max_tokens=args.max_tokens,
        min_tokens=args.min_tokens,
        skip_special_tokens=args.skip_special_tokens,
    )

    for idx, response in enumerate(responses):
        print(f"Response {idx + 1}:\n{response}\n")
