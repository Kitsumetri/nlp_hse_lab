import os
import logging
import math
import torch
from datetime import datetime
from transformers import (
    AutoModelForCausalLM,
    TrainingArguments,
    Trainer,
    DataCollatorForLanguageModeling,
    AutoTokenizer,
    TrainerCallback,
)
from datasets import Dataset
import pandas as pd

os.environ["TOKENIZERS_PARALLELISM"] = "true"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("training.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


class RussianGPT:
    def __init__(self, config):
        self.config = config
        self.model = None
        self.tokenizer = None
        self.dataset = None

        # Create output directories
        os.makedirs(self.config["output_dir"], exist_ok=True)
        os.makedirs(self.config["log_dir"], exist_ok=True)

    def prepare_data(self):
        """Load and process training data into chunks without splitting"""
        logger.info("Loading and chunking data...")
        df = pd.read_json(self.config["data_path"])
        full_text = "\n".join(df["title"] + " " + df["text"])
        chunked_text = [
            full_text[i : i + self.config["chunk_size"]]
            for i in range(0, len(full_text), self.config["chunk_size"])
        ]
        self.dataset = Dataset.from_dict({"text": chunked_text})
        return self

    def split_dataset(self):
        """Split the dataset into train/test"""
        logger.info("Splitting dataset...")
        split_dataset = self.dataset.train_test_split(
            test_size=self.config["test_size"], shuffle=True, seed=42
        )
        self.dataset = split_dataset
        return self

    def load_tokenizer(self):
        """Load pre-trained tokenizer using AutoTokenizer with fast=True"""
        logger.info("Loading pre-trained tokenizer...")
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config["model_checkpoint"], use_fast=True
        )
        return self

    def initialize_model(self):
        """Load pre-trained model from checkpoint"""
        logger.info("Loading pre-trained model...")
        self.model = AutoModelForCausalLM.from_pretrained(self.config["model_checkpoint"])
        logger.info(f"Model loaded with {self.model.num_parameters() / 1e6:.2f}M parameters")
        return self

    def tokenize_dataset(self):
        """Tokenize both train and test splits using the pre-trained tokenizer"""
        logger.info("Tokenizing dataset...")

        def tokenize_fn(examples):
            return self.tokenizer(
                examples["text"],
                truncation=True,
                max_length=self.config["n_positions"],
                padding="max_length",
                add_special_tokens=True,
            )

        self.dataset = self.dataset.map(
            tokenize_fn, batched=True, num_proc=1, remove_columns=["text"]
        )
        return self

    def train(self):
        """Execute full training pipeline (fine-tuning)"""
        logger.info(f"Dataset features: {self.dataset}")
        torch.cuda.empty_cache()

        training_args = TrainingArguments(
            output_dir=self.config["output_dir"],
            logging_dir=self.config["log_dir"],
            num_train_epochs=self.config["num_epochs"],
            per_device_train_batch_size=self.config["batch_size"],
            per_device_eval_batch_size=self.config["batch_size"] // 2,
            gradient_accumulation_steps=self.config["grad_accum_steps"],
            learning_rate=self.config["learning_rate"],
            weight_decay=0.1,
            bf16=torch.cuda.is_bf16_supported(),
            logging_steps=100,
            save_steps=250,
            eval_steps=250,  # Eval every 10 steps
            eval_strategy="steps",
            optim="adamw_torch_fused",
            gradient_checkpointing=True,
            report_to=["tensorboard"],
            dataloader_num_workers=4,
            torch_compile=True,
            resume_from_checkpoint=True,
            save_total_limit=2,
            load_best_model_at_end=True,
            metric_for_best_model="eval_loss",
            greater_is_better=False,
            max_grad_norm=1.0,
            warmup_steps=250,
            lr_scheduler_type="cosine",
        )

        # Создаём callback-и
        training_callback = TrainingProgressCallback()
        save_tokenizer_callback = SaveTokenizerCallback()
        inference_callback = InferenceLoggingCallback(self.config.get("eval_prompt", "Пример инференса"))

        trainer = Trainer(
            model=self.model,
            args=training_args,
            train_dataset=self.dataset["train"],
            eval_dataset=self.dataset["test"],
            data_collator=DataCollatorForLanguageModeling(
                tokenizer=self.tokenizer, mlm=False
            ),
            callbacks=[training_callback, save_tokenizer_callback, inference_callback],
        )

        # Передаём ссылку на trainer в callback для инференса
        inference_callback.trainer = trainer

        logging.info(
            f"GPU memory allocated: {torch.cuda.memory_allocated() / 1024**3:.2f} GB"
        )
        logger.info("Starting training...")
        trainer.train()
        logger.info("Training completed. Saving final model...")

        self.model.save_pretrained(os.path.join(self.config["output_dir"], "final_model"))
        self.tokenizer.save_pretrained(os.path.join(self.config["output_dir"], "final_model"))
        return self


class TrainingProgressCallback(TrainerCallback):
    def on_log(self, args, state, control, logs=None, **kwargs):
        if state.is_local_process_zero:
            loss = logs.get("loss", float("nan"))
            eval_loss = logs.get("eval_loss", float("nan"))
            lr = logs.get("learning_rate", float("nan"))
            loss_str = f"{loss:.4f}" if not math.isnan(loss) else "N/A"
            eval_str = f"{eval_loss:.4f}" if not math.isnan(eval_loss) else "N/A"
            lr_str = f"{lr:.2e}" if not math.isnan(lr) else "N/A"
            logger.info(
                f"Step {state.global_step} | Loss: {loss_str} | Eval Loss: {eval_str} | Learning Rate: {lr_str}"
            )


class SaveTokenizerCallback(TrainerCallback):
    def on_save(self, args, state, control, **kwargs):
        trainer = kwargs.get("trainer", None)
        if trainer is not None and hasattr(trainer.data_collator, "tokenizer") and trainer.data_collator.tokenizer is not None:
            tokenizer_save_path = os.path.join(args.output_dir, f"checkpoint-{state.global_step}")
            trainer.data_collator.tokenizer.save_pretrained(tokenizer_save_path)
            logger.info(f"Tokenizer saved at {tokenizer_save_path}")
        return control


class InferenceLoggingCallback(TrainerCallback):
    """
    Callback для логирования инференс-вывода каждые eval_steps.
    Использует фиксированный prompt, задаваемый в конфигурации (ключ 'eval_prompt').
    """
    def __init__(self, eval_prompt: str):
        self.eval_prompt = eval_prompt
        self.trainer = None  # Будет установлен вручную

    def on_evaluate(self, args, state, control, metrics=None, **kwargs):
        if self.trainer is None:
            logger.warning("Trainer не найден в on_evaluate")
            return control

        # Используем токенизатор из data_collator, так как атрибут tokenizer уже deprecated
        if not hasattr(self.trainer.data_collator, "tokenizer") or self.trainer.data_collator.tokenizer is None:
            logger.warning("Tokenizer не найден в trainer.data_collator")
            return control

        tokenizer = self.trainer.data_collator.tokenizer
        inputs = tokenizer(self.eval_prompt, return_tensors="pt")
        inputs = {k: v.to(self.trainer.model.device) for k, v in inputs.items()}

        generated_ids = self.trainer.model.generate(
            **inputs,
            max_new_tokens=50,
            do_sample=True,
            temperature=1.0,
            top_p=0.95,
            repetition_penalty=1.0,
        )
        generated_text = tokenizer.decode(generated_ids[0], skip_special_tokens=True)
        logger.info(
            f"Inference at step {state.global_step} | Prompt: '{self.eval_prompt}' | Generated: {generated_text}"
        )
        return control


if __name__ == "__main__":
    config = {
        "data_path": "data/ria_articles.json",
        "test_size": 0.05,
        "output_dir": f"./models/rugpt-{datetime.now().strftime('%Y%m%d-%H%M')}",
        "log_dir": "./logs",
        "chunk_size": 1024,
        "n_positions": 1024,
        "num_epochs": 5,
        "batch_size": 16,
        "grad_accum_steps": 4,
        "learning_rate": 5e-4,
        "eval_prompt": "Европа ",
        # Контрольный пункт предобученной модели (русскоязычная модель)
        "model_checkpoint": "sberbank-ai/rugpt3small_based_on_gpt2",
    }

    (
        RussianGPT(config)
        .prepare_data()
        .load_tokenizer()
        .split_dataset()
        .initialize_model()
        .tokenize_dataset()
        .train()
    )
