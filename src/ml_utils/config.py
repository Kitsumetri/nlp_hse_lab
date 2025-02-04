from dataclasses import dataclass, field
from typing import Optional, Dict
from sklearn.base import BaseEstimator

@dataclass
class PreprocessParams:
    spacy_model: str = "en_core_web_sm"
    remove_punct: bool = True
    custom_punct: str = r'[!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~]'
    remove_stopwords: bool = True
    lemmatize: bool = True
    stem: bool = False
    lowercase: bool = True
    min_token_length: int = 2
    verbose: bool = False

@dataclass
class TrainingParams:
    test_size: float = 0.2
    random_state: int = 42
    shuffle_split: bool = True
    n_jobs: int = 1
    verbose: bool = True

@dataclass
class Classifier:
    name: str
    estim: BaseEstimator
    param_grid: Optional[Dict] = field(default=None)
    tuning_params: bool = field(default=False)
    cv: int = field(default=3)