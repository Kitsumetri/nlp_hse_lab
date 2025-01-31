from dataclasses import dataclass

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

@dataclass
class TrainingParams:
    use_feature_selection: bool = False
    n_components: int = 100
    classifier: str = 'logistic'
    cv_folds: int = 5
    param_grid: dict = None
    scoring: str = 'accuracy'
    random_state: int = 42