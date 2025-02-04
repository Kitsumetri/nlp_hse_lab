from sklearn.base import BaseEstimator, TransformerMixin
from src.ml_utils.config import PreprocessParams
import pandas as pd
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from nltk.stem import PorterStemmer

class TextCleaner(BaseEstimator, TransformerMixin):
    def __init__(self, params: PreprocessParams):
        self.params = params

    def fit(self, X, y=None):
        return self

    def transform(self, X: pd.Series) -> pd.Series:
        X = X.astype(str)
        if self.params.lowercase:
            X = X.str.lower()
        if self.params.remove_punct:
            punct_pattern = self.params.custom_punct + ']' if not self.params.custom_punct.endswith(']') else self.params.custom_punct
            X = X.str.replace(punct_pattern, ' ', regex=True)
        X = X.str.replace(r'\s+', ' ', regex=True).str.strip()
        return X

class SpacyTokenizer(BaseEstimator, TransformerMixin):
    def __init__(self, params: PreprocessParams):
        self.params = params
        self.nlp = spacy.load(params.spacy_model, disable=["parser", "ner"])

    def fit(self, X: pd.Series, y=None):
        return self

    def transform(self, X: pd.Series) -> pd.Series:
        tokenized = X.apply(lambda text: [token.text for token in self.nlp(str(text))])
        return tokenized

class TokenProcessor(BaseEstimator, TransformerMixin):
    def __init__(self, params: PreprocessParams):
        self.params = params
        self.stopwords = set(STOP_WORDS)
        self.nlp = spacy.load(params.spacy_model, disable=["parser", "ner"])
        self.stemmer = PorterStemmer() if params.stem else None

    def fit(self, X: pd.Series, y=None):
        return self

    def transform(self, X: pd.Series) -> pd.Series:
        def _process_row(row):
            filtered = []
            for token in row:
                if len(token) < self.params.min_token_length:
                    continue
                if self.params.remove_stopwords and token.lower() in self.stopwords:
                    continue
                if self.params.lemmatize:
                    token = self.nlp(token)[0].lemma_
                elif self.params.stem:
                    token = self.stemmer.stem(token)
                filtered.append(token)
            return ' '.join(filtered)

        processed = X.apply(_process_row)
        return processed