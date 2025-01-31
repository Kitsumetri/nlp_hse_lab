from src.ml_utils.config import PreprocessParams
from sklearn.base import BaseEstimator, TransformerMixin
import re
import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from nltk.stem import PorterStemmer
import pandas as pd
from typing import List

class ColumnSelector(BaseEstimator, TransformerMixin):
    def __init__(self, columns: List[str]):
        self.columns = columns
        
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        if len(self.columns) == 1:
            return X[[self.columns[0]]]  # Возвращаем DataFrame
        return X[self.columns]

class TextCleaner(BaseEstimator, TransformerMixin):
    def __init__(self, params: PreprocessParams):
        self.params = params
        
    def fit(self, X, y=None):
        return self
        
    def transform(self, X):
        if isinstance(X, pd.DataFrame):
            X = X.iloc[:, 0]
            
        X = X.astype(str)
        
        if self.params.remove_punct:
            punct_pattern = self.params.custom_punct + ']' if not self.params.custom_punct.endswith(']') else self.params.custom_punct
            X = X.str.replace(punct_pattern, ' ', regex=True)
            X = X.str.replace(r'\s+', ' ', regex=True).str.strip()
            
        if self.params.lowercase:
            X = X.str.lower()
            
        return pd.DataFrame(X, columns=["cleaned_text"])  # 2D DataFrame

class SpacyTokenizer(BaseEstimator, TransformerMixin):
    def __init__(self, params: PreprocessParams):
        self.params = params
        self.nlp = spacy.load(params.spacy_model, disable=["parser", "ner"])
        
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X = X.apply(lambda text: [token.text for token in self.nlp(text)])
        return pd.DataFrame(X, columns=["tokenized_text"])  # 2D DataFrame

class TokenProcessor(BaseEstimator, TransformerMixin):
    def __init__(self, params: PreprocessParams):
        self.params = params
        self.stopwords = set(STOP_WORDS)
        self.nlp = spacy.load(params.spacy_model, disable=["parser", "ner"])
        self.stemmer = PorterStemmer() if params.stem else None
        
    def fit(self, X: pd.Series, y=None):
        return self

    def transform(self, X: pd.Series) -> pd.DataFrame:
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
            return filtered
        
        X = X.apply(_process_row)
        return pd.DataFrame(X, columns=["processed_tokens"])  # 2D DataFrame
