import pandas as pd
from src.ml_utils.transformers import TextCleaner, SpacyTokenizer, TokenProcessor
from src.ml_utils.config import PreprocessParams, TrainingParams, Classifier
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics import make_scorer, accuracy_score
from sklearn.experimental import enable_halving_search_cv  # noqa
from sklearn.model_selection import HalvingRandomSearchCV
import logging

def filter_n_most_common_categories(df: pd.DataFrame, n: int):
    if 'category' not in df.columns:
        raise ValueError("The DataFrame must contain a 'category' column.")
    top_n_categories = df['category'].value_counts().head(n).index.tolist()
    filtered_df = df[df['category'].apply(lambda x: x in top_n_categories)]
    return filtered_df

def create_text_pipeline(params: PreprocessParams):
    return Pipeline([
        ('cleaner', TextCleaner(params)),
        ('tokenizer', SpacyTokenizer(params)),
        ('processor', TokenProcessor(params)),
        ('vectorizer', TfidfVectorizer())
    ], verbose=params.verbose)

def get_feature_pipeline(params: PreprocessParams):
    text_columns = ['text', 'title']
    text_transformers = [
        (f'{col}_pipeline', create_text_pipeline(params), col)
        for col in text_columns
    ]

    transformer = ColumnTransformer(
        transformers=[
            *text_transformers,
            ('tags_preprocess', OneHotEncoder(sparse_output=False, drop='first'), ['tags']),
        ],
        remainder='drop',
        verbose=params.verbose,
        sparse_threshold=1,
    )

    pipe = Pipeline(steps=[('column_processor', transformer)], verbose=params.verbose)
    
    return pipe

def tuning_params_step(classifier: Classifier,
                       train_params: TrainingParams,
                       X_train, y_train, 
                       scoring_metric='accuracy', 
                       factor=2, 
                       min_resources='smallest', 
                       max_resources='auto'):
    estimator = classifier.estim
    param_grid = classifier.param_grid

    if param_grid is not None and classifier.tuning_params:
        if scoring_metric == 'accuracy':
            scorer = make_scorer(accuracy_score)
        else:
            scorer = make_scorer(scoring_metric)


        halving_random_search = HalvingRandomSearchCV(
            estimator=estimator,
            param_distributions=param_grid,
            n_candidates='exhaust',
            factor=factor,
            resource='n_samples',
            max_resources=max_resources,
            min_resources=min_resources,
            cv=classifier.cv,
            scoring=scorer,
            random_state=train_params.random_state,
            n_jobs=train_params.n_jobs,
            verbose=train_params.verbose
        )
        
        logging.info(f'Tuning params step has started for {classifier.name}')
        halving_random_search.fit(X_train, y_train)
        best_estimator = halving_random_search.best_estimator_
        best_params = halving_random_search.best_params_

        logging.info(f"Best Parameters for {classifier.name}: {best_params}")
        logging.info(f"Best Estimator for {classifier.name}: {best_estimator}")

    else:
        best_estimator = estimator
        best_params = None
        logging.warning(f"No tuning performed for {classifier.name}. Using base estimator.")

    return best_estimator, best_params

def train_step(_X_train, _y_train, clf: Classifier, train_params: TrainingParams):
    if clf.tuning_params:
        best_estimator, best_params = tuning_params_step(
            classifier=clf,
            train_params=train_params,
            X_train=_X_train,
            y_train=_y_train,
            scoring_metric='accuracy',
            factor=2,
            min_resources='smallest',
            max_resources='auto'
        )
        clf.estim = best_estimator.set_params(**best_params)
    logging.info(f'Fit step has started for {clf.name}')
    clf.estim.fit(_X_train, _y_train)
    logging.info('Fit step finished successfully!')
    return clf
