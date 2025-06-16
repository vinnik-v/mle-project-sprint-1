import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_validate
from sklearn.metrics import make_scorer, mean_absolute_error, mean_squared_error, r2_score
import joblib
import json
import yaml
import os
import numpy as np

def evaluate_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    target_col = params['target_col']
    index_col = params['index_col']
    n_splits = params['n_splits']
    metrics = params['metrics']
    random_state = params['random_state']

    # Загружаем обученный пайплайн
    with open('models/fitted_model.pkl', 'rb') as fd:
        pipeline = joblib.load(fd)

    # Загружаем данные
    data = pd.read_csv('data/initial_data.csv', index_col=index_col)
    data = data[(data[target_col] > 1_000_000) & (data[target_col] < 50_000_000)]

    x = data.drop(columns=target_col)
    y = data[target_col]

    cv = StratifiedKFold(n_splits=n_splits, shuffle=True, random_state=random_state)

    scorers = {
        'mae': make_scorer(mean_absolute_error),
        'rmse': make_scorer(mean_squared_error, squared=False),
        'r2': make_scorer(r2_score)
    }

    selected_scorers = {k: v for k, v in scorers.items() if k in metrics}

    scores = cross_validate(
        pipeline,
        x, y,
        cv=cv,
        scoring=selected_scorers,
        n_jobs=-1
    )

    # Усреднение метрик
    average_scores = {
        metric: float(np.mean(scores[f'test_{metric}']))
        for metric in selected_scorers.keys()
    }

    os.makedirs('cv_results', exist_ok=True)
    with open('cv_results/cv_res.json', 'w') as fd:
        json.dump(average_scores, fd, indent=2)

if __name__ == '__main__':
    evaluate_model()
