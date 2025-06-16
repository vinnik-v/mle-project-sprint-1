import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.model_selection import train_test_split
from catboost import CatBoostRegressor
from cat_boost_encoder_wrapper import CatBoostEncoderWrapper
import yaml
import os
import joblib

def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    index_col = params['index_col']
    target_col = params['target_col']
    test_size = params['test_size']
    random_state = params['random_state']
    stratify_bins = params['stratify_bins']
    drop_strategy = params['one_hot_drop']
    verbose = params['catboost_verbose']

    data = pd.read_csv('data/initial_data.csv', index_col=index_col)

    data = data[(data[target_col] > 1_000_000) & (data[target_col] < 50_000_000)]

    x = data.drop(columns=target_col)
    y = data[target_col]

    # Цена - непрерывный числовой показатель, нужно произвести бинирование - 
    # разбиение цен на несколько групп с примерно одинаковым количеством объектов
    # в нашем случае выделяем 10 групп и предотвращаем их повторение - duplicates='drop'
    price_bins = pd.qcut(y, q=stratify_bins)

    x_tr, x_val, y_tr, y_val = train_test_split(
        x, y,
        test_size=test_size,
        stratify=price_bins,
        random_state=random_state
    )

    # Тренировочная выборка
    num_features_tr = x_tr.select_dtypes(include=['float', 'int']).drop(columns=['building_type_int'])
    cat_features_tr = x_tr[['building_type_int']]
    binary_cat_features_tr = x_tr.select_dtypes(include='bool')

    binary_cols = binary_cat_features_tr.columns.tolist()
    non_binary_cat_cols = cat_features_tr.columns.tolist()
    num_cols = num_features_tr.columns.tolist()

    # определите список трансформаций в рамках ColumnTransformer
    preprocessor = ColumnTransformer(
        transformers=[
            ('binary', OneHotEncoder(drop=drop_strategy, sparse_output=False), binary_cols),
            ('non_binary', CatBoostEncoderWrapper(), non_binary_cat_cols),
            ('scaler', StandardScaler(), num_cols)
        ],
        verbose_feature_names_out=False
    )


    model = CatBoostRegressor(
        verbose=verbose,
        random_seed=random_state
    )

    # Пайплайн
    pipeline = Pipeline(
        [
            ('preprocessing', preprocessor),
            ('model', model)
        ]
    )

    # Обучение
    pipeline.fit(x_tr, y_tr)

    # Сохраняем обученную модель в models/fitted_model.pkl
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)

if __name__ == '__main__':
    fit_model()
