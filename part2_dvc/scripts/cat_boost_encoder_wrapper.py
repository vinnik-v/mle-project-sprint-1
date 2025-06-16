from sklearn.base import BaseEstimator, TransformerMixin
from category_encoders import CatBoostEncoder

# Обертка для CatBoostEncoder
class CatBoostEncoderWrapper(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.encoder = CatBoostEncoder()
        self.feature_names = None

    def fit(self, X, y=None):
        self.encoder.fit(X, y)
        self.feature_names = X.columns.tolist()
        return self

    def transform(self, X):
        return self.encoder.transform(X)

    def get_feature_names_out(self, input_features=None):
        return self.feature_names