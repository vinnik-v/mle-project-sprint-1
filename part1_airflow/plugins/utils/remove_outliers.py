import pandas as pd

# Удаление выбросов
def remove_outliers(data, threshold=1.5):
    num_cols = data.select_dtypes(include=['float', 'int']).columns
    num_cols = [col for col in num_cols if col not in ['latitude', 'longitude', 'price', 'id', 'building_type_int']]
    outlier_flags = pd.DataFrame(False, index=data.index, columns=num_cols)

    for col in num_cols:
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        lower = Q1 - threshold * IQR
        upper = Q3 + threshold * IQR
        outlier_flags[col] = ~data[col].between(lower, upper)

    rows_with_outliers = outlier_flags.any(axis=1)
    data = data[~rows_with_outliers]

    return data
