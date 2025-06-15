def fill_missing_values(data):
    num_cols = data.select_dtypes(include=[float, int]).columns
    num_cols = [col for col in num_cols if col not in ['latitude', 'longitude', 'price', 'id', 'building_type_int']]

    for col in num_cols:
        # считаем, что 0 и NaN — это пропуски
        mask = (data[col] == 0) | (data[col].isna())
        if mask.any():
            fill_value = data.loc[~mask, col].mean()  # среднее по нормальным значениям
            data.loc[mask, col] = fill_value

    return data
