def fill_missing_values(data):
    # Числовые непрерывные признаки
    num_cols = ['floor', 'kitchen_area', 'living_area', 'total_area', 'ceiling_height',
                'flats_count', 'floors_total', 'build_year']

    for col in num_cols:
        if col in data.columns:
            mask = (data[col] == 0) | (data[col].isna())
            if mask.any():
                fill_value = data.loc[~mask, col].mean()
                data.loc[mask, col] = fill_value

    # Булевые признаки
    bool_cols = ['is_apartment', 'studio', 'has_elevator']
    for col in bool_cols:
        if col in data.columns:
            mask = data[col].isna()
            if mask.any():
                mode_val = data[col].mode(dropna=True)
                if not mode_val.empty:
                    data[col].fillna(mode_val[0], inplace=True)

    # Категориальные признаки
    cat_cols = ['building_type_int']
    for col in cat_cols:
        if col in data.columns:
            mask = (data[col] == 0) | (data[col].isna())
            if mask.any():
                mode_val = data.loc[~mask, col].mode()
                if not mode_val.empty:
                    data.loc[mask, col] = mode_val[0]

    return data
