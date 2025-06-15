# удаление дубликатов
def remove_duplicates(data):
    # Удаляем дубликаты по признакам (кроме id)
    feature_cols = data.columns.drop('id').tolist()
    is_duplicated_features = data.duplicated(subset=feature_cols, keep=False)

    # Удаляем все строки с повторяющимися id
    duplicated_ids = data['id'][data['id'].duplicated(keep=False)]

    # Объединяем условия на удаление
    to_drop = is_duplicated_features | data['id'].isin(duplicated_ids)

    data = data[~to_drop].reset_index(drop=True)
    return data
