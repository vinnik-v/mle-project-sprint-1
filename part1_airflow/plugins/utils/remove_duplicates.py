# удаление дубликатов
def remove_duplicates(data):
    """
    Удаляет полные дубликаты по признакам (кроме id) и дубликаты id, оставляя последнюю строку.
    """
    # Удалим дубликаты по признакам, игнорируя id
    feature_cols = [col for col in data.columns if col != 'id']
    data = data.drop_duplicates(subset=feature_cols, keep='last')

    # Удалим дубликаты по id, оставляя последнюю строку
    data = data.drop_duplicates(subset='id', keep='last')

    return data.reset_index(drop=True)
