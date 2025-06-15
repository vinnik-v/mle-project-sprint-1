import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.fill_missing_values import fill_missing_values
from utils.remove_duplicates import remove_duplicates
from utils.remove_outliers import remove_outliers

def create_table(**kwargs):
    """
    Создаёт таблицу flats_buildings_clean, если она не существует.
    """
    from sqlalchemy import MetaData, Table, Column, Integer, BigInteger, Float, Boolean, UniqueConstraint

    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()

    metadata = MetaData()

    flats_buildings_clean = Table(
        'flats_buildings_clean', metadata,
        Column('id', BigInteger, primary_key=True),
        Column('floor', BigInteger),
        Column('is_apartment', Boolean),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', BigInteger),
        Column('studio', Boolean),
        Column('total_area', Float),
        Column('price', BigInteger),
        Column('build_year', BigInteger),
        Column('building_type_int', BigInteger),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', BigInteger),
        Column('floors_total', BigInteger),
        Column('has_elevator', Boolean),
        UniqueConstraint('id', name='flats_buildings_clean_unique_id')
    )

    metadata.create_all(engine, checkfirst=True)

def extract(**kwargs):
    """
    Извлекает данные из базы destination_db и сохраняет их в XCom.
    """
    hook = PostgresHook('destination_db')
    engine = hook.get_sqlalchemy_engine()

    sql = """
        SELECT * FROM public.flats_buildings
    """

    df = pd.read_sql(sql, engine)

    return df.to_dict(orient='list')

def transform(**kwargs):
    """
    Преобразует данные из XCom и сохраняет обратно в XCom.
    """
    ti = kwargs['ti']
    raw_dict = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(raw_dict)

    # Удаление строк с id == 0
    df = df[df['id'] != 0]

    # Удаление строк с building_type_int == 0
    df = df[df['building_type_int'] != 0]

    # Преобразования
    df = remove_duplicates(df)
    df = fill_missing_values(df)
    df = remove_outliers(df)

    return df.to_dict(orient='list')

def load(**kwargs):
    """
    Загружает данные из XCom в таблицу flats_buildings_clean.
    """
    ti = kwargs['ti']
    transformed_dict = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(transformed_dict)

    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="flats_buildings_clean",
        replace=True,
        target_fields=df.columns.tolist(),
        replace_index=['id'],
        rows=df.values.tolist()
    )