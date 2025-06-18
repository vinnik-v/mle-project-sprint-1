import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from utils.fill_missing_values import fill_missing_values
from utils.remove_duplicates import remove_duplicates
from utils.remove_outliers import remove_outliers

logger = logging.getLogger(__name__)

def create_table(**kwargs):
    """
    Создаёт таблицу flats_buildings_clean, если она не существует.
    """
    from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, Float, Boolean, UniqueConstraint
    from sqlalchemy.exc import SQLAlchemyError

    try:
        logger.info("Создание таблицы flats_buildings_clean...")
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        metadata = MetaData()

        Table(
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
        logger.info("Таблица flats_buildings_clean успешно создана (если не существовала).")

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

def extract(**kwargs):
    """
    Извлекает данные из базы destination_db и сохраняет их в XCom.
    """
    try:
        logger.info("Извлечение данных из таблицы flats_buildings...")
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        sql = "SELECT * FROM public.flats_buildings"
        df = pd.read_sql(sql, engine)

        logger.info(f"Извлечено {len(df)} строк.")
        return df.to_dict(orient='list')

    except Exception as e:
        logger.error(f"Ошибка при извлечении данных: {e}")
        raise

def transform(**kwargs):
    """
    Преобразует данные из XCom и сохраняет обратно в XCom.
    """
    logger.info("Начало трансформации данных...")
    ti = kwargs['ti']
    raw_dict = ti.xcom_pull(task_ids='extract')
    df = pd.DataFrame.from_dict(raw_dict)
    logger.info(f"Исходные данные: {df.shape[0]} строк.")

    df = df[df['id'] != 0]
    df = df[df['building_type_int'] != 0]
    logger.info(f"После удаления строк с id=0 и building_type_int=0: {df.shape[0]} строк.")

    df = remove_duplicates(df)
    df = fill_missing_values(df)
    df = remove_outliers(df)

    # Явное приведение типов
    df = df.astype({
        'id': 'int64',
        'floor': 'float64',
        'is_apartment': 'bool',
        'kitchen_area': 'float64',
        'living_area': 'float64',
        'rooms': 'float64',
        'studio': 'bool',
        'total_area': 'float64',
        'price': 'int64',
        'build_year': 'float64',
        'building_type_int': 'int64',
        'latitude': 'float64',
        'longitude': 'float64',
        'ceiling_height': 'float64',
        'flats_count': 'float64',
        'floors_total': 'float64',
        'has_elevator': 'bool'
    })

    logger.info(f"После всех преобразований: {df.shape[0]} строк.")
    return df.to_dict(orient='list')

def load(**kwargs):
    """
    Загружает данные из XCom в таблицу flats_buildings_clean.
    """
    try:
        logger.info("Загрузка данных в таблицу flats_buildings_clean...")
        ti = kwargs['ti']
        transformed_dict = ti.xcom_pull(task_ids='transform')
        df = pd.DataFrame.from_dict(transformed_dict)

        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_buildings_clean",
            replace=True,
            target_fields=df.columns.tolist(),
            replace_index=['id'],
            rows=df.values.tolist()
        )
        logger.info(f"Загружено строк: {len(df)}.")

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        raise
