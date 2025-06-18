import pandas as pd
import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def create_table(**kwargs):
    """
    Создаёт таблицу flats_buildings, если она не существует.
    """
    from sqlalchemy import MetaData, Table, Column, BigInteger, Float, Boolean, UniqueConstraint
    from sqlalchemy.exc import SQLAlchemyError

    try:
        logger.info("Создание таблицы flats_buildings...")
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()
        metadata = MetaData()

        flats_buildings = Table(
            'flats_buildings', metadata,
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
            UniqueConstraint('id', name='flats_buildings_unique_id')
        )

        metadata.create_all(engine, checkfirst=True)
        logger.info("Таблица flats_buildings успешно создана (если не существовала).")

    except SQLAlchemyError as e:
        logger.error(f"Ошибка при создании таблицы flats_buildings: {e}")
        raise

def extract(**kwargs):
    """
    Извлекает данные из базы destination_db и сохраняет их в XCom.
    """
    try:
        logger.info("Извлечение данных из таблиц flats и buildings...")
        hook = PostgresHook('destination_db')
        engine = hook.get_sqlalchemy_engine()

        sql = """
            SELECT
                fl.id,
                fl.floor, 
                fl.is_apartment, 
                fl.kitchen_area, 
                fl.living_area, 
                fl.rooms, 
                fl.studio, 
                fl.total_area, 
                fl.price,
                bld.build_year,
                bld.building_type_int,
                bld.latitude,
                bld.longitude,
                bld.ceiling_height,
                bld.flats_count,
                bld.floors_total,
                bld.has_elevator
            FROM
                public.flats fl
            LEFT JOIN public.buildings bld
            ON fl.building_id = bld.id
        """

        df = pd.read_sql(sql, engine)
        logger.info(f"Извлечено строк: {len(df)}.")
        return df.to_dict(orient='list')

    except Exception as e:
        logger.error(f"Ошибка при извлечении данных: {e}")
        raise

def load(**kwargs):
    """
    Загружает данные из XCom в таблицу flats_buildings.
    """
    try:
        logger.info("Начало загрузки данных в таблицу flats_buildings...")
        ti = kwargs['ti']
        transformed_dict = ti.xcom_pull(task_ids='extract')
        df = pd.DataFrame.from_dict(transformed_dict)

        hook = PostgresHook('destination_db')
        hook.insert_rows(
            table="flats_buildings",
            replace=True,
            target_fields=df.columns.tolist(),
            replace_index=['id'],
            rows=df.values.tolist()
        )
        logger.info(f"Загружено строк: {len(df)}.")

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в таблицу flats_buildings: {e}")
        raise
