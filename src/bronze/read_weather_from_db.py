import os
import pandas as pd # type: ignore
from sqlalchemy import Table, MetaData, select # type: ignore
from utils.create_engine_db import create_engine_db  # type: ignore
from utils.config import load_variables  # type: ignore

def read_last_date_from_weather_silver(cod_location: list[int] = None) -> pd.DataFrame:
    engine = create_engine_db()

    CONFIG_VARS = load_variables()

    if cod_location is None:
        locations_ids = [int(id.strip()) for id in CONFIG_VARS['API_LOCATIONS_TO_EXTRACT'].split(',')]
    else:
        locations_ids = cod_location

    metadata = MetaData()
    weather_hist = Table('weather_hist', metadata, schema='silver', autoload_with=engine)
    with engine.connect() as conn:
            stmt = select(
                        weather_hist.c.cod_location,
                        weather_hist.c.time,
                ).distinct(
                    weather_hist.c.cod_location
                ).where(
                    weather_hist.c.cod_location.in_(locations_ids)
                ).order_by(
                    weather_hist.c.cod_location,
                    weather_hist.c.time.desc()
                )
            data = pd.read_sql(stmt, conn)

    return data

def read_last_date_from_weather_gold(cod_location: list[int] = None) -> pd.DataFrame:
    engine = create_engine_db()

    CONFIG_VARS = load_variables()

    if cod_location is None:
        locations_ids = [int(id.strip()) for id in CONFIG_VARS['API_LOCATIONS_TO_EXTRACT'].split(',')]
    else:
        locations_ids = cod_location

    metadata = MetaData()
    fact_weather = Table('fact_weather', metadata, schema='gold', autoload_with=engine)
    dim_date = Table('dim_date', metadata, schema='gold', autoload_with=engine)
    with engine.connect() as conn:
            stmt = select(
                        fact_weather.c.cod_location,
                        dim_date.c.date,
                ).select_from(
                     fact_weather.outerjoin(
                        dim_date,
                        fact_weather.c.date_id == dim_date.c.id
                     )
                ).distinct(
                    fact_weather.c.cod_location
                ).where(
                    fact_weather.c.cod_location.in_(locations_ids)
                ).order_by(
                    fact_weather.c.cod_location,
                    fact_weather.c.date_id.desc()
                )
            data = pd.read_sql(stmt, conn)

    return data