import os
import pandas as pd # type: ignore
from sqlalchemy import Table, MetaData, select # type: ignore
from create_engine_db import create_engine_db  # type: ignore
from config import load_variables  # type: ignore

def read_weather_from_db() -> pd.DataFrame:
    engine = create_engine_db()

    CONFIG_VARS = load_variables()

    locations_ids = [int(id.strip()) for id in CONFIG_VARS['API_LOCATIONS_TO_EXTRACT'].split(',')]

    metadata = MetaData()
    weather_hist = Table('weather_hist', metadata, autoload_with=engine)
    with engine.connect() as conn:
            stmt = select(
                        weather_hist.c.cod_location,
                        weather_hist.c.location,
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