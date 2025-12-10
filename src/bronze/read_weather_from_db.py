import os
import pandas as pd # type: ignore
from sqlalchemy import Table, MetaData, select # type: ignore
from utils.create_engine_db import create_engine_db  # type: ignore
from utils.config import load_variables  # type: ignore

def read_weather_from_db(cod_location: list[int] = None) -> pd.DataFrame:
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