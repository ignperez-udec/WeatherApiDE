import os
import pandas as pd # type: ignore
from sqlalchemy import Table, MetaData, select # type: ignore
from create_engine_db import create_engine_db  # type: ignore
from config import load_variables  # type: ignore

def read_locations_from_db() -> pd.DataFrame:
    engine = create_engine_db()
    
    CONFIG_VARS = load_variables()

    locations_ids = [int(id.strip()) for id in CONFIG_VARS['API_LOCATIONS_TO_EXTRACT'].split(',')]

    metadata = MetaData()
    locations = Table('locations', metadata, autoload_with=engine)
    with engine.connect() as conn:
            stmt = select(
                        locations.c.cod_location,
                        locations.c.location,
                        locations.c.longitude,
                        locations.c.latitude,
                  ).where(
                        locations.c.cod_location.in_(locations_ids)
                  ).order_by(
                        locations.c.cod_location)
            data = pd.read_sql(stmt, conn)

    return data
