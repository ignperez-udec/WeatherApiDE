import os
from sqlalchemy import create_engine # type: ignore
from sqlalchemy.engine import Engine # type: ignore

def create_engine_db() -> Engine: # type: ignore
    user = os.getenv('DWH_DB_USER', 'USER')
    password = os.getenv('DWH_DB_PASSWORD', 'PASSWORD')
    host = os.getenv('DWH_DB_HOST', 'localhost')
    db = os.getenv('DWH_DB_NAME', 'DB')
    port = os.getenv('DWH_DB_PORT', 5432)
    DATABASE_URL = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(DATABASE_URL, future=True)

    return engine