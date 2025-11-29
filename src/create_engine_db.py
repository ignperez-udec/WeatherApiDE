import os
from sqlalchemy import create_engine # type: ignore
from sqlalchemy.engine import Engine # type: ignore

def create_engine_db(db: str) -> Engine: # type: ignore
    user = os.getenv(db.upper() + '_DB_USER', 'USER')
    password = os.getenv(db.upper() + '_DB_PASSWORD', 'PASSWORD')
    host = os.getenv(db.upper() + '_DB_HOST', 'localhost')
    db = os.getenv(db.upper() + '_DB_NAME', 'DB')
    DATABASE_URL = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{db}'
    engine = create_engine(DATABASE_URL, future=True)

    return engine