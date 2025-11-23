import os
from sqlalchemy import create_engine # type: ignore
from sqlalchemy.engine import Engine # type: ignore

def create_engine_db() -> Engine: # type: ignore
    user = os.getenv('DB_USER', 'USER')
    password = os.getenv('DB_PASSWORD', 'PASSWORD')
    host = os.getenv('DB_HOST', 'localhost')
    port = os.getenv('DB_PORT', '5432')
    db = os.getenv('DB_NAME', 'DB')
    DATABASE_URL = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(DATABASE_URL, future=True)

    return engine