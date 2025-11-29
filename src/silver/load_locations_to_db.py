import pandas as pd # type: ignore
import re
from typing import Tuple
from create_engine_db import create_engine_db  # type: ignore
from sqlalchemy import MetaData, Table, Column, String, Float, BigInteger # type: ignore

def parse_dms_string(dms: str) -> Tuple[float, float, float, str | None]:
    s = dms.strip()
    pattern = (
        r"(?P<deg>-?\d+(?:\.\d+)?)[°\\s]*"
        r"(?:(?P<min>\d+(?:\.\d+)?)[\'\\s]*)?"
        r"(?:(?P<sec>\d+(?:\.\d+)?)[\"\\s]*)?"
        r"(?P<dir>[NnSsEeWw])?"
    )
    m = re.search(pattern, s)
    if not m:
        raise ValueError(f"Unrecognized DMS format: '{dms}'")

    deg = float(m.group('deg'))
    minute = float(m.group('min')) if m.group('min') else 0.0
    sec = float(m.group('sec')) if m.group('sec') else 0.0
    direction = m.group('dir').upper() if m.group('dir') else None
    return deg, minute, sec, direction

def dms_string_to_decimal(dms: str) -> float:
    deg, minute, sec, direction = parse_dms_string(dms)
    sign = -1 if (deg < 0) else 1
    dd = abs(deg) + minute / 60.0 + sec / 3600.0
    if direction and direction in ('S', 'W'):
        dd = -dd
    return sign * dd

def load_locations_to_db(path_parquet: str):  # type: ignore
    locations = pd.read_parquet(path_parquet)

    locations.rename(columns={
        'CUT (Código Único Territorial)': 'cod_location',
        'Nombre': 'location',
        'Provincia': 'province',
        'Región': 'region',
        'Latitud': 'latitude',
        'Longitud': 'longitude'
    }, inplace=True)  

    locations = locations[['cod_location', 'location', 'province', 'region', 'latitude', 'longitude']]

    locations['cod_location'] = locations['cod_location'].astype(int)

    locations['latitude'] = locations['latitude'].apply(dms_string_to_decimal)
    locations['longitude'] = locations['longitude'].apply(dms_string_to_decimal)

    engine = create_engine_db('silver')

    metadata = MetaData()
    locations_table = Table(
            "locations",
            metadata,
            Column("cod_location", BigInteger, primary_key=True),
            Column("location", String),
            Column("province", String),
            Column("region", String),
            Column("latitude", Float),
            Column("longitude", Float),
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        locations.to_sql("locations", conn, if_exists="replace", index=False, method="multi", chunksize=1000)
        conn.commit()