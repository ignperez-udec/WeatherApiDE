import pandas as pd # type: ignore
from sqlalchemy import MetaData, Table, Column, Date, DateTime, Float, String, Integer, BigInteger, UniqueConstraint # type: ignore
from sqlalchemy.dialects.postgresql import insert as pg_insert  # type: ignore
from sqlalchemy.engine import Engine # type: ignore
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from utils.create_engine_db import create_engine_db  # type: ignore

def upsert_weather_hist(engine: Engine, data: pd.DataFrame, chunk_size=1000):
    records = data.to_dict(orient='records')

    metadata = MetaData()
    weather = Table('weather_hist', metadata, autoload_with=engine)
    conflict_keys = ['cod_location', 'time']

    with engine.begin() as conn:
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i+chunk_size]
            stmt = pg_insert(weather).values(chunk)

            update_cols = {
                c.name: stmt.excluded[c.name]
                for c in weather.columns
                if c.name not in conflict_keys
            }

            stmt = stmt.on_conflict_do_update(index_elements=conflict_keys, set_=update_cols)
            conn.execute(stmt)

def load_weather_to_db(path_list: list[dict]):  # type: ignore
    for dict_locations in path_list:
        print('\tReading historical weather data from json file:', dict_locations['path_json'])
        
        location_weather_hist = pd.read_json(dict_locations['path_json'])

        location_weather_hist.rename(columns={
            'temperature_2m_mean': 'temperature_mean',
            'temperature_2m_max': 'temperature_max',
            'temperature_2m_min': 'temperature_min', 
            'wind_speed_10m_max': 'wind_speed_max',
            'wind_gusts_10m_max': 'wind_gusts_max',
            'wind_direction_10m_dominant': 'wind_direction_dominant',
            'relative_humidity_2m_mean': 'relative_humidity_mean',
            'relative_humidity_2m_max': 'relative_humidity_max',
            'relative_humidity_2m_min': 'relative_humidity_min',
            'wind_gusts_10m_mean': 'wind_gusts_mean',
            'wind_speed_10m_mean': 'wind_speed_mean',
            'wind_gusts_10m_min': 'wind_gusts_min',
            'wind_speed_10m_min': 'wind_speed_min'
        }, inplace=True)

        location_weather_hist['cod_location'] = dict_locations['cod_location']

        location_weather_hist['time'] = pd.to_datetime(location_weather_hist['time']).dt.date
        location_weather_hist['sunrise'] = pd.to_datetime(location_weather_hist['sunrise'], format='%Y-%m-%dT%H:%M')
        location_weather_hist['sunset'] = pd.to_datetime(location_weather_hist['sunset'], format='%Y-%m-%dT%H:%M')

        engine = create_engine_db('silver')

        metadata = MetaData()
        weather_hist_table = Table(
            "weather_hist",
            metadata,
            Column("cod_location", BigInteger),
            Column("time", Date),
            Column("weather_code", Integer),
            Column("temperature_mean", Float),
            Column("temperature_max", Float),
            Column("temperature_min", Float),
            Column("sunrise", DateTime),
            Column("sunset", DateTime),
            Column("precipitation_sum", Float),
            Column("rain_sum", Float),
            Column("snowfall_sum", Float),
            Column("precipitation_hours", Float),
            Column("wind_direction_dominant", Integer),
            Column("shortwave_radiation_sum", Float),
            Column("daylight_duration", Float),
            Column("sunshine_duration", Float),
            Column("relative_humidity_mean", Float),
            Column("relative_humidity_max", Float),
            Column("relative_humidity_min", Float),
            Column("pressure_msl_mean", Float),
            Column("pressure_msl_max", Float),
            Column("pressure_msl_min", Float),
            Column("cloud_cover_mean", Integer),
            Column("cloud_cover_max", Integer),
            Column("cloud_cover_min", Integer),
            Column("wind_speed_mean", Float),
            Column("wind_speed_max", Float),
            Column("wind_speed_min", Float),
            Column("wind_gusts_mean", Float),
            Column("wind_gusts_max", Float),
            Column("wind_gusts_min", Float),
            UniqueConstraint('cod_location', 'time', name='uq_location_time')
        )
        metadata.create_all(engine)

        with engine.connect() as conn:
            upsert_weather_hist(engine, location_weather_hist)