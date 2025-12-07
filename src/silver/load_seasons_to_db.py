import pandas as pd # type: ignore
import sys
from pathlib import Path
import dateparser # type: ignore
from sqlalchemy import MetaData, Table, Column, Integer, DateTime # type: ignore
import sys
from pathlib import Path
from utils.create_engine_db import create_engine_db  # type: ignore

def load_seasons_to_db(path_txt: str):
    with open(path_txt, 'r') as f:
        lines = f.readlines()

    year = []
    spring_equinox = []
    summer_solstice = []
    autumn_equinox = []
    winter_solstice = []

    flag=''
    for line in lines:
        line = line.replace('\n', '')

        if line.isdigit():
            year.append(int(line))
            flag = ''
        elif line=='Equinoccio de Primavera:':
            flag='spring_equinox'
            continue
        elif line=='Solsticio de Verano:':
            flag='summer_solstice'
            continue
        elif line=='Equinoccio de Otoño:':
            flag='autumn_equinox'
            continue
        elif line=='Solsticio de Invierno:':
            flag='winter_solstice'
            continue

        if flag=='spring_equinox':
            spring_equinox.append(dateparser.parse(line).replace(year=year[-1]))
            flag = ''
        elif flag=='summer_solstice':
            summer_solstice.append(dateparser.parse(line).replace(year=year[-1]))
            flag = ''
        elif flag=='autumn_equinox':
            autumn_equinox.append(dateparser.parse(line).replace(year=year[-1]))
            flag = ''
        elif flag=='winter_solstice':
            winter_solstice.append(dateparser.parse(line).replace(year=year[-1]))
            flag = ''

    seasons = pd.DataFrame({
        'year': year,
        'spring_equinox': spring_equinox,
        'summer_solstice': summer_solstice,
        'autumn_equinox': autumn_equinox,
        'winter_solstice': winter_solstice
    })

    engine = create_engine_db()

    metadata = MetaData(schema="silver")
    seasons_table = Table(
            "seasons",
            metadata,
            Column("year", Integer, primary_key=True),
            Column("spring_equinox", DateTime),
            Column("summer_solstice", DateTime),
            Column("autumn_equinox", DateTime),
            Column("winter_solstice", DateTime)
    )
    metadata.create_all(engine)

    with engine.connect() as conn:
        seasons.to_sql("seasons", conn, schema='silver', if_exists="replace", index=False, method="multi", chunksize=1000)
        conn.commit()