import pandas as pd # type: ignore
import requests
from io import StringIO
import os
import sys
from pathlib import Path
from utils.config import load_variables

URL = 'https://es.wikipedia.org/wiki/Anexo:Comunas_de_Chile#Tabla'

def get_locations_wiki() -> str:

    CONFIG_VARS = load_variables()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(URL, headers=headers, timeout=15)
    resp.raise_for_status()
    data = pd.read_html(StringIO(resp.text))[0]

    path_parquet = os.path.join(CONFIG_VARS['DATA_BRONZE_PATH'], 'comunas_wiki.parquet')

    data.to_parquet(path_parquet, 
                    index=False, 
                    compression='snappy')
    
    return path_parquet