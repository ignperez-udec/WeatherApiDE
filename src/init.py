from bronze.get_locations_wiki import get_locations_wiki  # type: ignore
from silver.load_locations_to_db import load_locations_to_db  # type: ignore
from bronze.read_locations_from_db import read_locations_from_db  # type: ignore
from bronze.get_weather import get_historical_weather  # type: ignore
from silver.load_weather_to_db import load_weather_to_db  # type: ignore
from bronze.get_season import get_season  # type: ignore
from silver.load_seasons_to_db import load_seasons_to_db  # type: ignore
from utils.logger import reset_log, config_log  # type: ignore
from utils.init_flag import write_init_flag  # type: ignore
import logging
import sys
from gold.dbt_scripts import run_dbt_gold_scripts  # type: ignore

#Init logger
reset_log(name_log='init')
logger = config_log(name_log='init', level=logging.INFO)

def handle_exception(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.critical("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

sys.excepthook = handle_exception

if __name__ == "__main__":
    try:
        logger.info('Starting init...')

        ###LOCATIONS DATA ETL PROCESS###
        #Bronze Layer
        logger.info('Extracting locations data from wikipedia...')
        locations_wiki_path = get_locations_wiki()

        #Silver Layer
        logger.info('Cleaning and loading locations data to database...')
        load_locations_to_db(locations_wiki_path)


        ###SEASONS DATA ETL PROCESS###
        #Bronze Layer
        logger.info('Extracting seasons data from website...')
        seasons_path = get_season(logger)

        #Silver Layer
        logger.info('Loading seasons data to database...')
        load_seasons_to_db(seasons_path)


        ###HISTORICAL WEATHER DATA ETL PROCESS###
        #Bronze Layer
        logger.info('Reading locations data from database...')
        locations = read_locations_from_db()

        logger.info('Extracting daily historical weather data for locations...')
        weather_hist_list_path = get_historical_weather(locations.to_dict(orient='records'), logger)

        #Silver Layer
        logger.info('Loading historical weather data to database...')
        load_weather_to_db(weather_hist_list_path, logger)


        ###PROCESS GOLD LAYER###
        logger.info('Running dbt scripts...')
        run_dbt_gold_scripts(logger)


        #Finish init 
        write_init_flag('DONE')
        logger.info('Finished!')

    except:
        #Error in flag
        write_init_flag('ERROR')
