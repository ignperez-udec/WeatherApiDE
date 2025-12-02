from bronze.get_locations_wiki import get_locations_wiki  # type: ignore
from silver.load_locations_to_db import load_locations_to_db  # type: ignore
from bronze.read_locations_from_db import read_locations_from_db  # type: ignore
from bronze.get_weather import get_historical_weather  # type: ignore
from silver.load_weather_to_db import load_weather_to_db  # type: ignore
from bronze.get_season import get_season  # type: ignore
from silver.load_seasons_to_db import load_seasons_to_db  # type: ignore



if __name__ == "__main__":
    ###LOCATIONS DATA ETL PROCESS###
    #Bronze Layer
    print('Extracting locations data from wikipedia...')
    locations_wiki_path = get_locations_wiki()

    #Silver Layer
    print('Cleaning and loading locations data to database...')
    load_locations_to_db(locations_wiki_path)


    ###SEASONS DATA ETL PROCESS###
    #Bronze Layer
    print('Extracting seasons data from website...')
    seasons_path = get_season()

    #Silver Layer
    print('Loading seasons data to database...')
    load_seasons_to_db(seasons_path)


    ###HISTORICAL WEATHER DATA ETL PROCESS###
    #Bronze Layer
    print('Reading locations data from database...')
    locations = read_locations_from_db()

    print('Extracting daily historical weather data for locations...')
    weather_hist_list_path = get_historical_weather(locations.to_dict(orient='records'))

    #Silver Layer
    print('Loading historical weather data to database...')
    load_weather_to_db(weather_hist_list_path)


    #Finish init    
    print('Finished!')