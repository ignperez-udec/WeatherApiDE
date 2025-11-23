from bronze.get_locations_wiki import get_locations_wiki  # type: ignore
from silver.load_locations_to_db import load_locations_to_db  # type: ignore
from bronze.read_locations_from_db import read_locations_from_db  # type: ignore
from bronze.get_weather import get_historical_weather  # type: ignore
from silver.load_weather_to_db import load_weather_to_db  # type: ignore

if __name__ == "__main__":
    ###LOCATIONS DATA ETL PROCESS###
    #Bronze Layer
    print('Extracting locations data from wikipedia...')
    locations_wiki_path = get_locations_wiki()

    #Silver Layer
    print('Cleaning and loading locations data to database...')
    load_locations_to_db(locations_wiki_path)

    ###HISTORICAL WEATHER DATA ETL PROCESS###
    #Bronze Layer
    print('Reading locations data from database...')
    locations = read_locations_from_db()

    print('Extracting daily historical weather data for locations...')
    weather_hist_list_path = get_historical_weather(locations.to_dict(orient='records'))

    #Silver Layer
    print('Loading historical weather data to database...')
    load_weather_to_db(weather_hist_list_path)


"""     #Bronce ETL Process
    print('Extracting comunas data from wikipedia...')
    comunas_wiki = extract_comunas(url)
    print('Comunas data extracted successfully:')

    print('Creating database engine...')
    engine = create_engine_db()

    print('Loading comunas data to database...')
    load_comunas_to_db(engine, comunas_wiki)
    print('Comunas data loaded to database successfully.')

    print('Reading comunas data back from database for verification...')
    comunas_df = read_comunas_from_db(engine)
    print('Comunas data read successfully:')

    print('Getting historical weather data for comunas...')
    weather_hist = get_historical_weather(comunas_df.to_dict(orient='records'))
    print('Historical weather data retrieval complete.')

    print('Loading historical weather data to database...')
    load_weather_hist_to_db(engine, weather_hist)
    print('Historical weather data loaded to database successfully.') """