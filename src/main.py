from bronze.get_weather import get_daily_weather  # type: ignore
from bronze.read_locations_from_db import read_locations_from_db  # type: ignore
from bronze.read_weather_from_db import read_weather_from_db  # type: ignore
from silver.load_weather_to_db import load_weather_to_db  # type: ignore

if __name__ == "__main__":
    ###WEATHER DATA DAILY ETL PROCESS###
    #Bronze Layer
    print('Reading weather data from database to retrieve the last updated date...')
    weather = read_weather_from_db()
    last_date = weather['time'].drop_duplicates()[0]

    print('Reading locations data from database to extract daily weather...')
    locations = read_locations_from_db()

    print('Extracting daily weather data from last updated date:', last_date)
    weather_daily_list_path = get_daily_weather(locations.to_dict(orient='records'), last_date)

    #Silver Layer
    print('Loading daily weather data to database...')
    load_weather_to_db(weather_daily_list_path)