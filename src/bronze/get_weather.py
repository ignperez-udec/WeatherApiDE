import requests
import os
import datetime
from time import sleep
import pandas as pd # type: ignore
from config import load_variables  # type: ignore

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_PARAMS = "weather_code,temperature_2m_mean,temperature_2m_max,temperature_2m_min,sunrise,sunset,precipitation_sum,rain_sum," \
"snowfall_sum,precipitation_hours,wind_speed_10m_max,wind_gusts_10m_max,wind_direction_10m_dominant,shortwave_radiation_sum" \
",daylight_duration,sunshine_duration,cloud_cover_mean,cloud_cover_max,cloud_cover_min,relative_humidity_2m_mean," \
"relative_humidity_2m_max,relative_humidity_2m_min,pressure_msl_mean,pressure_msl_max,pressure_msl_min," \
"wind_gusts_10m_mean,wind_speed_10m_mean,wind_gusts_10m_min,wind_speed_10m_min"
CONFIG_VARS = load_variables()

def search_data_in_files(cod_location: int) -> bool:

	files = os.listdir(CONFIG_VARS['DATA_BRONZE_PATH'])
	
	for file in files:
		if str(cod_location) in file and 'weather_historical_' in file:
			return True
	
	return False

def get_historical_weather(locations: list[dict]) -> list[dict]:
	path_list = []

	for i in range(len(locations)):
		print('\tExtracting historical data for location:', locations[i]['location'])

		path_parquet = os.path.join(CONFIG_VARS['DATA_BRONZE_PATH'], 'weather_historical_' + str(locations[i]['cod_location']) + '.parquet')

		if search_data_in_files(locations[i]['cod_location']):
			print('\t\tData already extracted. Skipping...')
			path_list.append({
				'path_parquet': path_parquet, 
				'location': locations[i]['location'],
				'cod_location': locations[i]['cod_location']
			})
			continue
		
		params = {
			"latitude": locations[i]['latitude'],
			"longitude": locations[i]['longitude'],
			"start_date": "2025-11-01",
			"end_date": "2025-11-20",
			#"end_date": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
			"daily": DAILY_PARAMS
		}

		try:
			response = requests.get(BASE_URL, params=params)
			data = response.json()
			df = pd.DataFrame(data['daily'])
		except:
			print('\t\tError extracting data for location:', locations[i]['location'])
			print('\t\t' + data['reason'])
			break

		df.to_parquet(path_parquet, 
				index=False, 
				compression='snappy')
		
		path_list.append({
			'cod_location': locations[i]['cod_location'],
			'location': locations[i]['location'],
			'path_parquet': path_parquet, 
		})

		sleep(30)

	return path_list

def get_daily_weather(locations: list[dict], last_date: datetime.date) -> list[dict]:
	path_list = []

	for i in range(len(locations)):
		if last_date + datetime.timedelta(days=1) == datetime.datetime.now() - datetime.timedelta(days=1):
			print('\tNo new data to extract for location:', locations[i]['location'])
			continue

		print('\tExtracting daily data for location:', locations[i]['location'])

		path_parquet = os.path.join(CONFIG_VARS['DATA_BRONZE_PATH'], 'weather_daily_' + str(locations[i]['cod_location']) + '_' + last_date.strftime('%Y-%m-%d') + '.parquet')

		params = {
			"latitude": locations[i]['latitude'],
			"longitude": locations[i]['longitude'],
			"start_date": (last_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
			"end_date": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
			"daily": DAILY_PARAMS
		}

		try:
			response = requests.get(BASE_URL, params=params)
			data = response.json()
			df = pd.DataFrame(data['daily'])
		except:
			print('\t\tError extracting data for location:', locations[i]['location'])
			print('\t\t' + data['reason'])
			break

		df.to_parquet(path_parquet, 
				index=False, 
				compression='snappy')
		
		path_list.append({
			'cod_location': locations[i]['cod_location'],
			'location': locations[i]['location'],
			'path_parquet': path_parquet, 
		})

		sleep(30)

	return path_list
		
