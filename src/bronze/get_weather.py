import requests
import os
import datetime
from time import sleep
import json
import sys
from pathlib import Path
from utils.config import load_variables
from utils.logger import config_log
import logging
from bronze.read_weather_from_db import read_weather_from_db  # type: ignore

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

def extract_last_date(cod_location: int) -> datetime.datetime:
	weather = read_weather_from_db([cod_location])

	return weather['time'].drop_duplicates()[0]

def get_historical_weather(locations: list[dict], logger: logging.Logger) -> list[dict]:
	path_list = []
	
	for i in range(len(locations)):
		logger.info('\tExtracting historical data for location: ' + locations[i]['location'])

		path_json = os.path.join(CONFIG_VARS['DATA_BRONZE_PATH'], 'weather_historical_' + str(locations[i]['cod_location']) + '.json')

		if search_data_in_files(locations[i]['cod_location']):
			logger.info('\t\tData already extracted. Skipping...')
			path_list.append({
				'path_json': path_json,
				'cod_location': locations[i]['cod_location']
			})
			continue
		
		params = {
			"latitude": locations[i]['latitude'],
			"longitude": locations[i]['longitude'],
			"start_date": "1940-01-01",
			"end_date": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
			"daily": DAILY_PARAMS
		}

		try:
			response = requests.get(BASE_URL, params=params)
			out = response.json()
			data = out['daily']
		except:
			logger.error('\t\tError extracting data for location: ' + locations[i]['location'])
			logger.error('\t\t' + out['reason'])
			break

		with open(path_json, 'w') as f:
			json.dump(data, f, indent=4)
		
		path_list.append({
			'path_json': path_json,
			'cod_location': locations[i]['cod_location'] 
		})

		sleep(30)

	return path_list

def get_daily_weather(locations: list[dict], logger: logging.Logger) -> list[dict]:
	path_list = []

	for i in range(len(locations)):
		logger.info('\tReading weather data from database to retrieve the last updated date for location:', locations[i]['location'])
		last_date = extract_last_date(locations[i]['cod_location'])

		if last_date + datetime.timedelta(days=1) == datetime.datetime.now() - datetime.timedelta(days=1):
			logger.info('\tNo new data to extract for location: ' + locations[i]['location'])
			continue

		logger.info('\tExtracting daily data for location:', locations[i]['location'])

		path_json = os.path.join(CONFIG_VARS['DATA_BRONZE_PATH'], 'weather_daily_' + str(locations[i]['cod_location']) + '_' + last_date.strftime('%Y-%m-%d') + '.json')

		params = {
			"latitude": locations[i]['latitude'],
			"longitude": locations[i]['longitude'],
			"start_date": (last_date + datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
			"end_date": (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"),
			"daily": DAILY_PARAMS
		}

		try:
			response = requests.get(BASE_URL, params=params)
			out = response.json()
			data = out['daily']
		except:
			logger.error('\t\tError extracting data for location: ' + locations[i]['location'])
			logger.error('\t\t' + out['reason'])
			raise Exception(out['reason'])

		with open(path_json, 'w') as f:
			json.dump(data, f, indent=4)
		
		path_list.append({
			'path_json': path_json,
			'cod_location': locations[i]['cod_location']
		})

		sleep(30)

	return path_list
		
