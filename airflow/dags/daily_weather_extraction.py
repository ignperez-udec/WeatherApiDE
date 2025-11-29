from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import sys, os
from src.email import notify_failure, notify_success

sys.path.append("/opt/airflow/src")

from bronze.get_weather import get_daily_weather  # type: ignore
from bronze.read_locations_from_db import read_locations_from_db  # type: ignore
from bronze.read_weather_from_db import read_weather_from_db  # type: ignore
from silver.load_weather_to_db import load_weather_to_db  # type: ignore

# ---------------------------------------
# ETL PROCESS - DAILY WEATHER DATA
# ---------------------------------------

def extract_last_date(**kwargs):
    print('Reading weather data from database to retrieve the last updated date...')

    weather = read_weather_from_db()

    if weather.empty:
        raise ValueError("No weather data found in the database.")

    last_date = weather['time'].drop_duplicates()[0]

    return last_date

def extract_locations(**kwargs):
    print('Reading locations data from database to extract daily weather...')

    locations = read_locations_from_db()

    if locations.empty:
        raise ValueError("No locations found in the database.")

    locations = locations.to_dict(orient='records')

    return locations

def extract_daily_weather(**kwargs):
    ti = kwargs['ti']
    last_date = ti.xcom_pull(task_ids='extract_last_date')
    locations = ti.xcom_pull(task_ids='extract_locations')

    print('Extracting daily weather data from last updated date:', last_date)
    weather_daily_list_path = get_daily_weather(locations, last_date)

    if weather_daily_list_path is None or len(weather_daily_list_path) == 0:
        raise ValueError("No daily weather data was extracted.")

    return weather_daily_list_path

def load_daily_weather(**kwargs):
    ti = kwargs['ti']
    weather_daily_list_path = ti.xcom_pull(task_ids='extract_daily_weather')

    print('Loading daily weather data to database...')
    load_weather_to_db(weather_daily_list_path)

# ---------------------------------------
# DAG CONFIGURATION
# ---------------------------------------

with DAG(
    dag_id='daily_weather_etl',
    default_args={
        'owner': 'airflow',
        'depends_on_past': True,
        'retries': 5,
        'retry_delay': timedelta(seconds=10),
    },
    description='ETL process to extract daily weather data and load it into the database',
    start_date=datetime(2020, 1, 1),
    schedule_interval='0 22 * * *',
    on_failure_callback=notify_failure,
    on_success_callback=notify_success,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    extract_last_date_task = PythonOperator(
        task_id='extract_last_date',
        python_callable=extract_last_date
    )

    extract_locations_task = PythonOperator(
        task_id='extract_locations',
        python_callable=extract_locations
    )

    extract_daily_weather_task = PythonOperator(
        task_id='extract_daily_weather',
        python_callable=extract_daily_weather
    )

    load_daily_weather_task = PythonOperator(
        task_id='load_daily_weather',
        python_callable=load_daily_weather
    )

    [extract_last_date_task, extract_locations_task] >> extract_daily_weather_task >> load_daily_weather_task