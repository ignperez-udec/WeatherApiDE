from airflow import DAG
from airflow.operators.python_operator import PythonOperator # type: ignore
from datetime import datetime, timedelta
import sys
from datetime import datetime
from src.email import notify_failure, notify_success
from src.log import get_logger, log_failure_error, log_retry_error # type: ignore

sys.path.append("/opt/airflow/src")

from bronze.get_weather import get_daily_weather  # type: ignore
from bronze.read_locations_from_db import read_locations_from_db  # type: ignore
from bronze.read_weather_from_db import read_weather_from_db  # type: ignore
from silver.load_weather_to_db import load_weather_to_db  # type: ignore
from utils.logger import reset_log  # type: ignore

# ---------------------------------------
# ETL PROCESS - DAILY WEATHER DATA
# ---------------------------------------

def init_dag(**kwargs):
    reset_log(name_log='daily_weather_etl')
    logger = get_logger(name='daily_weather_etl')
    logger.info('Starting daily weather ETL process...')

def extract_last_date(**kwargs):
    logger = get_logger(name='daily_weather_etl')

    logger.info('Reading weather data from database to retrieve the last updated date...')

    weather = read_weather_from_db()

    if weather.empty:
        raise ValueError("No weather data found in the database.")

    last_date = weather['time'].drop_duplicates()[0]

    return last_date

def extract_locations(**kwargs):
    logger = get_logger(name='daily_weather_etl')

    logger.info('Reading locations data from database to extract daily weather...')

    locations = read_locations_from_db()

    if locations.empty:
        raise ValueError("No locations found in the database.")

    locations = locations.to_dict(orient='records')

    return locations

def extract_daily_weather(**kwargs):
    logger = get_logger(name='daily_weather_etl')

    ti = kwargs['ti']
    last_date = ti.xcom_pull(task_ids='extract_last_date')
    locations = ti.xcom_pull(task_ids='extract_locations')

    logger.info('Extracting daily weather data from last updated date: ' + datetime.strftime(last_date, '%Y-%m-%d'))
    weather_daily_list_path = get_daily_weather(locations, last_date, logger)

    if weather_daily_list_path is None or len(weather_daily_list_path) == 0:
        raise ValueError("No daily weather data was extracted.")

    return weather_daily_list_path

def load_daily_weather(**kwargs):
    logger = get_logger(name='daily_weather_etl')

    ti = kwargs['ti']
    weather_daily_list_path = ti.xcom_pull(task_ids='extract_daily_weather')

    logger.info('Loading daily weather data to database...')
    load_weather_to_db(weather_daily_list_path)

def finish_dag(**kwargs):
    logger = get_logger(name='daily_weather_etl')
    logger.info('Finished!')

# ---------------------------------------
# RETRY CONFIGURATION
# ---------------------------------------

def log_retry(context):
    logger = get_logger(name='daily_weather_etl')
    log_retry_error(context, logger)

# ---------------------------------------
# FAILURE CONFIGURATION
# ---------------------------------------

def log_failure(context):
    logger = get_logger(name='daily_weather_etl')
    log_failure_error(context, logger)

def on_failure_callback(context):
    log_failure(context)
    notify_failure(context)

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
        'on_retry_callback': log_retry
    },
    description='ETL process to extract daily weather data and load it into the database',
    start_date=datetime(2020, 1, 1),
    schedule_interval='0 22 * * *',
    on_failure_callback=on_failure_callback,
    on_success_callback=notify_success,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    init_dag_task = PythonOperator(
        task_id='init_dag',
        python_callable=init_dag,
        on_failure_callback=log_failure
    )

    extract_last_date_task = PythonOperator(
        task_id='extract_last_date',
        python_callable=extract_last_date,
        on_failure_callback=log_failure
    )

    extract_locations_task = PythonOperator(
        task_id='extract_locations',
        python_callable=extract_locations,
        on_failure_callback=log_failure
    )

    extract_daily_weather_task = PythonOperator(
        task_id='extract_daily_weather',
        python_callable=extract_daily_weather,
        on_failure_callback=log_failure
    )

    load_daily_weather_task = PythonOperator(
        task_id='load_daily_weather',
        python_callable=load_daily_weather,
        on_failure_callback=log_failure
    )

    finish_dag_task = PythonOperator(
        task_id='finish_dag',
        python_callable=finish_dag,
        on_failure_callback=log_failure
    )

    init_dag_task >> [extract_last_date_task, extract_locations_task] >> extract_daily_weather_task >> load_daily_weather_task >> finish_dag_task